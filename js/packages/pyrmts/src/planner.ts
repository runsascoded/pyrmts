// Pure query planner. Given a pyramid + viewport, choose an output tier and
// emit a segmented plan describing which shards to read and where to
// re-aggregate. No I/O.

import { addSpan, binsInRange, fixedDurationMs, parseDuration, shardPeriodsCovering } from './axis.js'
import type { Bin, Duration, Pyramid, Tier } from './types.js'

export type SmoothMode = 'centered' | 'trailing'

// `Duration`: snap to nearest representable width that's an integer multiple
// of the resolved output bin. `{ auto: true }`: width = `multiplier × outputBin`,
// then snap. Default multiplier 50 (matches awair's observed feel).
export type SmoothingSpec = Duration | { auto: true; multiplier?: number }

export interface PlanQueryInput {
  range: { from: Date; to: Date }
  binBudget: number
  // tier_name → latest complete bin instant. Missing tier means "complete
  // through `range.to`" (consumer is responsible for accuracy). Coarser tiers
  // are clamped to never exceed finer ones' watermarks (a coarse tier can't
  // hold data past where its finer source ends).
  watermarks?: Record<string, Date>
  // tier_name → earliest available bin instant. Missing tier means "available
  // since the beginning of time" (no clamp). Coarser tiers are clamped to
  // never start before finer ones' earliest (a coarser tier is built from a
  // finer one, so it can't have data earlier than its source — symmetric to
  // `watermarks` propagation, opposite direction). Use for pyramids with
  // heterogeneous dim coverage so the planner doesn't emit shard keys for
  // periods that pre-date a dim's data start.
  earliestWatermarks?: Record<string, Date>
  // dim_name → value, for `{dim_name}` placeholders in the key template
  // (e.g. `awair-{device_id}/{tier}/{period}.parquet`).
  filter?: Record<string, string | number>
  // Server-side rolling-window smoothing over the monoid state. Plan extends
  // segments outward by the smoothing buffer (centered: ±count/2; trailing:
  // -count on the leading edge) so smoothing has full context at the visible
  // edges. Edge extensions clamp against earliest/latest watermarks — the
  // window just has fewer bins of context near data boundaries.
  smoothing?: SmoothingSpec
  smoothMode?: SmoothMode  // default 'centered'
}

export const DEFAULT_AUTO_MULTIPLIER = 50

export interface PlanSegment {
  from: Date
  to: Date
  shardTier: Tier
  keys: string[]
  // If true, this segment uses a finer tier than the output; the stitcher
  // must monoid-coarsen its rows up to outputTier.bin.
  reaggregate: boolean
}

export interface QueryPlan {
  outputTier: Tier
  outputBin: Bin
  segments: PlanSegment[]
  // raw-tier effective watermark, if it falls inside the query range.
  // Anything past this is *live tail* — consumer's hot-path concern.
  authoritativeEnd: Date | null
  // Visible time range (what the caller asked for). Stitcher trims rows
  // outside this back out after applying the rolling-window smoother.
  visibleRange: { from: Date; to: Date }
  // Smoothing resolution. `null` when no smoothing was requested. `smoothBin`
  // is the snapped Duration label (e.g. `1h`). `smoothBinCount` is the number
  // of output-tier bins per smoothed value. `smoothSourceTier` is the tier
  // the rolling-window combine sources from — always equals `outputTier.name`
  // for v0.1 (the spec's "tier downshift" optimization is deferred); future
  // implementations may pick a coarser tier for wide windows.
  smoothing: {
    smoothBin: Duration
    smoothBinCount: number
    smoothMode: SmoothMode
    smoothSourceTier: string
  } | null
}

export function planQuery(pyramid: Pyramid, input: PlanQueryInput): QueryPlan {
  if (pyramid.axis !== 'time') {
    throw new Error(`planQuery: axis '${pyramid.axis}' not yet implemented (only 'time')`)
  }
  if (pyramid.tiers.length === 0) {
    throw new Error('planQuery: pyramid has no tiers')
  }
  const { from, to } = input.range
  if (to <= from) {
    throw new Error(`planQuery: empty range (${from.toISOString()} → ${to.toISOString()})`)
  }

  const outputTier = pickTier(pyramid.tiers, from, to, input.binBudget)
  const outputIdx = pyramid.tiers.indexOf(outputTier)
  const earliest = effectiveEarliestWatermarks(pyramid.tiers, input.earliestWatermarks ?? {})

  // Resolve smoothing → snapped (smoothBin, smoothBinCount) and extend the
  // planning window outward so segments have buffer rows for the rolling
  // pass at the visible edges. The visible range stays as the caller asked;
  // the stitcher trims back to `visibleRange` after smoothing.
  const smoothMode: SmoothMode = input.smoothMode ?? 'centered'
  const smoothing = input.smoothing !== undefined
    ? resolveSmoothing(input.smoothing, outputTier.bin as Duration, from, to, smoothMode)
    : null
  const { from: plannedFrom, to: plannedTo } = smoothing
    ? extendForSmoothing(from, to, outputTier.bin as Duration, smoothing.smoothBinCount, smoothMode)
    : { from, to }
  // Watermarks clamp to the *extended* window so the buffer can include
  // post-`to` bins for centered smoothing (otherwise the trailing buffer
  // gets silently truncated to the original `to`).
  const effective = effectiveWatermarks(pyramid.tiers, input.watermarks ?? {}, plannedTo)

  // Walk from output tier down to finest, emitting one segment per tier
  // covering the gap up to that tier's effective watermark. Each tier's
  // segment is clamped on the left by its earliest watermark — if the
  // entire candidate segment falls before that, skip the tier entirely.
  const segments: PlanSegment[] = []
  let cursor = plannedFrom
  for (let i = outputIdx; i >= 0; i--) {
    const tier = pyramid.tiers[i]!
    const tierEnd = clamp(effective[tier.name]!, cursor, plannedTo)
    const earlyT = earliest[tier.name]
    const tierStart = earlyT && earlyT.getTime() > cursor.getTime() ? earlyT : cursor
    if (tierEnd > tierStart) {
      segments.push({
        from: tierStart,
        to: tierEnd,
        shardTier: tier,
        keys: shardKeys(pyramid, tier, tierStart, tierEnd, input.filter ?? {}),
        reaggregate: i !== outputIdx,
      })
    }
    cursor = tierEnd
    if (cursor >= plannedTo) break
  }

  const rawWm = effective[pyramid.tiers[0]!.name]!
  const authoritativeEnd = rawWm < to ? rawWm : null

  return {
    outputTier,
    outputBin: outputTier.bin,
    segments,
    authoritativeEnd,
    visibleRange: { from, to },
    smoothing: smoothing
      ? {
          smoothBin: smoothing.smoothBin,
          smoothBinCount: smoothing.smoothBinCount,
          smoothMode,
          smoothSourceTier: outputTier.name,
        }
      : null,
  }
}

// Finest tier whose bin count fits the budget. If even the coarsest tier
// exceeds the budget (over-narrow viewport for the data range), return the
// coarsest tier anyway — the chart can downsample further on the client.
function pickTier(tiers: Tier[], from: Date, to: Date, binBudget: number): Tier {
  for (const tier of tiers) {
    const count = binsInRange(from, to, tier.bin as `${number}${'min' | 'h' | 'd' | 'mo' | 'y'}`)
    if (count <= binBudget) return tier
  }
  return tiers[tiers.length - 1]!
}

// Each tier's effective watermark = min(declared, next-finer-tier's effective).
// Walks finest → coarsest, propagating the finer tier's bound forward.
function effectiveWatermarks(
  tiers: Tier[],
  declared: Record<string, Date>,
  rangeTo: Date,
): Record<string, Date> {
  const out: Record<string, Date> = {}
  // Finer tiers default to rangeTo (treat unspecified as "complete enough").
  let finerBound = new Date(8.64e15)
  for (const tier of tiers) {
    const decl = declared[tier.name]
    const eff = decl
      ? new Date(Math.min(decl.getTime(), finerBound.getTime()))
      : finerBound
    // Clamp to rangeTo so watermarks past the query don't leak through.
    const clamped = eff.getTime() > rangeTo.getTime() ? rangeTo : eff
    out[tier.name] = clamped
    finerBound = eff
  }
  return out
}

// Each tier's effective earliest = max(declared, next-finer-tier's effective).
// Walks finest → coarsest, propagating the finer tier's bound forward.
// Unspecified tiers have no clamp (treat as -infinity); a finer tier's
// declared value carries up to coarser tiers that didn't declare one
// (coarser tiers can't have data before their finer source did).
function effectiveEarliestWatermarks(
  tiers: Tier[],
  declared: Record<string, Date>,
): Record<string, Date | undefined> {
  const out: Record<string, Date | undefined> = {}
  let finerBound: Date | undefined = undefined
  for (const tier of tiers) {
    const decl = declared[tier.name]
    let eff: Date | undefined
    if (decl && finerBound) {
      eff = decl.getTime() > finerBound.getTime() ? decl : finerBound
    } else {
      eff = decl ?? finerBound
    }
    out[tier.name] = eff
    finerBound = eff
  }
  return out
}

function shardKeys(
  pyramid: Pyramid,
  tier: Tier,
  from: Date,
  to: Date,
  filter: Record<string, string | number>,
): string[] {
  const periods = shardPeriodsCovering(from, to, tier.shard)
  return periods.map(p =>
    substituteKey(pyramid.keyTemplate, {
      ...filter,
      tier: tier.name,
      period: p.label,
    }),
  )
}

function substituteKey(
  template: string,
  values: Record<string, string | number>,
): string {
  return template.replace(/\{(\w+)\}/g, (_, name: string) => {
    if (!(name in values)) {
      throw new Error(`planQuery: missing key template value for {${name}}`)
    }
    return String(values[name])
  })
}

function clamp(t: Date, lo: Date, hi: Date): Date {
  if (t.getTime() < lo.getTime()) return lo
  if (t.getTime() > hi.getTime()) return hi
  return t
}

// Catalog of "nice" widths used when snapping a user-supplied smoothing
// window to a representable Duration. All fixed-width (no mo/y); calendar
// outputs (mo/y) snap to integer-count in their own unit instead.
const NICE_WIDTHS: Array<{ label: Duration; ms: number }> = [
  { label: '1min', ms: 60_000 },
  { label: '2min', ms: 2 * 60_000 },
  { label: '5min', ms: 5 * 60_000 },
  { label: '10min', ms: 10 * 60_000 },
  { label: '15min', ms: 15 * 60_000 },
  { label: '30min', ms: 30 * 60_000 },
  { label: '1h', ms: 60 * 60_000 },
  { label: '2h', ms: 2 * 60 * 60_000 },
  { label: '3h', ms: 3 * 60 * 60_000 },
  { label: '4h', ms: 4 * 60 * 60_000 },
  { label: '6h', ms: 6 * 60 * 60_000 },
  { label: '8h', ms: 8 * 60 * 60_000 },
  { label: '12h', ms: 12 * 60 * 60_000 },
  { label: '1d', ms: 24 * 60 * 60_000 },
  { label: '2d', ms: 2 * 24 * 60 * 60_000 },
  { label: '3d', ms: 3 * 24 * 60 * 60_000 },
  { label: '7d', ms: 7 * 24 * 60 * 60_000 },
  { label: '14d', ms: 14 * 24 * 60 * 60_000 },
  { label: '30d', ms: 30 * 24 * 60 * 60_000 },
]

interface ResolvedSmoothing {
  smoothBin: Duration
  smoothBinCount: number
}

// Resolve a `SmoothingSpec` to a snapped (smoothBin, smoothBinCount) tuple.
// Snapping picks the closest candidate that's an integer multiple of the
// output bin, with `smoothBinCount` ≥ 1 and ≤ floor(visibleRangeBins / 4)
// (so smoothing can't dominate the visible range — pathological cases
// degrade to 1× output bin = no-op).
function resolveSmoothing(
  spec: SmoothingSpec,
  outputBin: Duration,
  from: Date,
  to: Date,
  _mode: SmoothMode,
): ResolvedSmoothing {
  const outSpan = parseDuration(outputBin)
  const visibleBins = binsInRange(from, to, outputBin)
  const maxCount = Math.max(1, Math.floor(visibleBins / 4))

  // Calendar output bins (mo/y) — only same-unit integer counts make sense.
  if (outSpan.unit === 'mo' || outSpan.unit === 'y') {
    if (typeof spec === 'string') {
      const s = parseDuration(spec)
      if (s.unit !== outSpan.unit) {
        throw new Error(
          `planQuery: smoothing ${spec} is incompatible with calendar output bin ${outputBin} ` +
          `(use ${outSpan.unit} for both)`,
        )
      }
      const count = Math.max(1, Math.min(maxCount, Math.round(s.count / outSpan.count)))
      return { smoothBin: `${count * outSpan.count}${outSpan.unit}` as Duration, smoothBinCount: count }
    }
    const mult = spec.multiplier ?? DEFAULT_AUTO_MULTIPLIER
    const count = Math.max(1, Math.min(maxCount, mult))
    return { smoothBin: `${count * outSpan.count}${outSpan.unit}` as Duration, smoothBinCount: count }
  }

  // Fixed-width output bin — snap an ms target to the nearest nice width that
  // divides cleanly into the output bin.
  const outputBinMs = fixedDurationMs(outputBin)
  const desiredMs = typeof spec === 'string'
    ? fixedDurationMs(spec)
    : (spec.multiplier ?? DEFAULT_AUTO_MULTIPLIER) * outputBinMs

  let best: { label: Duration; count: number } | null = null
  let bestDist = Infinity
  for (const c of NICE_WIDTHS) {
    if (c.ms < outputBinMs) continue   // can't smooth below output granularity
    if (c.ms % outputBinMs !== 0) continue   // must be integer multiple
    const count = c.ms / outputBinMs
    if (count > maxCount) continue
    const dist = Math.abs(c.ms - desiredMs)
    if (dist < bestDist) {
      bestDist = dist
      best = { label: c.label, count }
    }
  }
  // Fall back to 1× output bin if nothing fits — degenerate but well-defined
  // (smoothing == output, i.e. no-op).
  if (best === null) return { smoothBin: outputBin, smoothBinCount: 1 }
  return { smoothBin: best.label, smoothBinCount: best.count }
}

// Extend the visible [from, to) outward by the smoothing buffer so the
// rolling pass has full context at every visible bin. For window size N:
//   centered: lead = ceil((N-1)/2), tail = floor((N-1)/2) (past-biased on ties)
//   trailing: lead = N - 1, tail = 0
// N = 1 is a no-op (smoothing window == output bin).
function extendForSmoothing(
  from: Date,
  to: Date,
  outputBin: Duration,
  smoothBinCount: number,
  mode: SmoothMode,
): { from: Date; to: Date } {
  const N = smoothBinCount
  const leadBins = mode === 'centered' ? Math.ceil((N - 1) / 2) : N - 1
  const tailBins = mode === 'centered' ? Math.floor((N - 1) / 2) : 0
  const outSpan = parseDuration(outputBin)
  // Step `outputBin` outward via `addSpan` (calendar-correct for mo/y).
  return {
    from: addSpan(from, { count: -leadBins * outSpan.count, unit: outSpan.unit }),
    to: addSpan(to, { count: tailBins * outSpan.count, unit: outSpan.unit }),
  }
}

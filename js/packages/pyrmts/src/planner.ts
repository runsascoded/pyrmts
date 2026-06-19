// Pure query planner. Given a pyramid + viewport, choose an output tier and
// emit a segmented plan describing which shards to read and where to
// re-aggregate. No I/O.

import { addSpan, binsInRange, fixedDurationMs, floorToSpan, parseDuration, shardPeriodsCovering } from './axis.js'
import type { Bin, Duration, Pyramid, Tier } from './types.js'

export type SmoothMode = 'centered' | 'trailing'

// `Duration`: snap to nearest representable width that's an integer multiple
// of the resolved output bin. `{ auto: true }`: width = `multiplier × outputBin`,
// then snap. Default multiplier 50 (matches awair's observed feel).
export type SmoothingSpec = Duration | { auto: true; multiplier?: number }

export interface PlanQueryInput {
  range: { from: Date; to: Date }
  binBudget: number
  // Caller-requested output bin width. When set, the planner uses this width
  // instead of `binBudget`-driven `pickTier`. Two cases:
  //
  // (a) `targetBin` exactly matches a stored tier's bin → that tier is the
  //     output tier; behaves like today's single-tier path.
  // (b) `targetBin` doesn't match any tier → ragged decomposition. Each
  //     output bin is packed with a minimum-item set of finer-tier bins via
  //     DP (shortest path on aligned positions); `outputTier` is omitted in
  //     the result. Caller can serve arbitrary widths the pyramid doesn't
  //     materialize directly, at the cost of one row per packing atom.
  //
  // Restrictions: fixed-width units only (`min`/`h`/`d`). Calendar-variable
  // units (`mo`/`y`) throw — divisibility against fixed-width tiers is
  // undefined for variable months/years. At least one tier's bin must
  // exactly divide `targetBin` (which is trivially true if the finest tier
  // is the same unit), else throws.
  //
  // `binBudget` is ignored when `targetBin` is set (caller asserts the bin
  // width they want); restoring budget enforcement is left to the caller
  // (compute `binsInRange(range, targetBin)` beforehand).
  targetBin?: Duration
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
  // Stored tier serving the query. Omitted when `targetBin` was supplied
  // and didn't match any tier's bin (ragged-decomposition path — segments
  // mix multiple finer tiers, none of which is "the" output tier). Consumers
  // should treat `outputBin` as the authoritative output width and check
  // `outputTier` only when they need a stored-tier-specific affordance
  // (e.g. tier-name in display metadata).
  outputTier?: Tier
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

  if (input.targetBin !== undefined) {
    return planRagged(pyramid, input, input.targetBin)
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

// Ragged-decomposition planner: caller specifies the exact output bin width
// (`targetBin`); planner packs each output bin with a minimum-item set of
// finer-tier atoms via DP (shortest path on tier-bin-aligned positions),
// then coalesces adjacent same-tier atoms into segments. The DP runs once
// per (eligible-tier-set, phase) — output bins sharing a watermark band and
// a phase mod LCM(tier widths) reuse cached results.
function planRagged(
  pyramid: Pyramid,
  input: PlanQueryInput,
  targetBin: Duration,
): QueryPlan {
  const { from, to } = input.range
  const tParsed = parseDuration(targetBin)
  if (tParsed.unit === 'mo' || tParsed.unit === 'y') {
    throw new Error(
      `planQuery: targetBin '${targetBin}' is calendar-variable; ragged decomposition supports fixed-width units only`,
    )
  }
  const targetBinMs = fixedDurationMs(targetBin)
  const eligibleTiers: { tier: Tier; ms: number }[] = []
  for (const tier of pyramid.tiers) {
    const tb = parseDuration(tier.bin as Duration)
    if (tb.unit === 'mo' || tb.unit === 'y') continue
    const ms = fixedDurationMs(tier.bin as Duration)
    if (ms > targetBinMs) continue
    eligibleTiers.push({ tier, ms })
  }
  if (eligibleTiers.length === 0) {
    throw new Error(
      `planQuery: no tier with fixed-width bin ≤ targetBin '${targetBin}' (pyramid tiers: ${pyramid.tiers.map(t => t.bin).join(', ')})`,
    )
  }
  // Necessary condition: gcd of eligible tier widths divides targetBinMs (by
  // Bezout, integer linear combinations of tier widths span multiples of their
  // gcd). Sufficient for decomposition existence ignoring alignment; the
  // per-bin DP may still fail when strict-equality alignment dead-ends (e.g.
  // tiers={2,3}, target=5: gcd=1 divides 5, but no path through {0,2,3,4} to 5).
  let tierGcd = eligibleTiers[0]!.ms
  for (let i = 1; i < eligibleTiers.length; i++) {
    tierGcd = gcd(tierGcd, eligibleTiers[i]!.ms)
  }
  if (targetBinMs % tierGcd !== 0) {
    throw new Error(
      `planQuery: no decomposition of targetBin '${targetBin}' from eligible tiers (gcd ${tierGcd} doesn't divide ${targetBinMs})`,
    )
  }
  // Order finest-first for DP iteration determinism; the DP itself doesn't
  // require ordering for correctness (it minimizes path length).
  eligibleTiers.sort((a, b) => a.ms - b.ms)

  // `outputTier` is the stored tier matching `targetBin` exactly, if any.
  // Used downstream (smoothing source-tier, plan metadata). When omitted,
  // the segments mix multiple tiers and consumers must rely on `outputBin`.
  const outputTier = eligibleTiers.find(e => e.ms === targetBinMs)?.tier

  // Smoothing snaps against `targetBin` (the output's bin width) just as the
  // budget path snaps against `outputTier.bin`.
  const smoothMode: SmoothMode = input.smoothMode ?? 'centered'
  const smoothing = input.smoothing !== undefined
    ? resolveSmoothing(input.smoothing, targetBin, from, to, smoothMode)
    : null
  const { from: plannedFrom, to: plannedTo } = smoothing
    ? extendForSmoothing(from, to, targetBin, smoothing.smoothBinCount, smoothMode)
    : { from, to }

  const effective = effectiveWatermarks(pyramid.tiers, input.watermarks ?? {}, plannedTo)
  const earliest = effectiveEarliestWatermarks(pyramid.tiers, input.earliestWatermarks ?? {})

  // Output bins span [floor(plannedFrom), ceil(plannedTo)] in `targetBin`
  // strides. Each bin contributes one or more sub-bin atoms (per the DP).
  const targetSpan = tParsed
  const firstBinStart = floorToSpan(plannedFrom, targetSpan)

  // Per-(eligible-set, phase) DP cache. Phase = bin-start ms mod L, where L
  // is LCM of all tier-ms (and targetBin) — different bins with the same
  // phase produce identical decompositions modulo a uniform offset.
  const phaseCacheByKey = new Map<string, Map<number, PackedAtom[] | null>>()
  const lcmAll = eligibleTiers.reduce((l, { ms }) => lcm(l, ms), targetBinMs)

  interface Atom { tier: Tier; absStartMs: number; absEndMs: number }
  const atoms: Atom[] = []
  let binStart = firstBinStart
  while (binStart < plannedTo) {
    const binStartMs = binStart.getTime()
    const binEnd = addSpan(binStart, targetSpan)
    const binEndMs = binEnd.getTime()

    // Per-bin eligible tiers: watermark covers binEnd AND earliest ≤ binStart.
    const perBin: { tier: Tier; ms: number }[] = []
    for (const e of eligibleTiers) {
      if (effective[e.tier.name]!.getTime() < binEndMs) continue
      const earlyT = earliest[e.tier.name]
      if (earlyT && earlyT.getTime() > binStartMs) continue
      perBin.push(e)
    }
    if (perBin.length === 0) {
      // No tier covers this bin — skip (consumer sees a gap row at this bin).
      binStart = binEnd
      continue
    }
    const cacheKey = perBin.map(e => e.tier.name).join(',')
    let phaseCache = phaseCacheByKey.get(cacheKey)
    if (phaseCache === undefined) {
      phaseCache = new Map()
      phaseCacheByKey.set(cacheKey, phaseCache)
    }
    const phase = ((binStartMs % lcmAll) + lcmAll) % lcmAll
    let path = phaseCache.get(phase)
    if (path === undefined) {
      path = decomposeBin(perBin, targetBinMs, phase)
      phaseCache.set(phase, path)
    }
    if (path === null) {
      throw new Error(
        `planQuery: cannot decompose output bin starting at ${binStart.toISOString()} ` +
        `(targetBin '${targetBin}', eligible tiers ${cacheKey})`,
      )
    }
    for (const atom of path) {
      atoms.push({
        tier: atom.tier,
        absStartMs: binStartMs + atom.offsetMs,
        absEndMs: binStartMs + atom.offsetMs + atom.durationMs,
      })
    }
    binStart = binEnd
  }

  // Coalesce adjacent same-tier atoms into segments.
  const segments: PlanSegment[] = []
  if (atoms.length > 0) {
    let curr = { ...atoms[0]! }
    for (let i = 1; i < atoms.length; i++) {
      const next = atoms[i]!
      if (next.tier === curr.tier && next.absStartMs === curr.absEndMs) {
        curr.absEndMs = next.absEndMs
      } else {
        segments.push(emitSegment(pyramid, curr, targetBinMs, input.filter ?? {}))
        curr = { ...next }
      }
    }
    segments.push(emitSegment(pyramid, curr, targetBinMs, input.filter ?? {}))
  }

  const rawWm = effective[pyramid.tiers[0]!.name]!
  const authoritativeEnd = rawWm < to ? rawWm : null

  return {
    ...(outputTier !== undefined ? { outputTier } : {}),
    outputBin: targetBin,
    segments,
    authoritativeEnd,
    visibleRange: { from, to },
    smoothing: smoothing
      ? {
          smoothBin: smoothing.smoothBin,
          smoothBinCount: smoothing.smoothBinCount,
          smoothMode,
          smoothSourceTier: outputTier?.name ?? `<ragged:${targetBin}>`,
        }
      : null,
  }
}

interface PackedAtom {
  tier: Tier
  offsetMs: number    // offset from bin start
  durationMs: number  // = fixedDurationMs(tier.bin)
}

// DP: find the minimum-item set of tier atoms tiling [binStartMs, binEndMs).
// Each atom is one full bin of some eligible tier, with `cursor % tier.ms === 0`
// in absolute (epoch) ms — the strict-equality alignment rule. Returns null
// when no decomposition exists (alignment dead-ends with no finer tier to
// fall back to). Memoized on the absolute cursor; runs O(B/g × n_tiers) where
// B = bin width, g = gcd of eligible tier ms.
function decomposeBin(
  eligibleTiers: { tier: Tier; ms: number }[],
  targetBinMs: number,
  binStartMs: number,
): PackedAtom[] | null {
  const binEndMs = binStartMs + targetBinMs
  const memo = new Map<number, PackedAtom[] | null>()
  function solve(cursor: number): PackedAtom[] | null {
    if (cursor === binEndMs) return []
    const cached = memo.get(cursor)
    if (cached !== undefined) return cached
    let best: PackedAtom[] | null = null
    for (const { tier, ms } of eligibleTiers) {
      if (cursor + ms > binEndMs) continue
      if (cursor % ms !== 0) continue
      const sub = solve(cursor + ms)
      if (sub === null) continue
      const candidate: PackedAtom[] = [
        { tier, offsetMs: cursor - binStartMs, durationMs: ms },
        ...sub,
      ]
      if (best === null || candidate.length < best.length) best = candidate
    }
    memo.set(cursor, best)
    return best
  }
  return solve(binStartMs)
}

function emitSegment(
  pyramid: Pyramid,
  range: { tier: Tier; absStartMs: number; absEndMs: number },
  targetBinMs: number,
  filter: Record<string, string | number>,
): PlanSegment {
  const fromDate = new Date(range.absStartMs)
  const toDate = new Date(range.absEndMs)
  const tierMs = fixedDurationMs(range.tier.bin as Duration)
  return {
    from: fromDate,
    to: toDate,
    shardTier: range.tier,
    keys: shardKeys(pyramid, range.tier, fromDate, toDate, filter),
    reaggregate: tierMs !== targetBinMs,
  }
}

function gcd(a: number, b: number): number {
  while (b !== 0) {
    const t = b
    b = a % b
    a = t
  }
  return a
}

function lcm(a: number, b: number): number {
  return (a / gcd(a, b)) * b
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

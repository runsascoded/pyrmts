// Gap discovery: enumerate the minimal cover a pyramid's ladders need
// over a time range, then optionally subtract the set already recorded
// in a `ShardIndex`. Pure axis arithmetic + set diff — no fill
// orchestration. Consumers (ctbk fsck, rides-v3 fsck) wrap this with
// their own source readers and dependency-ordered fill loop. See
// `specs/done/gap-discovery.md`.
//
// The cover for `[from, to)` per tier with shards `[s_0, …, s_max]`:
//   - Max-shard tiles fill `[floor(from, s_max), floor(to, s_max))` (the
//     closed-history region). The first tile may start before `from` —
//     the shard's notional period contains pre-genesis time the
//     materializer just leaves empty.
//   - The trailing partial-max window `[floor(to, s_max), to)` is tiled
//     greedily by the largest non-max rung that fits at each step. The
//     ladder is divisibility-chained, so `cur` is always rung-aligned.
//     If `to - cur` falls below the smallest non-max rung the residual
//     is left for the next-finer tier (whose bin can represent that
//     resolution).
//
// The two-region split mirrors the api-worker planner walk
// (largest-rung-first, falling back to smaller rungs). Historical
// periods need only the max-shard; redundant smaller-rung copies aren't
// part of the cover.

import { addSpan, floorToSpan, parseDuration } from './axis.js'
import type { Invalidation } from './invalidation.js'
import { staleKeysFor } from './invalidation.js'
import { shardKey } from './keys.js'
import type { Pyramid, Shard, Tier } from './types.js'
import type { ShardIndex } from './shard-index.js'

export interface ExpectedShard {
  tier: string
  shardDur: Shard
  periodStart: Date
  periodEnd: Date  // exclusive
  // Intersection of `[periodStart, periodEnd)` with `[range.from, range.to)`.
  // Equal to the raw period for shards fully inside the query range; clipped
  // for shards that straddle either boundary (e.g. genesis at head, `now` at
  // tail). Materializers that require full source coverage should count
  // sources whose period intersects `[effectiveStart, effectiveEnd)`, not the
  // raw shard period — otherwise a genesis-straddling shard registers as
  // "missing inputs" for every pre-genesis source that will never exist.
  effectiveStart: Date
  effectiveEnd: Date
  // Pre-substituted `keyTemplate` path. Same shape as `RecordedShard.key`
  // so set-diff matches by (tier, shardDur, periodStart) — see
  // `listMissingShards` below.
  key: string
}

// Per-tier minimal cover of `range`. See file docstring.
//
// Shards whose period is entirely outside `[range.from, range.to)` are
// omitted — no in-range data could ever land in them. Shards that straddle
// either boundary are emitted with `effective{Start,End}` clipped to the
// query range so materializers can compute `inputsExpected` correctly.
//
// `filter` supplies additional `{name}` values for keyTemplate substitution
// (e.g. `{ device_id: 17617 }` for an awair-style multi-tenant layout).
// `{tier}`, `{shard}`, and `{period}` are filled internally.
export function listExpectedShards(
  pyramid: Pyramid,
  range: { from: Date; to: Date },
  filter: Record<string, string | number> = {},
): ExpectedShard[] {
  const out: ExpectedShard[] = []
  for (const tier of pyramid.tiers) {
    coverForTier(pyramid, tier, range.from, range.to, filter, out)
  }
  return out
}

function coverForTier(
  pyramid: Pyramid,
  tier: Tier,
  from: Date,
  to: Date,
  filter: Record<string, string | number>,
  out: ExpectedShard[],
): void {
  const shards = tier.shards
  const maxShard = shards[shards.length - 1]!
  const maxSpan = parseDuration(maxShard)
  const lastMax = floorToSpan(to, maxSpan)
  const firstMax = floorToSpan(from, maxSpan)

  // Closed-history region: max-shard tiles. First tile may straddle `from`
  // (its period extends into pre-genesis time); `makeExpected` clips
  // `effectiveStart` accordingly. `next > from` is guaranteed by
  // definition of `floorToSpan`, so no pre-genesis pruning needed here.
  let cur = firstMax
  while (cur < lastMax) {
    const next = addSpan(cur, maxSpan)
    out.push(makeExpected(pyramid, tier, maxShard, cur, next, from, to, filter))
    cur = next
  }

  // Trailing partial-max window: greedy largest-fitting-rung descent.
  // When `firstMax === lastMax` (short range within a single max-shard tile),
  // `cur` starts at `lastMax` which may be < `from`; the greedy walk can
  // then pick tiles ending at or before `from` (e.g. ctbk avail-v3
  // `/3d/1440d/2017-04-24` when genesis is 2026-04-08). Prune those —
  // they carry no in-range data.
  const nonMax = shards.slice(0, -1)
  cur = lastMax
  while (cur < to) {
    const chosen = largestFittingRung(nonMax, cur, to)
    if (chosen === null) break
    const [rung, rungEnd] = chosen
    if (rungEnd > from) {
      out.push(makeExpected(pyramid, tier, rung, cur, rungEnd, from, to, filter))
    }
    cur = rungEnd
  }
}

// Largest rung r with `cur` r-aligned and `cur + r ≤ upper`.
function largestFittingRung(
  rungs: readonly Shard[],
  cur: Date,
  upper: Date,
): [Shard, Date] | null {
  for (let i = rungs.length - 1; i >= 0; i--) {
    const r = rungs[i]!
    const span = parseDuration(r)
    if (floorToSpan(cur, span).getTime() !== cur.getTime()) continue
    const rEnd = addSpan(cur, span)
    if (rEnd <= upper) return [r, rEnd]
  }
  return null
}

function makeExpected(
  pyramid: Pyramid,
  tier: Tier,
  shardDur: Shard,
  start: Date,
  end: Date,
  from: Date,
  to: Date,
  filter: Record<string, string | number>,
): ExpectedShard {
  return {
    tier: tier.name,
    shardDur,
    periodStart: start,
    periodEnd: end,
    effectiveStart: start < from ? from : start,
    effectiveEnd: end > to ? to : end,
    key: shardKey(pyramid, tier.name, shardDur, start, filter),
  }
}

// Subtract `shardIndex`-recorded shards from the expected set for
// `pyramidName`. Returns the gap list. Index-driven, not storage-driven:
// a shard that exists on R2 but isn't recorded in the index is "missing"
// from this function's POV — the planner uses the index as ground truth,
// so a non-indexed shard is invisible to queries anyway. Consumers
// wanting a storage-only check can layer `storage.head(key)` on top.
//
// `invalidations`, when supplied, folds in stale-from-invalidation
// keys: any recorded shard overlapping a journal entry whose
// `requestedAt` is newer than the shard's `writtenAt` joins the gap
// list (same mechanic as Python `discover_gaps(invalidations=…)`,
// mtimes sourced from the index's `writtenAt` instead of storage).
// Consumers with storage mtimes should call `staleKeysFor` directly.
//
// Throws if the index has inventory disabled (`D1ShardIndex(
// { skipInventory: true })` / `ManifestShardIndex(
// { includeInventory: false })`) — gap discovery would otherwise report
// every expected shard as missing.
export async function listMissingShards(
  pyramid: Pyramid,
  pyramidName: string,
  shardIndex: ShardIndex,
  range: { from: Date; to: Date },
  filter: Record<string, string | number> = {},
  opts: { invalidations?: Invalidation[] } = {},
): Promise<ExpectedShard[]> {
  const expected = listExpectedShards(pyramid, range, filter)
  const recorded = await shardIndex.listShards(pyramidName)
  const recordedByDiff = new Map<string, Date | undefined>()
  for (const r of recorded) {
    recordedByDiff.set(diffKey(r.tier, r.shardDur, r.periodStart.getTime()), r.writtenAt)
  }
  const missing = expected.filter(e =>
    !recordedByDiff.has(diffKey(e.tier, e.shardDur, e.periodStart.getTime())),
  )
  const invs = opts.invalidations ?? []
  if (invs.length === 0) return missing
  // Build `{key: writtenAt}` for the recorded-and-expected subset so
  // `staleKeysFor` can apply the overlap staleness rule. Unknown
  // `writtenAt` (older manifests without the field) → null → treated
  // as fresh (backends that can't report mtimes shouldn't trigger
  // rebuilds — same rule Python's `split_stale` uses).
  const mtimes = new Map<string, Date | null>()
  for (const e of expected) {
    const dk = diffKey(e.tier, e.shardDur, e.periodStart.getTime())
    if (!recordedByDiff.has(dk)) continue  // already in `missing`; skip
    mtimes.set(e.key, recordedByDiff.get(dk) ?? null)
  }
  const stale = staleKeysFor(expected, mtimes, invs)
  if (stale.size === 0) return missing
  const seenKeys = new Set(missing.map(m => m.key))
  const staleShards = expected.filter(e => stale.has(e.key) && !seenKeys.has(e.key))
  return [...missing, ...staleShards]
}

// `(tier, shardDur, periodStartMs)` is the natural identity for an
// expected/recorded shard pair — the inventory's PK in D1, the manifest
// inventory's match-by-fields key. Use `\x00` as separator since none of
// the components can contain a null byte.
function diffKey(tier: string, shardDur: Shard, periodStartMs: number): string {
  return `${tier}\x00${shardDur}\x00${periodStartMs}`
}

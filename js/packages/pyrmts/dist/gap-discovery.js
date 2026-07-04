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
import { addSpan, floorToSpan, formatPeriod, parseDuration } from './axis.js';
import { substituteKey } from './keys.js';
// Per-tier minimal cover of `range`. See file docstring.
//
// `filter` supplies additional `{name}` values for keyTemplate substitution
// (e.g. `{ device_id: 17617 }` for an awair-style multi-tenant layout).
// `{tier}`, `{shard}`, and `{period}` are filled internally.
export function listExpectedShards(pyramid, range, filter = {}) {
    const out = [];
    for (const tier of pyramid.tiers) {
        coverForTier(pyramid, tier, range.from, range.to, filter, out);
    }
    return out;
}
function coverForTier(pyramid, tier, from, to, filter, out) {
    const shards = tier.shards;
    const maxShard = shards[shards.length - 1];
    const maxSpan = parseDuration(maxShard);
    const lastMax = floorToSpan(to, maxSpan);
    const firstMax = floorToSpan(from, maxSpan);
    // Closed-history region: max-shard tiles.
    let cur = firstMax;
    while (cur < lastMax) {
        const next = addSpan(cur, maxSpan);
        out.push(makeExpected(pyramid, tier, maxShard, cur, next, filter));
        cur = next;
    }
    // Trailing partial-max window: greedy largest-fitting-rung descent.
    const nonMax = shards.slice(0, -1);
    cur = lastMax;
    while (cur < to) {
        const chosen = largestFittingRung(nonMax, cur, to);
        if (chosen === null)
            break;
        const [rung, rungEnd] = chosen;
        out.push(makeExpected(pyramid, tier, rung, cur, rungEnd, filter));
        cur = rungEnd;
    }
}
// Largest rung r with `cur` r-aligned and `cur + r ≤ upper`.
function largestFittingRung(rungs, cur, upper) {
    for (let i = rungs.length - 1; i >= 0; i--) {
        const r = rungs[i];
        const span = parseDuration(r);
        if (floorToSpan(cur, span).getTime() !== cur.getTime())
            continue;
        const rEnd = addSpan(cur, span);
        if (rEnd <= upper)
            return [r, rEnd];
    }
    return null;
}
function makeExpected(pyramid, tier, shardDur, start, end, filter) {
    const span = parseDuration(shardDur);
    return {
        tier: tier.name,
        shardDur,
        periodStart: start,
        periodEnd: end,
        key: substituteKey(pyramid.keyTemplate, {
            ...filter,
            tier: tier.name,
            shard: shardDur,
            period: formatPeriod(start, span),
        }),
    };
}
// Subtract `shardIndex`-recorded shards from the expected set for
// `pyramidName`. Returns the gap list. Index-driven, not storage-driven:
// a shard that exists on R2 but isn't recorded in the index is "missing"
// from this function's POV — the planner uses the index as ground truth,
// so a non-indexed shard is invisible to queries anyway. Consumers
// wanting a storage-only check can layer `storage.head(key)` on top.
//
// Throws if the index has inventory disabled (`D1ShardIndex(
// { skipInventory: true })` / `ManifestShardIndex(
// { includeInventory: false })`) — gap discovery would otherwise report
// every expected shard as missing.
export async function listMissingShards(pyramid, pyramidName, shardIndex, range, filter = {}) {
    const expected = listExpectedShards(pyramid, range, filter);
    const recorded = await shardIndex.listShards(pyramidName);
    const seen = new Set();
    for (const r of recorded) {
        seen.add(diffKey(r.tier, r.shardDur, r.periodStart.getTime()));
    }
    return expected.filter(e => !seen.has(diffKey(e.tier, e.shardDur, e.periodStart.getTime())));
}
// `(tier, shardDur, periodStartMs)` is the natural identity for an
// expected/recorded shard pair — the inventory's PK in D1, the manifest
// inventory's match-by-fields key. Use `\x00` as separator since none of
// the components can contain a null byte.
function diffKey(tier, shardDur, periodStartMs) {
    return `${tier}\x00${shardDur}\x00${periodStartMs}`;
}
//# sourceMappingURL=gap-discovery.js.map
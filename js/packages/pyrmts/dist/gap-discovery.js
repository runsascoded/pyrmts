// Gap discovery: enumerate every shard a pyramid's ladders declare for
// a range, then subtract the set already recorded in a `ShardIndex`.
// Pure axis arithmetic + set diff — no fill orchestration. Consumers
// (ctbk fsck, rides-v3 fsck) wrap this with their own source readers
// and dependency-ordered fill loop. See
// `specs/done/gap-discovery.md`.
import { shardPeriodsCovering } from './axis.js';
import { substituteKey } from './keys.js';
// List every shard the pyramid's ladders declare for `range`. Pure
// enumeration over the YAML — no storage access, no index access. Useful
// as a ground-truth set to diff against, or for "how many shards would a
// full backfill produce?" planning.
//
// `filter` supplies additional `{name}` values for keyTemplate substitution
// (e.g. `{ device_id: 17617 }` for an awair-style multi-tenant layout).
// `{tier}`, `{shard}`, and `{period}` are filled internally.
export function listExpectedShards(pyramid, range, filter = {}) {
    const out = [];
    for (const tier of pyramid.tiers) {
        for (const shardDur of tier.shards) {
            const periods = shardPeriodsCovering(range.from, range.to, shardDur);
            for (const p of periods) {
                out.push({
                    tier: tier.name,
                    shardDur,
                    periodStart: p.start,
                    periodEnd: p.end,
                    key: substituteKey(pyramid.keyTemplate, {
                        ...filter,
                        tier: tier.name,
                        shard: shardDur,
                        period: p.label,
                    }),
                });
            }
        }
    }
    return out;
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
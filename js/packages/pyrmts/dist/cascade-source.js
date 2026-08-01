// Strict-cascade source selection + shard readiness. TS ports of
// `pyrmts_engine.materialize.{source_tier_for,buildable_at}` — see
// `specs/source-readiness-pending.md`.
import { ceilToSpan, fixedDurationMs, parseDuration } from './axis.js';
const DAY_MS = 24 * 60 * 60_000;
// Approximate width for ordering only (calendar units use nominal 30d/365d).
function approxMs(dur) {
    const { count, unit } = parseDuration(dur);
    if (unit === 'mo')
        return count * 30 * DAY_MS;
    if (unit === 'y')
        return count * 365 * DAY_MS;
    return fixedDurationMs(dur);
}
// Strict-cascade source tier: the largest tier T' with `bin(T') < bin(tier)`
// AND `bin(tier) % bin(T') == 0` (divisibility keeps floor-then-sum exact).
// `null` for the base tier (raw-ingest territory); throws for any other tier
// without a divisor (a malformed ladder — the base bin must divide everything).
export function sourceTierFor(pyramid, tierName) {
    const tiers = pyramid.tiers;
    if (tierName === tiers[0].name)
        return null;
    const tierIdx = tiers.findIndex((t) => t.name === tierName);
    if (tierIdx < 0)
        throw new Error(`sourceTierFor: no tier '${tierName}'`);
    const targetMs = approxMs(String(tiers[tierIdx].bin));
    let best = null;
    let bestMs = 0;
    for (const cand of tiers.slice(0, tierIdx)) {
        const candMs = approxMs(String(cand.bin));
        if (candMs >= targetMs || targetMs % candMs !== 0)
            continue;
        if (candMs > bestMs) {
            best = cand;
            bestMs = candMs;
        }
    }
    if (best === null) {
        throw new Error(`no source tier for /${tierName} — pyramid ladder is malformed`);
    }
    return best;
}
// Earliest instant a shard of `tierName` ending at `periodEnd` can be built:
// its strict-cascade source cover is complete only once the smallest-rung
// source tile containing the tail closes — recursively, since that tile's
// own sources must close first. The smallest source rung is the binding
// constraint (the rung list is a divisibility chain, so no coarser tile
// closes earlier). Equals `periodEnd` for span-aligned endings (the
// majority) and for the base tier (raw-ingest territory).
export function shardBuildableAt(pyramid, tierName, periodEnd) {
    const src = sourceTierFor(pyramid, tierName);
    if (src === null)
        return periodEnd;
    const srcEnd = ceilToSpan(periodEnd, parseDuration(String(src.shards[0])));
    const rec = shardBuildableAt(pyramid, src.name, srcEnd);
    return rec > srcEnd ? rec : srcEnd;
}
//# sourceMappingURL=cascade-source.js.map
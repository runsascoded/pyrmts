// Per-tier shard-duration ladder validation. Each tier carries an ascending,
// divisibility-chained `shards: Shard[]` — `shards[0]` is the smallest (≥
// `tier.bin`); `shards.at(-1)` is the largest. See
// `specs/done/unified-shard-ladder.md`.
import { fixedDurationMs, parseDuration } from './axis.js';
// Validate every tier's `shards` ladder. Throws on:
//   - empty `shards`
//   - smallest shard < tier.bin (sub-bin shards make no sense)
//   - not ascending
//   - duplicates
//   - non-divisibility (each shard must divide the next; promotion is concat)
//   - mixing fixed-width and calendar-variable (`mo`/`y`) durations in one ladder
//   - `1run` outside step-axis
export function validateLadders(pyramid) {
    const out = [];
    for (const tier of pyramid.tiers) {
        if (!Array.isArray(tier.shards) || tier.shards.length === 0) {
            throw new Error(`validateLadders: tier '${tier.name}' has empty shards`);
        }
        out.push(validateLadder(tier));
    }
    return out;
}
function validateLadder(tier) {
    const binMs = binMsOrThrow(tier);
    const shardsMs = [];
    let prevMs = null;
    for (let i = 0; i < tier.shards.length; i++) {
        const shard = tier.shards[i];
        const ms = shardMsOrNull(shard, tier.name);
        if (ms !== null) {
            if (binMs !== null && ms < binMs) {
                throw new Error(`validateLadders: tier '${tier.name}' shards[${i}]='${shard}' (${ms}ms) ` +
                    `is smaller than bin '${tier.bin}' (${binMs}ms)`);
            }
            if (prevMs !== null) {
                if (ms <= prevMs) {
                    throw new Error(`validateLadders: tier '${tier.name}' shards not ascending ` +
                        `(shards[${i}]='${shard}' (${ms}ms) <= shards[${i - 1}] (${prevMs}ms))`);
                }
                if (ms % prevMs !== 0) {
                    throw new Error(`validateLadders: tier '${tier.name}' shards[${i}]='${shard}' (${ms}ms) ` +
                        `is not a multiple of shards[${i - 1}] (${prevMs}ms); ladder must be a ` +
                        `divisibility chain so promotion is clean concat`);
                }
            }
        }
        shardsMs.push({ shard, ms });
        prevMs = ms;
    }
    return { tier, shardsMs };
}
// `null` = calendar-variable (mo/y) or step-axis ('1run'). Variable-width
// entries are allowed in a ladder, but they don't participate in
// divisibility checks against fixed-width siblings (calendar/step are their
// own universes); mixing emits a single-element ladder per axis.
function shardMsOrNull(shard, tierName) {
    if (shard === '1run')
        return null;
    const parsed = parseDuration(shard);
    if (parsed.unit === 'mo' || parsed.unit === 'y')
        return null;
    return fixedDurationMs(shard);
}
function binMsOrThrow(tier) {
    const bin = tier.bin;
    if (bin.endsWith('step') || bin.endsWith('steps') || bin.endsWith('ksteps') || bin.endsWith('msteps')) {
        return null; // step-axis bins don't measure in ms
    }
    const parsed = parseDuration(bin);
    if (parsed.unit === 'mo' || parsed.unit === 'y')
        return null;
    return fixedDurationMs(bin);
}
//# sourceMappingURL=ladder.js.map
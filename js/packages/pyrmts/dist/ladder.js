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
//   - multi-rung tier with `keyTemplate` missing `{shard}` (rungs starting
//     on the same period would collide on one key)
export function validateLadders(pyramid) {
    const out = [];
    for (const tier of pyramid.tiers) {
        if (!Array.isArray(tier.shards) || tier.shards.length === 0) {
            throw new Error(`validateLadders: tier '${tier.name}' has empty shards`);
        }
        if (tier.shards.length > 1 && !pyramid.keyTemplate.includes('{shard}')) {
            throw new Error(`validateLadders: tier '${tier.name}' has a multi-rung ladder ` +
                `(${JSON.stringify(tier.shards)}) but keyTemplate ` +
                `'${pyramid.keyTemplate}' is missing the '{shard}' placeholder — ` +
                `rungs starting on the same period would collide on one key. ` +
                `Add '{shard}' to the template (e.g. ` +
                `'.../{tier}/{shard}/{period}.parquet') or collapse the tier to a ` +
                `single shard rung.`);
        }
        out.push(validateLadder(tier));
    }
    return out;
}
function validateLadder(tier) {
    const binMs = binMsOrThrow(tier);
    const binMonths = isStepBin(tier.bin) ? null : monthsOrNull(tier.bin);
    if (!isStepBin(tier.bin)) {
        validateMonthSpan(tier.bin, tier.name);
    }
    const shardsMs = [];
    let prevMs = null;
    let prevMonths = null;
    let prevNominal = null;
    for (let i = 0; i < tier.shards.length; i++) {
        const shard = tier.shards[i];
        const ms = shardMsOrNull(shard, tier.name);
        // Calendar checks (`specs/calendar-units.md`): `Nmo` must tile a year;
        // calendar-calendar pairs divide in months (`y` ≡ `12mo`); mixed
        // fixed/calendar pairs assert nominal-width (30d/365d) ascension and
        // divisibility (`specs/calendar-rung-consolidation.md`).
        const months = shard === '1run' ? null : monthsOrNull(shard);
        if (shard !== '1run') {
            validateMonthSpan(shard, tier.name);
            if (months !== null) {
                if (i === 0 && binMonths !== null) {
                    if (months < binMonths) {
                        throw new Error(`validateLadders: tier '${tier.name}' shards[0]='${shard}' ` +
                            `is smaller than bin '${tier.bin}' (in months)`);
                    }
                    if (months % binMonths !== 0) {
                        throw new Error(`validateLadders: tier '${tier.name}' bin '${tier.bin}' does not ` +
                            `divide shards[0]='${shard}' (in months)`);
                    }
                }
                if (prevMonths !== null && months % prevMonths !== 0) {
                    throw new Error(`validateLadders: tier '${tier.name}' shards[${i - 1}]='${tier.shards[i - 1]}' ` +
                        `does not divide shards[${i}]='${shard}' (in months)`);
                }
            }
            const nominal = nominalMs(shard);
            // Both-fixed pairs are covered by the exact integer-ms checks below.
            if (prevNominal !== null && (months !== null || prevMonths !== null) && nominal <= prevNominal) {
                throw new Error(`validateLadders: tier '${tier.name}' shards not ascending ` +
                    `(shards[${i}]='${shard}' <= shards[${i - 1}]='${tier.shards[i - 1]}' by nominal width)`);
            }
            // Calendar-calendar pairs are exact-checked in months above; mixed
            // pairs chain by nominal width so same-tier consolidation tiling
            // stays sane.
            if (prevNominal !== null &&
                (months !== null) !== (prevMonths !== null) &&
                nominal % prevNominal !== 0) {
                throw new Error(`validateLadders: tier '${tier.name}' shards[${i - 1}]='${tier.shards[i - 1]}' ` +
                    `does not divide shards[${i}]='${shard}' (by nominal width)`);
            }
            prevNominal = nominal;
        }
        prevMonths = months;
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
    if (isStepBin(bin)) {
        return null; // step-axis bins don't measure in ms
    }
    const parsed = parseDuration(bin);
    if (parsed.unit === 'mo' || parsed.unit === 'y')
        return null;
    return fixedDurationMs(bin);
}
function isStepBin(bin) {
    return bin.endsWith('step') || bin.endsWith('steps') || bin.endsWith('ksteps') || bin.endsWith('msteps');
}
// Calendar durations in months (`y` ≡ `12mo`), `null` for fixed-width.
function monthsOrNull(dur) {
    const parsed = parseDuration(dur);
    if (parsed.unit === 'mo')
        return parsed.count;
    if (parsed.unit === 'y')
        return 12 * parsed.count;
    return null;
}
function validateMonthSpan(dur, tierName) {
    const parsed = parseDuration(dur);
    if (parsed.unit === 'mo' && 12 % parsed.count !== 0) {
        throw new Error(`validateLadders: tier '${tierName}' month-span '${dur}' doesn't tile a ` +
            `year evenly (12 % ${parsed.count} !== 0)`);
    }
}
// Width for ordering only — calendar entries use nominal 30d/365d.
function nominalMs(dur) {
    const parsed = parseDuration(dur);
    const dayMs = 24 * 60 * 60_000;
    if (parsed.unit === 'mo')
        return parsed.count * 30 * dayMs;
    if (parsed.unit === 'y')
        return parsed.count * 365 * dayMs;
    return fixedDurationMs(dur);
}
//# sourceMappingURL=ladder.js.map
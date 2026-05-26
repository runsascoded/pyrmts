// Pure query planner. Given a pyramid + viewport, choose an output tier and
// emit a segmented plan describing which shards to read and where to
// re-aggregate. No I/O.
import { binsInRange, shardPeriodsCovering } from './axis.js';
export function planQuery(pyramid, input) {
    if (pyramid.axis !== 'time') {
        throw new Error(`planQuery: axis '${pyramid.axis}' not yet implemented (only 'time')`);
    }
    if (pyramid.tiers.length === 0) {
        throw new Error('planQuery: pyramid has no tiers');
    }
    const { from, to } = input.range;
    if (to <= from) {
        throw new Error(`planQuery: empty range (${from.toISOString()} → ${to.toISOString()})`);
    }
    const outputTier = pickTier(pyramid.tiers, from, to, input.binBudget);
    const outputIdx = pyramid.tiers.indexOf(outputTier);
    const effective = effectiveWatermarks(pyramid.tiers, input.watermarks ?? {}, to);
    const earliest = effectiveEarliestWatermarks(pyramid.tiers, input.earliestWatermarks ?? {});
    // Walk from output tier down to finest, emitting one segment per tier
    // covering the gap up to that tier's effective watermark. Each tier's
    // segment is clamped on the left by its earliest watermark — if the
    // entire candidate segment falls before that, skip the tier entirely.
    const segments = [];
    let cursor = from;
    for (let i = outputIdx; i >= 0; i--) {
        const tier = pyramid.tiers[i];
        const tierEnd = clamp(effective[tier.name], cursor, to);
        const earlyT = earliest[tier.name];
        const tierStart = earlyT && earlyT.getTime() > cursor.getTime() ? earlyT : cursor;
        if (tierEnd > tierStart) {
            segments.push({
                from: tierStart,
                to: tierEnd,
                shardTier: tier,
                keys: shardKeys(pyramid, tier, tierStart, tierEnd, input.filter ?? {}),
                reaggregate: i !== outputIdx,
            });
        }
        cursor = tierEnd;
        if (cursor >= to)
            break;
    }
    const rawWm = effective[pyramid.tiers[0].name];
    const authoritativeEnd = rawWm < to ? rawWm : null;
    return {
        outputTier,
        outputBin: outputTier.bin,
        segments,
        authoritativeEnd,
    };
}
// Finest tier whose bin count fits the budget. If even the coarsest tier
// exceeds the budget (over-narrow viewport for the data range), return the
// coarsest tier anyway — the chart can downsample further on the client.
function pickTier(tiers, from, to, binBudget) {
    for (const tier of tiers) {
        const count = binsInRange(from, to, tier.bin);
        if (count <= binBudget)
            return tier;
    }
    return tiers[tiers.length - 1];
}
// Each tier's effective watermark = min(declared, next-finer-tier's effective).
// Walks finest → coarsest, propagating the finer tier's bound forward.
function effectiveWatermarks(tiers, declared, rangeTo) {
    const out = {};
    // Finer tiers default to rangeTo (treat unspecified as "complete enough").
    let finerBound = new Date(8.64e15);
    for (const tier of tiers) {
        const decl = declared[tier.name];
        const eff = decl
            ? new Date(Math.min(decl.getTime(), finerBound.getTime()))
            : finerBound;
        // Clamp to rangeTo so watermarks past the query don't leak through.
        const clamped = eff.getTime() > rangeTo.getTime() ? rangeTo : eff;
        out[tier.name] = clamped;
        finerBound = eff;
    }
    return out;
}
// Each tier's effective earliest = max(declared, next-finer-tier's effective).
// Walks finest → coarsest, propagating the finer tier's bound forward.
// Unspecified tiers have no clamp (treat as -infinity); a finer tier's
// declared value carries up to coarser tiers that didn't declare one
// (coarser tiers can't have data before their finer source did).
function effectiveEarliestWatermarks(tiers, declared) {
    const out = {};
    let finerBound = undefined;
    for (const tier of tiers) {
        const decl = declared[tier.name];
        let eff;
        if (decl && finerBound) {
            eff = decl.getTime() > finerBound.getTime() ? decl : finerBound;
        }
        else {
            eff = decl ?? finerBound;
        }
        out[tier.name] = eff;
        finerBound = eff;
    }
    return out;
}
function shardKeys(pyramid, tier, from, to, filter) {
    const periods = shardPeriodsCovering(from, to, tier.shard);
    return periods.map(p => substituteKey(pyramid.keyTemplate, {
        ...filter,
        tier: tier.name,
        period: p.label,
    }));
}
function substituteKey(template, values) {
    return template.replace(/\{(\w+)\}/g, (_, name) => {
        if (!(name in values)) {
            throw new Error(`planQuery: missing key template value for {${name}}`);
        }
        return String(values[name]);
    });
}
function clamp(t, lo, hi) {
    if (t.getTime() < lo.getTime())
        return lo;
    if (t.getTime() > hi.getTime())
        return hi;
    return t;
}
//# sourceMappingURL=planner.js.map
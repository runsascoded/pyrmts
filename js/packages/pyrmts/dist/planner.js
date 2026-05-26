// Pure query planner. Given a pyramid + viewport, choose an output tier and
// emit a segmented plan describing which shards to read and where to
// re-aggregate. No I/O.
import { addSpan, binsInRange, fixedDurationMs, parseDuration, shardPeriodsCovering } from './axis.js';
export const DEFAULT_AUTO_MULTIPLIER = 50;
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
    const earliest = effectiveEarliestWatermarks(pyramid.tiers, input.earliestWatermarks ?? {});
    // Resolve smoothing → snapped (smoothBin, smoothBinCount) and extend the
    // planning window outward so segments have buffer rows for the rolling
    // pass at the visible edges. The visible range stays as the caller asked;
    // the stitcher trims back to `visibleRange` after smoothing.
    const smoothMode = input.smoothMode ?? 'centered';
    const smoothing = input.smoothing !== undefined
        ? resolveSmoothing(input.smoothing, outputTier.bin, from, to, smoothMode)
        : null;
    const { from: plannedFrom, to: plannedTo } = smoothing
        ? extendForSmoothing(from, to, outputTier.bin, smoothing.smoothBinCount, smoothMode)
        : { from, to };
    // Watermarks clamp to the *extended* window so the buffer can include
    // post-`to` bins for centered smoothing (otherwise the trailing buffer
    // gets silently truncated to the original `to`).
    const effective = effectiveWatermarks(pyramid.tiers, input.watermarks ?? {}, plannedTo);
    // Walk from output tier down to finest, emitting one segment per tier
    // covering the gap up to that tier's effective watermark. Each tier's
    // segment is clamped on the left by its earliest watermark — if the
    // entire candidate segment falls before that, skip the tier entirely.
    const segments = [];
    let cursor = plannedFrom;
    for (let i = outputIdx; i >= 0; i--) {
        const tier = pyramid.tiers[i];
        const tierEnd = clamp(effective[tier.name], cursor, plannedTo);
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
        if (cursor >= plannedTo)
            break;
    }
    const rawWm = effective[pyramid.tiers[0].name];
    const authoritativeEnd = rawWm < to ? rawWm : null;
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
// Catalog of "nice" widths used when snapping a user-supplied smoothing
// window to a representable Duration. All fixed-width (no mo/y); calendar
// outputs (mo/y) snap to integer-count in their own unit instead.
const NICE_WIDTHS = [
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
];
// Resolve a `SmoothingSpec` to a snapped (smoothBin, smoothBinCount) tuple.
// Snapping picks the closest candidate that's an integer multiple of the
// output bin, with `smoothBinCount` ≥ 1 and ≤ floor(visibleRangeBins / 4)
// (so smoothing can't dominate the visible range — pathological cases
// degrade to 1× output bin = no-op).
function resolveSmoothing(spec, outputBin, from, to, _mode) {
    const outSpan = parseDuration(outputBin);
    const visibleBins = binsInRange(from, to, outputBin);
    const maxCount = Math.max(1, Math.floor(visibleBins / 4));
    // Calendar output bins (mo/y) — only same-unit integer counts make sense.
    if (outSpan.unit === 'mo' || outSpan.unit === 'y') {
        if (typeof spec === 'string') {
            const s = parseDuration(spec);
            if (s.unit !== outSpan.unit) {
                throw new Error(`planQuery: smoothing ${spec} is incompatible with calendar output bin ${outputBin} ` +
                    `(use ${outSpan.unit} for both)`);
            }
            const count = Math.max(1, Math.min(maxCount, Math.round(s.count / outSpan.count)));
            return { smoothBin: `${count * outSpan.count}${outSpan.unit}`, smoothBinCount: count };
        }
        const mult = spec.multiplier ?? DEFAULT_AUTO_MULTIPLIER;
        const count = Math.max(1, Math.min(maxCount, mult));
        return { smoothBin: `${count * outSpan.count}${outSpan.unit}`, smoothBinCount: count };
    }
    // Fixed-width output bin — snap an ms target to the nearest nice width that
    // divides cleanly into the output bin.
    const outputBinMs = fixedDurationMs(outputBin);
    const desiredMs = typeof spec === 'string'
        ? fixedDurationMs(spec)
        : (spec.multiplier ?? DEFAULT_AUTO_MULTIPLIER) * outputBinMs;
    let best = null;
    let bestDist = Infinity;
    for (const c of NICE_WIDTHS) {
        if (c.ms < outputBinMs)
            continue; // can't smooth below output granularity
        if (c.ms % outputBinMs !== 0)
            continue; // must be integer multiple
        const count = c.ms / outputBinMs;
        if (count > maxCount)
            continue;
        const dist = Math.abs(c.ms - desiredMs);
        if (dist < bestDist) {
            bestDist = dist;
            best = { label: c.label, count };
        }
    }
    // Fall back to 1× output bin if nothing fits — degenerate but well-defined
    // (smoothing == output, i.e. no-op).
    if (best === null)
        return { smoothBin: outputBin, smoothBinCount: 1 };
    return { smoothBin: best.label, smoothBinCount: best.count };
}
// Extend the visible [from, to) outward by the smoothing buffer so the
// rolling pass has full context at every visible bin. For window size N:
//   centered: lead = ceil((N-1)/2), tail = floor((N-1)/2) (past-biased on ties)
//   trailing: lead = N - 1, tail = 0
// N = 1 is a no-op (smoothing window == output bin).
function extendForSmoothing(from, to, outputBin, smoothBinCount, mode) {
    const N = smoothBinCount;
    const leadBins = mode === 'centered' ? Math.ceil((N - 1) / 2) : N - 1;
    const tailBins = mode === 'centered' ? Math.floor((N - 1) / 2) : 0;
    const outSpan = parseDuration(outputBin);
    // Step `outputBin` outward via `addSpan` (calendar-correct for mo/y).
    return {
        from: addSpan(from, { count: -leadBins * outSpan.count, unit: outSpan.unit }),
        to: addSpan(to, { count: tailBins * outSpan.count, unit: outSpan.unit }),
    };
}
//# sourceMappingURL=planner.js.map
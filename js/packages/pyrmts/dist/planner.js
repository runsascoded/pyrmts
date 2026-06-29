// Pure query planner. Given a pyramid + viewport, choose an output tier and
// emit a segmented plan describing which shards to read and where to
// re-aggregate. No I/O.
import { addSpan, binsInRange, fixedDurationMs, floorToSpan, parseDuration, shardPeriodsCovering } from './axis.js';
import { validateLadders } from './ladder.js';
import { encodeWatermarkKey } from './shard-index.js';
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
    // Validate per-tier shard ladders.
    validateLadders(pyramid);
    if (input.targetBin !== undefined) {
        return planRagged(pyramid, input, input.targetBin);
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
    // Build the 2D `(tier, shardDur)` effective-watermark grid.
    const grid = effectiveShardWatermarks(pyramid, input.watermarks ?? {}, input.earliestPerShard ?? {});
    // Cursor-aware walk: at each cursor position, try the LARGEST shard
    // duration at the output tier first; if its watermark doesn't reach
    // past cursor (or is before its earliest), try the next-smaller; etc.;
    // then fall to next-finer tier. Emit one segment per period chosen.
    // Adjacent same-(tier, shardDur) segments coalesce after the walk.
    //
    // "Covers cursor" check: `effective > cursor`. The shard's containing
    // period [periodStart, periodEnd) has data sealed up to `effective`;
    // any cursor position strictly less than effective has data available.
    // Emitted segment is clipped to `min(plannedTo, effective, periodEnd)`.
    const rawSegments = [];
    let cursor = plannedFrom;
    walk: while (cursor.getTime() < plannedTo.getTime()) {
        for (let i = outputIdx; i >= 0; i--) {
            const tier = pyramid.tiers[i];
            const tierGrid = grid.byTier[tier.name];
            const earlyT = earliest[tier.name];
            // Try shard durations LARGEST first.
            for (let j = tier.shards.length - 1; j >= 0; j--) {
                const shardDur = tier.shards[j];
                const entry = tierGrid[shardDur];
                if (entry === undefined)
                    continue;
                const span = parseDuration(shardDur);
                const periodStart = floorToSpan(cursor, span);
                const periodEnd = addSpan(periodStart, span);
                // Covers cursor? Data exists at this position.
                if (entry.effective.getTime() <= cursor.getTime())
                    continue;
                // Per-shard earliest gate? earliestPerShard > periodStart means
                // this specific (tier, shardDur) doesn't cover periodStart.
                if (entry.earliestEntry && entry.earliestEntry.getTime() > periodStart.getTime())
                    continue;
                // Per-tier earliest gate? Applies uniformly across shard durations.
                // If earlyT > cursor, clip segment start.
                const segFrom = earlyT && earlyT.getTime() > cursor.getTime() ? earlyT : cursor;
                // Clip to plannedTo, effective (partial-fill case), and periodEnd.
                const upperMs = Math.min(plannedTo.getTime(), entry.effective.getTime(), periodEnd.getTime());
                const segTo = new Date(upperMs);
                if (segTo.getTime() <= segFrom.getTime()) {
                    // Entire range [cursor, segTo) gated by per-tier earliest. The
                    // entry "owns" this range; finer tiers / smaller shardDurs pick
                    // up *after* it, not inside it. Advance cursor to segTo (= entry's
                    // effective end clipped to period/plannedTo).
                    cursor = segTo;
                    continue walk;
                }
                rawSegments.push({
                    from: segFrom,
                    to: segTo,
                    shardTier: tier,
                    shardDur,
                    keys: shardKeys(pyramid, tier, shardDur, segFrom, segTo, input.filter ?? {}),
                    reaggregate: i !== outputIdx,
                });
                cursor = segTo;
                continue walk;
            }
        }
        // No tier/shardDur covered cursor — give up; tail is uncovered.
        break;
    }
    // Coalesce adjacent same-(tier, shardDur) segments. Re-derive `keys` on
    // the coalesced range so the keys list matches the new bounds.
    const segments = [];
    for (const seg of rawSegments) {
        const last = segments[segments.length - 1];
        if (last !== undefined
            && last.shardTier === seg.shardTier
            && last.shardDur === seg.shardDur
            && last.to.getTime() === seg.from.getTime()) {
            last.to = seg.to;
            last.keys = shardKeys(pyramid, last.shardTier, last.shardDur, last.from, last.to, input.filter ?? {});
        }
        else {
            segments.push({ ...seg });
        }
    }
    // raw-tier max effective (across all shard durations) is the authoritative
    // end — past it, the consumer's hot path takes over.
    const rawTierName = pyramid.tiers[0].name;
    const rawGrid = grid.byTier[rawTierName];
    let rawMaxMs = 0;
    for (const [, entry] of Object.entries(rawGrid)) {
        if (entry.effective.getTime() > rawMaxMs)
            rawMaxMs = entry.effective.getTime();
    }
    const rawWm = new Date(rawMaxMs);
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
// Ragged-decomposition planner: caller specifies the exact output bin width
// (`targetBin`); planner packs each output bin with a minimum-item set of
// finer-tier atoms via DP (shortest path on tier-bin-aligned positions),
// then coalesces adjacent same-tier atoms into segments. The DP runs once
// per (eligible-tier-set, phase) — output bins sharing a watermark band and
// a phase mod LCM(tier widths) reuse cached results.
//
// The ragged path uses each eligible tier's LARGEST shard duration
// (`tier.shards.at(-1)`) for all segments — it's a coarse-output path where
// fine-grained shard-duration fall-through doesn't help (each output bin is
// already an arbitrary width; we just need data at the chosen tier).
function planRagged(pyramid, input, targetBin) {
    const { from, to } = input.range;
    const tParsed = parseDuration(targetBin);
    if (tParsed.unit === 'mo' || tParsed.unit === 'y') {
        throw new Error(`planQuery: targetBin '${targetBin}' is calendar-variable; ragged decomposition supports fixed-width units only`);
    }
    const targetBinMs = fixedDurationMs(targetBin);
    const eligibleTiers = [];
    for (const tier of pyramid.tiers) {
        const tb = parseDuration(tier.bin);
        if (tb.unit === 'mo' || tb.unit === 'y')
            continue;
        const ms = fixedDurationMs(tier.bin);
        if (ms > targetBinMs)
            continue;
        eligibleTiers.push({ tier, ms });
    }
    if (eligibleTiers.length === 0) {
        throw new Error(`planQuery: no tier with fixed-width bin ≤ targetBin '${targetBin}' (pyramid tiers: ${pyramid.tiers.map(t => t.bin).join(', ')})`);
    }
    let tierGcd = eligibleTiers[0].ms;
    for (let i = 1; i < eligibleTiers.length; i++) {
        tierGcd = gcd(tierGcd, eligibleTiers[i].ms);
    }
    if (targetBinMs % tierGcd !== 0) {
        throw new Error(`planQuery: no decomposition of targetBin '${targetBin}' from eligible tiers (gcd ${tierGcd} doesn't divide ${targetBinMs})`);
    }
    eligibleTiers.sort((a, b) => a.ms - b.ms);
    const outputTier = eligibleTiers.find(e => e.ms === targetBinMs)?.tier;
    const smoothMode = input.smoothMode ?? 'centered';
    const smoothing = input.smoothing !== undefined
        ? resolveSmoothing(input.smoothing, targetBin, from, to, smoothMode)
        : null;
    const { from: plannedFrom, to: plannedTo } = smoothing
        ? extendForSmoothing(from, to, targetBin, smoothing.smoothBinCount, smoothMode)
        : { from, to };
    // For ragged, use largest-shard-duration watermark per tier.
    const effective = effectiveLargestShardWatermarks(pyramid.tiers, input.watermarks ?? {}, plannedTo);
    const earliest = effectiveEarliestWatermarks(pyramid.tiers, input.earliestWatermarks ?? {});
    const targetSpan = tParsed;
    const firstBinStart = floorToSpan(plannedFrom, targetSpan);
    const phaseCacheByKey = new Map();
    const lcmAll = eligibleTiers.reduce((l, { ms }) => lcm(l, ms), targetBinMs);
    const atoms = [];
    let binStart = firstBinStart;
    while (binStart < plannedTo) {
        const binStartMs = binStart.getTime();
        const binEnd = addSpan(binStart, targetSpan);
        const binEndMs = binEnd.getTime();
        const perBin = [];
        for (const e of eligibleTiers) {
            if (effective[e.tier.name].getTime() < binEndMs)
                continue;
            const earlyT = earliest[e.tier.name];
            if (earlyT && earlyT.getTime() > binStartMs)
                continue;
            perBin.push(e);
        }
        if (perBin.length === 0) {
            binStart = binEnd;
            continue;
        }
        const cacheKey = perBin.map(e => e.tier.name).join(',');
        let phaseCache = phaseCacheByKey.get(cacheKey);
        if (phaseCache === undefined) {
            phaseCache = new Map();
            phaseCacheByKey.set(cacheKey, phaseCache);
        }
        const phase = ((binStartMs % lcmAll) + lcmAll) % lcmAll;
        let path = phaseCache.get(phase);
        if (path === undefined) {
            path = decomposeBin(perBin, targetBinMs, phase);
            phaseCache.set(phase, path);
        }
        if (path === null) {
            throw new Error(`planQuery: cannot decompose output bin starting at ${binStart.toISOString()} ` +
                `(targetBin '${targetBin}', eligible tiers ${cacheKey})`);
        }
        for (const atom of path) {
            atoms.push({
                tier: atom.tier,
                absStartMs: binStartMs + atom.offsetMs,
                absEndMs: binStartMs + atom.offsetMs + atom.durationMs,
            });
        }
        binStart = binEnd;
    }
    const segments = [];
    if (atoms.length > 0) {
        let curr = { ...atoms[0] };
        for (let i = 1; i < atoms.length; i++) {
            const next = atoms[i];
            if (next.tier === curr.tier && next.absStartMs === curr.absEndMs) {
                curr.absEndMs = next.absEndMs;
            }
            else {
                segments.push(emitRaggedSegment(pyramid, curr, targetBinMs, input.filter ?? {}));
                curr = { ...next };
            }
        }
        segments.push(emitRaggedSegment(pyramid, curr, targetBinMs, input.filter ?? {}));
    }
    const rawWm = effective[pyramid.tiers[0].name];
    const authoritativeEnd = rawWm < to ? rawWm : null;
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
    };
}
function decomposeBin(eligibleTiers, targetBinMs, binStartMs) {
    const binEndMs = binStartMs + targetBinMs;
    const memo = new Map();
    function solve(cursor) {
        if (cursor === binEndMs)
            return [];
        const cached = memo.get(cursor);
        if (cached !== undefined)
            return cached;
        let best = null;
        for (const { tier, ms } of eligibleTiers) {
            if (cursor + ms > binEndMs)
                continue;
            if (cursor % ms !== 0)
                continue;
            const sub = solve(cursor + ms);
            if (sub === null)
                continue;
            const candidate = [
                { tier, offsetMs: cursor - binStartMs, durationMs: ms },
                ...sub,
            ];
            if (best === null || candidate.length < best.length)
                best = candidate;
        }
        memo.set(cursor, best);
        return best;
    }
    return solve(binStartMs);
}
function emitRaggedSegment(pyramid, range, targetBinMs, filter) {
    const fromDate = new Date(range.absStartMs);
    const toDate = new Date(range.absEndMs);
    const tierMs = fixedDurationMs(range.tier.bin);
    const shardDur = range.tier.shards[range.tier.shards.length - 1];
    return {
        from: fromDate,
        to: toDate,
        shardTier: range.tier,
        shardDur,
        keys: shardKeys(pyramid, range.tier, shardDur, fromDate, toDate, filter),
        reaggregate: tierMs !== targetBinMs,
    };
}
function gcd(a, b) {
    while (b !== 0) {
        const t = b;
        b = a % b;
        a = t;
    }
    return a;
}
function lcm(a, b) {
    return (a / gcd(a, b)) * b;
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
// Build the 2D `(tier, shardDur)` watermark grid.
//
// Propagation rules:
//   - WITHIN tier (smallest shardDur → largest):
//     `effective[t, s] = min(declared[t, s], effective[t, prev-smaller-s])`
//     Larger shards are built from smaller via promotion; can't be fresher
//     than the smaller they're built from.
//   - ACROSS tiers (finest → coarsest):
//     `effective[coarser, *] = min(its current eff, max-eff-of-finer-tier)`
//     A coarser tier (built from finer) can't be fresher than its source.
//
// Undeclared `(tier, shardDur)` cells default to FAR_FUTURE — the planner
// treats them as "complete enough". Single-shard ladders without
// watermarks behave as today. Consumers with partial coverage should pass
// real watermarks for every (tier, shardDur) they care about.
//
// NOTE: the grid is NOT clamped to the query's `plannedTo` — the walk
// reads `effective` to check whether a shard's full period is sealed
// (`effective ≥ periodEnd`), then clips emitted segments to `plannedTo`
// separately. Clamping the grid would break the sealed check for any
// query strictly shorter than a shard period.
function effectiveShardWatermarks(pyramid, declared, earliestPerShard) {
    const out = {};
    const FAR_FUTURE = new Date(8.64e15);
    let finerTierMax = FAR_FUTURE; // FAR_FUTURE = no bound for the finest tier
    for (const tier of pyramid.tiers) {
        // shards ascending (by ladder convention). Build entries with
        // within-tier `min` propagation: smaller bounds larger.
        let withinTierBound = FAR_FUTURE;
        const tierEntries = {};
        let tierMax = new Date(0);
        for (let i = 0; i < tier.shards.length; i++) {
            const shardDur = tier.shards[i];
            const key = encodeWatermarkKey(tier.name, shardDur);
            const dec = declared[key] ?? FAR_FUTURE;
            // Within-tier: min(declared, prev-smaller's effective).
            const withinEff = dec.getTime() < withinTierBound.getTime() ? dec : withinTierBound;
            // Cross-tier: min(within-tier, finer-tier-max).
            const crossEff = withinEff.getTime() < finerTierMax.getTime() ? withinEff : finerTierMax;
            const earliestEntry = earliestPerShard[key];
            tierEntries[shardDur] = {
                effective: crossEff,
                ...(earliestEntry !== undefined ? { earliestEntry } : {}),
            };
            if (crossEff.getTime() > tierMax.getTime())
                tierMax = crossEff;
            withinTierBound = crossEff;
        }
        out[tier.name] = tierEntries;
        finerTierMax = tierMax;
    }
    return { byTier: out };
}
// For ragged-decomposition planning: per-tier effective watermark using
// each tier's LARGEST shard duration. Within-tier propagation already
// folded into `effectiveShardWatermarks`; this is just the largest-shard
// projection.
function effectiveLargestShardWatermarks(tiers, declared, rangeTo) {
    const out = {};
    const FAR_FUTURE = new Date(8.64e15);
    let finerBound = FAR_FUTURE;
    for (const tier of tiers) {
        // Within-tier: min over all shardDurs at this tier.
        let withinMin = FAR_FUTURE;
        for (const shardDur of tier.shards) {
            const dec = declared[encodeWatermarkKey(tier.name, shardDur)] ?? FAR_FUTURE;
            if (dec.getTime() < withinMin.getTime())
                withinMin = dec;
        }
        // The largest shard's effective is at most the smallest-shard's
        // declared (within-tier propagation). Then cross-tier: bound by finer.
        const cross = withinMin.getTime() < finerBound.getTime() ? withinMin : finerBound;
        const clamped = cross.getTime() > rangeTo.getTime() ? rangeTo : cross;
        out[tier.name] = clamped;
        finerBound = cross; // un-clamped for cross-tier propagation
    }
    return out;
}
// Per-tier effective earliest: max(declared, finer-tier's effective).
// Walks finest → coarsest. Unspecified tiers have no clamp.
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
function shardKeys(pyramid, tier, shardDur, from, to, filter) {
    const periods = shardPeriodsCovering(from, to, shardDur);
    return periods.map(p => substituteKey(pyramid.keyTemplate, {
        ...filter,
        tier: tier.name,
        shard: shardDur,
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
function resolveSmoothing(spec, outputBin, from, to, _mode) {
    const outSpan = parseDuration(outputBin);
    const visibleBins = binsInRange(from, to, outputBin);
    const maxCount = Math.max(1, Math.floor(visibleBins / 4));
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
    const outputBinMs = fixedDurationMs(outputBin);
    const desiredMs = typeof spec === 'string'
        ? fixedDurationMs(spec)
        : (spec.multiplier ?? DEFAULT_AUTO_MULTIPLIER) * outputBinMs;
    let best = null;
    let bestDist = Infinity;
    for (const c of NICE_WIDTHS) {
        if (c.ms < outputBinMs)
            continue;
        if (c.ms % outputBinMs !== 0)
            continue;
        const count = c.ms / outputBinMs;
        if (count > maxCount)
            continue;
        const dist = Math.abs(c.ms - desiredMs);
        if (dist < bestDist) {
            bestDist = dist;
            best = { label: c.label, count };
        }
    }
    if (best === null)
        return { smoothBin: outputBin, smoothBinCount: 1 };
    return { smoothBin: best.label, smoothBinCount: best.count };
}
function extendForSmoothing(from, to, outputBin, smoothBinCount, mode) {
    const N = smoothBinCount;
    const leadBins = mode === 'centered' ? Math.ceil((N - 1) / 2) : N - 1;
    const tailBins = mode === 'centered' ? Math.floor((N - 1) / 2) : 0;
    const outSpan = parseDuration(outputBin);
    return {
        from: addSpan(from, { count: -leadBins * outSpan.count, unit: outSpan.unit }),
        to: addSpan(to, { count: tailBins * outSpan.count, unit: outSpan.unit }),
    };
}
//# sourceMappingURL=planner.js.map
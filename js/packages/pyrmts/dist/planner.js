// Pure query planner. Given a pyramid + viewport, choose an output tier and
// emit a segmented plan describing which shards to read and where to
// re-aggregate. No I/O.
import { addSpan, binsInRange, fixedDurationMs, floorToSpan, parseDuration, shardPeriodsCovering } from './axis.js';
import { validatePartials } from './partials.js';
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
    // Validate `pyramid.partials` (no-op when unset).
    const validated = validatePartials(pyramid);
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
    // Watermarks clamp to the *extended* window so the buffer can include
    // post-`to` bins for centered smoothing (otherwise the trailing buffer
    // gets silently truncated to the original `to`).
    const grid = effectiveShardWatermarks(pyramid, validated, input.watermarks ?? {}, input.earliestPerCadence ?? {}, plannedTo);
    // Walk from output tier down to finest. Within each tier, walk
    // `(canonical, partials sorted by ascending effective)` — emit a
    // segment per shard up to its effective watermark. Once a tier's
    // shards are exhausted (cursor reaches the tier's max effective),
    // fall through to the next-finer tier.
    //
    // Cursor advances UNCONDITIONALLY to each entry's effective end, even
    // when no segment is emitted (e.g. the tier's earliest exceeds the
    // segment's range). A tier "owns" its watermark range; if its data
    // isn't there, the finer tier picks up *after* the coarser's range,
    // not inside it. This matches the canonical-only behavior the existing
    // tests codify.
    const segments = [];
    let cursor = plannedFrom;
    for (let i = outputIdx; i >= 0; i--) {
        const tier = pyramid.tiers[i];
        const earlyT = earliest[tier.name];
        const entries = grid.byTier[tier.name];
        for (const entry of entries) {
            const segEnd = entry.effective.getTime() < plannedTo.getTime() ? entry.effective : plannedTo;
            // Floors:
            //   - cursor          tier-walk position
            //   - earlyT          per-tier earliest (propagated up-ladder)
            //   - entry.earlyE    per-(tier, cadence) earliest (no propagation)
            let segStart = cursor;
            if (earlyT && earlyT.getTime() > segStart.getTime())
                segStart = earlyT;
            // Per-cadence is a "this specific file isn't there" gate, distinct from
            // the per-tier "tier owns this range" semantics. When it ENTIRELY covers
            // the entry's range, fall through to subsequent entries (and the next
            // coarser tier) — DON'T advance cursor past the gated entry. When it
            // partially gates, clamp segStart forward and advance cursor normally;
            // the pre-gate portion is dropped (same as per-tier earliest today).
            const earlyE = entry.earliestEntry;
            const earlyEFullyGates = earlyE !== undefined && earlyE.getTime() >= segEnd.getTime();
            if (!earlyEFullyGates && earlyE && earlyE.getTime() > segStart.getTime()) {
                segStart = earlyE;
            }
            if (!earlyEFullyGates && segEnd.getTime() > segStart.getTime()) {
                segments.push({
                    from: segStart,
                    to: segEnd,
                    shardTier: tier,
                    shardCadence: entry.cadence,
                    keys: shardKeys(pyramid, tier, entry.cadence, segStart, segEnd, input.filter ?? {}),
                    reaggregate: i !== outputIdx,
                });
            }
            if (!earlyEFullyGates && segEnd.getTime() > cursor.getTime())
                cursor = segEnd;
            if (cursor.getTime() >= plannedTo.getTime())
                break;
        }
        if (cursor.getTime() >= plannedTo.getTime())
            break;
    }
    // The raw-tier "max effective" (across canonical + partials) is the
    // authoritative end — past it, the consumer's hot path takes over.
    const rawEntries = grid.byTier[pyramid.tiers[0].name];
    const rawWm = rawEntries[rawEntries.length - 1].effective;
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
    // Necessary condition: gcd of eligible tier widths divides targetBinMs (by
    // Bezout, integer linear combinations of tier widths span multiples of their
    // gcd). Sufficient for decomposition existence ignoring alignment; the
    // per-bin DP may still fail when strict-equality alignment dead-ends (e.g.
    // tiers={2,3}, target=5: gcd=1 divides 5, but no path through {0,2,3,4} to 5).
    let tierGcd = eligibleTiers[0].ms;
    for (let i = 1; i < eligibleTiers.length; i++) {
        tierGcd = gcd(tierGcd, eligibleTiers[i].ms);
    }
    if (targetBinMs % tierGcd !== 0) {
        throw new Error(`planQuery: no decomposition of targetBin '${targetBin}' from eligible tiers (gcd ${tierGcd} doesn't divide ${targetBinMs})`);
    }
    // Order finest-first for DP iteration determinism; the DP itself doesn't
    // require ordering for correctness (it minimizes path length).
    eligibleTiers.sort((a, b) => a.ms - b.ms);
    // `outputTier` is the stored tier matching `targetBin` exactly, if any.
    // Used downstream (smoothing source-tier, plan metadata). When omitted,
    // the segments mix multiple tiers and consumers must rely on `outputBin`.
    const outputTier = eligibleTiers.find(e => e.ms === targetBinMs)?.tier;
    // Smoothing snaps against `targetBin` (the output's bin width) just as the
    // budget path snaps against `outputTier.bin`.
    const smoothMode = input.smoothMode ?? 'centered';
    const smoothing = input.smoothing !== undefined
        ? resolveSmoothing(input.smoothing, targetBin, from, to, smoothMode)
        : null;
    const { from: plannedFrom, to: plannedTo } = smoothing
        ? extendForSmoothing(from, to, targetBin, smoothing.smoothBinCount, smoothMode)
        : { from, to };
    const effective = effectiveWatermarks(pyramid.tiers, input.watermarks ?? {}, plannedTo);
    const earliest = effectiveEarliestWatermarks(pyramid.tiers, input.earliestWatermarks ?? {});
    // Output bins span [floor(plannedFrom), ceil(plannedTo)] in `targetBin`
    // strides. Each bin contributes one or more sub-bin atoms (per the DP).
    const targetSpan = tParsed;
    const firstBinStart = floorToSpan(plannedFrom, targetSpan);
    // Per-(eligible-set, phase) DP cache. Phase = bin-start ms mod L, where L
    // is LCM of all tier-ms (and targetBin) — different bins with the same
    // phase produce identical decompositions modulo a uniform offset.
    const phaseCacheByKey = new Map();
    const lcmAll = eligibleTiers.reduce((l, { ms }) => lcm(l, ms), targetBinMs);
    const atoms = [];
    let binStart = firstBinStart;
    while (binStart < plannedTo) {
        const binStartMs = binStart.getTime();
        const binEnd = addSpan(binStart, targetSpan);
        const binEndMs = binEnd.getTime();
        // Per-bin eligible tiers: watermark covers binEnd AND earliest ≤ binStart.
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
            // No tier covers this bin — skip (consumer sees a gap row at this bin).
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
    // Coalesce adjacent same-tier atoms into segments.
    const segments = [];
    if (atoms.length > 0) {
        let curr = { ...atoms[0] };
        for (let i = 1; i < atoms.length; i++) {
            const next = atoms[i];
            if (next.tier === curr.tier && next.absStartMs === curr.absEndMs) {
                curr.absEndMs = next.absEndMs;
            }
            else {
                segments.push(emitSegment(pyramid, curr, targetBinMs, input.filter ?? {}));
                curr = { ...next };
            }
        }
        segments.push(emitSegment(pyramid, curr, targetBinMs, input.filter ?? {}));
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
// DP: find the minimum-item set of tier atoms tiling [binStartMs, binEndMs).
// Each atom is one full bin of some eligible tier, with `cursor % tier.ms === 0`
// in absolute (epoch) ms — the strict-equality alignment rule. Returns null
// when no decomposition exists (alignment dead-ends with no finer tier to
// fall back to). Memoized on the absolute cursor; runs O(B/g × n_tiers) where
// B = bin width, g = gcd of eligible tier ms.
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
function emitSegment(pyramid, range, targetBinMs, filter) {
    const fromDate = new Date(range.absStartMs);
    const toDate = new Date(range.absEndMs);
    const tierMs = fixedDurationMs(range.tier.bin);
    return {
        from: fromDate,
        to: toDate,
        shardTier: range.tier,
        shardCadence: null,
        keys: shardKeys(pyramid, range.tier, null, fromDate, toDate, filter),
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
// Build the 2D `(tier, cadence)` watermark grid.
//
// Propagation rules (both `min`-style — coarser is bounded by finer's
// effective so the planner doesn't trust data the finer path hasn't
// surfaced):
//   - WITHIN tier (finest cadence → coarsest, ending at canonical):
//     `effective[t, s] = min(declared[t, s], effective[t, next-finer-s])`
//   - ACROSS tiers (finest → coarsest):
//     `effective[coarser, *] = min(its current eff, max-eff-of-finer-tier)`
//     i.e. the freshest data available in the finer tier (= its finest
//     cadence's effective) bounds every shard of the coarser tier.
//
// Keys in `declared` follow the `ShardIndex` encoded convention —
// `${tier}` for canonical, `${tier}@${cadence}` for partials. Tier-only
// keys (today's convention) are still valid for canonical-only
// pyramids; no migration required.
function effectiveShardWatermarks(pyramid, validated, declared, earliestPerCadence, rangeTo) {
    const out = {};
    const FAR_FUTURE = new Date(8.64e15);
    let finerTierMax = FAR_FUTURE; // bounds coarser tiers; FAR_FUTURE = no bound for finest
    for (const tier of pyramid.tiers) {
        // Raw declared values for this tier's shards. Order: canonical, then
        // partials sorted COARSEST → FINEST (largest cadence first). Finest is
        // last because within-tier propagation walks finest→coarsest (reversed).
        //
        // Default semantics differ for canonical vs partial:
        //   - canonical missing → FAR_FUTURE ("complete enough"; matches the
        //     pre-partial-shards behavior consumers depend on).
        //   - partial missing → SKIPPED from the grid entirely. A declared-
        //     in-config cadence that hasn't been sealed yet shouldn't poison
        //     within-tier propagation (else the absent partial would drag
        //     canonical's effective down to epoch 0).
        const cadencesAsc = validated?.perTier[tier.name] ?? []; // already ascending ms
        const cadencesByCoarsestFirst = [...cadencesAsc].reverse();
        const decls = [];
        decls.push({
            cadence: null,
            declared: declared[encodeWatermarkKey(tier.name, null)] ?? FAR_FUTURE,
        });
        for (const c of cadencesByCoarsestFirst) {
            const dec = declared[encodeWatermarkKey(tier.name, c)];
            if (dec === undefined)
                continue;
            decls.push({ cadence: c, declared: dec });
        }
        // Within-tier `min` propagation, finest → coarsest. Reversed `decls`
        // has [finest_cadence, ..., coarsest_cadence, canonical].
        const effByCadence = new Map();
        let withinTierBound = FAR_FUTURE;
        for (let i = decls.length - 1; i >= 0; i--) {
            const { cadence, declared: d } = decls[i];
            const eff = d.getTime() < withinTierBound.getTime() ? d : withinTierBound;
            effByCadence.set(cadence, eff);
            withinTierBound = eff;
        }
        // Cross-tier bound + clamp to rangeTo. Track this tier's max for the
        // next-coarser tier's pass. Per-entry `earliestEntry` rides alongside —
        // pure projection of `earliestPerCadence` keyed by encoded (tier, cadence).
        // It does NOT participate in `min` propagation: it's a statement about
        // THIS specific shard's file availability, not about underlying data
        // shape the tier could synthesize.
        const entries = [];
        let tierMax = new Date(0);
        for (const { cadence } of decls) {
            const e = effByCadence.get(cadence);
            const cross = e.getTime() < finerTierMax.getTime() ? e : finerTierMax;
            const clampedMs = Math.min(cross.getTime(), rangeTo.getTime());
            const final = new Date(clampedMs);
            const earliestEntry = earliestPerCadence[encodeWatermarkKey(tier.name, cadence)];
            entries.push({
                cadence,
                effective: final,
                ...(earliestEntry !== undefined ? { earliestEntry } : {}),
            });
            if (clampedMs > tierMax.getTime())
                tierMax = new Date(clampedMs);
        }
        // Walk order: ascending effective. Ties → coarser shard first
        // (canonical > largest cadence > … > smallest cadence) so canonical
        // wins when its watermark equals a partial's.
        entries.sort((a, b) => {
            const dt = a.effective.getTime() - b.effective.getTime();
            if (dt !== 0)
                return dt;
            const aMs = a.cadence === null ? Number.POSITIVE_INFINITY : fixedDurationMs(a.cadence);
            const bMs = b.cadence === null ? Number.POSITIVE_INFINITY : fixedDurationMs(b.cadence);
            return bMs - aMs;
        });
        out[tier.name] = entries;
        finerTierMax = tierMax;
    }
    return { byTier: out };
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
function shardKeys(pyramid, tier, cadence, from, to, filter) {
    // Partials use the cadence as the shard span; canonical uses tier.shard.
    // Template selection mirrors: `partialKey` adds a `{shard}` placeholder
    // (the cadence label, e.g. `1h`) on top of `keyTemplate`'s placeholders.
    if (cadence !== null) {
        if (pyramid.partialKey === undefined) {
            throw new Error(`shardKeys: pyramid.partialKey is required when emitting partial-shard segments`);
        }
        const periods = shardPeriodsCovering(from, to, cadence);
        return periods.map(p => substituteKey(pyramid.partialKey, {
            ...filter,
            tier: tier.name,
            shard: cadence,
            period: p.label,
        }));
    }
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
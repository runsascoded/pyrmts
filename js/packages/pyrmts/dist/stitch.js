// Stitcher. Merges per-segment shard rows into the output bin shape, applying
// monoid combines when a segment is at a finer-than-output granularity. When
// `plan.smoothing` is set, runs a rolling-window combine pass after stitching
// to produce `<metric>_smooth_<suffix>` columns, then trims to the visible
// range.
//
// Input rows must contain the bin column (`pyramid.binCol`, an int64 UTC ms
// timestamp for time-axis pyramids), dim columns, and metric state columns
// (per `stateColumns(monoid, name)`). Extra columns are copied from the
// first row that creates a given output entry; collisions are silently
// dropped — keep shard schemas tight.
import { floorToSpan, parseDuration } from './axis.js';
import { getMonoid } from './monoids.js';
export function stitch(input) {
    const { pyramid, plan, shardRows } = input;
    if (shardRows.length !== plan.segments.length) {
        throw new Error(`stitch: shardRows length ${shardRows.length} ≠ segments ${plan.segments.length}`);
    }
    const outputSpan = parseDuration(plan.outputBin);
    const { binCol } = pyramid;
    const dimNames = pyramid.dims.map(d => d.name);
    // Aggregation map: `${outBinStartMs}\x00${dimValues...}` → Row.
    const agg = new Map();
    for (let i = 0; i < plan.segments.length; i++) {
        const seg = plan.segments[i];
        const rows = shardRows[i];
        const fromMs = seg.from.getTime();
        const toMs = seg.to.getTime();
        for (const row of rows) {
            const ts = row[binCol];
            if (typeof ts !== 'number') {
                throw new Error(`stitch: row missing numeric '${binCol}' column (got ${typeof ts})`);
            }
            if (ts < fromMs || ts >= toMs)
                continue;
            const outBinStart = floorToSpan(new Date(ts), outputSpan).getTime();
            const aggKey = aggKeyFor(outBinStart, dimNames, row);
            const existing = agg.get(aggKey);
            if (existing === undefined) {
                const fresh = { ...row, [binCol]: outBinStart };
                // Give each monoid a chance to normalize its state in the fresh row
                // (e.g. histogram parses JSON strings + detaches from the source).
                for (const metric of pyramid.metrics) {
                    getMonoid(metric.monoid).init?.(fresh, metric.name);
                }
                agg.set(aggKey, fresh);
            }
            else {
                for (const metric of pyramid.metrics) {
                    getMonoid(metric.monoid).combine(existing, row, metric.name);
                }
            }
        }
    }
    const sorted = sortRows([...agg.values()], binCol, dimNames);
    if (plan.smoothing) {
        smoothInPlace(sorted, pyramid, plan);
    }
    // Trim to the caller's visible range (buffer rows from smoothing extension
    // get dropped here). For non-smoothed plans visibleRange === input range,
    // so this is a no-op (segments don't extend past the range).
    const visFrom = plan.visibleRange.from.getTime();
    const visTo = plan.visibleRange.to.getTime();
    return sorted.filter(r => {
        const ts = r[binCol];
        return ts >= visFrom && ts < visTo;
    });
}
// Rolling-window combine pass. For each output row, combines the surrounding
// N rows of its own dim slice using each metric's monoid, writing the result
// into `<metric>_smooth_<suffix>` columns. O(rows × N) per metric — see
// `specs/done/server-side-smoothing.md` §4 for the optimization roadmap.
function smoothInPlace(rows, pyramid, plan) {
    const s = plan.smoothing;
    const N = s.smoothBinCount;
    if (N <= 1) {
        // 1-bin window = no-op; just mirror the raw state into the smooth cols
        // so consumers can always read `_smooth_*` without conditional plumbing.
        for (const row of rows) {
            for (const metric of pyramid.metrics) {
                const monoid = getMonoid(metric.monoid);
                for (const suffix of monoid.stateSuffixes) {
                    row[`${metric.name}_smooth${suffix}`] = row[`${metric.name}${suffix}`];
                }
            }
        }
        return;
    }
    const lead = s.smoothMode === 'centered' ? Math.ceil((N - 1) / 2) : N - 1;
    const tail = s.smoothMode === 'centered' ? Math.floor((N - 1) / 2) : 0;
    const dimNames = pyramid.dims.map(d => d.name);
    // Group rows by dim slice; rows within each slice remain in time order
    // (sortRows above sorted by binStart first, then dims).
    const slices = new Map();
    for (const row of rows) {
        let key = '';
        for (const name of dimNames) {
            key += '\x00';
            key += String(row[name]);
        }
        let arr = slices.get(key);
        if (!arr) {
            arr = [];
            slices.set(key, arr);
        }
        arr.push(row);
    }
    for (const slice of slices.values()) {
        for (let i = 0; i < slice.length; i++) {
            const startIdx = Math.max(0, i - lead);
            const endIdx = Math.min(slice.length - 1, i + tail);
            const acc = {};
            for (let j = startIdx; j <= endIdx; j++) {
                for (const metric of pyramid.metrics) {
                    getMonoid(metric.monoid).combine(acc, slice[j], metric.name);
                }
            }
            const row = slice[i];
            for (const metric of pyramid.metrics) {
                const monoid = getMonoid(metric.monoid);
                for (const suffix of monoid.stateSuffixes) {
                    row[`${metric.name}_smooth${suffix}`] = acc[`${metric.name}${suffix}`];
                }
            }
        }
    }
}
function aggKeyFor(binStart, dimNames, row) {
    // \x00 is a safe separator — unlikely to appear in real dim values.
    let key = String(binStart);
    for (const name of dimNames) {
        key += '\x00';
        key += String(row[name]);
    }
    return key;
}
function sortRows(rows, binCol, dimNames) {
    rows.sort((a, b) => {
        const ta = a[binCol];
        const tb = b[binCol];
        if (ta !== tb)
            return ta - tb;
        for (const name of dimNames) {
            const va = String(a[name]);
            const vb = String(b[name]);
            if (va < vb)
                return -1;
            if (va > vb)
                return 1;
        }
        return 0;
    });
    return rows;
}
//# sourceMappingURL=stitch.js.map
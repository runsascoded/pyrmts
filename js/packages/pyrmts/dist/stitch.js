// Stitcher. Merges per-segment shard rows into the output bin shape, applying
// monoid combines when a segment is at a finer-than-output granularity.
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
    return sortRows([...agg.values()], binCol, dimNames);
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
// Adapter helpers for bridging non-pyrmts shard formats into the canonical
// stitch-input shape.
//
// `pivotTallToHistogram` collapses tall-format rows (one row per category
// per group) into wide-format rows with a `{category: count}` histogram
// column. Useful for consumers whose existing build pipeline emits tall
// shards (ctbk's `avail_agg.py`) but who want to read through pyrmts's
// histogram-monoid stitcher without rewriting the builder first.
// Pivot tall-format rows into wide rows with a histogram column. Group-key
// columns are preserved in the output; the category and count columns are
// consumed (removed); the histogramCol is added.
//
// Order of input rows within a group is irrelevant. Duplicate categories
// within a group are summed.
export function pivotTallToHistogram(rows, opts) {
    const { histogramCol, categoryCol, countCol, groupBy } = opts;
    const groups = new Map();
    for (const row of rows) {
        const key = groupBy.map(c => String(row[c])).join('\x00');
        const category = String(row[categoryCol]);
        const count = row[countCol] ?? 0;
        let out = groups.get(key);
        if (out === undefined) {
            out = {};
            for (const c of groupBy)
                out[c] = row[c];
            out[histogramCol] = {};
            groups.set(key, out);
        }
        const hist = out[histogramCol];
        hist[category] = (hist[category] ?? 0) + count;
    }
    return [...groups.values()];
}
//# sourceMappingURL=pivot.js.map
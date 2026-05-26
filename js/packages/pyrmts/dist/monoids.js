// Monoid catalog. Each monoid defines (a) the column-suffix layout for how
// its state is stored alongside a metric, and (b) an associative+commutative
// combine that merges two states.
//
// Storage convention: for metric named `foo` with monoid M (suffixes ['_a',
// '_b']), the parquet shard has columns `foo_a` and `foo_b`. Stitcher reads
// both, combines element-wise.
//
// v0.1 ships `sum` and `count`. `histogram`/`topk`/`hll`/`tdigest` follow
// the same shape but aren't implemented yet.
const SUM_SUFFIXES = ['_n', '_sum', '_sumsq'];
const sum = {
    stateSuffixes: SUM_SUFFIXES,
    combine(target, source, name) {
        for (const suffix of SUM_SUFFIXES) {
            const col = `${name}${suffix}`;
            const t = target[col] ?? 0;
            const s = source[col] ?? 0;
            target[col] = t + s;
        }
    },
};
const count = {
    stateSuffixes: [''],
    combine(target, source, name) {
        const t = target[name] ?? 0;
        const s = source[name] ?? 0;
        target[name] = t + s;
    },
};
// Histogram: state is a `{ category: count }` map. Stored as one column
// (JSON parquet logical type recommended — hyparquet round-trips JS objects
// transparently). Combine merges maps, summing overlapping keys.
//
// Keys are coerced to strings (JS object keys are strings anyway, and JSON
// can't represent non-string keys). Consumers with int-valued categories
// (e.g. ctbk avail's `num_bikes_available`) should stringify when building.
const histogram = {
    stateSuffixes: [''],
    init(target, name) {
        target[name] = parseHist(target[name]);
    },
    combine(target, source, name) {
        const t = parseHist(target[name]);
        const s = parseHist(source[name]);
        for (const k in s) {
            t[k] = (t[k] ?? 0) + s[k];
        }
        target[name] = t;
    },
};
function parseHist(v) {
    if (v === null || v === undefined)
        return {};
    if (typeof v === 'string')
        return JSON.parse(v);
    if (typeof v === 'object')
        return { ...v };
    throw new Error(`histogram: unexpected value type ${typeof v} (got ${String(v)})`);
}
const MONOIDS = {
    sum,
    count,
    histogram,
};
export function getMonoid(name) {
    const m = MONOIDS[name];
    if (!m)
        throw new Error(`Monoid '${name}' not yet implemented`);
    return m;
}
// Column names a metric occupies in a shard, given its monoid.
export function stateColumns(monoid, metricName) {
    return getMonoid(monoid).stateSuffixes.map(s => `${metricName}${s}`);
}
//# sourceMappingURL=monoids.js.map
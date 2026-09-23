// Index-free diff walk — the TS twin of `pyrmts_engine.bench_diff` and the
// deployable engine for a diff treemap between ANY two scans. See
// ../../../../specs/multi-scan-consolidation.md ("Bake-off").
//
// Two snapshots, each a `(depth, path)`-sorted path-index parquet (dir rows
// carry rolled-up size/count), read over `Storage`. Expanding a directory =
// listing its children on both sides (row groups located by bisection over the
// footer's `(depth, path)` min/max, read once per request via the RG cache),
// merge-joining by name, and queueing the changed subdirs. A dir whose size on
// both sides and |Δ| are all below the render `floor` (bytes) is never
// expanded — that is the bound; `budget` is a backstop.
//
// `order: 'level'` (default) expands every pending dir of the shallowest level
// in one round, all listings in flight together (a child can only be listed
// after its parent's listing returned; siblings are independent), so the
// dependent round trips equal the depth of the expanded tree. Every range
// request hyparquet issues is counted (`stats.gets`, `stats.bytes`) and timed
// per round (`stats.roundMs`), so a run over a real network measures itself.
import { parquetMetadataAsync, parquetReadObjects } from 'hyparquet';
import { EtagConflict } from './types.js';
/** pyrmts / cw path indexes: `b` bytes, `o` objects. disk-tree: `size`, `n_desc`. */
export const DEFAULT_WALK_COLS = { path: 'path', depth: 'depth', size: 'b', count: 'o' };
export function newWalkStats() {
    return {
        requests: 0, gets: 0, footerGets: 0, bytes: 0, rgDecodes: 0, rgCacheHits: 0, footerParses: 0,
        listings: 0, expansions: 0, rounds: [], roundMs: [],
        ms: { footer: 0, locate: 0, fetch: 0, decode: 0, post: 0 },
    };
}
/** CPU-ish ms: everything except network wait. */
export function cpuMs(s) {
    return s.ms.footer + s.ms.locate + s.ms.decode + s.ms.post;
}
/** Rounds that issued at least one GET. */
export function roundTrips(s) {
    return s.rounds.filter(n => n > 0).length;
}
/** Modelled wall: CPU + ⌈GETs / parallel⌉ trips per dependent round × RTT. */
export function wallModel(s, rttMs, parallel = 8) {
    const trips = s.rounds.reduce((acc, n) => acc + Math.ceil(n / Math.max(1, parallel)), 0);
    return cpuMs(s) + trips * rttMs;
}
/** The per-group summaries from a parsed footer — the producer side of
 * `RowGroupIndex.groups`. Requires `(depth, path)` statistics. */
export function rowGroupSummaries(metadata, cols = DEFAULT_WALK_COLS) {
    const first = metadata.row_groups[0];
    if (!first)
        return [];
    const idx = (name) => {
        const i = first.columns.findIndex(c => c.meta_data?.path_in_schema.join('.') === name);
        if (i < 0)
            throw new Error(`rowGroupSummaries: column '${name}' not in the file`);
        return i;
    };
    const di = idx(cols.depth);
    const pi = idx(cols.path);
    const out = [];
    let cursor = 0;
    for (const rg of metadata.row_groups) {
        const ds = rg.columns[di]?.meta_data?.statistics;
        const ps = rg.columns[pi]?.meta_data?.statistics;
        if (!ds || !ps || ds.min_value === undefined || ps.min_value === undefined) {
            throw new Error('rowGroupSummaries: the file lacks (depth, path) row-group statistics');
        }
        const numRows = Number(rg.num_rows);
        out.push({
            rowStart: cursor, numRows,
            depthMin: num(ds.min_value), depthMax: num(ds.max_value),
            pathMin: str(ps.min_value), pathMax: str(ps.max_value),
        });
        cursor += numRows;
    }
    return out;
}
/** A `RowGroupIndex` over an already-parsed footer (in-memory `rowGroup`),
 * e.g. to build what a producer persists. */
export function rowGroupIndexFromMetadata(metadata, size, etag, cols = DEFAULT_WALK_COLS) {
    return {
        size,
        ...(etag !== undefined ? { etag } : {}),
        schema: metadata.schema,
        groups: rowGroupSummaries(metadata, cols),
        rowGroup: (i) => metadata.row_groups[i],
    };
}
function num(v) {
    if (typeof v === 'bigint')
        return Number(v);
    if (typeof v === 'number')
        return v;
    return 0;
}
function str(v) {
    if (typeof v === 'string')
        return v;
    if (v instanceof Uint8Array)
        return new TextDecoder().decode(v);
    return String(v);
}
function cmpKey(a, b) {
    if (a[0] !== b[0])
        return a[0] - b[0];
    return a[1] < b[1] ? -1 : a[1] > b[1] ? 1 : 0;
}
/** First index `i` in sorted `arr` with `arr[i] >= key`. */
function bisectLeft(arr, key) {
    let lo = 0;
    let hi = arr.length;
    while (lo < hi) {
        const mid = (lo + hi) >> 1;
        if (cmpKey(arr[mid], key) < 0)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo;
}
/** Per-directory children reader over one snapshot parquet. */
export class SnapshotReader {
    storage;
    key;
    cols;
    stats;
    schema = [];
    size = 0;
    etag;
    index;
    groupMeta = new Map();
    rgLo = [];
    rgHi = [];
    rgRowStart = [];
    rgRows = [];
    rgCache;
    metadataCache;
    initialFetchSize;
    opened = false;
    constructor(storage, key, opts = {}) {
        this.storage = storage;
        this.key = key;
        this.cols = opts.cols ?? DEFAULT_WALK_COLS;
        this.stats = opts.stats ?? newWalkStats();
        this.rgCache = opts.rgCache === false ? null : new Map();
        this.metadataCache = opts.metadataCache;
        this.initialFetchSize = opts.initialFetchSize ?? 64 * 1024;
        this.index = opts.rowGroups;
    }
    /** Build the RG key ranges from the pre-supplied index, the cached footer,
     * or a footer read. */
    async open() {
        if (this.opened)
            return this;
        const t0 = performance.now();
        let summaries;
        if (this.index !== undefined) {
            this.size = this.index.size;
            this.etag = this.index.etag;
            this.schema = this.index.schema;
            summaries = this.index.groups;
        }
        else {
            let metadata;
            const cached = this.metadataCache?.get(this.key);
            if (cached !== undefined) {
                metadata = cached.metadata;
                this.size = cached.size;
                this.etag = cached.etag;
            }
            else {
                const head = await this.storage.head(this.key);
                if (head === null)
                    throw new Error(`SnapshotReader: object not found: ${this.key}`);
                this.size = head.size;
                metadata = await parquetMetadataAsync(this.file(false), { initialFetchSize: this.initialFetchSize });
                this.stats.footerParses++;
                if (head.etag !== undefined) {
                    this.etag = head.etag;
                    this.metadataCache?.set(this.key, { etag: head.etag, size: head.size, metadata });
                }
            }
            this.schema = metadata.schema;
            summaries = rowGroupSummaries(metadata, this.cols);
            metadata.row_groups.forEach((rg, i) => this.groupMeta.set(i, Promise.resolve(rg)));
        }
        if (summaries.length === 0)
            throw new Error(`SnapshotReader: ${this.key} has no row groups`);
        for (const g of summaries) {
            this.rgLo.push([g.depthMin, g.pathMin]);
            this.rgHi.push([g.depthMax, g.pathMax]);
            this.rgRowStart.push(g.rowStart);
            this.rgRows.push(g.numRows);
        }
        this.stats.ms.footer += performance.now() - t0;
        this.opened = true;
        return this;
    }
    /** Per-group metadata: from the parsed footer, or `rowGroups.rowGroup(i)` (memoized). */
    rowGroupMeta(i) {
        let p = this.groupMeta.get(i);
        if (p === undefined) {
            if (this.index === undefined)
                throw new Error(`SnapshotReader: no metadata for row group ${i}`);
            p = Promise.resolve(this.index.rowGroup(i));
            this.groupMeta.set(i, p);
        }
        return p;
    }
    // An AsyncBuffer over Storage for the footer read (cold open only).
    file(guard) {
        const { storage, key, size, stats } = this;
        const opts = guard && this.etag !== undefined ? { ifMatch: this.etag } : undefined;
        return {
            byteLength: size,
            async slice(start, end) {
                const e = end ?? size;
                if (guard)
                    stats.gets++;
                else
                    stats.footerGets++;
                stats.bytes += e - start;
                const bytes = await storage.getRange(key, start, e, opts);
                return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
            },
        };
    }
    /** Row groups whose key range intersects `[(depth, lo), (depth, hi))`, as `[first, last)`. */
    locate(depth, lo, hi) {
        const first = bisectLeft(this.rgHi, [depth, lo]);
        const last = bisectLeft(this.rgLo, [depth, hi]);
        return [first, Math.max(first, last)];
    }
    // Byte span of a row group (all column chunks; dictionary pages first).
    static span(rg) {
        let lo = Infinity;
        let hi = 0;
        for (const c of rg.columns) {
            const m = c.meta_data;
            const start = Number(m.dictionary_page_offset ?? m.data_page_offset);
            lo = Math.min(lo, start);
            hi = Math.max(hi, start + Number(m.total_compressed_size));
        }
        return [lo, hi];
    }
    // Read row groups `[i, j)`: ONE range GET for the run's byte span (with
    // If-Match when the etag is known), then decode only the four walk columns
    // from memory through a synthetic footer holding just this run's groups.
    // Returns one row array per RG.
    async readRun(i, j) {
        const c = this.cols;
        const groups = await Promise.all(Array.from({ length: j - i }, (_, k) => this.rowGroupMeta(i + k)));
        const [lo] = SnapshotReader.span(groups[0]);
        const [, hi] = SnapshotReader.span(groups[groups.length - 1]);
        const t0 = performance.now();
        this.stats.gets++;
        this.stats.bytes += hi - lo;
        const opts = this.etag !== undefined ? { ifMatch: this.etag } : undefined;
        const buf = await this.storage.getRange(this.key, lo, hi, opts);
        const t1 = performance.now();
        const ab = buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
        const file = {
            byteLength: this.size,
            slice: (start, end) => {
                const e = end ?? this.size;
                if (start < lo || e > hi)
                    throw new Error(`SnapshotReader: slice [${start}, ${e}) outside fetched run [${lo}, ${hi})`);
                return ab.slice(start - lo, e - lo);
            },
        };
        const numRows = groups.reduce((n, g) => n + Number(g.num_rows), 0);
        const metadata = {
            version: 2, schema: this.schema, num_rows: BigInt(numRows), row_groups: groups, metadata_length: 0,
        };
        const rows = await parquetReadObjects({
            file, metadata, rowStart: 0, rowEnd: numRows, columns: [c.path, c.depth, c.size, c.count],
        });
        const t2 = performance.now();
        this.stats.ms.fetch += t1 - t0;
        this.stats.ms.decode += t2 - t1;
        const out = [];
        let off = 0;
        for (let k = i; k < j; k++) {
            const n = this.rgRows[k];
            out.push(rows.slice(off, off + n));
            off += n;
            this.stats.requests++;
            this.stats.rgDecodes++;
        }
        return out;
    }
    async readRgs(first, last) {
        const pending = [];
        let i = first;
        while (i < last) {
            const cached = this.rgCache?.get(i);
            if (cached !== undefined) {
                this.stats.rgCacheHits++;
                pending.push(cached);
                i++;
                continue;
            }
            // Contiguous uncached run → one GET; register every RG's promise before
            // awaiting so concurrent listings of the same row groups share it.
            let j = i + 1;
            while (j < last && !this.rgCache?.has(j))
                j++;
            const base = i;
            const run = this.readRun(base, j);
            for (let k = base; k < j; k++) {
                const p = run.then(parts => parts[k - base]);
                this.rgCache?.set(k, p);
                pending.push(p);
            }
            i = j;
        }
        return Promise.all(pending);
    }
    async list(depth, lo, hi) {
        await this.open();
        const c = this.cols;
        const t0 = performance.now();
        const [first, last] = this.locate(depth, lo, hi);
        const t1 = performance.now();
        const groups = await this.readRgs(first, last);
        const t2 = performance.now();
        const out = new Map();
        for (const rows of groups) {
            for (const r of rows) {
                if (num(r[c.depth]) !== depth)
                    continue;
                const p = str(r[c.path]);
                if (p < lo || p >= hi)
                    continue;
                out.set(p, { size: num(r[c.size]), count: num(r[c.count]) });
            }
        }
        const t3 = performance.now();
        this.stats.ms.locate += t1 - t0;
        this.stats.ms.post += t3 - t2;
        return out;
    }
    /** `{size, count}` of one node, or null if absent. */
    async node(path) {
        const depth = path === '' || path === '.' ? 0 : path.split('/').length;
        const l = await this.list(depth, path, path + '\0');
        return l.get(path) ?? null;
    }
    /** Direct children of `prefix` (a node at `depth`). */
    async children(prefix, depth) {
        this.stats.listings++;
        const lo = prefix ? `${prefix}/` : '';
        const hi = prefix ? `${prefix}0` : '\x7f';
        return this.list(depth + 1, lo, hi);
    }
}
/** Smallest subtree (bytes) that can be drawn: a node's area ≈ its share of
 * the page root × the canvas, so `rootSize × cellPx² / (width × height)`. */
export function renderFloor(rootSize, widthPx, heightPx, minCellPx = 4) {
    return Math.floor(rootSize * (minCellPx * minCellPx) / Math.max(1, widthPx * heightPx));
}
async function mapPool(items, parallel, fn) {
    const out = new Array(items.length);
    let next = 0;
    const workers = Array.from({ length: Math.max(1, Math.min(parallel, items.length)) }, async () => {
        while (next < items.length) {
            const i = next++;
            out[i] = await fn(items[i]);
        }
    });
    await Promise.all(workers);
    return out;
}
/** Pruned recursive diff between two snapshots under `pagePath`. */
export async function walkDiff(ra, rb, pagePath = '', opts = {}) {
    const floor = opts.floor ?? 0;
    const budget = opts.budget ?? 10_000;
    const order = opts.order ?? 'level';
    const parallel = opts.parallel ?? 8;
    const stats = ra.stats;
    const getsOf = () => stats.gets + (rb.stats === stats ? 0 : rb.stats.gets);
    const pageDepth = pagePath === '' || pagePath === '.' ? 0 : pagePath.split('/').length;
    const rows = [];
    const byPath = new Map();
    let queue = [{ prio: 0, depth: pageDepth, seq: 0, path: pagePath }];
    let seq = 0;
    let expansions = 0;
    let truncated = false;
    stats.rounds = [];
    stats.roundMs = [];
    await Promise.all([ra.open(), rb.open()]);
    while (queue.length) {
        if (expansions >= budget) {
            truncated = true;
            break;
        }
        let batch;
        if (order === 'level') {
            const level = Math.min(...queue.map(q => q.depth));
            const atLevel = queue.filter(q => q.depth === level);
            batch = atLevel.slice(0, budget - expansions);
            queue = queue.filter(q => q.depth !== level).concat(atLevel.slice(batch.length));
        }
        else {
            queue.sort((x, y) => x.prio - y.prio || x.seq - y.seq);
            batch = [queue.shift()];
        }
        const gets0 = getsOf();
        const t0 = performance.now();
        const listings = await mapPool(batch, parallel, async (q) => {
            const [ca, cb] = await Promise.all([ra.children(q.path, q.depth), rb.children(q.path, q.depth)]);
            return { q, ca, cb };
        });
        stats.roundMs.push(performance.now() - t0);
        stats.rounds.push(getsOf() - gets0);
        for (const { q, ca, cb } of listings) {
            expansions++;
            stats.expansions++;
            const self = byPath.get(q.path);
            if (self)
                self.expanded = true;
            const names = [...new Set([...ca.keys(), ...cb.keys()])].sort();
            for (const name of names) {
                const a = ca.get(name);
                const b = cb.get(name);
                const sa = a?.size ?? 0, na = a?.count ?? 0;
                const sb = b?.size ?? 0, nb = b?.count ?? 0;
                let status;
                if (!a)
                    status = 'added';
                else if (!b)
                    status = 'removed';
                else if (sa !== sb || na !== nb)
                    status = 'changed';
                else
                    status = 'unchanged';
                if (status === 'unchanged')
                    continue;
                const row = {
                    path: name, depth: q.depth + 1 - pageDepth, status,
                    sizeA: sa, sizeB: sb, countA: na, countB: nb, expanded: false, pruned: false,
                };
                byPath.set(name, row);
                rows.push(row);
                if (status === 'changed') {
                    // Only a changed node present on both sides can hide change below
                    // it; added/removed rows already tell the whole story.
                    if (Math.max(sa, sb) < floor && Math.abs(sb - sa) < floor) {
                        row.pruned = true;
                    }
                    else {
                        // No `kind` column in general (a prefix with one object and the
                        // object itself both carry count 1): a leaf shows as an empty listing.
                        seq++;
                        queue.push({ prio: -Math.abs(sb - sa), depth: q.depth + 1, seq, path: name });
                    }
                }
            }
        }
    }
    for (const q of queue) {
        const r = byPath.get(q.path);
        if (r) {
            r.pruned = true;
            truncated = true;
        }
    }
    rows.sort((x, y) => Math.abs(y.sizeB - y.sizeA) - Math.abs(x.sizeB - x.sizeA) || (x.path < y.path ? -1 : 1));
    return { rows, expansions, truncated };
}
export { EtagConflict };
//# sourceMappingURL=walkdiff.js.map
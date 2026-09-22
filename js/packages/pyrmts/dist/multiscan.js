// Multi-scan read primitives — the TS twin of `pyrmts.multiscan`'s read path
// (Python `diff_scans` / `series_for` / `extract_table`), for serving the
// observation-axis plots (diff / dTM and "size over time") off a consolidated
// multi-scan shard. See ../../../../specs/multi-scan-consolidation.md.
//
// A multi-scan shard folds a re-observation (scan) axis into one tile. The
// `interval` encoder stores one row per maximal run of consecutive scans in
// which a key is present with constant state (SCD-2), with `__scan_lo` /
// `__scan_hi` bounds; `densify` stores the full key×scan grid with a `__scan`
// column. Both round-trip. These functions are pure over an in-memory
// `MultiScan`; `readMultiScan` parses one from parquet bytes (self-describing
// via the `pyrmts.multiscan` KV-metadata the Python writer attaches).
//
// State/identity must match Python exactly: additive monoids (sum/count) fill
// an absent cell with 0, histogram with null; a real row never equals the
// all-identity tuple, so "absent" and "present" never collide.
import { parquetMetadata, parquetReadObjects } from 'hyparquet';
import { stateColumns } from './monoids.js';
export const MULTISCAN_META_KEY = 'pyrmts.multiscan';
export const SCAN_COL = '__scan';
export const SCAN_LO = '__scan_lo';
export const SCAN_HI = '__scan_hi';
export function keyStateCols(schema) {
    const keyCols = [schema.binCol, ...schema.dims.map(d => d.name)];
    const stateCols = schema.metrics.flatMap(m => stateColumns(m.monoid, m.name));
    return { keyCols, stateCols };
}
/** The monoid identity per state column — the fill for an absent cell. Mirrors
 * Python `_identities`: additive (sum/count) → 0, histogram → null. */
export function identities(schema) {
    const ids = {};
    for (const m of schema.metrics) {
        const additive = m.monoid === 'sum' || m.monoid === 'count';
        for (const c of stateColumns(m.monoid, m.name))
            ids[c] = additive ? 0 : null;
    }
    return ids;
}
function idTuple(schema, stateCols) {
    const ids = identities(schema);
    return stateCols.map(c => ids[c]);
}
function keyStr(row, keyCols) {
    return JSON.stringify(keyCols.map(c => row[c] ?? null));
}
function stateOf(row, stateCols) {
    return stateCols.map(c => row[c] ?? null);
}
function stateEq(a, b) {
    if (a.length !== b.length)
        return false;
    for (let i = 0; i < a.length; i++)
        if (!Object.is(a[i], b[i]))
            return false;
    return true;
}
function cmpVal(a, b) {
    if (a === b)
        return 0;
    if (a === null || a === undefined)
        return -1;
    if (b === null || b === undefined)
        return 1;
    return a < b ? -1 : a > b ? 1 : 0;
}
function sortByKey(rows, schema) {
    const order = [...schema.dims.map(d => d.name), schema.binCol];
    return [...rows].sort((r1, r2) => {
        for (const c of order) {
            const d = cmpVal(r1[c], r2[c]);
            if (d !== 0)
                return d;
        }
        return 0;
    });
}
function changesetRows(changed, keyCols, stateCols, schema) {
    const rows = changed.map(({ keyRow, sa, sb }) => {
        const row = {};
        for (const c of keyCols)
            row[c] = keyRow[c];
        stateCols.forEach((c, i) => {
            row[`${c}__a`] = sa[i];
            row[`${c}__b`] = sb[i];
        });
        return row;
    });
    return sortByKey(rows, schema);
}
/** Reconstruct member `scan`'s original rows (logical round-trip), sorted
 * `(*dims, binCol)`. Throws if `scan` is not a member. */
export function extractScan(ms, schema, scan) {
    const idx = ms.scans.indexOf(scan);
    if (idx < 0)
        throw new Error(`extractScan: '${scan}' not a member scan (${ms.scans.join(', ')})`);
    const { keyCols, stateCols } = keyStateCols(schema);
    const id = idTuple(schema, stateCols);
    const out = [];
    for (const r of ms.rows) {
        const inScan = ms.encoder === 'interval'
            ? Number(r[SCAN_LO]) <= idx && idx <= Number(r[SCAN_HI])
            : Number(r[SCAN_COL]) === idx;
        if (!inScan)
            continue;
        const state = stateOf(r, stateCols);
        if (stateEq(state, id))
            continue; // absent (all-identity) — not a real row
        const row = {};
        for (const c of keyCols)
            row[c] = r[c];
        for (const c of stateCols)
            row[c] = r[c];
        out.push(row);
    }
    return sortByKey(out, schema);
}
/** The universal changeset between two scan states (any two row-sets —
 * consolidated-and-extracted or raw): one row per key whose state differs, with
 * before (`__a`) and after (`__b`) state columns. Birth = identity→v, death =
 * v→identity. */
export function diffTables(rowsA, rowsB, schema) {
    const { keyCols, stateCols } = keyStateCols(schema);
    const id = idTuple(schema, stateCols);
    const a = new Map();
    const b = new Map();
    for (const r of rowsA)
        a.set(keyStr(r, keyCols), { keyRow: r, state: stateOf(r, stateCols) });
    for (const r of rowsB)
        b.set(keyStr(r, keyCols), { keyRow: r, state: stateOf(r, stateCols) });
    const changed = [];
    for (const k of new Set([...a.keys(), ...b.keys()])) {
        const sa = a.get(k)?.state ?? id;
        const sb = b.get(k)?.state ?? id;
        if (!stateEq(sa, sb))
            changed.push({ keyRow: (a.get(k) ?? b.get(k)).keyRow, sa, sb });
    }
    return changesetRows(changed, keyCols, stateCols, schema);
}
function keyIntervals(ms, keyCols, stateCols) {
    const out = new Map();
    for (const r of ms.rows) {
        const k = keyStr(r, keyCols);
        let e = out.get(k);
        if (!e) {
            e = { keyRow: r, runs: [] };
            out.set(k, e);
        }
        e.runs.push({ lo: Number(r[SCAN_LO]), hi: Number(r[SCAN_HI]), state: stateOf(r, stateCols) });
    }
    return out;
}
function asOf(runs, idx, id) {
    for (const run of runs)
        if (run.lo <= idx && idx <= run.hi)
            return run.state;
    return id;
}
/** The sparse diff of two member scans (same changeset shape as `diffTables`).
 * For `interval`, reads only the keys with a run boundary inside the span —
 * O(changes-in-span). Equivalent to `diffTables(extractScan(a), extractScan(b))`. */
export function diffScans(ms, schema, scanA, scanB) {
    const ia = ms.scans.indexOf(scanA);
    const ib = ms.scans.indexOf(scanB);
    if (ia < 0)
        throw new Error(`diffScans: '${scanA}' not a member scan (${ms.scans.join(', ')})`);
    if (ib < 0)
        throw new Error(`diffScans: '${scanB}' not a member scan (${ms.scans.join(', ')})`);
    if (ms.encoder !== 'interval') {
        return diffTables(extractScan(ms, schema, scanA), extractScan(ms, schema, scanB), schema);
    }
    const { keyCols, stateCols } = keyStateCols(schema);
    const id = idTuple(schema, stateCols);
    const lo = Math.min(ia, ib);
    const hi = Math.max(ia, ib);
    const intervals = keyIntervals(ms, keyCols, stateCols);
    // Candidate keys: a run boundary in the span `(lo, hi]` — a superset of the
    // changed keys; the `stateEq` test below makes the result exact.
    const cand = new Set();
    for (const r of ms.rows) {
        const rlo = Number(r[SCAN_LO]);
        const rhi = Number(r[SCAN_HI]);
        if ((lo < rlo && rlo <= hi) || (lo <= rhi && rhi < hi))
            cand.add(keyStr(r, keyCols));
    }
    const changed = [];
    for (const k of [...cand].sort()) {
        const info = intervals.get(k);
        const sa = asOf(info.runs, ia, id);
        const sb = asOf(info.runs, ib, id);
        if (!stateEq(sa, sb))
            changed.push({ keyRow: info.keyRow, sa, sb });
    }
    return changesetRows(changed, keyCols, stateCols, schema);
}
/** A key's value stream across every member scan (the "size over time" line) —
 * `[{ scan, state }]`, absent scans carrying the monoid identity. `key` is a
 * row carrying the key columns (`binCol` + dims). */
export function seriesFor(ms, schema, key) {
    const { keyCols, stateCols } = keyStateCols(schema);
    const id = idTuple(schema, stateCols);
    const k = keyStr(key, keyCols);
    const toRow = (state) => Object.fromEntries(stateCols.map((c, i) => [c, state[i]]));
    if (ms.encoder === 'interval') {
        const runs = keyIntervals(ms, keyCols, stateCols).get(k)?.runs ?? [];
        return ms.scans.map((scan, j) => ({ scan, state: toRow(asOf(runs, j, id)) }));
    }
    const byScan = new Map();
    for (const r of ms.rows) {
        if (keyStr(r, keyCols) === k)
            byScan.set(Number(r[SCAN_COL]), stateOf(r, stateCols));
    }
    return ms.scans.map((scan, j) => ({ scan, state: toRow(byScan.get(j) ?? id) }));
}
/** The routing decision: the archive covering `scan`, or null (the caller then
 * falls back to the single-scan `ShardIndex`). Assumes at most one covering
 * entry per tile (the driver never double-consolidates a scan). */
export function resolveScan(entries, scan) {
    for (const e of entries) {
        const foldIndex = e.scans.indexOf(scan);
        if (foldIndex >= 0)
            return { key: e.key, foldIndex, encoder: e.encoder };
    }
    return null;
}
function entryFromRow(r) {
    return {
        dataset: r.dataset,
        tier: r.tier,
        shardDur: r.shard_dur,
        periodStart: r.period_start,
        periodEnd: r.period_end,
        key: r.key,
        scans: r.scans,
        encoder: r.encoder,
        writtenAt: r.written_at,
        ...(r.digests ? { digests: r.digests } : {}),
    };
}
/** Parse the JSONL routing manifest (as `pyrmts_engine.StorageJsonlMultiScanIndex`
 * writes it), optionally filtering to one `dataset` scope. */
export function parseMultiScanIndex(bytes, dataset) {
    const text = new TextDecoder().decode(bytes);
    const entries = [];
    for (const line of text.split('\n')) {
        if (!line.trim())
            continue;
        const entry = entryFromRow(JSON.parse(line));
        if (dataset === undefined || entry.dataset === dataset)
            entries.push(entry);
    }
    return entries;
}
/** Stitch a key's over-time line across capped-K sealed groups: order the tile's
 * manifest entries by scan span, load each group's shard (via `load` — the
 * consumer's footer-pruned fetch), `seriesFor` within it, and concat in scan
 * order. Keeps the IO in the consumer and the routing/ordering in pyrmts. Pass
 * only one tile's entries (one dataset + tier + period lineage). */
export async function seriesAcrossGroups(entries, schema, key, load) {
    const ordered = [...entries].sort((a, b) => {
        if (a.periodStart !== b.periodStart)
            return a.periodStart - b.periodStart;
        const [a0, b0] = [a.scans[0] ?? '', b.scans[0] ?? ''];
        return a0 < b0 ? -1 : a0 > b0 ? 1 : 0;
    });
    const out = [];
    for (const e of ordered) {
        const ms = await load(e.key);
        out.push(...seriesFor(ms, schema, key));
    }
    return out;
}
function normalizeRow(row) {
    const out = {};
    for (const k in row) {
        const v = row[k];
        out[k] = typeof v === 'bigint' ? Number(v) : v;
    }
    return out;
}
function toArrayBuffer(bytes) {
    const ab = new ArrayBuffer(bytes.byteLength);
    new Uint8Array(ab).set(bytes);
    return ab;
}
function asyncBufferFromBytes(bytes) {
    const ab = toArrayBuffer(bytes);
    return { byteLength: ab.byteLength, slice: (start, end) => ab.slice(start, end) };
}
/** Parse a `MultiScan` from parquet bytes — rows (int64 normalized to number,
 * matching the fetch path) plus the self-describing `pyrmts.multiscan`
 * KV-metadata (encoder / member scans / digests) the Python writer attaches. */
export async function readMultiScan(bytes) {
    const metadata = parquetMetadata(toArrayBuffer(bytes));
    const kv = metadata.key_value_metadata?.find(k => k.key === MULTISCAN_META_KEY)?.value;
    if (!kv)
        throw new Error('readMultiScan: parquet has no pyrmts.multiscan metadata');
    const meta = JSON.parse(kv);
    const raw = await parquetReadObjects({ file: asyncBufferFromBytes(bytes), metadata });
    const digests = meta.digests && Object.keys(meta.digests).length ? meta.digests : undefined;
    return {
        rows: raw.map(normalizeRow),
        scans: meta.scans,
        encoder: meta.encoder,
        ...(digests ? { digests } : {}),
    };
}
//# sourceMappingURL=multiscan.js.map
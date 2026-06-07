// Parquet decode over Storage. Bridges pyrmts' Storage interface to
// hyparquet's AsyncBuffer.
//
// When `opts.binCol + opts.range` are supplied, row groups are pruned via
// their column statistics: only RGs whose binCol min/max overlaps the
// requested range are read. The Range header on `Storage.getRange` means
// the worker fetches only the relevant byte ranges; smaller payload +
// smaller decode cost.
import { parquetMetadataAsync, parquetReadObjects } from 'hyparquet';
const DEFAULT_INITIAL_FETCH_SIZE = 64 * 1024;
// Read rows from a single parquet shard, optionally pruning row groups by
// the bin column's statistics.
//
// hyparquet decodes INT64 columns as JS BigInt; this wrapper normalizes to
// Number transparently. ms timestamps + typical counts fit safely below
// `Number.MAX_SAFE_INTEGER` (≈ 9e15); callers with larger ints need to
// access hyparquet output directly.
export async function fetchShardData(storage, key, opts) {
    const head = await storage.head(key);
    if (head === null) {
        if (opts?.tolerate404)
            return [];
        throw new Error(`fetchShardData: object not found: ${key}`);
    }
    // Phase tag flips from `metadata` → `data` after metadata is read. The
    // trace wrapper consults this mutable cell on each slice so the same
    // AsyncBuffer instance can emit correctly-tagged entries across phases.
    const phaseRef = { current: 'metadata' };
    const file = opts?.trace !== undefined
        ? asyncBufferFromStorageTraced(storage, key, head.size, opts.trace, phaseRef)
        : asyncBufferFromStorage(storage, key, head.size);
    const initialFetchSize = opts?.initialFetchSize ?? DEFAULT_INITIAL_FETCH_SIZE;
    const metadata = await parquetMetadataAsync(file, { initialFetchSize });
    phaseRef.current = 'data';
    const hasBinPrune = opts?.binCol !== undefined && opts.range !== undefined;
    const hasFilters = opts?.filters !== undefined && opts.filters.length > 0;
    if (!hasBinPrune && !hasFilters) {
        // No prune → read everything.
        const rows = await parquetReadObjects({ file, metadata });
        return rows.map(normalizeRow);
    }
    const runs = selectRowGroupRuns(metadata, opts);
    if (runs.length === 0)
        return [];
    // Read each contiguous run of matching RGs in one parquetReadObjects call.
    const perRun = await Promise.all(runs.map(({ rowStart, rowEnd }) => parquetReadObjects({ file, metadata, rowStart, rowEnd })));
    return perRun.flat().map(normalizeRow);
}
// Build a `StorageBackend` that fetches parquet shards over a byte-level
// `Storage`. Keys are interpreted as shard file paths; the planner provides
// them in `segment.keys`. `keyTemplate` is accepted for future API symmetry
// (D1Backend uses it for the table name) but currently unused by this
// backend — the planner has already substituted templates into keys.
export function parquetBackend(storage, _keyTemplate) {
    return {
        name: 'parquet',
        async fetchSegment(segment, opts) {
            const perShard = await Promise.all(segment.keys.map(k => fetchShardData(storage, k, opts)));
            return perShard.flat();
        },
    };
}
// Walk the file's row groups, pick those whose stats AND-satisfy every
// predicate built from `opts`, and coalesce adjacent picked RGs into runs
// of (rowStart, rowEnd). Returns zero or more runs.
function selectRowGroupRuns(metadata, opts) {
    const predicates = [];
    if (opts.binCol !== undefined && opts.range !== undefined) {
        const p = makeBinRangePredicate(metadata, opts.binCol, opts.range);
        // Column missing → can't prune by binCol; just skip this predicate
        // (the per-row range filter in stitch still drops out-of-range rows).
        if (p)
            predicates.push(p);
    }
    for (const filter of opts.filters ?? []) {
        const p = makeFilterPredicate(metadata, filter);
        if (p)
            predicates.push(p);
    }
    const runs = [];
    let rowCursor = 0;
    let currentRun = null;
    for (const rg of metadata.row_groups) {
        const numRows = Number(rg.num_rows);
        const rgStart = rowCursor;
        const rgEnd = rowCursor + numRows;
        rowCursor = rgEnd;
        const pass = predicates.every(p => {
            const stats = rg.columns[p.colIdx]?.meta_data?.statistics;
            return p.check(stats?.min_value, stats?.max_value);
        });
        if (!pass) {
            if (currentRun) {
                runs.push(currentRun);
                currentRun = null;
            }
            continue;
        }
        if (currentRun === null) {
            currentRun = { rowStart: rgStart, rowEnd: rgEnd };
        }
        else {
            currentRun.rowEnd = rgEnd;
        }
    }
    if (currentRun)
        runs.push(currentRun);
    return runs;
}
function makeBinRangePredicate(metadata, binCol, range) {
    const colIdx = findColumnIndex(metadata, binCol);
    if (colIdx === -1)
        return null;
    const fromMs = range.from.getTime();
    const toMs = range.to.getTime();
    return {
        colIdx,
        check(rawMin, rawMax) {
            const min = decodeStatValue(rawMin);
            const max = decodeStatValue(rawMax);
            if (min === null || max === null)
                return true; // stats missing → must read
            return !(max < fromMs || min >= toMs);
        },
    };
}
function makeFilterPredicate(metadata, filter) {
    const colIdx = findColumnIndex(metadata, filter.col);
    if (colIdx === -1)
        return null;
    if ('range' in filter) {
        const { min: rMin, max: rMax } = filter.range;
        return {
            colIdx,
            check(rawMin, rawMax) {
                const min = decodeStatValue(rawMin);
                const max = decodeStatValue(rawMax);
                if (min === null || max === null)
                    return true;
                return !(rMax < min || rMin > max);
            },
        };
    }
    // values: set membership. String values compared lex; numeric compared
    // numerically. Mixed-type stats (e.g. string values vs numeric stats)
    // fall back to "must read".
    const values = filter.values;
    return {
        colIdx,
        check(rawMin, rawMax) {
            if (rawMin === undefined || rawMin === null)
                return true;
            if (rawMax === undefined || rawMax === null)
                return true;
            const min = decodeArbitraryStatValue(rawMin);
            const max = decodeArbitraryStatValue(rawMax);
            if (min === null || max === null)
                return true;
            return values.some(v => {
                if (typeof v !== typeof min || typeof v !== typeof max)
                    return true;
                return v >= min && v <= max;
            });
        },
    };
}
function findColumnIndex(metadata, name) {
    const firstRg = metadata.row_groups[0];
    if (!firstRg)
        return -1;
    return firstRg.columns.findIndex(c => {
        const path = c.meta_data?.path_in_schema;
        return path?.length === 1 && path[0] === name;
    });
}
// Decode an int64-ms timestamp stat value. hyparquet exposes parquet's
// min_value/max_value as the decoded JS value: BigInt for INT64,
// number/Date for newer logical types. Normalize to a JS number for
// comparison with Date.getTime(). Returns null if absent/undecodable.
function decodeStatValue(v) {
    if (v === undefined || v === null)
        return null;
    if (typeof v === 'bigint')
        return Number(v);
    if (typeof v === 'number')
        return v;
    if (v instanceof Date)
        return v.getTime();
    if (typeof v === 'string') {
        const t = new Date(v);
        return Number.isNaN(t.getTime()) ? null : t.getTime();
    }
    return null;
}
// Decode a non-timestamp stat value (string column → string; numeric column
// → number). Used by arbitrary-column filters where the type is unknown.
// Distinct from `decodeStatValue` (which tries to parse strings as dates).
function decodeArbitraryStatValue(v) {
    if (v === undefined || v === null)
        return null;
    if (typeof v === 'string')
        return v;
    if (typeof v === 'number')
        return v;
    if (typeof v === 'bigint')
        return Number(v);
    return null;
}
function normalizeRow(row) {
    const out = {};
    for (const k in row) {
        const v = row[k];
        out[k] = typeof v === 'bigint' ? Number(v) : v;
    }
    return out;
}
function asyncBufferFromStorageTraced(storage, key, byteLength, trace, phaseRef) {
    return {
        byteLength,
        async slice(start, end) {
            const effectiveEnd = end ?? byteLength;
            const t0 = performance.now();
            const bytes = await storage.getRange(key, start, effectiveEnd);
            const ms = performance.now() - t0;
            trace.push({
                key,
                start,
                end: effectiveEnd,
                length: effectiveEnd - start,
                ms: Math.round(ms * 100) / 100,
                phase: phaseRef.current,
            });
            return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
        },
    };
}
function asyncBufferFromStorage(storage, key, byteLength) {
    return {
        byteLength,
        async slice(start, end) {
            const effectiveEnd = end ?? byteLength;
            const bytes = await storage.getRange(key, start, effectiveEnd);
            return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
        },
    };
}
//# sourceMappingURL=fetch.js.map
// Parquet decode over Storage. Bridges pyrmts' Storage interface to
// hyparquet's AsyncBuffer.
//
// When `opts.binCol + opts.range` are supplied, row groups are pruned via
// their column statistics: only RGs whose binCol min/max overlaps the
// requested range are read. The Range header on `Storage.getRange` means
// the worker fetches only the relevant byte ranges; smaller payload +
// smaller decode cost.

import { parquetMetadataAsync, parquetReadObjects, type AsyncBuffer, type FileMetaData } from 'hyparquet'
import { EtagConflict, type ColumnFilter, type FetchOptionsBase, type FetchSegment, type Row, type Storage, type StorageBackend } from './types.js'

// Parquet-backend-specific options. Extends the shared `FetchOptionsBase`
// (binCol / range / filters / tolerate404) with parquet-only knobs.
export interface FetchOptions extends FetchOptionsBase {
  // Initial bytes-from-EOF fetched to read parquet metadata. Hyparquet's
  // default is 512KB, which over-fetches small shards. 64KB is enough to
  // catch the footer of typical shards in one round trip; hyparquet falls
  // back to a second fetch if the metadata is bigger.
  initialFetchSize?: number
  // Optional byte-range trace. If supplied, every `slice(start, end)` call
  // on the underlying parquet file appends a `FetchTrace` entry. Used by
  // worker `?debug=1` paths to surface the actual request pattern (count,
  // sizes, gaps) so callers can spot RG-prune misses, over-fetched
  // columns, footer-read churn, etc.
  trace?: FetchTrace[]
  // Decoded-footer cache. Without it every call pays a `head` round trip, a
  // footer range fetch and a full metadata decode (O(#RGs × #cols) Thrift)
  // per shard. On a hit the `head` is skipped too: data ranges are read with
  // `If-Match: <cached etag>`, so a rewritten shard surfaces as an
  // `EtagConflict`, the entry is dropped and the call falls back to the cold
  // path. A module-level `new Map()` in a Worker is a per-isolate cache that
  // survives across requests on a warm isolate; wrap the Cache API / KV / D1
  // behind the same two methods for a cross-isolate one.
  metadataCache?: MetadataCache
}

/** A cached decoded footer plus what validates it. */
export interface CachedMetadata {
  etag: string
  size: number
  metadata: FileMetaData
}

/** Map-like store for decoded parquet footers, keyed by storage key
 * (`Map<string, CachedMetadata>` satisfies it). */
export interface MetadataCache {
  get(key: string): CachedMetadata | undefined
  set(key: string, entry: CachedMetadata): void
  delete?(key: string): void
}

/** One observed `slice(start, end)` against a parquet file. */
export interface FetchTrace {
  /** Storage key (parquet path) the slice was against. */
  key: string
  /** Inclusive lower byte offset. */
  start: number
  /** Exclusive upper byte offset. */
  end: number
  /** `end - start`. Length in bytes of the range that came back. */
  length: number
  /** Wall-clock milliseconds for the slice (storage call). */
  ms: number
  /** Bucketed phase the slice happened in: `metadata` (footer / metadata
   *  read) or `data` (column-chunk reads after planning). Heuristic — the
   *  first 1-2 slices per file are the footer; subsequent slices are
   *  column chunks. */
  phase: 'metadata' | 'data'
}

const DEFAULT_INITIAL_FETCH_SIZE = 64 * 1024

// Read rows from a single parquet shard, optionally pruning row groups by
// the bin column's statistics.
//
// hyparquet decodes INT64 columns as JS BigInt; this wrapper normalizes to
// Number transparently. ms timestamps + typical counts fit safely below
// `Number.MAX_SAFE_INTEGER` (≈ 9e15); callers with larger ints need to
// access hyparquet output directly.
export async function fetchShardData(
  storage: Storage,
  key: string,
  opts?: FetchOptions,
): Promise<Row[]> {
  const cache = opts?.metadataCache
  const cached = cache?.get(key)
  if (cached !== undefined) {
    // Warm path: no `head`, no footer; every data range carries If-Match.
    try {
      return await readShard(storage, key, cached.size, cached.metadata, opts, cached.etag)
    } catch (e) {
      if (!(e instanceof EtagConflict)) throw e
      cache?.delete?.(key)
    }
  }
  const head = await storage.head(key)
  if (head === null) {
    if (opts?.tolerate404) return []
    throw new Error(`fetchShardData: object not found: ${key}`)
  }
  // Phase tag flips from `metadata` → `data` after metadata is read. The
  // trace wrapper consults this mutable cell on each slice so the same
  // AsyncBuffer instance can emit correctly-tagged entries across phases.
  const phaseRef: { current: 'metadata' | 'data' } = { current: 'metadata' }
  const file = opts?.trace !== undefined
    ? asyncBufferFromStorageTraced(storage, key, head.size, opts.trace, phaseRef)
    : asyncBufferFromStorage(storage, key, head.size)
  const initialFetchSize = opts?.initialFetchSize ?? DEFAULT_INITIAL_FETCH_SIZE
  const metadata = await parquetMetadataAsync(file, { initialFetchSize })
  if (cache !== undefined && head.etag !== undefined) {
    cache.set(key, { etag: head.etag, size: head.size, metadata })
  }
  phaseRef.current = 'data'
  return readRows(file, metadata, opts)
}

// Read a shard whose footer is already decoded: data ranges only, each with
// `If-Match: etag` so a rewrite since the footer was cached fails loudly.
async function readShard(
  storage: Storage,
  key: string,
  size: number,
  metadata: FileMetaData,
  opts: FetchOptions | undefined,
  etag: string,
): Promise<Row[]> {
  const phaseRef: { current: 'metadata' | 'data' } = { current: 'data' }
  const file = opts?.trace !== undefined
    ? asyncBufferFromStorageTraced(storage, key, size, opts.trace, phaseRef, etag)
    : asyncBufferFromStorage(storage, key, size, etag)
  return readRows(file, metadata, opts)
}

async function readRows(file: AsyncBuffer, metadata: FileMetaData, opts: FetchOptions | undefined): Promise<Row[]> {
  const hasBinPrune = opts?.binCol !== undefined && opts.range !== undefined
  const hasFilters = opts?.filters !== undefined && opts.filters.length > 0
  if (!hasBinPrune && !hasFilters) {
    // No prune → read everything.
    const rows = await parquetReadObjects({ file, metadata })
    return rows.map(normalizeRow)
  }

  const runs = selectRowGroupRuns(metadata, opts!)
  if (runs.length === 0) return []

  // Read each contiguous run of matching RGs in one parquetReadObjects call.
  const perRun = await Promise.all(
    runs.map(({ rowStart, rowEnd }) =>
      parquetReadObjects({ file, metadata, rowStart, rowEnd }),
    ),
  )
  return perRun.flat().map(normalizeRow)
}

// Build a `StorageBackend` that fetches parquet shards over a byte-level
// `Storage`. Keys are interpreted as shard file paths; the planner provides
// them in `segment.keys`. `keyTemplate` is accepted for future API symmetry
// (D1Backend uses it for the table name) but currently unused by this
// backend — the planner has already substituted templates into keys.
export function parquetBackend(storage: Storage, _keyTemplate?: string): StorageBackend<FetchOptions> {
  return {
    name: 'parquet',
    async fetchSegment(segment: FetchSegment, opts?: FetchOptions): Promise<Row[]> {
      const perShard = await Promise.all(
        segment.keys.map(k => fetchShardData(storage, k, opts)),
      )
      return perShard.flat()
    },
  }
}

// One RG predicate: given a row group's stats for `colIdx`, decide whether
// that RG could contain a matching row. Returns true if the RG *must* be
// read (either the predicate provably overlaps, or stats are missing /
// undecodable). Predicates AND together.
interface RgPredicate {
  colIdx: number
  check(rawMin: unknown, rawMax: unknown): boolean
}

// Walk the file's row groups, pick those whose stats AND-satisfy every
// predicate built from `opts`, and coalesce adjacent picked RGs into runs
// of (rowStart, rowEnd). Returns zero or more runs.
function selectRowGroupRuns(
  metadata: FileMetaData,
  opts: FetchOptions,
): { rowStart: number; rowEnd: number }[] {
  const predicates: RgPredicate[] = []
  if (opts.binCol !== undefined && opts.range !== undefined) {
    const p = makeBinRangePredicate(metadata, opts.binCol, opts.range)
    // Column missing → can't prune by binCol; just skip this predicate
    // (the per-row range filter in stitch still drops out-of-range rows).
    if (p) predicates.push(p)
  }
  for (const filter of opts.filters ?? []) {
    const p = makeFilterPredicate(metadata, filter)
    if (p) predicates.push(p)
  }

  const runs: { rowStart: number; rowEnd: number }[] = []
  let rowCursor = 0
  let currentRun: { rowStart: number; rowEnd: number } | null = null

  for (const rg of metadata.row_groups) {
    const numRows = Number(rg.num_rows)
    const rgStart = rowCursor
    const rgEnd = rowCursor + numRows
    rowCursor = rgEnd

    const pass = predicates.every(p => {
      const stats = rg.columns[p.colIdx]?.meta_data?.statistics
      return p.check(stats?.min_value, stats?.max_value)
    })

    if (!pass) {
      if (currentRun) {
        runs.push(currentRun)
        currentRun = null
      }
      continue
    }
    if (currentRun === null) {
      currentRun = { rowStart: rgStart, rowEnd: rgEnd }
    } else {
      currentRun.rowEnd = rgEnd
    }
  }
  if (currentRun) runs.push(currentRun)
  return runs
}

function makeBinRangePredicate(
  metadata: FileMetaData,
  binCol: string,
  range: { from: Date; to: Date },
): RgPredicate | null {
  const colIdx = findColumnIndex(metadata, binCol)
  if (colIdx === -1) return null
  const fromMs = range.from.getTime()
  const toMs = range.to.getTime()
  return {
    colIdx,
    check(rawMin, rawMax) {
      const min = decodeStatValue(rawMin)
      const max = decodeStatValue(rawMax)
      if (min === null || max === null) return true   // stats missing → must read
      return !(max < fromMs || min >= toMs)
    },
  }
}

function makeFilterPredicate(
  metadata: FileMetaData,
  filter: ColumnFilter,
): RgPredicate | null {
  const colIdx = findColumnIndex(metadata, filter.col)
  if (colIdx === -1) return null
  if ('range' in filter) {
    const { min: rMin, max: rMax } = filter.range
    return {
      colIdx,
      check(rawMin, rawMax) {
        const min = decodeStatValue(rawMin)
        const max = decodeStatValue(rawMax)
        if (min === null || max === null) return true
        return !(rMax < min || rMin > max)
      },
    }
  }
  // values: set membership. String values compared lex; numeric compared
  // numerically. Mixed-type stats (e.g. string values vs numeric stats)
  // fall back to "must read".
  const values = filter.values
  return {
    colIdx,
    check(rawMin, rawMax) {
      if (rawMin === undefined || rawMin === null) return true
      if (rawMax === undefined || rawMax === null) return true
      const min = decodeArbitraryStatValue(rawMin)
      const max = decodeArbitraryStatValue(rawMax)
      if (min === null || max === null) return true
      return (values as readonly (string | number)[]).some(v => {
        if (typeof v !== typeof min || typeof v !== typeof max) return true
        return v >= min && v <= max
      })
    },
  }
}

function findColumnIndex(metadata: FileMetaData, name: string): number {
  const firstRg = metadata.row_groups[0]
  if (!firstRg) return -1
  return firstRg.columns.findIndex(c => {
    const path = c.meta_data?.path_in_schema
    return path?.length === 1 && path[0] === name
  })
}

// Decode an int64-ms timestamp stat value. hyparquet exposes parquet's
// min_value/max_value as the decoded JS value: BigInt for INT64,
// number/Date for newer logical types. Normalize to a JS number for
// comparison with Date.getTime(). Returns null if absent/undecodable.
function decodeStatValue(v: unknown): number | null {
  if (v === undefined || v === null) return null
  if (typeof v === 'bigint') return Number(v)
  if (typeof v === 'number') return v
  if (v instanceof Date) return v.getTime()
  if (typeof v === 'string') {
    const t = new Date(v)
    return Number.isNaN(t.getTime()) ? null : t.getTime()
  }
  return null
}

// Decode a non-timestamp stat value (string column → string; numeric column
// → number). Used by arbitrary-column filters where the type is unknown.
// Distinct from `decodeStatValue` (which tries to parse strings as dates).
function decodeArbitraryStatValue(v: unknown): string | number | null {
  if (v === undefined || v === null) return null
  if (typeof v === 'string') return v
  if (typeof v === 'number') return v
  if (typeof v === 'bigint') return Number(v)
  return null
}

function normalizeRow(row: Record<string, unknown>): Row {
  const out: Row = {}
  for (const k in row) {
    const v = row[k]
    out[k] = typeof v === 'bigint' ? Number(v) : v
  }
  return out
}

function asyncBufferFromStorageTraced(
  storage: Storage,
  key: string,
  byteLength: number,
  trace: FetchTrace[],
  phaseRef: { current: 'metadata' | 'data' },
  ifMatch?: string,
): AsyncBuffer {
  const rangeOpts = ifMatch !== undefined ? { ifMatch } : undefined
  return {
    byteLength,
    async slice(start: number, end?: number): Promise<ArrayBuffer> {
      const effectiveEnd = end ?? byteLength
      const t0 = performance.now()
      const bytes = await storage.getRange(key, start, effectiveEnd, rangeOpts)
      const ms = performance.now() - t0
      trace.push({
        key,
        start,
        end: effectiveEnd,
        length: effectiveEnd - start,
        ms: Math.round(ms * 100) / 100,
        phase: phaseRef.current,
      })
      return bytes.buffer.slice(
        bytes.byteOffset,
        bytes.byteOffset + bytes.byteLength,
      ) as ArrayBuffer
    },
  }
}

function asyncBufferFromStorage(
  storage: Storage,
  key: string,
  byteLength: number,
  ifMatch?: string,
): AsyncBuffer {
  const rangeOpts = ifMatch !== undefined ? { ifMatch } : undefined
  return {
    byteLength,
    async slice(start: number, end?: number): Promise<ArrayBuffer> {
      const effectiveEnd = end ?? byteLength
      const bytes = await storage.getRange(key, start, effectiveEnd, rangeOpts)
      return bytes.buffer.slice(
        bytes.byteOffset,
        bytes.byteOffset + bytes.byteLength,
      ) as ArrayBuffer
    },
  }
}

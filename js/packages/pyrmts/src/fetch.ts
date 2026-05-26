// Parquet decode over Storage. Bridges pyrmts' Storage interface to
// hyparquet's AsyncBuffer.
//
// When `opts.binCol + opts.range` are supplied, row groups are pruned via
// their column statistics: only RGs whose binCol min/max overlaps the
// requested range are read. The Range header on `Storage.getRange` means
// the worker fetches only the relevant byte ranges; smaller payload +
// smaller decode cost.

import { parquetMetadataAsync, parquetReadObjects, type AsyncBuffer, type FileMetaData } from 'hyparquet'
import type { Row } from './monoids.js'
import type { Storage } from './types.js'

export interface FetchOptions {
  // Column to use for RG-level filtering. For time-axis pyramids, this is
  // the bin column (`pyramid.binCol`).
  binCol?: string
  // Range to filter to. For time-axis pyramids, supply the segment's
  // [from, to). Only RGs whose `binCol` min/max overlaps this range are
  // fetched + decoded.
  range?: { from: Date; to: Date }
  // Initial bytes-from-EOF fetched to read parquet metadata. Hyparquet's
  // default is 512KB, which over-fetches small shards. 64KB is enough to
  // catch the footer of typical shards in one round trip; hyparquet falls
  // back to a second fetch if the metadata is bigger.
  initialFetchSize?: number
  // Treat missing objects (`storage.head` → null) as empty shards instead
  // of throwing. For pyramids with heterogeneous dim coverage (e.g. some
  // devices haven't existed long enough to have data in older shard
  // periods), the planner emits shard keys that don't yet exist; enabling
  // this lets the query succeed with empty bins for those gaps instead of
  // erroring. Default: false (fail loudly — usually a config bug).
  tolerate404?: boolean
  // Arbitrary-column RG pruning. AND-combined with the `binCol + range`
  // filter (if any) and with each other. Each filter is either set
  // membership (`values`) or a closed numeric range (`range`). An RG is
  // skipped only if its stats *prove* no row could match; missing stats
  // default to "must read" (safer than throwing or omitting data —
  // downstream per-row filters can still drop unwanted rows).
  filters?: ColumnFilter[]
}

export type ColumnFilter =
  | { col: string; values: readonly string[] | readonly number[] }
  | { col: string; range: { min: number; max: number } }

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
  const head = await storage.head(key)
  if (head === null) {
    if (opts?.tolerate404) return []
    throw new Error(`fetchShardData: object not found: ${key}`)
  }
  const file = asyncBufferFromStorage(storage, key, head.size)

  const initialFetchSize = opts?.initialFetchSize ?? DEFAULT_INITIAL_FETCH_SIZE
  const metadata = await parquetMetadataAsync(file, { initialFetchSize })

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

// Read rows across all shard keys in a segment, concatenated. Keys fetched
// in parallel; rows preserve per-shard order.
export async function fetchSegmentRows(
  storage: Storage,
  keys: string[],
  opts?: FetchOptions,
): Promise<Row[]> {
  const perShard = await Promise.all(keys.map(k => fetchShardData(storage, k, opts)))
  return perShard.flat()
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

function asyncBufferFromStorage(
  storage: Storage,
  key: string,
  byteLength: number,
): AsyncBuffer {
  return {
    byteLength,
    async slice(start: number, end?: number): Promise<ArrayBuffer> {
      const effectiveEnd = end ?? byteLength
      const bytes = await storage.getRange(key, start, effectiveEnd)
      return bytes.buffer.slice(
        bytes.byteOffset,
        bytes.byteOffset + bytes.byteLength,
      ) as ArrayBuffer
    },
  }
}

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
  const head = await storage.head(key)
  if (head === null) {
    throw new Error(`fetchShardData: object not found: ${key}`)
  }
  const file = asyncBufferFromStorage(storage, key, head.size)

  const initialFetchSize = opts?.initialFetchSize ?? DEFAULT_INITIAL_FETCH_SIZE
  const metadata = await parquetMetadataAsync(file, { initialFetchSize })

  // No filter → read everything.
  if (opts?.binCol === undefined || opts.range === undefined) {
    const rows = await parquetReadObjects({ file, metadata })
    return rows.map(normalizeRow)
  }

  const runs = selectRowGroupRuns(metadata, opts.binCol, opts.range)
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

// Walk the file's row groups, pick those whose `binCol` stats overlap
// `range`, and coalesce adjacent picked RGs into runs of (rowStart, rowEnd).
// Returns an array of zero or more runs.
function selectRowGroupRuns(
  metadata: FileMetaData,
  binCol: string,
  range: { from: Date; to: Date },
): { rowStart: number; rowEnd: number }[] {
  const fromMs = range.from.getTime()
  const toMs = range.to.getTime()
  const colIdx = findColumnIndex(metadata, binCol)
  if (colIdx === -1) {
    // Column not found — fall back to reading everything (safer than throwing
    // mid-pipeline; downstream stitch's per-row range filter will still drop
    // out-of-range rows).
    return [{ rowStart: 0, rowEnd: Number(metadata.num_rows) }]
  }

  const runs: { rowStart: number; rowEnd: number }[] = []
  let rowCursor = 0
  let currentRun: { rowStart: number; rowEnd: number } | null = null

  for (const rg of metadata.row_groups) {
    const numRows = Number(rg.num_rows)
    const rgStart = rowCursor
    const rgEnd = rowCursor + numRows
    rowCursor = rgEnd

    const stats = rg.columns[colIdx]?.meta_data?.statistics
    const min = decodeStatValue(stats?.min_value)
    const max = decodeStatValue(stats?.max_value)

    // Stats present + provably outside range → skip.
    if (min !== null && max !== null && (max < fromMs || min >= toMs)) {
      if (currentRun) {
        runs.push(currentRun)
        currentRun = null
      }
      continue
    }

    // Either overlaps or stats missing (must read defensively).
    if (currentRun === null) {
      currentRun = { rowStart: rgStart, rowEnd: rgEnd }
    } else {
      currentRun.rowEnd = rgEnd
    }
  }
  if (currentRun) runs.push(currentRun)
  return runs
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

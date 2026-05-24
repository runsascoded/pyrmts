// Parquet decode over Storage. Bridges pyrmts' Storage interface to
// hyparquet's AsyncBuffer, then runs parquetReadObjects to materialize a
// shard's rows as plain JS objects (one per parquet row).
//
// v0.1 reads whole files — no column projection or row-group pruning. Those
// are pure perf optimizations layered on later; the API surface here stays
// the same.

import { parquetReadObjects, type AsyncBuffer } from 'hyparquet'
import type { Row } from './monoids.js'
import type { Storage } from './types.js'

// Read all rows of a single parquet shard.
//
// Parquet INT64 columns are decoded by hyparquet as JS BigInt. This wrapper
// converts BigInts to Number transparently — ms timestamps and typical bin
// counts fit safely within `Number.MAX_SAFE_INTEGER` (≈ 9e15). Callers with
// counts past that limit need to access raw hyparquet output instead.
export async function fetchShardData(
  storage: Storage,
  key: string,
): Promise<Row[]> {
  const head = await storage.head(key)
  if (head === null) {
    throw new Error(`fetchShardData: object not found: ${key}`)
  }
  const file = asyncBufferFromStorage(storage, key, head.size)
  const rows = await parquetReadObjects({ file })
  return rows.map(normalizeRow)
}

function normalizeRow(row: Record<string, unknown>): Row {
  const out: Row = {}
  for (const k in row) {
    const v = row[k]
    out[k] = typeof v === 'bigint' ? Number(v) : v
  }
  return out
}

// Read rows from all shards a segment covers, concatenated. Shards within a
// segment are fetched in parallel; rows preserve per-shard order.
export async function fetchSegmentRows(
  storage: Storage,
  keys: string[],
): Promise<Row[]> {
  const perShard = await Promise.all(keys.map(k => fetchShardData(storage, k)))
  return perShard.flat()
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

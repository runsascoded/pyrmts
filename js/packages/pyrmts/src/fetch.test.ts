// Tests for row-group filtering and missing-shard tolerance in fetchShardData.

import { parquetWriteBuffer } from 'hyparquet-writer'
import { describe, expect, test } from 'vitest'
import { fetchSegmentRows, fetchShardData } from './fetch.js'
import { memStorage } from './storage.js'
import type { Storage } from './types.js'

const ms = (iso: string): number => new Date(iso).getTime()

// 10,000 rows of 1-minute data starting 2026-01-01T00:00Z, split into 4 RGs
// of 2500 rows each. Big enough that metadata overhead doesn't dominate and
// RG-skipping byte savings are observable.
function multiRgParquet(): Uint8Array {
  const baseMs = ms('2026-01-01T00:00:00Z')
  const ts: bigint[] = []
  const value: number[] = []
  for (let i = 0; i < 10_000; i++) {
    ts.push(BigInt(baseMs + i * 60_000))
    value.push(i)
  }
  const buf = parquetWriteBuffer({
    columnData: [
      { name: 'ts', type: 'INT64', data: ts },
      { name: 'value', type: 'INT32', data: value },
    ],
    rowGroupSize: 2500,
    statistics: true,
  })
  return new Uint8Array(buf)
}

// RG-span helper: 2500 rows × 60s = 150_000s = 41h40m per RG.
// RG0: 2026-01-01T00:00 .. 2026-01-02T17:39
// RG1: 2026-01-02T17:40 .. 2026-01-04T11:19
// RG2: 2026-01-04T11:20 .. 2026-01-06T04:59
// RG3: 2026-01-06T05:00 .. 2026-01-07T22:39
const RG_MIDPOINT_2 = ms('2026-01-05T08:00:00Z')   // mid-RG2

// Wrap a Storage with instrumentation tracking total bytes requested via
// getRange. Useful for asserting that RG pruning actually reduced I/O.
function instrumented(inner: Storage): { storage: Storage; bytesRead: () => number } {
  let bytes = 0
  const storage: Storage = {
    head: (key) => inner.head(key),
    getRange: async (key, start, end) => {
      bytes += end - start
      return inner.getRange(key, start, end)
    },
    get: (key) => inner.get(key),
    put: (key, data) => inner.put(key, data),
    list: (prefix) => inner.list(prefix),
  }
  return { storage, bytesRead: () => bytes }
}

describe('fetchShardData: RG filtering', () => {
  const KEY = 'shard.parquet'

  async function setup() {
    const bytes = multiRgParquet()
    const inner = memStorage()
    await inner.put(KEY, bytes)
    const head = await inner.head(KEY)
    return { inner, fileSize: head!.size }
  }

  test('no filter: returns all 10,000 rows', async () => {
    const { inner } = await setup()
    const rows = await fetchShardData(inner, KEY)
    expect(rows).toHaveLength(10_000)
  })

  test('range covering only one RG returns only that RG\'s rows', async () => {
    // Range entirely within RG2 — only RG2 should be read.
    const { inner } = await setup()
    const rows = await fetchShardData(inner, KEY, {
      binCol: 'ts',
      range: {
        from: new Date(RG_MIDPOINT_2 - 10 * 60_000),
        to: new Date(RG_MIDPOINT_2 + 10 * 60_000),
      },
    })
    expect(rows).toHaveLength(2500)    // entire RG2
    expect(rows[0]!.ts).toBe(ms('2026-01-04T11:20:00Z'))
    expect(rows[rows.length - 1]!.ts).toBe(ms('2026-01-06T04:59:00Z'))
  })

  test('range covering two adjacent RGs reads both', async () => {
    const { inner } = await setup()
    const rows = await fetchShardData(inner, KEY, {
      binCol: 'ts',
      range: {
        from: new Date('2026-01-03T00:00:00Z'),   // mid-RG1
        to: new Date('2026-01-05T00:00:00Z'),     // mid-RG2
      },
    })
    expect(rows).toHaveLength(5000)    // RG1 + RG2
  })

  test('range with no overlapping RG returns empty', async () => {
    const { inner } = await setup()
    const rows = await fetchShardData(inner, KEY, {
      binCol: 'ts',
      range: {
        from: new Date('2027-01-01T00:00:00Z'),
        to: new Date('2027-01-02T00:00:00Z'),
      },
    })
    expect(rows).toEqual([])
  })

  test('reduces bytes read vs unfiltered', async () => {
    const { inner } = await setup()
    const a = instrumented(inner)
    await fetchShardData(a.storage, KEY)
    const fullBytes = a.bytesRead()

    const b = instrumented(inner)
    await fetchShardData(b.storage, KEY, {
      binCol: 'ts',
      range: {
        from: new Date(RG_MIDPOINT_2 - 10 * 60_000),
        to: new Date(RG_MIDPOINT_2 + 10 * 60_000),
      },
    })
    const filteredBytes = b.bytesRead()

    // Filtered should be meaningfully smaller (we skipped 3 of 4 RGs of
    // similar size; allow ~40% as a defensive bound vs hyparquet internals).
    expect(filteredBytes).toBeLessThan(fullBytes * 0.6)
  })

  test('falls back to reading everything when binCol not found', async () => {
    // Mistyped binCol — defensive fallback rather than throw mid-pipeline.
    const { inner } = await setup()
    const rows = await fetchShardData(inner, KEY, {
      binCol: 'not_a_column',
      range: {
        from: new Date('2026-01-01T00:00:00Z'),
        to: new Date('2026-01-01T00:30:00Z'),
      },
    })
    expect(rows).toHaveLength(10_000)
  })
})

describe('fetchShardData: tolerate404', () => {
  test('throws on missing object by default', async () => {
    const storage = memStorage()
    await expect(fetchShardData(storage, 'missing.parquet')).rejects.toThrow(
      'fetchShardData: object not found: missing.parquet',
    )
  })

  test('returns [] for missing object when tolerate404 is true', async () => {
    const storage = memStorage()
    const rows = await fetchShardData(storage, 'missing.parquet', { tolerate404: true })
    expect(rows).toEqual([])
  })

  test('still reads present objects when tolerate404 is true', async () => {
    const storage = memStorage()
    await storage.put('present.parquet', multiRgParquet())
    const rows = await fetchShardData(storage, 'present.parquet', { tolerate404: true })
    expect(rows).toHaveLength(10_000)
  })
})

describe('fetchSegmentRows: tolerate404', () => {
  test('concatenates present + tolerated-missing shards', async () => {
    const storage = memStorage()
    await storage.put('a.parquet', multiRgParquet())
    // 'b.parquet' intentionally missing
    await storage.put('c.parquet', multiRgParquet())
    const rows = await fetchSegmentRows(
      storage,
      ['a.parquet', 'b.parquet', 'c.parquet'],
      { tolerate404: true },
    )
    expect(rows).toHaveLength(20_000)
  })

  test('throws on first missing shard by default', async () => {
    const storage = memStorage()
    await storage.put('a.parquet', multiRgParquet())
    await expect(
      fetchSegmentRows(storage, ['a.parquet', 'b.parquet']),
    ).rejects.toThrow('fetchShardData: object not found: b.parquet')
  })
})

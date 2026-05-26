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

// Fixture for arbitrary-column filter tests. Rows are sorted by `station_id`,
// not `ts`, so per-RG `ts` stats span the whole day (typical of ctbk's
// `(station_id, dt)`-sorted shadow shards). station_id stats are tight per-RG
// and prunable; ts stats are not.
//
// 10,000 rows × 4 RGs × 2500 rows. station_id = 's00000'..'s09999'. device_id
// numeric column with same ordering (so device_id stats are also per-RG-tight).
function stationSortedParquet(): Uint8Array {
  const baseMs = ms('2026-01-01T00:00:00Z')
  const ts: bigint[] = []
  const stationId: string[] = []
  const deviceId: number[] = []
  const value: number[] = []
  for (let i = 0; i < 10_000; i++) {
    // ts cycles within the day so every RG spans the full day; binCol can't prune.
    ts.push(BigInt(baseMs + (i % 1440) * 60_000))
    stationId.push(`s${i.toString().padStart(5, '0')}`)
    deviceId.push(i)
    value.push(i)
  }
  const buf = parquetWriteBuffer({
    columnData: [
      { name: 'ts', type: 'INT64', data: ts },
      { name: 'station_id', type: 'STRING', data: stationId },
      { name: 'device_id', type: 'INT32', data: deviceId },
      { name: 'value', type: 'INT32', data: value },
    ],
    rowGroupSize: 2500,
    statistics: true,
  })
  return new Uint8Array(buf)
}

describe('fetchShardData: arbitrary-column filters', () => {
  const KEY = 'stations.parquet'

  async function setup() {
    const storage = memStorage()
    await storage.put(KEY, stationSortedParquet())
    return storage
  }

  test('values filter on string col prunes to the matching RG', async () => {
    const storage = await setup()
    // 's03000' lives in RG1 (rows 2500..4999) — only RG1 should be returned.
    const rows = await fetchShardData(storage, KEY, {
      filters: [{ col: 'station_id', values: ['s03000'] }],
    })
    expect(rows).toHaveLength(2500)
    const stations = rows.map(r => r.station_id as string)
    expect(stations[0]).toBe('s02500')
    expect(stations[stations.length - 1]).toBe('s04999')
  })

  test('values filter with multiple values selects multiple RGs', async () => {
    const storage = await setup()
    // 's00010' (RG0) + 's08000' (RG3) → RG0 + RG3, skipping RG1+RG2.
    const rows = await fetchShardData(storage, KEY, {
      filters: [{ col: 'station_id', values: ['s00010', 's08000'] }],
    })
    expect(rows).toHaveLength(5000)
    const stations = rows.map(r => r.station_id as string).sort()
    expect(stations[0]).toBe('s00000')
    expect(stations[2499]).toBe('s02499')
    expect(stations[2500]).toBe('s07500')
    expect(stations[4999]).toBe('s09999')
  })

  test('range filter on numeric col prunes to the matching RG', async () => {
    const storage = await setup()
    // device_id 5500..6000 lives entirely in RG2 (5000..7499).
    const rows = await fetchShardData(storage, KEY, {
      filters: [{ col: 'device_id', range: { min: 5500, max: 6000 } }],
    })
    expect(rows).toHaveLength(2500)
    const ids = rows.map(r => r.device_id as number)
    expect(ids[0]).toBe(5000)
    expect(ids[ids.length - 1]).toBe(7499)
  })

  test('no-match filter returns []', async () => {
    const storage = await setup()
    const rows = await fetchShardData(storage, KEY, {
      filters: [{ col: 'station_id', values: ['s99999'] }],
    })
    expect(rows).toEqual([])
  })

  test('filter + binCol range AND-combine (intersection)', async () => {
    // Pyramid query that wants devices 5500-6000 AND ts in the (single-day)
    // range. station_id stats prune to RG2; ts stats don't prune (every RG
    // spans the day). End result = RG2's rows.
    const storage = await setup()
    const rows = await fetchShardData(storage, KEY, {
      binCol: 'ts',
      range: {
        from: new Date('2026-01-01T00:00:00Z'),
        to: new Date('2026-01-02T00:00:00Z'),
      },
      filters: [{ col: 'device_id', range: { min: 5500, max: 6000 } }],
    })
    expect(rows).toHaveLength(2500)
  })

  test('filter on column missing from schema falls through (no prune)', async () => {
    const storage = await setup()
    const rows = await fetchShardData(storage, KEY, {
      filters: [{ col: 'not_a_column', values: ['anything'] }],
    })
    // Filter ineffective → reads everything.
    expect(rows).toHaveLength(10_000)
  })

  test('multiple filters AND together', async () => {
    const storage = await setup()
    // station_id matches RG1; device_id range matches RG2. Intersection: empty.
    const rows = await fetchShardData(storage, KEY, {
      filters: [
        { col: 'station_id', values: ['s03000'] },
        { col: 'device_id', range: { min: 5500, max: 6000 } },
      ],
    })
    expect(rows).toEqual([])
  })

  test('byte savings observable when filter prunes effectively', async () => {
    const storage = await setup()
    const inst1 = instrumented(storage)
    await fetchShardData(inst1.storage, KEY)
    const fullBytes = inst1.bytesRead()

    const inst2 = instrumented(storage)
    await fetchShardData(inst2.storage, KEY, {
      filters: [{ col: 'station_id', values: ['s00010'] }],
    })
    const prunedBytes = inst2.bytesRead()
    // 1 RG out of 4 → ~1/4 the data column bytes. Allow 60% to cover metadata.
    expect(prunedBytes).toBeLessThan(fullBytes * 0.6)
  })
})

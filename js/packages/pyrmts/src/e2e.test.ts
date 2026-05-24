// End-to-end read pipeline test: parquet shard → memStorage → planQuery →
// fetchSegmentRows → stitch. Validates that all the slices compose.

import { parquetWriteBuffer } from 'hyparquet-writer'
import { describe, expect, test } from 'vitest'
import { fetchSegmentRows } from './fetch.js'
import { planQuery } from './planner.js'
import { memStorage } from './storage.js'
import { stitch } from './stitch.js'
import type { Pyramid } from './types.js'

const ms = (iso: string): number => new Date(iso).getTime()

// Build a 3-row h1 parquet for awair (device 17617, 2026-01-01 hours 0/1/2).
function awairH1Parquet(): Uint8Array {
  const buf = parquetWriteBuffer({
    columnData: [
      {
        name: 'ts',
        type: 'INT64',
        data: [
          BigInt(ms('2026-01-01T00:00:00Z')),
          BigInt(ms('2026-01-01T01:00:00Z')),
          BigInt(ms('2026-01-01T02:00:00Z')),
        ],
      },
      { name: 'device_id', type: 'INT32', data: [17617, 17617, 17617] },
      { name: 'temp_n', type: 'INT32', data: [60, 60, 60] },
      { name: 'temp_sum', type: 'DOUBLE', data: [1200, 1260, 1320] },
      { name: 'temp_sumsq', type: 'DOUBLE', data: [24500, 26800, 29200] },
    ],
  })
  return new Uint8Array(buf)
}

const awair: Pyramid = {
  storage: memStorage(),  // replaced per test
  keyTemplate: 'awair-{device_id}/{tier}/{period}.parquet',
  axis: 'time',
  binCol: 'ts',
  dims: [{ name: 'device_id', type: 'int' }],
  metrics: [{ name: 'temp', monoid: 'sum' }],
  tiers: [
    { name: 'raw', bin: '1min', shard: '1mo' },
    { name: 'h1', bin: '1h', shard: '1mo' },
    { name: 'd1', bin: '1d', shard: '1y' },
    { name: 'mo1', bin: '1mo', shard: '1y' },
  ],
}

describe('end-to-end pipeline', () => {
  test('plan → fetch → stitch reads parquet shard and returns stitched rows', async () => {
    // Stand up an in-memory pyramid with one h1 shard.
    const data = new Map<string, Uint8Array>()
    data.set('awair-17617/h1/2026-01.parquet', awairH1Parquet())
    const pyramid: Pyramid = { ...awair, storage: memStorage(data) }

    const plan = planQuery(pyramid, {
      range: { from: new Date('2026-01-01T00:00:00Z'), to: new Date('2026-01-01T03:00:00Z') },
      binBudget: 100,
      filter: { device_id: 17617 },
    })
    expect(plan.outputTier.name).toBe('h1')
    expect(plan.segments).toHaveLength(1)
    expect(plan.segments[0]!.keys).toEqual(['awair-17617/h1/2026-01.parquet'])

    const shardRows = await Promise.all(
      plan.segments.map(seg => fetchSegmentRows(pyramid.storage, seg.keys)),
    )

    const out = stitch({ pyramid, plan, shardRows })

    expect(out).toEqual([
      {
        ts: ms('2026-01-01T00:00:00Z'),
        device_id: 17617,
        temp_n: 60,
        temp_sum: 1200,
        temp_sumsq: 24500,
      },
      {
        ts: ms('2026-01-01T01:00:00Z'),
        device_id: 17617,
        temp_n: 60,
        temp_sum: 1260,
        temp_sumsq: 26800,
      },
      {
        ts: ms('2026-01-01T02:00:00Z'),
        device_id: 17617,
        temp_n: 60,
        temp_sum: 1320,
        temp_sumsq: 29200,
      },
    ])
  })
})

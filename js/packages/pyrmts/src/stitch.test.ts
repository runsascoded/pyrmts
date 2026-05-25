import { describe, expect, test } from 'vitest'
import { planQuery } from './planner.js'
import { stitch, type Row } from './stitch.js'
import type { Pyramid, Storage } from './types.js'

const mockStorage: Storage = {
  head: async () => null,
  getRange: async () => new Uint8Array(),
  get: async () => null,
  put: async () => {},
  list: async function* () {},
}

const awair: Pyramid = {
  storage: mockStorage,
  keyTemplate: 'awair-{device_id}/{tier}/{period}.parquet',
  axis: 'time',
  binCol: 'ts',
  dims: [{ name: 'device_id', type: 'int' }],
  metrics: [
    { name: 'temp', monoid: 'sum' },
    { name: 'co2', monoid: 'sum' },
  ],
  tiers: [
    { name: 'raw', bin: '1min', shard: '1mo' },
    { name: 'h1', bin: '1h', shard: '1mo' },
    { name: 'd1', bin: '1d', shard: '1y' },
    { name: 'mo1', bin: '1mo', shard: '1y' },
  ],
}

const d = (iso: string): Date => new Date(iso)
const ms = (iso: string): number => new Date(iso).getTime()

const filter = { device_id: 17617 }

// Build a sum-monoid row (n, sum, sumsq) for a metric, for compact test fixtures.
function sumRow(ts: number, deviceId: number, metrics: Record<string, [n: number, sum: number, sumsq: number]>): Row {
  const r: Row = { ts, device_id: deviceId }
  for (const [name, [n, s, sq]] of Object.entries(metrics)) {
    r[`${name}_n`] = n
    r[`${name}_sum`] = s
    r[`${name}_sumsq`] = sq
  }
  return r
}

describe('stitch: pass-through (no reaggregate)', () => {
  test('single segment with rows already at output granularity', () => {
    const plan = planQuery(awair, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-01T03:00:00Z') },
      binBudget: 100,
      filter,
    })
    // 3h range, 100 budget → h1.
    expect(plan.outputTier.name).toBe('h1')

    const rows: Row[] = [
      sumRow(ms('2026-01-01T00:00:00Z'), 17617, { temp: [60, 1200, 24500], co2: [60, 24000, 9700000] }),
      sumRow(ms('2026-01-01T01:00:00Z'), 17617, { temp: [60, 1260, 26800], co2: [60, 25000, 10500000] }),
      sumRow(ms('2026-01-01T02:00:00Z'), 17617, { temp: [60, 1320, 29200], co2: [60, 26000, 11400000] }),
    ]
    const out = stitch({ pyramid: awair, plan, shardRows: [rows] })
    expect(out).toEqual(rows)
  })

  test('filters out rows outside segment range', () => {
    const plan = planQuery(awair, {
      range: { from: d('2026-01-01T01:00:00Z'), to: d('2026-01-01T02:00:00Z') },
      binBudget: 100,
      filter,
    })
    const rows: Row[] = [
      sumRow(ms('2026-01-01T00:00:00Z'), 17617, { temp: [60, 1200, 24500], co2: [60, 24000, 9700000] }),
      sumRow(ms('2026-01-01T01:00:00Z'), 17617, { temp: [60, 1260, 26800], co2: [60, 25000, 10500000] }),
      sumRow(ms('2026-01-01T02:00:00Z'), 17617, { temp: [60, 1320, 29200], co2: [60, 26000, 11400000] }),
    ]
    const out = stitch({ pyramid: awair, plan, shardRows: [rows] })
    expect(out).toEqual([rows[1]])
  })
})

describe('stitch: reaggregate to coarser output', () => {
  test('coarsens 60 raw rows into one h1 row', () => {
    // Hand-build a plan with a single reaggregate segment.
    const plan = planQuery(awair, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-01T01:00:00Z') },
      binBudget: 1,                 // forces coarsest (h1 wouldn't fit 1 bin budget? 1 bin fits)
      filter,
    })
    // 1h range, 1 budget → h1 has 1 bin → h1 selected.
    expect(plan.outputTier.name).toBe('h1')

    // Build raw (1min) rows for the full hour. Each minute contributes 1 reading.
    const rawRows: Row[] = []
    for (let m = 0; m < 60; m++) {
      const ts = ms('2026-01-01T00:00:00Z') + m * 60_000
      rawRows.push(sumRow(ts, 17617, { temp: [1, 20, 400], co2: [1, 400, 160_000] }))
    }
    // Override segment shardTier so the stitcher reaggregates.
    plan.segments[0]!.shardTier = awair.tiers[0]!     // raw
    plan.segments[0]!.reaggregate = true

    const out = stitch({ pyramid: awair, plan, shardRows: [rawRows] })
    expect(out).toEqual([
      sumRow(ms('2026-01-01T00:00:00Z'), 17617, { temp: [60, 1200, 24000], co2: [60, 24000, 9_600_000] }),
    ])
  })

  test('groups by dim within an output bin (multi-device)', () => {
    const plan = planQuery(awair, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-01T01:00:00Z') },
      binBudget: 1,
      filter,
    })
    plan.segments[0]!.shardTier = awair.tiers[0]!
    plan.segments[0]!.reaggregate = true

    const rawRows: Row[] = []
    for (let m = 0; m < 60; m++) {
      const ts = ms('2026-01-01T00:00:00Z') + m * 60_000
      rawRows.push(sumRow(ts, 17617, { temp: [1, 20, 400], co2: [1, 400, 160_000] }))
      rawRows.push(sumRow(ts, 99999, { temp: [1, 30, 900], co2: [1, 500, 250_000] }))
    }
    const out = stitch({ pyramid: awair, plan, shardRows: [rawRows] })
    expect(out).toEqual([
      sumRow(ms('2026-01-01T00:00:00Z'), 17617, { temp: [60, 1200, 24000], co2: [60, 24000, 9_600_000] }),
      sumRow(ms('2026-01-01T00:00:00Z'), 99999, { temp: [60, 1800, 54000], co2: [60, 30000, 15_000_000] }),
    ])
  })
})

describe('stitch: multi-segment (output + reaggregated tail)', () => {
  test('h1 pass-through + raw-reaggregated tail produces uniform h1 output', () => {
    const plan = planQuery(awair, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-01T03:00:00Z') },
      binBudget: 100,
      filter,
      watermarks: {
        raw: d('2026-01-01T03:00:00Z'),
        h1:  d('2026-01-01T02:00:00Z'),  // h1 only built through 02:00
      },
    })
    expect(plan.outputTier.name).toBe('h1')
    expect(plan.segments).toHaveLength(2)

    // Segment 0: h1, [00:00, 02:00). Two h1 rows pre-aggregated.
    const h1Rows: Row[] = [
      sumRow(ms('2026-01-01T00:00:00Z'), 17617, { temp: [60, 1200, 24000], co2: [60, 24000, 9_600_000] }),
      sumRow(ms('2026-01-01T01:00:00Z'), 17617, { temp: [60, 1260, 26500], co2: [60, 25200, 10_600_000] }),
    ]
    // Segment 1: raw, [02:00, 03:00). 60 raw rows.
    const rawRows: Row[] = []
    for (let m = 0; m < 60; m++) {
      const ts = ms('2026-01-01T02:00:00Z') + m * 60_000
      rawRows.push(sumRow(ts, 17617, { temp: [1, 21, 441], co2: [1, 410, 168_100] }))
    }

    const out = stitch({ pyramid: awair, plan, shardRows: [h1Rows, rawRows] })
    expect(out).toEqual([
      h1Rows[0],
      h1Rows[1],
      sumRow(ms('2026-01-01T02:00:00Z'), 17617, { temp: [60, 1260, 26460], co2: [60, 24600, 10_086_000] }),
    ])
  })
})

describe('stitch: histogram monoid', () => {
  // ctbk-style avail pyramid: station_id dim, single histogram metric.
  const availPyramid: Pyramid = {
    storage: mockStorage,
    keyTemplate: 'avail/{tier}/{period}.parquet',
    axis: 'time',
    binCol: 'ts',
    dims: [{ name: 'station_id', type: 'string' }],
    metrics: [{ name: 'states', monoid: 'histogram' }],
    tiers: [
      { name: 'raw', bin: '1min', shard: '1h' },
      { name: 'h1', bin: '1h', shard: '1mo' },
    ],
  }

  test('reaggregates per-minute histograms into one h1 histogram', () => {
    const plan = planQuery(availPyramid, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-01T01:00:00Z') },
      binBudget: 1,    // forces h1
    })
    expect(plan.outputTier.name).toBe('h1')

    plan.segments[0]!.shardTier = availPyramid.tiers[0]!
    plan.segments[0]!.reaggregate = true

    // 60 raw rows for station 'A', each contributing one state-minute. Cycle
    // the state through 0..4 → final histogram is {0:12, 1:12, 2:12, 3:12, 4:12}.
    const rows: Row[] = []
    for (let m = 0; m < 60; m++) {
      rows.push({
        ts: ms('2026-01-01T00:00:00Z') + m * 60_000,
        station_id: 'A',
        states: { [String(m % 5)]: 1 },
      })
    }
    const out = stitch({ pyramid: availPyramid, plan, shardRows: [rows] })
    expect(out).toEqual([
      {
        ts: ms('2026-01-01T00:00:00Z'),
        station_id: 'A',
        states: { '0': 12, '1': 12, '2': 12, '3': 12, '4': 12 },
      },
    ])
  })

  test('init detaches the first row\'s histogram from the source (no aliasing corruption)', () => {
    const plan = planQuery(availPyramid, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-01T01:00:00Z') },
      binBudget: 1,
    })
    plan.segments[0]!.shardTier = availPyramid.tiers[0]!
    plan.segments[0]!.reaggregate = true

    const firstHist = { '0': 5 }
    const rows: Row[] = [
      { ts: ms('2026-01-01T00:00:00Z'), station_id: 'A', states: firstHist },
      { ts: ms('2026-01-01T00:30:00Z'), station_id: 'A', states: { '1': 3 } },
    ]
    stitch({ pyramid: availPyramid, plan, shardRows: [rows] })
    // The original source row's histogram must not have been mutated by the
    // combine step (would have happened if init didn't shallow-copy).
    expect(firstHist).toEqual({ '0': 5 })
  })

  test('groups by dim — separate histograms per station_id', () => {
    const plan = planQuery(availPyramid, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-01T01:00:00Z') },
      binBudget: 1,
    })
    plan.segments[0]!.shardTier = availPyramid.tiers[0]!
    plan.segments[0]!.reaggregate = true

    const rows: Row[] = [
      { ts: ms('2026-01-01T00:00:00Z'), station_id: 'A', states: { '0': 5 } },
      { ts: ms('2026-01-01T00:10:00Z'), station_id: 'A', states: { '0': 3, '1': 2 } },
      { ts: ms('2026-01-01T00:00:00Z'), station_id: 'B', states: { '2': 7 } },
    ]
    const out = stitch({ pyramid: availPyramid, plan, shardRows: [rows] })
    expect(out).toEqual([
      { ts: ms('2026-01-01T00:00:00Z'), station_id: 'A', states: { '0': 8, '1': 2 } },
      { ts: ms('2026-01-01T00:00:00Z'), station_id: 'B', states: { '2': 7 } },
    ])
  })
})

describe('stitch: errors', () => {
  test('throws if shardRows length mismatches segments', () => {
    const plan = planQuery(awair, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-01T03:00:00Z') },
      binBudget: 100,
      filter,
    })
    expect(() => stitch({ pyramid: awair, plan, shardRows: [] }))
      .toThrow('stitch: shardRows length 0 ≠ segments 1')
  })

  test('throws on row missing the bin column', () => {
    const plan = planQuery(awair, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-01T03:00:00Z') },
      binBudget: 100,
      filter,
    })
    const bad: Row = { device_id: 17617, temp_n: 1, temp_sum: 20, temp_sumsq: 400 }
    expect(() => stitch({ pyramid: awair, plan, shardRows: [[bad]] }))
      .toThrow("stitch: row missing numeric 'ts' column (got undefined)")
  })
})

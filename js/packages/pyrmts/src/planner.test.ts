import { describe, expect, test } from 'vitest'
import { planQuery } from './planner.js'
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

// Project a plan to a comparable plain-data shape.
interface SegmentSnapshot {
  from: string
  to: string
  tier: string
  keys: string[]
  reaggregate: boolean
}

function segments(plan: ReturnType<typeof planQuery>): SegmentSnapshot[] {
  return plan.segments.map(s => ({
    from: s.from.toISOString(),
    to: s.to.toISOString(),
    tier: s.shardTier.name,
    keys: s.keys,
    reaggregate: s.reaggregate,
  }))
}

describe('planQuery: tier selection', () => {
  test('picks finest tier whose bin count fits the budget', () => {
    // 1mo range: raw=44640, h1=744, d1=31, mo1=1. Budget 1000 → h1.
    const plan = planQuery(awair, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-02-01T00:00:00Z') },
      binBudget: 1000,
      filter: { device_id: 17617 },
    })
    expect(plan.outputTier.name).toBe('h1')
    expect(plan.outputBin).toBe('1h')
  })

  test('falls back to coarsest tier when even it exceeds the budget', () => {
    // 1y range: mo1=12. Budget=5 → no tier fits → fall back to mo1.
    const plan = planQuery(awair, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2027-01-01T00:00:00Z') },
      binBudget: 5,
      filter: { device_id: 17617 },
    })
    expect(plan.outputTier.name).toBe('mo1')
  })

  test('picks raw when range is small enough', () => {
    // 30min range at 1min: 30 bins. Budget 100 → raw.
    const plan = planQuery(awair, {
      range: { from: d('2026-05-24T17:00:00Z'), to: d('2026-05-24T17:30:00Z') },
      binBudget: 100,
      filter: { device_id: 17617 },
    })
    expect(plan.outputTier.name).toBe('raw')
  })
})

describe('planQuery: segmentation (no watermarks)', () => {
  test('single segment covers full range from output tier', () => {
    const plan = planQuery(awair, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-02-01T00:00:00Z') },
      binBudget: 1000,
      filter: { device_id: 17617 },
    })
    expect(segments(plan)).toEqual([
      {
        from: '2026-01-01T00:00:00.000Z',
        to: '2026-02-01T00:00:00.000Z',
        tier: 'h1',
        keys: ['awair-17617/h1/2026-01.parquet'],
        reaggregate: false,
      },
    ])
    expect(plan.authoritativeEnd).toBe(null)
  })

  test('emits multiple shard keys when range spans shard boundaries', () => {
    // 2mo range at h1 (1mo shards) → 2 keys.
    const plan = planQuery(awair, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-03-01T00:00:00Z') },
      binBudget: 2000,
      filter: { device_id: 17617 },
    })
    expect(segments(plan)).toEqual([
      {
        from: '2026-01-01T00:00:00.000Z',
        to: '2026-03-01T00:00:00.000Z',
        tier: 'h1',
        keys: [
          'awair-17617/h1/2026-01.parquet',
          'awair-17617/h1/2026-02.parquet',
        ],
        reaggregate: false,
      },
    ])
  })
})

describe('planQuery: watermark-driven segmentation', () => {
  test('refines tail from finer tier when output watermark is mid-range', () => {
    const plan = planQuery(awair, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-02-01T00:00:00Z') },
      binBudget: 1000,
      filter: { device_id: 17617 },
      watermarks: {
        raw: d('2026-01-20T00:00:00Z'),
        h1: d('2026-01-15T00:00:00Z'),
      },
    })
    expect(segments(plan)).toEqual([
      {
        from: '2026-01-01T00:00:00.000Z',
        to: '2026-01-15T00:00:00.000Z',
        tier: 'h1',
        keys: ['awair-17617/h1/2026-01.parquet'],
        reaggregate: false,
      },
      {
        from: '2026-01-15T00:00:00.000Z',
        to: '2026-01-20T00:00:00.000Z',
        tier: 'raw',
        keys: ['awair-17617/raw/2026-01.parquet'],
        reaggregate: true,
      },
    ])
    expect(plan.authoritativeEnd?.toISOString()).toBe('2026-01-20T00:00:00.000Z')
  })

  test('walks through multiple finer tiers when each has progressively-newer watermark', () => {
    // 17mo range, budget 10000 → d1 is the finest that fits (~510 bins; h1 ~12k).
    const plan = planQuery(awair, {
      range: { from: d('2025-01-01T00:00:00Z'), to: d('2026-05-24T18:00:00Z') },
      binBudget: 10000,
      filter: { device_id: 17617 },
      watermarks: {
        mo1: d('2025-02-01T00:00:00Z'),
        d1:  d('2026-05-01T00:00:00Z'),
        h1:  d('2026-05-24T16:00:00Z'),
        raw: d('2026-05-24T16:55:00Z'),
      },
    })
    expect(plan.outputTier.name).toBe('d1')
    expect(segments(plan)).toEqual([
      {
        from: '2025-01-01T00:00:00.000Z',
        to:   '2026-05-01T00:00:00.000Z',
        tier: 'd1',
        keys: [
          'awair-17617/d1/2025.parquet',
          'awair-17617/d1/2026.parquet',
        ],
        reaggregate: false,
      },
      {
        from: '2026-05-01T00:00:00.000Z',
        to:   '2026-05-24T16:00:00.000Z',
        tier: 'h1',
        keys: ['awair-17617/h1/2026-05.parquet'],
        reaggregate: true,
      },
      {
        from: '2026-05-24T16:00:00.000Z',
        to:   '2026-05-24T16:55:00.000Z',
        tier: 'raw',
        keys: ['awair-17617/raw/2026-05.parquet'],
        reaggregate: true,
      },
    ])
    expect(plan.authoritativeEnd?.toISOString()).toBe('2026-05-24T16:55:00.000Z')
  })

  test('clamps a coarser watermark that exceeds a finer one (monotonicity)', () => {
    // h1 declared watermark > raw declared watermark (inconsistent). Effective
    // h1 watermark must be ≤ raw watermark — h1 can't have data past where
    // raw exists.
    const plan = planQuery(awair, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-02-01T00:00:00Z') },
      binBudget: 1000,
      filter: { device_id: 17617 },
      watermarks: {
        raw: d('2026-01-10T00:00:00Z'),
        h1:  d('2026-01-25T00:00:00Z'), // declared past raw's
      },
    })
    // h1 effective = min(h1 declared, raw effective) = 2026-01-10.
    expect(segments(plan)).toEqual([
      {
        from: '2026-01-01T00:00:00.000Z',
        to:   '2026-01-10T00:00:00.000Z',
        tier: 'h1',
        keys: ['awair-17617/h1/2026-01.parquet'],
        reaggregate: false,
      },
    ])
    expect(plan.authoritativeEnd?.toISOString()).toBe('2026-01-10T00:00:00.000Z')
  })

  test('omits authoritativeEnd when raw watermark covers the query', () => {
    const plan = planQuery(awair, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-02-01T00:00:00Z') },
      binBudget: 1000,
      filter: { device_id: 17617 },
      watermarks: {
        raw: d('2030-01-01T00:00:00Z'),
      },
    })
    expect(plan.authoritativeEnd).toBe(null)
  })
})

describe('planQuery: errors', () => {
  test('throws on step-axis pyramid', () => {
    const stepPyramid: Pyramid = { ...awair, axis: 'step' }
    expect(() => planQuery(stepPyramid, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-02-01T00:00:00Z') },
      binBudget: 100,
    })).toThrow("planQuery: axis 'step' not yet implemented (only 'time')")
  })

  test('throws on empty range', () => {
    expect(() => planQuery(awair, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-01T00:00:00Z') },
      binBudget: 100,
      filter: { device_id: 1 },
    })).toThrow(/^planQuery: empty range/)
  })

  test('throws on pyramid with no tiers', () => {
    const empty: Pyramid = { ...awair, tiers: [] }
    expect(() => planQuery(empty, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-02-01T00:00:00Z') },
      binBudget: 100,
    })).toThrow('planQuery: pyramid has no tiers')
  })

  test('throws on missing key-template filter value', () => {
    expect(() => planQuery(awair, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-02-01T00:00:00Z') },
      binBudget: 1000,
      // no `filter.device_id` supplied
    })).toThrow('planQuery: missing key template value for {device_id}')
  })
})

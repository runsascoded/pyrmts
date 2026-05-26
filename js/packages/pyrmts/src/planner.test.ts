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

describe('planQuery: earliestWatermarks', () => {
  test('clamps the leading segment when query begins before the earliest', () => {
    // Raw earliest at Jan-15 → segment for [Jan-01, Feb-01) starts at Jan-15.
    const plan = planQuery(awair, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-02-01T00:00:00Z') },
      binBudget: 1000,
      filter: { device_id: 17617 },
      earliestWatermarks: { raw: d('2026-01-15T00:00:00Z') },
    })
    expect(segments(plan)).toEqual([
      {
        from: '2026-01-15T00:00:00.000Z',
        to:   '2026-02-01T00:00:00.000Z',
        tier: 'h1',
        keys: ['awair-17617/h1/2026-01.parquet'],
        reaggregate: false,
      },
    ])
  })

  test('omits a tier entirely when its earliest exceeds its segment end', () => {
    // h1 watermark @ Jan-15 → segment 0 wants [Jan-01, Jan-15) on h1.
    // h1 earliest @ Jan-20 → entire segment is before earliest, drop it.
    // raw segment [Jan-15, Jan-25) still emitted (raw earliest @ Jan-10).
    const plan = planQuery(awair, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-25T00:00:00Z') },
      binBudget: 1000,
      filter: { device_id: 17617 },
      watermarks: {
        raw: d('2026-01-25T00:00:00Z'),
        h1: d('2026-01-15T00:00:00Z'),
      },
      earliestWatermarks: {
        raw: d('2026-01-10T00:00:00Z'),
        h1: d('2026-01-20T00:00:00Z'),
      },
    })
    expect(segments(plan)).toEqual([
      {
        from: '2026-01-15T00:00:00.000Z',
        to:   '2026-01-25T00:00:00.000Z',
        tier: 'raw',
        keys: ['awair-17617/raw/2026-01.parquet'],
        reaggregate: true,
      },
    ])
  })

  test('propagates finer earliest up to coarser tiers (finest-bound monotonicity)', () => {
    // Declare earliest at raw only; h1 / d1 / mo1 inherit. Query begins way
    // before any tier has data → output tier (mo1, given the 5-bin budget over
    // a year) gets clamped to raw's earliest.
    const plan = planQuery(awair, {
      range: { from: d('2025-01-01T00:00:00Z'), to: d('2026-01-01T00:00:00Z') },
      binBudget: 5,
      filter: { device_id: 17617 },
      earliestWatermarks: { raw: d('2025-10-01T00:00:00Z') },
    })
    expect(plan.outputTier.name).toBe('mo1')
    expect(segments(plan)).toEqual([
      {
        from: '2025-10-01T00:00:00.000Z',
        to:   '2026-01-01T00:00:00.000Z',
        tier: 'mo1',
        keys: ['awair-17617/mo1/2025.parquet'],
        reaggregate: false,
      },
    ])
  })

  test('coarser declared earliest > finer earliest wins for that coarser tier', () => {
    // raw earliest @ Jan-01; mo1 declared earliest @ Mar-01 (coarser tier
    // didn't backfill the earliest months). The mo1 segment should start at
    // Mar-01.
    const plan = planQuery(awair, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-06-01T00:00:00Z') },
      binBudget: 4,    // mo1 = 5 bins, exceeds → mo1 fallback (only tier within budget? no — 5 > 4 → mo1 still fallback)
      filter: { device_id: 17617 },
      earliestWatermarks: {
        raw: d('2026-01-01T00:00:00Z'),
        mo1: d('2026-03-01T00:00:00Z'),
      },
    })
    expect(plan.outputTier.name).toBe('mo1')
    expect(segments(plan)).toEqual([
      {
        from: '2026-03-01T00:00:00.000Z',
        to:   '2026-06-01T00:00:00.000Z',
        tier: 'mo1',
        keys: ['awair-17617/mo1/2026.parquet'],
        reaggregate: false,
      },
    ])
  })

  test('no clamp when query starts after all earliest watermarks', () => {
    const plan = planQuery(awair, {
      range: { from: d('2026-01-15T00:00:00Z'), to: d('2026-02-01T00:00:00Z') },
      binBudget: 1000,
      filter: { device_id: 17617 },
      earliestWatermarks: { raw: d('2026-01-01T00:00:00Z') },
    })
    expect(segments(plan)).toEqual([
      {
        from: '2026-01-15T00:00:00.000Z',
        to:   '2026-02-01T00:00:00.000Z',
        tier: 'h1',
        keys: ['awair-17617/h1/2026-01.parquet'],
        reaggregate: false,
      },
    ])
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

describe('planQuery: smoothing', () => {
  test('no smoothing → plan.smoothing is null, visibleRange == input range', () => {
    const plan = planQuery(awair, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-02T00:00:00Z') },
      binBudget: 100,
      filter: { device_id: 17617 },
    })
    expect(plan.smoothing).toBeNull()
    expect(plan.visibleRange.from.toISOString()).toBe('2026-01-01T00:00:00.000Z')
    expect(plan.visibleRange.to.toISOString()).toBe('2026-01-02T00:00:00.000Z')
  })

  test('explicit duration snaps to nearest nice width that divides outputBin (centered)', () => {
    // 1d range at budget 100 → outputBin = 1h (24 bins). `smooth=4h` → snap to 4h
    // (exact: 4 × 1h). Centered extension: lead=2, tail=1 (past-biased for even).
    const plan = planQuery(awair, {
      range: { from: d('2026-01-02T00:00:00Z'), to: d('2026-01-03T00:00:00Z') },
      binBudget: 100,
      filter: { device_id: 17617 },
      smoothing: '4h',
    })
    expect(plan.smoothing).toEqual({
      smoothBin: '4h',
      smoothBinCount: 4,
      smoothMode: 'centered',
      smoothSourceTier: 'h1',
    })
    // Extension: [02:00 - 2h, 03:00 + 1h) = [2026-01-01T22:00, 2026-01-03T01:00)
    expect(plan.segments[0]!.from.toISOString()).toBe('2026-01-01T22:00:00.000Z')
    expect(plan.segments[plan.segments.length - 1]!.to.toISOString()).toBe('2026-01-03T01:00:00.000Z')
    // Visible range stays as caller asked.
    expect(plan.visibleRange.from.toISOString()).toBe('2026-01-02T00:00:00.000Z')
    expect(plan.visibleRange.to.toISOString()).toBe('2026-01-03T00:00:00.000Z')
  })

  test('snaps off-nice request to nearest nice width', () => {
    // 1d at budget 100 → h1 (1h). `smooth=37min` < 1h → snap to 1h (smallest
    // candidate that's an integer multiple of the output bin).
    const plan = planQuery(awair, {
      range: { from: d('2026-01-02T00:00:00Z'), to: d('2026-01-03T00:00:00Z') },
      binBudget: 100,
      filter: { device_id: 17617 },
      smoothing: '37min',
    })
    expect(plan.smoothing?.smoothBin).toBe('1h')
    expect(plan.smoothing?.smoothBinCount).toBe(1)
  })

  test('auto picks multiplier × outputBin (default 50, snapped)', () => {
    // 1d at budget 100 → h1 (1h). 50 × 1h = 50h → snap nearest: closest nice
    // widths above 1h that divide 1h are 1h/2h/3h/4h/6h/8h/12h/1d/2d/3d/7d/14d/30d.
    // 50h closest → 2d (48h, dist 2h) vs 3d (72h, dist 22h) → 2d.
    const plan = planQuery(awair, {
      range: { from: d('2026-01-02T00:00:00Z'), to: d('2026-01-03T00:00:00Z') },
      binBudget: 100,
      filter: { device_id: 17617 },
      smoothing: { auto: true },
    })
    // 1d range, 1h output bin → 24 visible bins; maxCount = floor(24/4) = 6.
    // 50× ms target = 50h, snap nearest among {1h..6h} (count ≤ 6) = 6h.
    expect(plan.smoothing?.smoothBin).toBe('6h')
    expect(plan.smoothing?.smoothBinCount).toBe(6)
  })

  test('auto with explicit multiplier', () => {
    // 7d range at budget 1000 → h1 (168 bins). 25 × 1h = 25h → snap to 1d (24h,
    // closest among ≤ 42 = floor(168/4)).
    const plan = planQuery(awair, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-08T00:00:00Z') },
      binBudget: 1000,
      filter: { device_id: 17617 },
      smoothing: { auto: true, multiplier: 25 },
    })
    expect(plan.smoothing?.smoothBin).toBe('1d')
    expect(plan.smoothing?.smoothBinCount).toBe(24)
  })

  test('trailing mode extends only the leading edge', () => {
    // smooth=2h trailing at 1h output: lead = N-1 = 1, tail = 0.
    const plan = planQuery(awair, {
      range: { from: d('2026-01-02T00:00:00Z'), to: d('2026-01-03T00:00:00Z') },
      binBudget: 100,
      filter: { device_id: 17617 },
      smoothing: '2h',
      smoothMode: 'trailing',
    })
    expect(plan.smoothing?.smoothMode).toBe('trailing')
    expect(plan.segments[0]!.from.toISOString()).toBe('2026-01-01T23:00:00.000Z')
    expect(plan.segments[plan.segments.length - 1]!.to.toISOString()).toBe('2026-01-03T00:00:00.000Z')
  })

  test('window clamped to max(1, floor(visibleBins/4)) to prevent pathological cases', () => {
    // 4h range at budget 100 → raw (1min, 240 bins). visibleBins/4 = 60.
    // smoothing 30d → way past clamp → smoothBinCount capped at 60 → snap nearest
    // nice width whose count ≤ 60 = 30min (count 30) or 1h (count 60). Closest
    // to 30d is 1h (bigger).
    const plan = planQuery(awair, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-01T04:00:00Z') },
      binBudget: 100,
      filter: { device_id: 17617 },
      smoothing: '30d',
    })
    expect(plan.smoothing?.smoothBinCount).toBeLessThanOrEqual(60)
    expect(plan.smoothing?.smoothBinCount).toBeGreaterThanOrEqual(1)
  })

  test('earliestWatermark clamps the smoothing-buffer extension (degrades gracefully)', () => {
    // smoothing wants to extend ~2h before the visible from, but the earliest
    // watermark cuts it off — no error, just less context near the edge.
    const plan = planQuery(awair, {
      range: { from: d('2026-01-02T00:00:00Z'), to: d('2026-01-03T00:00:00Z') },
      binBudget: 100,
      filter: { device_id: 17617 },
      smoothing: '4h',
      earliestWatermarks: { h1: d('2026-01-02T00:00:00Z') },
    })
    // Without clamp, segment from would be 2026-01-01T22:00. With clamp it
    // shifts forward to the earliest watermark.
    expect(plan.segments[0]!.from.toISOString()).toBe('2026-01-02T00:00:00.000Z')
  })
})

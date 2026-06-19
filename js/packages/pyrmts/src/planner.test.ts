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

describe('planQuery: targetBin (ragged decomposition)', () => {
  // Pyramid used for ragged-decomp tests: closely-spaced fine-tier fixed-width
  // bins so DP packs them across phase classes within a 5min window.
  const ragged: Pyramid = {
    storage: mockStorage,
    keyTemplate: 'r/{tier}/{period}.parquet',
    axis: 'time',
    binCol: 'ts',
    dims: [],
    metrics: [{ name: 'v', monoid: 'sum' }],
    tiers: [
      { name: 't1', bin: '1min', shard: '1d' },
      { name: 't2', bin: '2min', shard: '1d' },
      { name: 't3', bin: '3min', shard: '1d' },
      { name: 't5', bin: '5min', shard: '1d' },
    ],
  }

  // Atomized view of segments for DP-output assertions: each entry is one
  // tier-bin atom (no coalescing). Use to compare against the expected
  // per-output-bin DP path.
  interface Atom {
    from: string
    to: string
    tier: string
  }
  function atoms(plan: ReturnType<typeof planQuery>): Atom[] {
    const out: Atom[] = []
    for (const s of plan.segments) {
      const tierMs = parseInt(/^(\d+)/.exec(s.shardTier.bin)![1]!, 10) * 60_000
      const fromMs = s.from.getTime()
      const toMs = s.to.getTime()
      for (let t = fromMs; t < toMs; t += tierMs) {
        out.push({
          from: new Date(t).toISOString(),
          to: new Date(t + tierMs).toISOString(),
          tier: s.shardTier.name,
        })
      }
    }
    return out
  }

  test('targetBin matching a stored tier exactly → outputTier set, single coalesced segment', () => {
    // targetBin=5min, eligible tiers={1,2,3,5}. DP per output bin picks 5min
    // (1 atom). After coalescing: one segment covering the full range.
    const plan = planQuery(ragged, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-01T00:15:00Z') },
      binBudget: 1000,
      targetBin: '5min',
    })
    expect(plan.outputBin).toBe('5min')
    expect(plan.outputTier?.name).toBe('t5')
    expect(segments(plan)).toEqual([{
      from: '2026-01-01T00:00:00.000Z',
      to:   '2026-01-01T00:15:00.000Z',
      tier: 't5',
      keys: ['r/t5/2026-01-01.parquet'],
      reaggregate: false,
    }])
  })

  test('targetBin not in tiers → outputTier omitted, DP packs per bin', () => {
    // targetBin=5min, eligible={1,2,3}. DP per bin (binStart at multiples of
    // 5min mod LCM(1,2,3,5)=30min, 6 distinct phases).
    // Phase analysis for the 6 /5min bins in [0min, 30min):
    //   [0,5):  3@0, 1@3, 1@4
    //   [5,10): 1@5, 3@6, 1@9
    //   [10,15): 1@10, 1@11, 3@12
    //   [15,20): 3@15, 1@18, 1@19
    //   [20,25): 1@20, 3@21, 1@24
    //   [25,30): 1@25, 1@26, 3@27
    const ragged3: Pyramid = {
      ...ragged,
      tiers: ragged.tiers.filter(t => t.name !== 't5' && t.name !== 't2'),
    }
    const plan = planQuery(ragged3, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-01T00:30:00Z') },
      binBudget: 1000,
      targetBin: '5min',
    })
    expect(plan.outputBin).toBe('5min')
    expect(plan.outputTier).toBeUndefined()
    expect(atoms(plan)).toEqual([
      { from: '2026-01-01T00:00:00.000Z', to: '2026-01-01T00:03:00.000Z', tier: 't3' },
      { from: '2026-01-01T00:03:00.000Z', to: '2026-01-01T00:04:00.000Z', tier: 't1' },
      { from: '2026-01-01T00:04:00.000Z', to: '2026-01-01T00:05:00.000Z', tier: 't1' },
      { from: '2026-01-01T00:05:00.000Z', to: '2026-01-01T00:06:00.000Z', tier: 't1' },
      { from: '2026-01-01T00:06:00.000Z', to: '2026-01-01T00:09:00.000Z', tier: 't3' },
      { from: '2026-01-01T00:09:00.000Z', to: '2026-01-01T00:10:00.000Z', tier: 't1' },
      { from: '2026-01-01T00:10:00.000Z', to: '2026-01-01T00:11:00.000Z', tier: 't1' },
      { from: '2026-01-01T00:11:00.000Z', to: '2026-01-01T00:12:00.000Z', tier: 't1' },
      { from: '2026-01-01T00:12:00.000Z', to: '2026-01-01T00:15:00.000Z', tier: 't3' },
      { from: '2026-01-01T00:15:00.000Z', to: '2026-01-01T00:18:00.000Z', tier: 't3' },
      { from: '2026-01-01T00:18:00.000Z', to: '2026-01-01T00:19:00.000Z', tier: 't1' },
      { from: '2026-01-01T00:19:00.000Z', to: '2026-01-01T00:20:00.000Z', tier: 't1' },
      { from: '2026-01-01T00:20:00.000Z', to: '2026-01-01T00:21:00.000Z', tier: 't1' },
      { from: '2026-01-01T00:21:00.000Z', to: '2026-01-01T00:24:00.000Z', tier: 't3' },
      { from: '2026-01-01T00:24:00.000Z', to: '2026-01-01T00:25:00.000Z', tier: 't1' },
      { from: '2026-01-01T00:25:00.000Z', to: '2026-01-01T00:26:00.000Z', tier: 't1' },
      { from: '2026-01-01T00:26:00.000Z', to: '2026-01-01T00:27:00.000Z', tier: 't1' },
      { from: '2026-01-01T00:27:00.000Z', to: '2026-01-01T00:30:00.000Z', tier: 't3' },
    ])
  })

  test('coalesces adjacent same-tier atoms across output-bin boundaries', () => {
    // The /3min atoms at [12,15) (in bin [10,15)) and [15,18) (in bin [15,20))
    // are adjacent same-tier; they coalesce to one [12,18) segment spanning
    // two output bins. The stitcher derives output bin from row.ts at
    // floor-to-5min, so this is fine.
    const ragged3: Pyramid = {
      ...ragged,
      tiers: ragged.tiers.filter(t => t.name !== 't5' && t.name !== 't2'),
    }
    const plan = planQuery(ragged3, {
      range: { from: d('2026-01-01T00:10:00Z'), to: d('2026-01-01T00:20:00Z') },
      binBudget: 1000,
      targetBin: '5min',
    })
    // Coalesced segments for [10,20):
    //   t1 [10,12) – bin [10,15) atoms 1@10, 1@11
    //   t3 [12,18) – coalesced from 3@12 (bin [10,15)) and 3@15 (bin [15,20))
    //   t1 [18,20) – bin [15,20) atoms 1@18, 1@19
    expect(segments(plan)).toEqual([
      {
        from: '2026-01-01T00:10:00.000Z',
        to:   '2026-01-01T00:12:00.000Z',
        tier: 't1',
        keys: ['r/t1/2026-01-01.parquet'],
        reaggregate: true,
      },
      {
        from: '2026-01-01T00:12:00.000Z',
        to:   '2026-01-01T00:18:00.000Z',
        tier: 't3',
        keys: ['r/t3/2026-01-01.parquet'],
        reaggregate: true,
      },
      {
        from: '2026-01-01T00:18:00.000Z',
        to:   '2026-01-01T00:20:00.000Z',
        tier: 't1',
        keys: ['r/t1/2026-01-01.parquet'],
        reaggregate: true,
      },
    ])
  })

  test('DP beats greedy when coarsest-first would force more atoms (4+4+1 vs 6+1+1+1)', () => {
    // tiers={1,4,6}, target=9min at bin start = 0 (6-aligned). Greedy
    // coarsest-first picks 6@0 then can only fill 3 × 1min → 4 atoms total.
    // DP picks 4@0, 4@4, 1@8 → 3 atoms. Asserting 3-atom result confirms DP.
    const ragged9: Pyramid = {
      ...ragged,
      tiers: [
        { name: 't1', bin: '1min', shard: '1d' },
        { name: 't4', bin: '4min', shard: '1d' },
        { name: 't6', bin: '6min', shard: '1d' },
      ],
    }
    const plan = planQuery(ragged9, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-01T00:09:00Z') },
      binBudget: 1000,
      targetBin: '9min',
    })
    expect(atoms(plan)).toEqual([
      { from: '2026-01-01T00:00:00.000Z', to: '2026-01-01T00:04:00.000Z', tier: 't4' },
      { from: '2026-01-01T00:04:00.000Z', to: '2026-01-01T00:08:00.000Z', tier: 't4' },
      { from: '2026-01-01T00:08:00.000Z', to: '2026-01-01T00:09:00.000Z', tier: 't1' },
    ])
  })

  test('watermark restricts coarser tiers to the older part of the range', () => {
    // tiers={1m, 5m}, target=5m, range covers two 5m bins. Watermark for t5
    // ends mid-range → first bin can use t5; second bin can't, falls back
    // to 5 × t1.
    const ragged2: Pyramid = {
      ...ragged,
      tiers: [
        { name: 't1', bin: '1min', shard: '1d' },
        { name: 't5', bin: '5min', shard: '1d' },
      ],
    }
    const plan = planQuery(ragged2, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-01T00:10:00Z') },
      binBudget: 1000,
      targetBin: '5min',
      watermarks: { t5: d('2026-01-01T00:05:00Z') },
    })
    expect(atoms(plan)).toEqual([
      { from: '2026-01-01T00:00:00.000Z', to: '2026-01-01T00:05:00.000Z', tier: 't5' },
      { from: '2026-01-01T00:05:00.000Z', to: '2026-01-01T00:06:00.000Z', tier: 't1' },
      { from: '2026-01-01T00:06:00.000Z', to: '2026-01-01T00:07:00.000Z', tier: 't1' },
      { from: '2026-01-01T00:07:00.000Z', to: '2026-01-01T00:08:00.000Z', tier: 't1' },
      { from: '2026-01-01T00:08:00.000Z', to: '2026-01-01T00:09:00.000Z', tier: 't1' },
      { from: '2026-01-01T00:09:00.000Z', to: '2026-01-01T00:10:00.000Z', tier: 't1' },
    ])
  })

  test('throws when no eligible tier has bin ≤ targetBin', () => {
    // tiers={1h, 1d}; targetBin=5min — both are coarser than the target, so
    // no decomposition is possible at all.
    const coarse: Pyramid = {
      ...ragged,
      tiers: [
        { name: 'h1', bin: '1h', shard: '1mo' },
        { name: 'd1', bin: '1d', shard: '1y' },
      ],
    }
    expect(() => planQuery(coarse, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-01T00:30:00Z') },
      binBudget: 1000,
      targetBin: '5min',
    })).toThrow(/no tier with fixed-width bin ≤ targetBin/)
  })

  test('throws when gcd of eligible tier widths does not divide targetBin', () => {
    // tiers={2min, 4min}; targetBin=5min. gcd=2 doesn't divide 5 → no
    // integer linear combination of tier widths equals 5, decomposition
    // impossible.
    const noOdd: Pyramid = {
      ...ragged,
      tiers: [
        { name: 't2', bin: '2min', shard: '1d' },
        { name: 't4', bin: '4min', shard: '1d' },
      ],
    }
    expect(() => planQuery(noOdd, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-01T00:30:00Z') },
      binBudget: 1000,
      targetBin: '5min',
    })).toThrow(/no decomposition of targetBin '5min'.*gcd 120000 doesn't divide 300000/)
  })

  test('throws for calendar-variable targetBin (mo/y)', () => {
    expect(() => planQuery(awair, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-03-01T00:00:00Z') },
      binBudget: 1000,
      targetBin: '1mo',
      filter: { device_id: 17617 },
    })).toThrow(/calendar-variable/)
  })

  test('throws on a specific bin where alignment dead-ends (gcd ok but per-bin DP fails)', () => {
    // tiers={2min, 3min}; targetBin=5min. gcd=1 divides 5, but per-bin DP
    // fails: from cursor=0, the strict-equality alignment rule leaves no
    // path to cursor=5 (every step lands on 2 or 4, with no tier of width 1).
    const noOne: Pyramid = {
      ...ragged,
      tiers: [
        { name: 't2', bin: '2min', shard: '1d' },
        { name: 't3', bin: '3min', shard: '1d' },
      ],
    }
    expect(() => planQuery(noOne, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-01T00:05:00Z') },
      binBudget: 1000,
      targetBin: '5min',
    })).toThrow(/cannot decompose output bin/)
  })

  test('smoothing snaps against targetBin in ragged mode', () => {
    // targetBin=5min, smoothing=15min → snap count 3 (15/5 = 3).
    const ragged3: Pyramid = {
      ...ragged,
      tiers: ragged.tiers.filter(t => t.name !== 't5' && t.name !== 't2'),
    }
    const plan = planQuery(ragged3, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-01T01:00:00Z') },
      binBudget: 1000,
      targetBin: '5min',
      smoothing: '15min',
    })
    expect(plan.smoothing?.smoothBin).toBe('15min')
    expect(plan.smoothing?.smoothBinCount).toBe(3)
    // No outputTier → smoothSourceTier carries a placeholder distinguishing
    // it from a real tier name; consumers needing a real tier need to read
    // segments[].shardTier instead.
    expect(plan.smoothing?.smoothSourceTier).toBe('<ragged:5min>')
  })
})

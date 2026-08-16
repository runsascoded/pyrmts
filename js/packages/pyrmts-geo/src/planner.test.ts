import { getResolution, latLngToCell } from 'h3-js'
import { memStorage, type Pyramid, type RecordedShard } from 'pyrmts'
import { describe, expect, test } from 'vitest'
import { h3Index } from './h3-index.js'
import { filterCellsAndRes, planGeoQuery, planGeoQueryFromInventory } from './planner.js'
import { s2Index } from './s2-index.js'
import type { GeoPyramid, SpatialIndex } from './spatial-index.js'

const d = (iso: string): Date => new Date(iso)
const mockStorage = memStorage()

// NYC-ish bbox (small enough to keep cell counts manageable in tests).
const NYC: { minLat: number; maxLat: number; minLng: number; maxLng: number } = {
  minLat: 40.70,
  maxLat: 40.78,
  minLng: -74.02,
  maxLng: -73.96,
}

// H3-flavored fixture: these tests pin H3 cell tokens/resolutions, so they
// pass `h3Index` explicitly (there is no default backend any more).
function ctbkPyramid(geoResolutions: number[]): GeoPyramid {
  return {
    storage: mockStorage,
    keyTemplate: 'trips/{tier}/{period}.parquet',
    axis: 'time',
    binCol: 'ts',
    dims: [
      { name: 'station_id', type: 'string' },
      { name: 'rideable', type: 'string' },
    ],
    metrics: [{ name: 'count', monoid: 'count' }],
    tiers: [
      { name: 'h1', bin: '1h', shards: ['1mo'] },
      { name: 'd1', bin: '1d', shards: ['1y'] },
      { name: 'mo1', bin: '1mo', shards: ['1y'] },
    ],
    geo: { cellCol: 'h3_cell', resolutions: geoResolutions, index: h3Index },
  }
}

// The standalone `bboxToCells(bbox, level)` helper was deleted along with
// the H3 default — it was an H3-only wrapper with no callers. Backends
// expose `bboxToCells` on the `SpatialIndex` itself; these cases now
// exercise it there.
describe('h3Index.bboxToCells', () => {
  test('returns cells at the requested resolution when bbox is large enough', () => {
    const cells = h3Index.bboxToCells(NYC, 7)
    expect(cells.length).toBeGreaterThan(0)
    expect(cells.every(c => getResolution(c) === 7)).toBe(true)
  })

  test('returns more cells at finer resolution', () => {
    const coarse = h3Index.bboxToCells(NYC, 7)
    const fine = h3Index.bboxToCells(NYC, 9)
    expect(fine.length).toBeGreaterThan(coarse.length)
  })

  test('returns empty when bbox is smaller than the resolution\'s centroid spacing', () => {
    // NYC bbox is ~6×9 km; res 4 hexes average ~1770 km². No centroid lands inside.
    expect(h3Index.bboxToCells(NYC, 4)).toEqual([])
  })
})

describe('planGeoQuery: resolution selection', () => {
  test('picks finest resolution whose cell count fits cellBudget', () => {
    // At NYC scale: res 9 yields ~hundreds; res 7 yields ~tens; res 5 yields ~1.
    const pyramid = ctbkPyramid([9, 7, 5])
    const plan = planGeoQuery(pyramid, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-02T00:00:00Z') },
      binBudget: 100,
      bbox: NYC,
      cellBudget: 30,    // res 7 should fit (~10s); res 9 won't
    })
    expect(plan.outputRes).toBe(7)
    expect(plan.outputCells.length).toBeLessThanOrEqual(30)
    expect(plan.outputCells.every(c => getResolution(c) === 7)).toBe(true)
  })

  test('falls back to coarsest resolution when nothing fits the budget', () => {
    const pyramid = ctbkPyramid([12, 9, 7])
    const plan = planGeoQuery(pyramid, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-02T00:00:00Z') },
      binBudget: 100,
      bbox: NYC,
      cellBudget: 1,    // no resolution fits 1 cell over NYC
    })
    expect(plan.outputRes).toBe(7)    // coarsest available
    expect(plan.outputCells.length).toBeGreaterThan(0)
  })

  test('skips empty-cell resolutions; falls back to bbox-center cell when no res has data', () => {
    // Tiny bbox + coarse-only resolutions → every resolution yields 0 cells.
    const tinyBbox = { minLat: 40.74, maxLat: 40.741, minLng: -73.99, maxLng: -73.989 }
    const pyramid = ctbkPyramid([3, 2])
    const plan = planGeoQuery(pyramid, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-02T00:00:00Z') },
      binBudget: 100,
      bbox: tinyBbox,
      cellBudget: 100,
    })
    expect(plan.outputRes).toBe(2)
    expect(plan.outputCells).toHaveLength(1)
    expect(getResolution(plan.outputCells[0]!)).toBe(2)
  })

  test('throws on pyramid with no geo config', () => {
    const pyramid: Pyramid = { ...ctbkPyramid([9]) }
    delete pyramid.geo
    expect(() => planGeoQuery(pyramid, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-02T00:00:00Z') },
      binBudget: 100,
      bbox: NYC,
      cellBudget: 100,
    })).toThrow('planGeoQuery: pyramid has no `geo` config')
  })
})

describe('planGeoQuery: segment shape', () => {
  // Pyramid with 1d shards so a 1-day query aligns with the shard period
  // boundary (the planner's seal check requires `effective ≥ periodEnd`,
  // and `effective` gets clamped to `rangeTo`).
  function ctbkPyramid1d(geoResolutions: number[]): GeoPyramid {
    return {
      storage: mockStorage,
      keyTemplate: 'trips/{tier}/{period}.parquet',
      axis: 'time',
      binCol: 'ts',
      dims: [
        { name: 'station_id', type: 'string' },
        { name: 'rideable', type: 'string' },
      ],
      metrics: [{ name: 'count', monoid: 'count' }],
      tiers: [
        { name: 'h1', bin: '1h', shards: ['1d'] },
        { name: 'd1', bin: '1d', shards: ['1y'] },
        { name: 'mo1', bin: '1mo', shards: ['1y'] },
      ],
      geo: { cellCol: 'h3_cell', resolutions: geoResolutions, index: h3Index },
    }
  }

  test('inherits time-axis segmentation from planQuery; each segment carries the cell list', () => {
    const pyramid = ctbkPyramid1d([9, 7, 5])
    const plan = planGeoQuery(pyramid, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-02T00:00:00Z') },
      binBudget: 100,
      bbox: NYC,
      cellBudget: 30,
    })
    // 1-day range at h1 = 24 bins ≤ 100 → h1 selected. One segment, h1 shard.
    expect(plan.outputTier.name).toBe('h1')
    expect(plan.segments).toHaveLength(1)
    expect(plan.segments[0]!.shardTier.name).toBe('h1')
    expect(plan.segments[0]!.cells).toBe(plan.outputCells)
  })

  test('watermark-driven time refinement keeps the same cell list across segments', () => {
    const pyramid = ctbkPyramid1d([9, 7, 5])
    const plan = planGeoQuery(pyramid, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-02T00:00:00Z') },
      binBudget: 100,
      bbox: NYC,
      cellBudget: 30,
      watermarks: { 'h1@1d': d('2026-01-01T12:00:00Z') },
    })
    // h1 watermark at noon → segments split (h1 for first half, no finer
    // tier exists → tail dropped; depends on planQuery semantics).
    // Just check that whatever segments come back share the same cell list.
    for (const seg of plan.segments) {
      expect(seg.cells).toBe(plan.outputCells)
    }
  })
})

describe('filterCellsAndRes', () => {
  // Two NYC cells at different resolutions for fixtures.
  const cell7 = latLngToCell(40.74, -73.99, 7)
  const cell9 = latLngToCell(40.74, -73.99, 9)
  const cell9Other = latLngToCell(40.72, -73.97, 9)

  test('keeps only rows at the requested resolution + in the allowed set', () => {
    const rows: Array<Record<string, unknown>> = [
      { h3_cell: cell7, ts: 1, count: 10 },
      { h3_cell: cell9, ts: 1, count: 5 },
      { h3_cell: cell9Other, ts: 1, count: 3 },
    ]
    expect(filterCellsAndRes(rows, 'h3_cell', 9, [cell9], h3Index)).toEqual([
      { h3_cell: cell9, ts: 1, count: 5 },
    ])
  })

  test('drops rows at the wrong resolution even if the cell ID happens to match a string', () => {
    const rows: Array<Record<string, unknown>> = [
      { h3_cell: cell7, ts: 1, count: 10 },
      { h3_cell: cell9, ts: 1, count: 5 },
    ]
    // outputRes=9, only cell9 should survive
    expect(filterCellsAndRes(rows, 'h3_cell', 9, [cell7, cell9], h3Index)).toEqual([
      { h3_cell: cell9, ts: 1, count: 5 },
    ])
  })

  test('drops rows with non-string cell columns (defensive)', () => {
    const rows: Array<Record<string, unknown>> = [
      { h3_cell: null, ts: 1, count: 10 },
      { h3_cell: 42, ts: 1, count: 10 },
      { h3_cell: cell9, ts: 1, count: 5 },
    ]
    expect(filterCellsAndRes(rows, 'h3_cell', 9, [cell9], h3Index)).toEqual([
      { h3_cell: cell9, ts: 1, count: 5 },
    ])
  })
})

describe('planGeoQuery: s2-backed pyramid', () => {
  // Mirror the h3 fixture above but with `s2Index` declared explicitly.
  // S2 levels: 10 ~3km, 8 ~25km, 6 ~250km. Pick 10/8/6 for a NYC-fit
  // pyramid that roughly matches the h3 [9, 7, 5] tier widths.
  function ctbkPyramidS2(geoResolutions: number[]): GeoPyramid {
    return {
      storage: mockStorage,
      keyTemplate: 'trips/{tier}/{period}.parquet',
      axis: 'time',
      binCol: 'ts',
      dims: [{ name: 'station_id', type: 'string' }],
      metrics: [{ name: 'count', monoid: 'count' }],
      tiers: [
        { name: 'h1', bin: '1h', shards: ['1mo'] },
        { name: 'd1', bin: '1d', shards: ['1y'] },
        { name: 'mo1', bin: '1mo', shards: ['1y'] },
      ],
      geo: { cellCol: 's2_cell', resolutions: geoResolutions, index: s2Index },
    }
  }

  test('picks the finest s2 level whose bbox cell count fits the budget', () => {
    const pyramid = ctbkPyramidS2([12, 10, 8])
    const plan = planGeoQuery(pyramid, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-02T00:00:00Z') },
      binBudget: 100,
      bbox: NYC,
      cellBudget: 50,
    })
    expect(plan.outputCells.length).toBeGreaterThan(0)
    expect(plan.outputCells.length).toBeLessThanOrEqual(50)
    // Every output cell at the chosen S2 level
    for (const cell of plan.outputCells) {
      expect(s2Index.cellLevel(cell)).toBe(plan.outputRes)
    }
  })

  test('threads s2Index through to filterCellsAndRes (different resolutions correctly dropped)', () => {
    const pyramid = ctbkPyramidS2([12, 10, 8])
    const plan = planGeoQuery(pyramid, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-02T00:00:00Z') },
      binBudget: 100,
      bbox: NYC,
      cellBudget: 1024,
    })
    const cellAtPlan = plan.outputCells[0]!
    const cellAtCoarser = s2Index.cellToParent(cellAtPlan, plan.outputRes - 2)
    const rows = [
      { s2_cell: cellAtPlan, ts: 1, count: 5 },
      { s2_cell: cellAtCoarser, ts: 1, count: 99 },  // wrong level, should drop
    ]
    const filtered = filterCellsAndRes(rows, 's2_cell', plan.outputRes, [cellAtPlan], s2Index)
    expect(filtered).toEqual([{ s2_cell: cellAtPlan, ts: 1, count: 5 }])
  })
})

describe('planGeoQuery: pre-computed outputCells', () => {
  // Wrap a backend so the test can assert `bboxToCells` was never called
  // when the caller supplies `outputCells`. Counting wrapper around
  // `h3Index` — every other method delegates unchanged.
  function countingIndex(): { index: SpatialIndex; calls: { bboxToCells: number } } {
    const calls = { bboxToCells: 0 }
    const index: SpatialIndex = {
      name: h3Index.name,
      maxLevel: h3Index.maxLevel,
      latLngToCell: (lat, lng, level) => h3Index.latLngToCell(lat, lng, level),
      cellLevel: c => h3Index.cellLevel(c),
      cellToParent: (c, level) =>
        level !== undefined ? h3Index.cellToParent(c, level) : h3Index.cellToParent(c),
      bboxToCells: (bbox, level) => {
        calls.bboxToCells += 1
        return h3Index.bboxToCells(bbox, level)
      },
      cellInSet: (c, level, set) => h3Index.cellInSet(c, level, set),
      minimalCover: (include, system, opts) => h3Index.minimalCover(include, system, opts),
    }
    return { index, calls }
  }

  function pyramidWithIndex(geoResolutions: number[], index: SpatialIndex): GeoPyramid {
    return {
      storage: mockStorage,
      keyTemplate: 'trips/{tier}/{period}.parquet',
      axis: 'time',
      binCol: 'ts',
      dims: [],
      metrics: [{ name: 'count', monoid: 'count' }],
      tiers: [{ name: 'h1', bin: '1h', shards: ['1mo'] }],
      geo: { cellCol: 'h3_cell', resolutions: geoResolutions, index },
    }
  }

  test('honors caller-supplied cells verbatim and reports the supplied res', () => {
    const { index, calls } = countingIndex()
    const pyramid = pyramidWithIndex([9, 7, 5], index)
    const cells = [
      latLngToCell(40.74, -73.99, 7),
      latLngToCell(40.72, -73.97, 7),
    ]
    const plan = planGeoQuery(pyramid, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-02T00:00:00Z') },
      binBudget: 100,
      outputCells: { res: 7, cells },
    })
    expect(plan.outputRes).toBe(7)
    expect(plan.outputCells).toEqual(cells)
    // `bboxToCells` must not be invoked when the caller supplied cells —
    // the whole point of this code path is to skip `pickResolution`.
    expect(calls.bboxToCells).toBe(0)
    // Each segment carries the same cell list.
    for (const seg of plan.segments) {
      expect(seg.cells).toEqual(cells)
    }
  })

  test('passes through res=-1 for mixed-resolution covers', () => {
    const { index } = countingIndex()
    const pyramid = pyramidWithIndex([9, 7, 5], index)
    const mixedCells = [
      latLngToCell(40.74, -73.99, 7),    // coarse parent
      latLngToCell(40.72, -73.97, 9),    // fine leaf
    ]
    const plan = planGeoQuery(pyramid, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-02T00:00:00Z') },
      binBudget: 100,
      outputCells: { res: -1, cells: mixedCells },
    })
    expect(plan.outputRes).toBe(-1)
    expect(plan.outputCells).toEqual(mixedCells)
  })

  test('returned outputCells is a fresh array (caller can mutate its input)', () => {
    const pyramid = pyramidWithIndex([7], countingIndex().index)
    const cells: readonly string[] = [latLngToCell(40.74, -73.99, 7)]
    const plan = planGeoQuery(pyramid, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-02T00:00:00Z') },
      binBudget: 100,
      outputCells: { res: 7, cells },
    })
    // Mutating the plan's outputCells must not affect the caller's
    // readonly input (defensive copy on input).
    plan.outputCells.push('decoy')
    expect(cells).toHaveLength(1)
  })

  test('throws when neither `bbox` nor `outputCells` provided', () => {
    const pyramid = pyramidWithIndex([7], countingIndex().index)
    expect(() => planGeoQuery(pyramid, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-02T00:00:00Z') },
      binBudget: 100,
    })).toThrow('pass either `bbox` or `outputCells`')
  })

  test('throws when both `bbox` and `outputCells` provided', () => {
    const pyramid = pyramidWithIndex([7], countingIndex().index)
    expect(() => planGeoQuery(pyramid, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-02T00:00:00Z') },
      binBudget: 100,
      bbox: NYC,
      cellBudget: 100,
      outputCells: { res: 7, cells: [latLngToCell(40.74, -73.99, 7)] },
    })).toThrow('pass `bbox` xor `outputCells`, not both')
  })

  test('throws when `bbox` provided without `cellBudget`', () => {
    const pyramid = pyramidWithIndex([7], countingIndex().index)
    expect(() => planGeoQuery(pyramid, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-02T00:00:00Z') },
      binBudget: 100,
      bbox: NYC,
    })).toThrow('`cellBudget` required when `bbox` is provided')
  })
})

describe('planGeoQuery: targetBin', () => {
  // `specs/calendar-composition-and-query-limits.md` §4: explicit widths
  // reach the core planner from the geo entry points, and geo metadata
  // (`outputRes`, per-segment cells) resolves on that path too — before
  // this, calendar queries had to route through the time-only planner and
  // filter cells post-hoc, so `outputRes` was unavailable.
  test('exact tier-bin match makes that tier the output tier', () => {
    const pyramid = ctbkPyramid([9, 7, 5])
    const plan = planGeoQuery(pyramid, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-04-01T00:00:00Z') },
      binBudget: 10_000,
      targetBin: '1mo',
      bbox: NYC,
      cellBudget: 500,
    })
    expect(plan.outputBin).toBe('1mo')
    expect(plan.outputTier?.name).toBe('mo1')
    expect(plan.outputRes).toBe(9)
    expect(plan.outputCells.length).toBeGreaterThan(0)
    // Geo metadata rides on every segment, same as the budget-driven path.
    expect(plan.segments.every(s => s.cells === plan.outputCells)).toBe(true)
  })

  test('non-materialized calendar width decomposes raggedly, keeping geo metadata', () => {
    const pyramid = ctbkPyramid([9, 7, 5])
    const plan = planGeoQuery(pyramid, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-07-01T00:00:00Z') },
      binBudget: 10_000,
      targetBin: '2mo',
      bbox: NYC,
      cellBudget: 500,
    })
    expect(plan.outputBin).toBe('2mo')
    // Ragged path: no single stored tier is "the" output tier...
    expect(plan.outputTier).toBeUndefined()
    // ...but `outputRes` is resolved, which is what §4 buys over the
    // time-only workaround (that path reports `outputRes: -1`).
    expect(plan.outputRes).toBe(9)
    expect(plan.segments.every(s => s.cells === plan.outputCells)).toBe(true)
    // 2mo bins pack from the materialized 1mo tier (calendar composition).
    expect([...new Set(plan.segments.map(s => s.shardTier.name))]).toEqual(['mo1'])
  })

  test('limits are delegated too', () => {
    const pyramid = ctbkPyramid([9, 7, 5])
    expect(() => planGeoQuery(pyramid, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-07-01T00:00:00Z') },
      binBudget: 10_000,
      targetBin: '1d',
      limits: { maxOutputBins: 30 },
      bbox: NYC,
      cellBudget: 500,
    })).toThrow('planQuery: bins limit exceeded (181 > 30)')
  })

  test('planGeoQueryFromInventory threads targetBin as well', () => {
    const pyramid = ctbkPyramid([9, 7, 5])
    const shards: RecordedShard[] = [
      { tier: 'mo1', shardDur: '1y', periodStart: d('2026-01-01T00:00:00Z'), periodEnd: d('2027-01-01T00:00:00Z'), key: 'trips/mo1/2026.parquet' },
    ]
    const plan = planGeoQueryFromInventory(pyramid, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-04-01T00:00:00Z') },
      binBudget: 10_000,
      targetBin: '1mo',
      bbox: NYC,
      cellBudget: 500,
    }, shards)
    expect(plan.outputBin).toBe('1mo')
    expect(plan.outputRes).toBe(9)
    expect(plan.segments.map(s => s.shardTier.name)).toEqual(['mo1'])
  })
})

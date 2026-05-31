// Conformance tests for the `SpatialIndex` interface. The
// `assertSpatialIndex` helper is reusable across backends — when H13 and
// S2 land they'll run the same suite.
//
// Per spec test plan §Phase 1: "Interface conformance: a tiny
// `assertSpatialIndex(idx)` helper exercises every method on the h3 impl
// with known inputs; serves as the contract any future backend (H13, S2)
// must satisfy."

import { memStorage, type Pyramid } from 'pyrmts'
import { describe, expect, test } from 'vitest'
import { getSpatialIndex, h3Index } from './h3-index.js'
import type { BBox, GeoPyramid, SpatialIndex } from './spatial-index.js'

const NYC: BBox = {
  minLat: 40.70,
  maxLat: 40.78,
  minLng: -74.02,
  maxLng: -73.96,
}

// Manhattan-ish point (Times Square area).
const SAMPLE_LAT = 40.758
const SAMPLE_LNG = -73.985

interface ConformanceOpts {
  // A point known to be within `latLng → cell` valid input space for the
  // backend. For h3, anywhere on the globe; for H13, same; for T4, depends
  // on icosahedral mapping.
  samplePoint: { lat: number; lng: number }
  // A bbox guaranteed to yield ≥1 cell at `sampleLevel` and ≥1 at the
  // coarser level.
  sampleBBox: BBox
  // A level where bboxToCells returns at least one cell for `sampleBBox`,
  // and where `latLngToCell` is well-defined for `samplePoint`.
  sampleLevel: number
  // Coarser level than `sampleLevel` for parent-of tests.
  coarserLevel: number
}

// Reusable conformance suite. Backends pass their concrete `SpatialIndex`
// + a set of known-good sample inputs.
export function assertSpatialIndex(
  index: SpatialIndex,
  opts: ConformanceOpts,
): void {
  const { samplePoint, sampleBBox, sampleLevel, coarserLevel } = opts

  test('name + maxLevel', () => {
    expect(typeof index.name).toBe('string')
    expect(index.name.length).toBeGreaterThan(0)
    expect(index.maxLevel).toBeGreaterThanOrEqual(sampleLevel)
  })

  test('latLngToCell + cellLevel round-trip', () => {
    const cell = index.latLngToCell(samplePoint.lat, samplePoint.lng, sampleLevel)
    expect(typeof cell).toBe('string')
    expect(index.cellLevel(cell)).toBe(sampleLevel)
  })

  test('cellToParent returns a coarser-level cell', () => {
    const cell = index.latLngToCell(samplePoint.lat, samplePoint.lng, sampleLevel)
    const parent = index.cellToParent(cell)
    expect(index.cellLevel(parent)).toBe(sampleLevel - 1)
  })

  test('cellToParent honors explicit target level', () => {
    const cell = index.latLngToCell(samplePoint.lat, samplePoint.lng, sampleLevel)
    const parent = index.cellToParent(cell, coarserLevel)
    expect(index.cellLevel(parent)).toBe(coarserLevel)
  })

  test('bboxToCells returns cells at the requested level', () => {
    const cells = index.bboxToCells(sampleBBox, sampleLevel)
    expect(cells.length).toBeGreaterThan(0)
    for (const c of cells) {
      expect(index.cellLevel(c)).toBe(sampleLevel)
    }
  })

  test('cellInSet: cell-in-include → true', () => {
    const cell = index.latLngToCell(samplePoint.lat, samplePoint.lng, sampleLevel)
    expect(index.cellInSet(cell, sampleLevel, { include: [cell], exclude: [] })).toBe(true)
  })

  test('cellInSet: cell-not-in-include → false', () => {
    const cell = index.latLngToCell(samplePoint.lat, samplePoint.lng, sampleLevel)
    expect(index.cellInSet(cell, sampleLevel, { include: [], exclude: [] })).toBe(false)
  })

  test('cellInSet: cell-in-exclude → false (overrides include)', () => {
    const cell = index.latLngToCell(samplePoint.lat, samplePoint.lng, sampleLevel)
    expect(
      index.cellInSet(cell, sampleLevel, { include: [cell], exclude: [cell] }),
    ).toBe(false)
  })

  test('cellInSet: wrong level → false (single-resolution Phase 1)', () => {
    const cell = index.latLngToCell(samplePoint.lat, samplePoint.lng, sampleLevel)
    expect(index.cellInSet(cell, coarserLevel, { include: [cell], exclude: [] })).toBe(false)
  })
}

describe('h3Index: SpatialIndex conformance', () => {
  assertSpatialIndex(h3Index, {
    samplePoint: { lat: SAMPLE_LAT, lng: SAMPLE_LNG },
    sampleBBox: NYC,
    sampleLevel: 9,
    coarserLevel: 7,
  })
})

describe('h3Index: Phase 1 minimalCover throws', () => {
  test('minimalCover throws not-implemented', () => {
    expect(() => h3Index.minimalCover([], [])).toThrow(/not implemented/)
  })
})

describe('getSpatialIndex: pyramid resolution', () => {
  function pyramid(geo: { cellCol: string; resolutions: number[]; index?: SpatialIndex } | undefined): GeoPyramid {
    return {
      storage: memStorage(),
      keyTemplate: 't/{tier}/{period}.parquet',
      axis: 'time',
      binCol: 'ts',
      dims: [],
      metrics: [{ name: 'count', monoid: 'count' }],
      tiers: [{ name: 'h1', bin: '1h', shard: '1mo' }],
      ...(geo !== undefined ? { geo } : {}),
    }
  }

  test('returns h3Index when geo.index is unset (back-compat)', () => {
    const p = pyramid({ cellCol: 'h3_cell', resolutions: [9, 7, 5] })
    expect(getSpatialIndex(p)).toBe(h3Index)
  })

  test('returns the explicit index when set', () => {
    const fakeIndex: SpatialIndex = {
      name: 'fake',
      maxLevel: 10,
      latLngToCell: () => 'x',
      cellLevel: () => 0,
      cellToParent: c => c,
      bboxToCells: () => [],
      cellInSet: () => false,
      minimalCover: () => ({ include: [], exclude: [] }),
    }
    const p = pyramid({ cellCol: 'cell', resolutions: [5, 3], index: fakeIndex })
    expect(getSpatialIndex(p)).toBe(fakeIndex)
  })

  test('throws when pyramid has no geo config', () => {
    const p = pyramid(undefined)
    expect(() => getSpatialIndex(p)).toThrow(/no `geo` config/)
  })
})

// Equivalence test: pyramids declared with `Pyramid` from core (no
// `index`) still satisfy `GeoPyramid` structurally — the refactor is
// back-compatible at the type level.
describe('GeoPyramid structural compatibility with core Pyramid', () => {
  test('Pyramid without geo.index is assignable to GeoPyramid', () => {
    const core: Pyramid = {
      storage: memStorage(),
      keyTemplate: 't/{tier}/{period}.parquet',
      axis: 'time',
      binCol: 'ts',
      dims: [],
      metrics: [{ name: 'count', monoid: 'count' }],
      tiers: [{ name: 'h1', bin: '1h', shard: '1mo' }],
      geo: { cellCol: 'h3_cell', resolutions: [9, 7, 5] },
    }
    const geo: GeoPyramid = core
    expect(getSpatialIndex(geo)).toBe(h3Index)
  })
})

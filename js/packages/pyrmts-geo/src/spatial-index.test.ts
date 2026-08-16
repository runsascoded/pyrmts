// Conformance tests for `h3Index` against the reusable
// `assertSpatialIndex` suite (in `spatial-index-conformance.ts`); plus
// tests for the `getSpatialIndex` pyramid resolver and structural
// back-compat of core `Pyramid` → `GeoPyramid`.

import { memStorage, parquetBackend, type Pyramid } from 'pyrmts'
import { describe, expect, test } from 'vitest'
import { h3Index } from './h3-index.js'
import { assertSpatialIndex } from './spatial-index-conformance.js'
import { getSpatialIndex } from './spatial-index.js'
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

describe('h3Index: SpatialIndex conformance', () => {
  assertSpatialIndex(h3Index, {
    samplePoint: { lat: SAMPLE_LAT, lng: SAMPLE_LNG },
    sampleBBox: NYC,
    sampleLevel: 9,
    coarserLevel: 7,
  })
})

describe('getSpatialIndex: pyramid resolution', () => {
  function pyramid(geo: { cellCol: string; resolutions: number[]; index?: SpatialIndex } | undefined): GeoPyramid {
    return {
      storage: parquetBackend(memStorage()),
      keyTemplate: 't/{tier}/{period}.parquet',
      axis: 'time',
      binCol: 'ts',
      dims: [],
      metrics: [{ name: 'count', monoid: 'count' }],
      tiers: [{ name: 'h1', bin: '1h', shards: ['1mo'] }],
      ...(geo !== undefined ? { geo } : {}),
    }
  }

  // There is no default backend: an unset `geo.index` is a config error,
  // not an implicit H3 pyramid. The old fallback is what pinned `h3-js`
  // into every consumer bundle.
  test('throws when geo.index is unset', () => {
    const p = pyramid({ cellCol: 'h3_cell', resolutions: [9, 7, 5] })
    expect(() => getSpatialIndex(p)).toThrow(
      'getSpatialIndex: pyramid `geo.index` is unset — set it explicitly ' +
        '(e.g. `index: s2Index`); there is no default backend',
    )
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
    expect(() => getSpatialIndex(p)).toThrow('getSpatialIndex: pyramid has no `geo` config')
  })
})

// Equivalence test: pyramids declared with `Pyramid` from core (no
// `index`) still satisfy `GeoPyramid` structurally — the widening is
// back-compatible at the type level. (Resolving one still throws at
// runtime; `index` is optional for assignability, required to serve.)
describe('GeoPyramid structural compatibility with core Pyramid', () => {
  test('Pyramid without geo.index is assignable to GeoPyramid', () => {
    const core: Pyramid = {
      storage: parquetBackend(memStorage()),
      keyTemplate: 't/{tier}/{period}.parquet',
      axis: 'time',
      binCol: 'ts',
      dims: [],
      metrics: [{ name: 'count', monoid: 'count' }],
      tiers: [{ name: 'h1', bin: '1h', shards: ['1mo'] }],
      geo: { cellCol: 'h3_cell', resolutions: [9, 7, 5] },
    }
    const geo: GeoPyramid = core
    expect(geo.geo).toEqual({ cellCol: 'h3_cell', resolutions: [9, 7, 5] })
    expect(getSpatialIndex({ ...geo, geo: { ...geo.geo!, index: h3Index } })).toBe(h3Index)
  })
})

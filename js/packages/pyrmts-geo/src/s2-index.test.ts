// S2 backend conformance + S2-specific properties.
//
// Reuses `assertSpatialIndex` from `spatial-index.test.ts` — same suite
// that h3Index passes; if S2's contract drifts from H3's, both fail.
//
// Plus S2-specific property tests per spec Phase 2 test plan:
//   - Exact lineage (the H3-BT problem S2 fixes by construction)
//   - 4 children tile parent (vs H3's 7-with-BT-slivers)
//   - Token round-trip
//   - bboxToCells coverage (every bbox-interior point's cell is in result)

import { s2 } from 's2js'
import { describe, expect, test } from 'vitest'
import { s2Index } from './s2-index.js'
import { assertSpatialIndex } from './spatial-index-conformance.js'
import type { BBox } from './spatial-index.js'

const { cellid, LatLng } = s2

const NYC: BBox = {
  minLat: 40.70,
  maxLat: 40.78,
  minLng: -74.02,
  maxLng: -73.96,
}

// Manhattan-ish point (Times Square area).
const SAMPLE_LAT = 40.758
const SAMPLE_LNG = -73.985

describe('s2Index: SpatialIndex conformance', () => {
  // S2 cells at level 10 are ~3km wide; level 8 ~25km. Levels chosen so
  // the NYC bbox yields ≥1 cell at both sampleLevel and coarserLevel.
  assertSpatialIndex(s2Index, {
    samplePoint: { lat: SAMPLE_LAT, lng: SAMPLE_LNG },
    sampleBBox: NYC,
    sampleLevel: 12,
    coarserLevel: 10,
  })
})

describe('s2Index: exact-lineage property (the H3-BT problem S2 fixes)', () => {
  // Plain H3 at any (r → r-1) transition: ~7% of points have
  // cellToParent(latLngToCell(L, r), r-1) ≠ latLngToCell(L, r-1).
  // S2 is BT-free by construction; this should hold for every L.
  test('cellToParent ∘ latLngToCell === latLngToCell at coarser level (1000 random points)', () => {
    const fails: Array<{ lat: number; lng: number; level: number }> = []
    // Use a non-Math.random sequence (deterministic) so we can debug
    // failures reproducibly: van-der-Corput-style halving over the NYC
    // bbox is good enough for property-test purposes.
    const N = 1000
    for (let i = 0; i < N; i++) {
      const u = (i + 0.5) / N
      const v = ((i * 7) % N + 0.5) / N
      const lat = 40.70 + u * 0.08
      const lng = -74.02 + v * 0.06
      for (const level of [8, 10, 12, 14]) {
        const cellR = s2Index.latLngToCell(lat, lng, level)
        const cellRm1 = s2Index.latLngToCell(lat, lng, level - 1)
        const parentOfR = s2Index.cellToParent(cellR, level - 1)
        if (parentOfR !== cellRm1) {
          fails.push({ lat, lng, level })
        }
      }
    }
    expect(fails).toEqual([])
  })
})

describe('s2Index: 4 children tile parent (quadtree)', () => {
  test('every level-r+1 cell is contained in exactly one of its parent\'s 4 children', () => {
    // Sample a parent, enumerate its 4 children (via s2js cellid.children),
    // confirm each child's cellToParent gives back the parent, and confirm
    // each child has distinct cellID space (no overlap).
    const parentCell = s2Index.latLngToCell(SAMPLE_LAT, SAMPLE_LNG, 10)
    const parentCi = cellid.fromToken(parentCell)
    const childrenCi = cellid.children(parentCi)

    expect(childrenCi.length).toBe(4)

    for (const childCi of childrenCi) {
      const childToken = cellid.toToken(childCi)
      expect(s2Index.cellLevel(childToken)).toBe(11)
      expect(s2Index.cellToParent(childToken, 10)).toBe(parentCell)
    }

    // Children are pairwise disjoint (no contains relation between
    // siblings).
    for (let i = 0; i < childrenCi.length; i++) {
      for (let j = i + 1; j < childrenCi.length; j++) {
        expect(cellid.contains(childrenCi[i]!, childrenCi[j]!)).toBe(false)
        expect(cellid.contains(childrenCi[j]!, childrenCi[i]!)).toBe(false)
      }
    }
  })
})

describe('s2Index: token round-trip', () => {
  test('fromToken(toToken(ci)) === ci for sampled cells', () => {
    const levels = [0, 5, 10, 15, 20, 25, 30]
    for (const level of levels) {
      // S2 level 0 is the face cell — latLngToCell rounds up to face for
      // level 0; just check round-trip semantics.
      const expectedLevel = level === 0 ? 0 : level
      const cell = s2Index.latLngToCell(SAMPLE_LAT, SAMPLE_LNG, level)
      const ci = cellid.fromToken(cell)
      const roundTripped = cellid.toToken(ci)
      expect(roundTripped).toBe(cell)
      expect(cellid.level(ci)).toBe(expectedLevel)
    }
  })
})

describe('s2Index: bboxToCells coverage', () => {
  // For an interior point of the bbox, its level-r cell must appear in
  // the bboxToCells result. This is the consumer-facing correctness
  // contract — if a row's cell isn't in the cover, it'd be dropped at
  // filter time, causing missing data.
  test('every interior-point cell at level appears in bboxToCells output', () => {
    const level = 14
    const cells = s2Index.bboxToCells(NYC, level)
    const cellSet = new Set(cells)

    // 100 grid points strictly inside NYC bbox (avoid boundaries to dodge
    // edge ambiguity)
    const margin = 0.001
    const N = 10
    const missing: Array<{ lat: number; lng: number; cell: string }> = []
    for (let i = 0; i < N; i++) {
      for (let j = 0; j < N; j++) {
        const lat = NYC.minLat + margin + (i + 0.5) * (NYC.maxLat - NYC.minLat - 2 * margin) / N
        const lng = NYC.minLng + margin + (j + 0.5) * (NYC.maxLng - NYC.minLng - 2 * margin) / N
        const cell = s2Index.latLngToCell(lat, lng, level)
        if (!cellSet.has(cell)) {
          missing.push({ lat, lng, cell })
        }
      }
    }
    expect(missing).toEqual([])
  })

  test('returns more cells at finer level (coarse vs fine)', () => {
    const coarse = s2Index.bboxToCells(NYC, 10)
    const fine = s2Index.bboxToCells(NYC, 14)
    expect(fine.length).toBeGreaterThan(coarse.length)
  })

  test('all returned cells are at the requested level', () => {
    const level = 12
    const cells = s2Index.bboxToCells(NYC, level)
    expect(cells.length).toBeGreaterThan(0)
    for (const cell of cells) {
      expect(s2Index.cellLevel(cell)).toBe(level)
    }
  })
})

describe('s2Index: LatLng round-trip via S2', () => {
  test('latLngToCell at varying levels produces nested cell IDs', () => {
    // Coarser levels' cells should contain finer levels' cells (S2 quadtree
    // is strictly nested).
    const fine = cellid.fromToken(s2Index.latLngToCell(SAMPLE_LAT, SAMPLE_LNG, 14))
    const mid = cellid.fromToken(s2Index.latLngToCell(SAMPLE_LAT, SAMPLE_LNG, 10))
    const coarse = cellid.fromToken(s2Index.latLngToCell(SAMPLE_LAT, SAMPLE_LNG, 6))
    expect(cellid.contains(mid, fine)).toBe(true)
    expect(cellid.contains(coarse, mid)).toBe(true)
    expect(cellid.contains(coarse, fine)).toBe(true)
  })
})

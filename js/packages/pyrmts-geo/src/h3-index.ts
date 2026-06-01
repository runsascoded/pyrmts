// H3 default `SpatialIndex` implementation. Wraps `h3-js` so the planner +
// serve handler can stay backend-agnostic. This is the back-compat path —
// pyramids without an explicit `geo.index` use this.
//
// Phase 1 scope: `latLngToCell`, `cellLevel`, `cellToParent`, `bboxToCells`,
// and single-level `cellInSet`. `minimalCover` is Phase 3 work; it throws
// here until that lands.

import {
  cellToParent as h3CellToParent,
  getResolution,
  latLngToCell as h3LatLngToCell,
  polygonToCells,
} from 'h3-js'
import { isCellInCover, minimalCover as runMinimalCover } from './spatial-index-cover.js'
import type {
  BBox,
  GeoPyramid,
  MinimalCoverOpts,
  SpatialIndex,
  SpatialSet,
} from './spatial-index.js'

export const h3Index: SpatialIndex<string> = {
  name: 'h3',
  maxLevel: 15,

  latLngToCell(lat, lng, level) {
    return h3LatLngToCell(lat, lng, level)
  },

  cellLevel(cell) {
    return getResolution(cell)
  },

  cellToParent(cell, level) {
    const target = level ?? getResolution(cell) - 1
    return h3CellToParent(cell, target)
  },

  bboxToCells(bbox: BBox, level) {
    const polygon: number[][] = [
      [bbox.minLat, bbox.minLng],
      [bbox.minLat, bbox.maxLng],
      [bbox.maxLat, bbox.maxLng],
      [bbox.maxLat, bbox.minLng],
      [bbox.minLat, bbox.minLng],
    ]
    return polygonToCells(polygon, level)
  },

  // Lineage-aware membership: walks up from `cell`, returning true on
  // first include hit, false on first exclude hit. The `level` parameter
  // still gates wrong-level rows (every shard has multiple resolutions;
  // `filterCellsAndRes` uses this to drop them).
  //
  // Caveat: H3's parent chain is BT-affected for ~7% of points at every
  // level transition. For exact lineage walks against a geographically-
  // defined cover, prefer s2Index. For lineage walks against a cover
  // built from H3-lineage operations (e.g., the `minimalCover` DP from
  // `spatial-index-cover.ts`), behavior is self-consistent.
  cellInSet(cell, level, set: SpatialSet<string>) {
    if (getResolution(cell) !== level) return false
    return isCellInCover(h3Index, cell, set)
  },

  minimalCover(include, system, opts?: MinimalCoverOpts) {
    // Backend-agnostic DP. NOTE: H3's parent chain has BT mismatches at
    // every level transition for ~7% of points — lineage walks here are
    // best-effort. For an exact mixed-resolution cover, prefer s2Index.
    return runMinimalCover(h3Index, include, system, opts)
  },
}

// Resolve the SpatialIndex for a pyramid. Pyramids without an explicit
// `geo.index` get the H3 default.
export function getSpatialIndex(pyramid: GeoPyramid): SpatialIndex {
  if (pyramid.geo === undefined) {
    throw new Error('getSpatialIndex: pyramid has no `geo` config')
  }
  return pyramid.geo.index ?? h3Index
}

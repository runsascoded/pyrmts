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

  // Phase 1: single-level set. A row's cell is "in" the set iff its level
  // matches the set's query level AND the cell is in `include` AND not in
  // `exclude`. Phase 4 will widen this to lineage-aware checks for
  // mixed-resolution sets.
  cellInSet(cell, level, set: SpatialSet<string>) {
    if (getResolution(cell) !== level) return false
    if (set.exclude.includes(cell)) return false
    return set.include.includes(cell)
  },

  minimalCover(_include, _system, _opts?: MinimalCoverOpts) {
    throw new Error('h3Index.minimalCover: not implemented (Phase 3 work — see specs/pluggable-spatial-backend.md)')
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

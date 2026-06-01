// H3 `SpatialIndex` implementation. Wraps `h3-js`. Default for pyramids
// without an explicit `geo.index` (preserves existing rides-v1 / avail-v2
// / etc behavior).
//
// **Recommended for**: fixed-level queries (single-resolution pyramids).
// **Mixed-resolution caveats**: H3 lineage walks have BT mismatches at
// every level transition for ~7% of points, so multi-level covers via
// `minimalCover` here are approximate. For exact mixed-resolution work,
// use `s2Index`.

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
  // still gates wrong-level rows.
  //
  // Caveat: H3's parent chain is BT-affected for ~7% of points at every
  // level transition. Exact for covers built from H3-lineage operations
  // (e.g., `minimalCover`'s tree DP, which uses the same parent chain)
  // but approximate against geographically-defined covers — use s2Index
  // for that.
  cellInSet(cell, level, set: SpatialSet<string>) {
    if (getResolution(cell) !== level) return false
    return isCellInCover(h3Index, cell, set)
  },

  minimalCover(include, system, opts?: MinimalCoverOpts) {
    // Backend-agnostic DP. H3 lineage walks have BT mismatches at every
    // level transition (~7% of points) — outputs are approximate. Use
    // s2Index for exact mixed-resolution covers.
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

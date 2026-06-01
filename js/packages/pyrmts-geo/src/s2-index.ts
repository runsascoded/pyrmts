// S2 backend for `SpatialIndex`. Wraps `s2js` — the pure-TS port of
// Google's `golang/geo` reference implementation. Runs in Cloudflare
// Workers (s2js has no Node-only deps).
//
// S2's structural wins over H3 (and over H13's deferred design):
//   - Branching factor 4 (vs 7 for H3, 13 for H13).
//   - Exact lineage: `cellToParent(cellAt(L, r), r-1) === cellAt(L, r-1)`
//     holds for every L (no BT mismatches at any level transition).
//   - `bboxToCells` uses S2's native `RegionCoverer`.
//
// Cell IDs are S2 hex tokens (`toToken`/`fromToken` round-trip). The
// `SpatialIndex<string>` contract uses tokens for storage + JSON-key
// stability; we convert to `bigint` `CellID` for the internal ops.
//
// Phase 2 scope: `latLngToCell`, `cellLevel`, `cellToParent`,
// `bboxToCells`, single-level `cellInSet`. `minimalCover` is Phase 3
// work; throws here until that lands.

import { s2 } from 's2js'
import type {
  BBox,
  MinimalCoverOpts,
  SpatialIndex,
  SpatialSet,
} from './spatial-index.js'

const { cellid, LatLng, Rect, RegionCoverer } = s2

// `bboxToCells` upper bound. S2's `RegionCoverer` uses `maxCells` both as
// an output cap and as a work-bound for the covering search; setting it
// arbitrarily high effectively asks for the complete level-uniform cover.
// 1M is well past anything we'd realistically query (NYC bbox at S2 lvl
// 14 ≈ 25k cells; per-tile global cover at S2 lvl 10 ≈ 1.5M, so we cap
// just under that to prevent runaway).
const COVERER_MAX_CELLS = 1_000_000

export const s2Index: SpatialIndex<string> = {
  name: 's2',
  maxLevel: 30,

  latLngToCell(lat, lng, level) {
    const leaf = cellid.fromLatLng(LatLng.fromDegrees(lat, lng))
    return cellid.toToken(cellid.parent(leaf, level))
  },

  cellLevel(cell) {
    return cellid.level(cellid.fromToken(cell))
  },

  cellToParent(cell, targetLevel) {
    const ci = cellid.fromToken(cell)
    const target = targetLevel ?? (cellid.level(ci) - 1)
    if (target < 0) throw new Error(`s2Index.cellToParent: target level ${target} < 0`)
    return cellid.toToken(cellid.parent(ci, target))
  },

  bboxToCells(bbox: BBox, level) {
    const sw = LatLng.fromDegrees(bbox.minLat, bbox.minLng)
    const ne = LatLng.fromDegrees(bbox.maxLat, bbox.maxLng)
    const rect = Rect.fromLatLng(sw).addPoint(ne)
    const coverer = new RegionCoverer({
      minLevel: level,
      maxLevel: level,
      maxCells: COVERER_MAX_CELLS,
    })
    const union = coverer.covering(rect)
    return Array.from(union, (ci) => cellid.toToken(ci))
  },

  // Phase 2: single-level set semantics (same shape as h3Index). Phase 4
  // widens to lineage-aware mixed-resolution sets via `cellid.contains`.
  cellInSet(cell, level, set: SpatialSet<string>) {
    const ci = cellid.fromToken(cell)
    if (cellid.level(ci) !== level) return false
    if (set.exclude.includes(cell)) return false
    return set.include.includes(cell)
  },

  minimalCover(_include, _system, _opts?: MinimalCoverOpts) {
    throw new Error('s2Index.minimalCover: not implemented (Phase 3 work — see specs/pluggable-spatial-backend.md)')
  },
}

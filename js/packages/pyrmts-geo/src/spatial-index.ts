// Pluggable spatial-index abstraction. The planner consumes this interface
// rather than calling h3-js directly, so backends drop in without changing
// planner/serve/query code. Concrete impls: `h3Index` (fixed-level legacy),
// `s2Index` (prod-ready multi-resolution). See
// `specs/done/pluggable-spatial-backend.md` for the architectural framing.

import type { GeoSpec, Pyramid } from 'pyrmts'

export interface BBox {
  minLat: number
  maxLat: number
  minLng: number
  maxLng: number
}

// A set of cells declared in mixed-resolution (lineage-aware) form:
// every point in (∪ include's regions) \ (∪ exclude's regions) is "in"
// the set. `include` cells may be at different levels; same for `exclude`.
// Row-membership checks via `SpatialIndex.cellInSet`.
export interface SpatialSet<C extends string = string> {
  include: C[]
  exclude: C[]
}

export interface MinimalCoverOpts {
  resolutions?: number[]
  allowSubtraction?: boolean
  maxLevel?: number
}

// Pluggable spatial index. Concrete implementations (`h3Index`, `h13Index`,
// `s2Index`) live in this package. The `C` type parameter is the cell ID
// string form — for H3 it's `'892a100...'`; for H13 it'll be
// `'<parent_h3>:<sr_idx>'`; for S2 it'll be S2's token form.
export interface SpatialIndex<C extends string = string> {
  // Identifier — `'h3' | 'h13' | 's2' | 't4'`. Used for routing + serialized
  // in `Pyramid.geo.index` for query-time dispatch.
  readonly name: string

  // Levels available for materialization. H3: 0-15. S2: 0-30. T4: arbitrary.
  readonly maxLevel: number

  // Cell id at the given level for a lat/lng. Exact (no BT artifacts).
  latLngToCell(lat: number, lng: number, level: number): C

  // Resolution / level of a cell id. Most backends encode this in the id;
  // H13 derives it from the parent component.
  cellLevel(cell: C): number

  // Parent cell id at `level - 1` (or at `level` if explicitly provided).
  // For H3 (default backend) this calls h3-js's `cellToParent` and may be
  // BT-affected; for H13 it's exact by construction.
  cellToParent(cell: C, level?: number): C

  // Bounding box → covering cells at the given level. Used by
  // `planGeoQuery` for bbox queries.
  bboxToCells(bbox: BBox, level: number): C[]

  // Whether a row's cell (at the given level) is in the mixed-resolution
  // set. Lineage-aware: walks up from `cell`, returning true on the
  // first include hit, false on the first exclude hit.
  cellInSet(cell: C, level: number, set: SpatialSet<C>): boolean

  // Minimal mixed-resolution cover of an include cell set within a system
  // cell set (e.g., "all stations in NYC"). Returns lineage-disjoint
  // include + exclude cells. Optimal for the |ops| objective.
  minimalCover(include: C[], system: C[], opts?: MinimalCoverOpts): SpatialSet<C>
}

// `GeoSpec` extended with the optional `index` slot. Pyramids without an
// `index` resolve to the H3 default at query time (back-compat).
export type GeoSpecWithIndex = GeoSpec & { index?: SpatialIndex }

// `Pyramid` extended to carry an optional `SpatialIndex`. Existing
// pyramids (typed as `Pyramid` from core, no index) are still assignable —
// `index` is optional, so the structural widening is back-compatible.
export type GeoPyramid = Omit<Pyramid, 'geo'> & { geo?: GeoSpecWithIndex }

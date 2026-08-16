// Pluggable spatial-index abstraction. The planner consumes this interface
// rather than calling a concrete backend directly, so backends drop in
// without changing planner/serve/query code. See
// `specs/done/pluggable-spatial-backend.md` for the architectural framing.
//
// The shipped backend is `s2Index` (`s2-index.ts`). `h3Index` still exists
// (`h3-index.ts`) but is **test-only** and deliberately unexported from the
// package index — it is the second implementation that keeps this interface
// honest in the conformance suite, and nothing more. Importing it drags
// ~195 KB (minified) of `h3-js` into the consumer's bundle, which is why
// no shipped code path may reference it.

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
  allowSubtraction?: boolean
  /** Stop the bottom-up roll-up at this level. Cover output won't contain
   *  cells coarser than `coarsestLevel`. Pass the shallowest materialized
   *  level of the consuming pyramid — coarser cells have no shards to
   *  query. Undefined → walk to backend root. */
  coarsestLevel?: number
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
  // Exact for S2; the (test-only) H3 backend's is BT-affected.
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

// `GeoSpec` extended with the optional `index` slot. The slot is optional
// so that a plain core `Pyramid` stays structurally assignable, but any
// pyramid actually served through this package must set it — see
// `getSpatialIndex`.
export type GeoSpecWithIndex = GeoSpec & { index?: SpatialIndex }

// `Pyramid` extended to carry an optional `SpatialIndex`. Existing
// pyramids (typed as `Pyramid` from core, no index) are still assignable —
// `index` is optional, so the structural widening is back-compatible.
export type GeoPyramid = Omit<Pyramid, 'geo'> & { geo?: GeoSpecWithIndex }

// Resolve the `SpatialIndex` for a pyramid. The index must be set
// explicitly: there is deliberately no default backend.
//
// This used to fall back to `h3Index`, which forced every consumer's
// bundle to carry `h3-js` (the fallback made `h3Index` reachable from the
// package index, and `h3-js` declares no `sideEffects`, so it could never
// be tree-shaken). Both known consumers already pass `index: s2Index`
// explicitly, and H3 is no longer a supported serving backend — pyramids
// keyed by H3 cells can't do exact multi-resolution aggregation at all.
export function getSpatialIndex(pyramid: GeoPyramid): SpatialIndex {
  if (pyramid.geo === undefined) {
    throw new Error('getSpatialIndex: pyramid has no `geo` config')
  }
  const { index } = pyramid.geo
  if (index === undefined) {
    throw new Error(
      'getSpatialIndex: pyramid `geo.index` is unset — set it explicitly ' +
        '(e.g. `index: s2Index`); there is no default backend',
    )
  }
  return index
}

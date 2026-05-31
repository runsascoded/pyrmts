# Spec: pluggable spatial backend — S2 + BT-aware H3, baked off

> Status: **draft** (2026-05-31). Architectural spec for `pyrmts-geo`,
> motivated by ctbk's rides-v3 perf work hitting H3's hierarchical
> approximation problem.

## Background

`pyrmts-geo` today is tightly coupled to H3 (resolutions [9, 7, 5];
`cellCol: string`; `planGeoQuery` picks a single `outputRes` from the
materialized levels; `filterCellsAndRes` filters rows whose cell at
that single resolution is in the bbox-derived cell set).

Two findings from ctbk's bakeoff:

### Finding 1: H3 children don't tile their parent

This is documented in H3's own highlights/indexing.md but the
operational consequence is rarely engaged with by the H3 ecosystem.
The hex parent at resolution r-1 has area A; its 7 children at r
have *total area* A but their spatial union ≠ A — the children's
union has 6 small triangular slivers (Boundary Triangles, BTs) that
extend outside the parent, and the parent has 6 corresponding
triangular slivers not covered by any child.

**The exact BT area is 1/14 of parent area (≈ 7.14%)**. Geometric
proof: subdivide each child hex into 12 congruent triangles; 1 of
those 12 is a BT (extending past the parent). 6 children have
visible BTs (the central child's BTs are fully inside the parent
geometry, hence invisible). So per parent: 6 × (1/12 × 1/7) = 1/14
of parent area in BTs.

For point membership: ~7-8% of stations in any real dataset are in
a BT at any single (r → r-1) transition. For ctbk's 2339 stations
across r4-r15, this matches the empirical measurement to within
measurement noise.

**Operational consequence**: `cellToParent(latLngToCell(L, r), r-1) ≠
latLngToCell(L, r-1)` for ~7-8% of points at any single transition.
Across 6 materialized levels, ~39% of points are in a BT *somewhere*
in their lineage.

For an aggregation pyramid materializing the same data at multiple
resolutions independently (via `latLngToCell` at each level), each
single-resolution row is exact. Mixed-resolution queries that combine
data from multiple levels can:

- **Double-count** lineage-consistent points (when allowed cells set
  has both a parent and one of its index-children).
- **Misclassify** BT-stuck points (their r9 cell parent ≠ their r8
  cell — depending on which level the query reads, they're "in"
  one cell or a different one).

### Finding 2: H3's `compactCells` is greedy union only

No standard H3 implementation (h3, h3-js, h3-py, h3o) ships
"minimal cover of a point set" or "compact with set subtraction"
primitives. `uber/h3#452` is an open feature request from
~2018 asking for an "exhaustive" compact option; no movement.

For region-defined-by-station-set use cases (ctbk's bread and
butter — NYC = "all stations in the Citi Bike NYC system"),
neither pure-union `compactCells` nor `polygonToCells` gives a
tight representation. We'd need a custom set-cover-flavored greedy
that considers both `+` and `−` ops on cells at every materialized
resolution.

## Two paths

### Path B: H3 with BT bookkeeping

Stay on H3. Materialize every relevant resolution (4-9 for
ctbk scales; more if needed). Model BTs as first-class storage
buckets — each BT gets its own row(s) in the pyramid at the
appropriate aggregation. Queries that span multiple resolutions
declare their precision-vs-cost preference: "ignore BTs (~7%/level
error acceptable)" or "include BT cells explicitly (exact)."

Pros: keeps the H3 viz ecosystem accessible; matches existing
ctbk pyramid layouts.

Cons: substantial pyrmts-geo work. Need:
- BT addressing (a 64-bit id space distinct from H3 cells, since
  BTs aren't H3-addressable directly)
- BT computation (which BT a point is in)
- BT-aware `compactCells` / `minimalCover`
- BT-aware `filterCellsAndRes` (rows match either a hex cell or
  an enclosing BT)
- BT-aware aggregation in builds (rides in BTs get their own rows)

Estimated effort: 2-4 weeks. Per-query overhead: 5-15% extra
storage (7-8% BT prevalence × 6 levels, minus empty BTs), 20-30%
extra cells in the worst case for minimalCover.

### Path C: S2

Migrate to S2's quadtree. Each cell has exactly 4 children that
tile the parent perfectly. `cellToParent(cellAt(L, r), r-1) =
cellAt(L, r-1)` exactly for every L. Mixed-resolution queries
are lineage-disjoint by construction; double-counting and BT
mismatches don't exist.

Pros: structurally clean. No special cases. Existing graduate-
quality literature on S2 indexing. The "minimal cover of a point
set" problem reduces to standard greedy compactCells which is
exact-by-construction.

Cons: full migration. Need:
- `s2-geometry`-bindings TS port or wrapper (Google ships C++,
  JS bindings exist via WebAssembly — `s2js` or hand-port)
- pyrmts-geo storage layer abstracted on `SpatialIndex` (head,
  parent, children, fromLatLng, ...)
- Python build pipeline (ctbk's ride aggregation) ported to S2
- R2 prefix change: `rides-v2-s2/`, `avail-v3-s2/`, etc.
- FE map work: switching from H3 cell viz to S2 cell viz
- Migration story for existing avail-v2 / rides-v1-v3 data

Estimated effort: 4-8 weeks. Storage: probably similar (more
levels in a 4-children tree, but each cell smaller).

## Recommendation: implement both, bake off

`pyrmts-geo` abstracts on a `SpatialIndex` interface. H3 and S2
become two implementations. Pyramids in pyrmts-geo declare which
index they're built against; queries route to the matching backend.

This way:

- **Path C ships first** (S2 backend, smaller surface, exact by
  construction). Validates the abstraction on a clean case.
- **Path B ships second** (H3-with-BTs). Validates the abstraction
  on the messy case + lets us measure whether the 1/14 BT mass is
  actually impactful at ctbk-scale aggregation, or if H3 without
  BT bookkeeping is "close enough" and the migration to S2 was the
  right move.
- ctbk picks per dataset: rides-v3 (S2) goes head-to-head with
  rides-v4 (H3+BT), both bake against existing rides-v2 (H3 single-
  res).

## Interface sketch (TS)

```ts
// Pluggable spatial index. Implementations live in pyrmts-spatial-h3
// and pyrmts-spatial-s2. Pyrmts-geo's planner depends only on this
// interface, not on a specific backend.
export interface SpatialIndex<C extends string = string> {
  /** Backend identifier — `'h3' | 's2' | 'h3-bt'`. Used for routing
   *  + serialized in `Pyramid.geo.index` for query-time dispatch. */
  readonly name: string

  /** Levels available for materialization. H3: 0-15. S2: 0-30. */
  readonly maxLevel: number

  /** Cell id at the given level for a lat/lng. Exact. */
  latLngToCell(lat: number, lng: number, level: number): C

  /** Parent cell id at level - 1. For H3-bt: may return a BT id
   *  instead of a hex id for points in a BT. */
  cellToParent(cell: C, level?: number): C

  /** Bounding box → covering cells at given level. Used by
   *  planGeoQuery for bbox queries. */
  bboxToCells(bbox: BBox, level: number): C[]

  /** Minimal mixed-resolution cover of a station set, with optional
   *  set-subtraction support. Returns lineage-disjoint cells.
   *  H3-bt implementation uses BT bookkeeping; S2 uses standard
   *  compactCells. Pure-union mode if `allowSubtraction = false`. */
  minimalCover(
    include: C[],
    system: C[],
    opts?: {
      resolutions?: number[]
      allowSubtraction?: boolean
      maxLevel?: number
    },
  ): { include: C[]; exclude: C[] }

  /** Given a row at level r and an allowed cell set spanning
   *  multiple levels, decide whether this row matches. Used by
   *  filterCellsAndResMixed. */
  cellInSet(cell: C, level: number, set: { include: C[]; exclude: C[] }): boolean
}
```

`Pyramid.geo` grows a `.index: SpatialIndex` slot. Existing
backwards-compat: missing `geo.index` defaults to H3 with current
behavior (preserves rides-v1 / avail-v2 / etc).

## Phasing

1. **Sketch the abstraction** (this spec). Get sign-off on interface
   shape before implementing either backend. ~3 days.
2. **S2 backend** (`pyrmts-spatial-s2`). Includes Python build helper
   (ctbk uses Python to write parquet shards). ~2 weeks.
3. **ctbk rides-v3 (S2)** rebuild. Bake off against v1/v2 on the
   `/v2?pyramid=v3` toggle. ~1 week.
4. **H3-with-BTs backend** (`pyrmts-spatial-h3-bt`). Full BT
   bookkeeping. ~3 weeks.
5. **ctbk rides-v4 (H3+BT)** rebuild. Bake off. ~1 week.
6. **Decide**: based on perf + precision + maintainability, pick
   the long-term backend. Retire the loser. ~1 week.

## Out of scope

- Migration of existing avail-v2 / avail-geo data. Those continue
  to work via the default-H3 path.
- Mixed-backend queries (single query reads both H3 and S2 data).
  Each query is single-backend; UI may route to one or another.
- FE map viz of cells. Currently we don't render cells; if/when we
  do, the backend dispatches.
- Polygon-shape queries (custom region drag-rect on map). Out of
  scope until we have a clear use case.

## Open questions

- **BT addressing**: do BTs get their own 64-bit ids in a parallel
  space (cleanest), or do we tag BTs as `(cell_id, edge_idx)`
  pairs (cheaper but messier query routing)?
- **Empty BTs**: at build time, do we materialize BT rows even
  when no points are in them (consistency), or only when populated
  (size optimization)?
- **Cross-backend tooling**: should `ctbk region-cells` output be
  backend-specific or backend-agnostic? (Probably backend-agnostic:
  it outputs the station set; the backend's `minimalCover` is called
  client- or build-time as needed.)
- **S2 library choice**: build a thin wrapper around the WASM port,
  or hand-port the essential math? WASM is simpler but ~50KB
  bundle; hand-port is rouge but tractable since we only need a
  small slice of S2's API.

## References

- H3 docs: highlights/indexing.md ("Hexagons do not cleanly
  subdivide into seven finer hexagons")
- `uber/h3#452` — open feature request for exhaustive compact
- S2 geometry library: https://s2geometry.io/
- ctbk bakeoff: see `~/c/hccs/ctbk/specs/done/rides-pyramid-v2.md`
  + the BT measurement work in commit history

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

### Path B: H13 — H3 with 13-disjoint-subregions addressing

Stay on H3 geometrically. Treat every parent P at resolution r-1 as
having **13 disjoint subregions (SRs) that perfectly tile P's area**:

- 1× central child hex (fully inside P)
- 6× fringe children (each = an H3 child hex minus its outward-
  protruding BT, ≈ 11/84 of P's area)
- 6× incoming BTs (1 from each of P's 6 sibling parents' children,
  ≈ 1/84 of P's area each)

Total = 12/84 + 6 × 11/84 + 6 × 1/84 = **84/84 = parent area exactly**.

Addressing at level r becomes `(parent_id_at_r-1, sr_idx ∈ 0..12)`
where 0 = central, 1-6 = fringes (ordered by direction), 7-12 =
incoming BTs (ordered by donating sibling direction). Computable
from any lat/lng via direct H3 ops:

```
P = h3.latlng_to_cell(L, r-1)            # exact parent
C = h3.latlng_to_cell(L, r)              # exact H3 child hex
lineage_P = h3.cell_to_parent(C, r-1)
if lineage_P == P:
    sr_idx = fringe_idx(C, P)            # 0-6, central or fringe
else:
    sr_idx = 7 + bt_direction(P, lineage_P)  # 7-12, incoming BT
```

The hierarchy is now **exactly a 13-ary tree** with no Pauli
exclusion, no BT-handling edge cases, no double-counting. Every
operation (parent, child, contains, aggregate) is structurally
clean.

**Critical insight: monoid sums + query-time subtraction make
"all SRs populated" irrelevant for compaction.** For a region R
that contains *most* of P's SRs:

```
count(R, P) = count(P) - Σ count(SR_i)  for SR_i ∉ R
```

So minimalCover is **mutually-recursive tree DP over include/
exclude states** — the dual of the standard "min-ops to encode this
subset of a hierarchy":

```
def encode_include(C):
    # Min-ops encoding of "include I-stations under C, nothing else"
    if C is pure_I or empty:     return [(C, +)]  if C has I else []
    explicit  = sum(encode_include(child) for child in C.children)
    via_parent = [(C, +)] + sum(encode_exclude(child) for child in C.children)
    return min(explicit, via_parent, key=len)

def encode_exclude(C):  # dual
    if C is pure_E:              return [(C, -)]
    if C has no E:               return []
    explicit  = sum(encode_exclude(child) for child in C.children)
    via_parent = [(C, -)] + sum(encode_include(child) for child in C.children)
    return min(explicit, via_parent, key=len)
```

Two visits per cell ⇒ O(|relevant tree|) total. Always exact,
always optimal for the op-count objective. Note: a naive bottom-up
greedy (decide swap-or-not at each parent level independently)
misses higher-level wins where multiple sibling swaps would
compose into a single grandparent-level swap. DP catches these.

The exact same DP applies identically to S2 (branching factor 4
instead of 13). The algorithm doesn't care about the geometry,
just the tree structure — so the H13 vs S2 bake-off compares
geometric properties only.

Pros: keeps the H3 ecosystem (h3-py, h3-js, viz tooling) accessible.
The addressing extension is small (~4 bits per row for `sr_idx`).
13-ary tree algorithms are mechanically identical to the standard
ones — they just have branching factor 13 instead of 7.

Cons: 13-children compaction is slightly less effective than S2's
4-children on dense data (more SRs need to be "right" to collapse).
For sparse-station regions, this doesn't matter — monoid subtraction
at query time handles partial coverage cleanly.

Estimated effort: 2-3 weeks. Per-query overhead: ~1 extra byte per
row (sr_idx). No row blow-up — each ride still contributes one row
per materialized resolution; it just carries the SR tag.

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

`pyrmts-geo` abstracts on a `SpatialIndex` interface. H13 and S2
become two implementations. Pyramids in pyrmts-geo declare which
index they're built against; queries route to the matching backend.

Both paths share the same query model: tree decomposition over the
disjoint subregion structure, with monoid arithmetic at each parent
(`P_count − Σ excluded_SR_counts`) handling partial coverage exactly.
The choice between H13 and S2 is now **purely about geometry +
library availability**, not about architectural complexity:

| | H13 | S2 |
|---|---|---|
| Branching factor | 13 | 4 |
| Lib | h3-js + small SR addressing wrapper | s2-geometry-js or hand port |
| Geometry | hexagonal (uniform 6-neighbor) | square (4-cardinal + 4-diagonal) |
| Ecosystem | Kepler.gl/deck.gl/Carto all native | sparse FE tooling |
| Migration risk | low (stay on H3 ops + add SR tag) | medium (replace planner geometry) |

This way:

- **H13 ships first** — keeps us on H3 ops + tooling; the extension
  is just the SR addressing layer + 13-ary tree algorithms.
- **S2 ships second** — for the bake-off on the cleaner geometry,
  and as backstop if H13 turns out to have a pitfall in practice.
- ctbk picks per dataset: rides-v3 (H13) goes head-to-head with
  rides-v4 (S2), both bake against existing rides-v2 (vanilla H3
  single-res).

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
2. **H13 backend** (`pyrmts-spatial-h13`). SR addressing + 13-ary
   tree algorithms on top of h3-js. Includes Python build helper
   (ctbk uses Python to write parquet shards). ~2 weeks.
3. **ctbk rides-v3 (H13)** rebuild. Bake off against v1/v2 on the
   `/v2?pyramid=v3` toggle. ~1 week.
4. **S2 backend** (`pyrmts-spatial-s2`). Square quadtree, library
   bindings. ~3 weeks (mostly library porting / wrapping).
5. **ctbk rides-v4 (S2)** rebuild. Bake off. ~1 week.
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

- **H13 SR encoding on disk**: do we store `(parent_id, sr_idx)` as
  two columns (cleaner) or pack into the existing 64-bit cell id by
  reserving 4 bits for sr_idx (denser; loses H3 compatibility for
  that column)? Lean toward two columns at first; revisit if size
  matters.
- **H13 sr_idx ordering**: 0 = central is obvious; ordering of 1-6
  fringes and 7-12 incoming BTs — by direction (N, NE, SE, S, SW,
  NW) or by H3 child index. Direction is more human-readable;
  index is cheaper to compute. Probably H3 child index for cells +
  sibling-parent direction for BTs.
- **Empty SRs**: at build time, do we materialize SR rows even
  when no points are in them (consistency / monoid identity), or
  only when populated (size optimization)? Sparse-only is the
  obvious win on storage; lookup logic just treats missing rows
  as count=0.
- **Cross-backend tooling**: `ctbk region-cells` output is backend-
  agnostic: it outputs the station set; the backend's `minimalCover`
  is called client- or build-time as needed.
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

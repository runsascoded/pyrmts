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

## Three paths (H13, S2; T4 deferred)

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

### Path D: T4 (equilateral-triangle rep-tile) — deferred

Implement only if H13's bake-off vs S2 reveals practical problems
worth a third comparison point.

T4 = 4-way midpoint subdivision of equilateral triangles. Connect
the midpoints of a parent triangle's three sides to get 4 children:
3 "corner" children sharing the parent's orientation, 1 "center"
child flipped (apex-down if parent apex-up). All exactly 1/4 of
parent area. Disjoint. Perfect tiling. No BTs, no SR addressing
extension, no special cases.

(T2 — 2-way bisection — doesn't preserve equilateral property
since splitting an equilateral triangle in half by midpoint-to-
vertex yields two 30-60-90 right triangles. T4 is the right name
for the rep-tile / midpoint-subdivision operation.)

**Why it's interesting**: simplest of the three backends. The H13
BT-area asymmetry (6 incoming-BT SRs at ≈1.2% each) likely leaves
those SRs sparsely-populated at high-resolution levels — paying
complexity (mixed-area children, two ordering conventions, 13-ary
tree) for SRs that rarely have data. T4 dodges this entirely: every
child is 25%, populated-vs-empty is purely data-driven, not
geometry-driven.

**Why deferred**: no TS lib ecosystem. `latLngToCell`, `bboxToCells`,
and viz integration are all hand-rolled. For ctbk's NYC scale (lat
range ~0.5°), the geometric distortion difference between S2 squares
and T4 triangles is below measurement noise — the bake-off would
mostly measure library quality, not geometry.

**Cheap once the abstraction is solid**: the `SpatialIndex` interface
and `minimalCover` DP are branching-factor-parameterized; T4
(branching 4) shares ~95% of code with S2 (branching 4) and reuses
the H13 DP verbatim. The marginal cost of *adding* T4 is small once
H13 + S2 are in. The big cost is the from-scratch geo primitives.

**Decision trigger**: include T4 if either (a) H13's BT-SRs prove
operationally noisy (planner edge cases, monoid-identity surprises,
RG-stats blowup from sparse SR rows), or (b) S2's WASM bundle or API
surface becomes a deployment problem. Otherwise skip — H13/S2 cover
the design space adequately.

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
- **T4 third (optional)** — only if the H13 vs S2 verdict leaves
  the design space unclear, or if H13's BT-area asymmetry shows
  up as a practical issue. See Path D.
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
6. **Decide H13 vs S2**: based on perf + precision +
   maintainability, pick the long-term backend. Retire the loser.
   ~1 week.
7. **(Optional) T4 backend** (`pyrmts-spatial-t4`). Only if step 6's
   verdict leaves the design space unclear, or H13's BT-SR
   asymmetry shows up as a real problem. T4's geo primitives are
   hand-rolled (no lib); its DP + interface conformance comes
   nearly free from H13/S2 work. ~1-2 weeks.

## Test plan

Phased to match the implementation phasing in `## Phasing`. Each phase
must land green before the next one starts.

### Phase 1 — Interface scaffolding

- **Back-compat**: every existing `pyrmts-geo` test (planner.test,
  query.test, serve.test, e2e.test) passes unchanged. The h3 default
  impl is a pure refactor — no observable behavior change.
- **Default-index inference**: a pyramid declared without `geo.index`
  resolves to the h3 default; querying it produces identical results
  pre/post the refactor (snapshotted as a small e2e case).
- **Interface conformance**: a tiny `assertSpatialIndex(idx)` helper
  exercises every method on the h3 impl with known inputs; serves as
  the contract any future backend (H13, S2) must satisfy.

### Phase 2 — H13 addressing primitives

- **BT mass-action** (statistical): uniform-random points across a
  large bbox, count what fraction land in incoming-BT SRs (idx 7-12)
  at each (r → r-1) transition. Expect 1/14 ≈ 7.14% ± Monte Carlo
  noise (N≥10k → 3σ band ≈ ±0.8pp). Confirms `srIdxFor` correctly
  identifies BTs and the SR-index space is exhaustive.
- **13 SRs tile parent** (Monte Carlo): for a sampled set of parents,
  uniform points in parent's bbox each land in exactly one of P's
  13 SRs (no double-assignment, no orphans — every point inside P
  gets a valid `(P, sr_idx)`).
- **Exact lineage** (property): `∀ L, r:
  cellToParent(latLngToH13(L, r), r-1) === latLngToH13(L, r-1)`.
  No BT artifacts in H13 lineage by construction.
- **Round-trip**: `H13(lat, lng)` → cell — and the SR's geometric
  region contains `(lat, lng)`. (Geometric containment via h3
  boundary polygon ∩ BT slivers.)
- **Direction enum consistency**: fringe SRs 1-6 and incoming BTs
  7-12 use the same `Direction` ordering (K, J, JK, I, IK, IJ). A
  table-test pins the mapping so future refactors can't silently
  permute it.

### Phase 3 — `minimalCover` DP

- **Small hand-cases** (exact-equality):
  - All-included (R == system): result is `[(root, +)]`.
  - All-excluded singleton: `[(root, +), (excluded_leaf, -)]`
    when `|excluded| < |root_children|`.
  - Single included leaf out of a sparse tree: `[(leaf, +)]`
    (no via-parent win available).
- **Cross-level win** (regression test): constructed input where
  swap composition at the grandparent wins (e.g., 3 of 7 children
  at level r-1 fully-include, but their grandparent's other 4
  children all exclude — DP should pick `(gp, +) - (4 children, -)`
  vs greedy's 3 separate `(child, +)`s). Asserts DP `|ops|` strictly
  less than bottom-up greedy `|ops|`.
- **DP optimality** (brute-force property): for trees with ≤3
  levels (≤7³ = 343 leaves on H3, ≤13³ = 2197 on H13 — keep test
  bounded to ~50 random leaves), enumerate every cover (subsets of
  the tree's cells × {+, -}) that produces the same include set,
  take the minimum `|ops|`, assert DP matches. Repeat across N
  random include-sets.
- **Lineage-disjoint output** (property): no cell in `result.include`
  is a descendant of another `result.include` cell, and same for
  `result.exclude`. (Lineage cleanness is a correctness property of
  the encoding, not a separate constraint.)
- **`allowSubtraction = false` mode**: result has empty `exclude[]`,
  matches the standard `compactCells`-like greedy union.

### Phase 4 — `cellInSet` + planner integration

- **`cellInSet` semantics** (property): for random L and a random
  `{include, exclude}` set, `cellInSet(latLngToCell(L, r), r, set)`
  iff L is in `(∪ include's regions) \ (∪ exclude's regions)`.
- **End-to-end H13 pyramid**: small synthetic in-memory pyramid
  (10s of cells across 2-3 levels), bbox query returns correct
  aggregates. Includes the `mempty` sparse-row case (excluded SR
  with no materialized data reads as 0 and the subtraction still
  composes correctly).
- **Back-compat E2E**: existing rides-v1 / avail-v2-style pyramid
  configs (no `geo.index`) produce byte-identical query output
  pre/post the refactor.
- **Smoothing × geo**: a windowed query that triggers both
  server-side smoothing and a multi-resolution geo cover stays
  consistent. (Smoothing operates per-cell, not across cells, so
  this is mostly a wiring check.)

### Cross-cutting

- **Both backends share `minimalCover` DP tests**: the DP is
  branching-factor-parameterized, so the optimality + cross-level-win
  tests run against both H3 (branching 7) and H13 (branching 13).
  When S2 lands (branching 4) it joins the same suite.
- **Performance smoke** (Phase 4): a "full system" bbox query
  (e.g., 2339 stations, 6 H13 levels) completes in <1s end-to-end
  on synthetic data. Not a real benchmark — just catches O(n²)
  regressions in the DP or filter path.

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

- **H13 SR encoding on disk**: two columns (`parent_id`, `sr_idx`).
  Packing into the H3 64-bit cell id (reserving 4 bits for `sr_idx`)
  saves ~5 bytes/row but breaks native h3-lib lookups on that column
  — which kills the main H13 win (staying in the H3 ecosystem). Two
  columns also gives RG stats on `sr_idx` for free, which the
  arbitrary-column pruner (`FetchOptions.filters`) can use directly.
- **H13 sr_idx ordering**: 0 = central; 1-6 = fringe children in
  canonical H3 direction order (K, J, JK, I, IK, IJ — h3-js's
  `Direction` enum); 7-12 = incoming BTs ordered by donor
  sibling-parent direction (same enum). One ordering convention
  across both ranges (direction throughout) — avoids "is sr_idx 3
  the SE fringe or the SE-direction BT?" ambiguity when reading
  rows. Marginally more compute at addressing time (translate H3
  child index → direction) than using raw H3 child indices for
  1-6, but the convention payoff outweighs the cost.
- **Empty SRs**: sparse-only at build time. Missing rows read as
  monoid identity. The planner contract must document the canonical
  `mempty` for each monoid (`count: 0`, `sum: 0`, `histogram: {}`,
  `minmax: (+∞, −∞)`) so that `count(P) − Σ excluded_SR_counts`
  composes correctly when some excluded SRs are absent (treated
  as 0).
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

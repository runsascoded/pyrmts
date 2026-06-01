# Spec: pluggable spatial backend — S2 + BT-aware H3, baked off

> Status: **done** (pyrmts side, Phases 1-4) — 2026-05-31. Architectural
> spec for `pyrmts-geo`, motivated by ctbk's rides-v3 perf work hitting
> H3's hierarchical approximation problem.
>
> ## Resolution
>
> Phases 1-4 shipped on branch `spatial-backend`:
>
> - **Phase 1** (`a5c0f60`): `SpatialIndex` interface +
>   `assertSpatialIndex` conformance suite + `h3Index` default impl.
>   Pure refactor; existing tests pass unchanged.
> - **Phase 2** (`6c16dbc`): `s2Index` backend on `s2js`. Exact-lineage
>   property holds for 4000 randomized checks (1000 points × 4 levels)
>   — the H3-BT problem that motivated the work is gone. Sole runtime
>   dep is `bigfloat` (pure JS, CFW-compatible).
> - **Phase 3** (`05c8cca`): backend-agnostic `minimalCover` DP. The
>   mutually-recursive `encode_include`/`encode_exclude` is optimal
>   for the |ops| objective; brute-force checked across 32 include
>   subsets on small trees. **DP strictly beats bottom-up greedy by 2
>   ops on the 14-of-16 grandchildren canonical case** (3 ops vs 5).
> - **Phase 4** (`98944b0`): lineage-aware `cellInSet` + consumer-
>   facing `filterCellsByCover`. Mixed-resolution sets work via
>   parent-chain walks; cell-in-exclude beats parent-in-include (the
>   `[(P,+), (child,-)]` motif).
>
> Phase 5 (ctbk rides-v3 bake-off) is downstream consumer work. H13
> (Phase 6) and T4 (Phase 7) remain deferred per the open question
> below.
>
> ## Spec-vs-impl deltas
>
> 1. **H13 "exact lineage" claim is wrong**: discovered during Phase 2
>    planning that H13's `cellToParent` can't satisfy
>    `cellToParent(latLngToH13(L, r), r-1) === latLngToH13(L, r-1)` —
>    H3 BT mismatches compound at every level transition regardless of
>    how H13 defines its parent function. Per-level 13-SR partition is
>    clean, but lineage walks aren't BT-free. The monoid subtraction
>    algebra (the real H13 win for queries) is unaffected. Pivoted to
>    S2-first; H13 stays deferred with this finding documented in
>    Path B.
> 2. **`cellLevel` added to interface**: not in the original interface
>    sketch; needed for the row-level resolution filter
>    (`filterCellsAndRes` drops wrong-resolution rows).
> 3. **`assertSpatialIndex` lives in `spatial-index-conformance.ts`**:
>    initially put in `spatial-index.test.ts` but imports triggered
>    duplicate test runs in consumer files. Moved to a non-test file.
> 4. **DP lives in `spatial-index-cover.ts`**: backend-agnostic
>    standalone module; both `h3Index.minimalCover` and
>    `s2Index.minimalCover` delegate to it via `runMinimalCover(index,
>    ...)`.
>
> ## Library
>
> `s2js` (missinglink/s2js — pure-TS port of golang/geo,
> Cloudflare-Workers compatible, full RegionCoverer). Survey in
> `tmp/s2-lib-survey.md`. Fallback: hand-port the RegionCoverer slice
> on top of nodes2ts.
>
> ## Migration / consumer-side wiring
>
> ctbk rides-v3 (S2 bake-off): declare pyramid `geo.index = s2Index`;
> at query time, compute `minimalCover(s2Index, filterStationIds,
> allSystemStations)`; pass the resulting `SpatialSet` to
> `filterCellsByCover` after `fetchSegmentRows`. The shadow-mode
> parity test compares the S2 cover output to the existing H3
> single-res output.
>
> 214 tests pass on the pyrmts side.

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

### Path B: H13 — H3 with 13-disjoint-subregions addressing — DEFERRED

**Deferred (2026-05-31)** in favor of S2 (Path C). See top-of-spec
note for why; in short: H13's per-level 13-SR partition is geometrically
clean, but lineage-walks (`cellToParent`) still compound H3's BT
mismatches at every level transition, so the "clean tree" framing
holds only for single-level addressing — not for cross-level
parent-of-parent walks. The monoid subtraction algebra for queries
(which is the real H13 win) doesn't need lineage walks, so H13 is
still a viable backend, just not a clearly-better one than S2 once
S2's library deployment risk is bounded.

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

### Path C: S2 — PRIMARY

Migrate to S2's quadtree. Each cell has exactly 4 children that
tile the parent perfectly. `cellToParent(cellAt(L, r), r-1) =
cellAt(L, r-1)` exactly for every L. Mixed-resolution queries
are lineage-disjoint by construction; double-counting and BT
mismatches don't exist.

**Primary backend (2026-05-31).** Library: `s2js`
(missinglink/s2js) — pure TS, Cloudflare Workers compatible,
feature-parity with `golang/geo`. The `bboxToCells` op uses S2's
native `RegionCoverer` (the most complex op; nodes2ts doesn't
have it, which ruled out the more-popular alternative).

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

## Recommendation: S2 primary, others deferred

`pyrmts-geo` abstracts on a `SpatialIndex` interface (shipped Phase 1).
S2 is the primary backend. H13 and T4 are deferred candidates with
known motivations and full design notes preserved in this spec — they
become live work if S2 fails for a specific reason (bundle size,
library bug, perf regression).

Comparison (post-pivot):

| | H13 (deferred) | S2 (primary) | T4 (deferred) |
|---|---|---|---|
| Branching factor | 13 | 4 | 4 |
| Lib | h3-js + SR addressing wrapper | `s2js` (pure TS, CFW-compat) | hand-port |
| Geometry | hex + slivers | square (warped) | equilateral triangle |
| Exact lineage walks | No (compounds H3 BTs at every level) | Yes | Yes |
| `minimalCover` impl | mutually-recursive DP (branching 13) | mutually-recursive DP (branching 4) | mutually-recursive DP (branching 4) |
| FE viz (if/when needed) | deck.gl native HexagonLayer | deck.gl s2 layer | hand-rolled |

This way:

- **S2 ships first** (Path C) — structural simplicity, exact lineage,
  monoid algebra works without any lineage-walk caveats. Built on
  `s2js`, pure-TS, CFW-compatible.
- **H13 deferred** (Path B) — only revisit if S2 has a deployment
  problem (bundle size in CFW, library bug, performance regression).
  See the Path B section for the exact-lineage finding that motivated
  the deprioritization.
- **T4 deferred further** (Path D) — only if both S2 *and* H13 land
  in interesting trouble. Cheap to add once the abstraction is solid.
- ctbk picks per dataset: rides-v3 (S2) bakes off against existing
  rides-v2 (vanilla H3 single-res). Same shadow-mode parity-check
  pattern as previous bake-offs.

## Interface sketch (TS)

> Status: shipped in Phase 1 (`a5c0f60`). The current implementation
> matches this sketch with one addition: `cellLevel(cell): number`
> for row-level resolution filtering. See
> `js/packages/pyrmts-geo/src/spatial-index.ts` for the live source.

```ts
// Pluggable spatial index. Default impl (`h3Index`) lives in
// `h3-index.ts`. S2 impl (`s2Index`) lands in Phase 2. Pyrmts-geo's
// planner depends only on this interface, not on a specific backend.
export interface SpatialIndex<C extends string = string> {
  /** Backend identifier — `'h3' | 's2' | 'h13' | 't4'`. Used for
   *  routing + serialized in `Pyramid.geo.index` for query-time
   *  dispatch. */
  readonly name: string

  /** Levels available for materialization. H3: 0-15. S2: 0-30. */
  readonly maxLevel: number

  /** Cell id at the given level for a lat/lng. Exact. */
  latLngToCell(lat: number, lng: number, level: number): C

  /** Resolution / level of a cell id. Most backends encode this in
   *  the id (H3, S2); H13 derives it from the parent component.
   *  Used by `filterCellsAndRes` to drop wrong-level rows. */
  cellLevel(cell: C): number

  /** Parent cell id at level - 1 (or at `level` if provided). For
   *  H3 (default backend) this calls h3-js's `cellToParent` and is
   *  BT-affected for points in a BT; for S2 it's exact. */
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

(Revised 2026-05-31 — see top-of-spec pivot note.)

1. ✅ **Interface scaffolding** — `SpatialIndex` + `h3Index` default
   in `pyrmts-geo`. `assertSpatialIndex` conformance suite. Committed
   `a5c0f60`. ~1 day.
2. **S2 backend** (in-tree in `pyrmts-geo`, split out if it bloats
   deps). `s2Index: SpatialIndex` backed by `s2js`. ~3-5 days.
3. **`minimalCover` DP** — Phase-3-from-original-plan. Mutually-
   recursive `encodeInclude`/`encodeExclude` over the
   branching-factor-4 quadtree. ~2-3 days.
4. **Planner integration** — `cellInSet` mixed-resolution,
   `filterCellsAndResMixed`, end-to-end H13/S2 pyramid tests.
   ~1-2 days.
5. **ctbk rides-v3 (S2)** rebuild. Bake off against existing
   rides-v2 (vanilla H3 single-res). ~1 week.
6. **(Deferred) H13 backend** — only if S2 has deployment problems
   (CFW bundle limit hit, library bug). ~2 weeks.
7. **(Deferred) T4 backend** — only if both S2 and H13 are
   unsatisfactory. ~1-2 weeks.

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

### Phase 2 — S2 backend

- **Conformance suite** (reuse): run `assertSpatialIndex(s2Index, opts)`
  from Phase 1. Same shape that h3Index passes.
- **Exact lineage** (property — easy on S2): `∀ L, r:
  cellToParent(latLngToCell(L, r), r-1) === latLngToCell(L, r-1)`.
  Holds by construction on S2's quadtree. ~1000 random points across
  multiple levels.
- **4 children tile parent** (property): `cellChildren(P)` returns
  exactly 4 cells; their union has the same area as P (exact within
  numerical tolerance); no overlap with each other or with non-
  children. Tested via `contains` + area arithmetic.
- **Token round-trip**: `fromToken(toToken(ci)) === ci` for sample
  cells. (Cell ID is `bigint` internally; we serialize as hex token
  string for the `SpatialIndex<string>` contract.)
- **Latitude / longitude round-trip (per-level)**: `cellInSet(
  latLngToCell(L, r), r, {include: [latLngToCell(L, r)], exclude: []})
  === true`. Per-level containment, sanity check on token equality.
- **`bboxToCells` coverage**: for an NYC-scale bbox at multiple S2
  levels, returns a cell set that covers the bbox (every bbox-interior
  point's level-r cell is in the set). Doesn't test minimality of
  the cover; that's S2's RegionCoverer behavior and we trust the lib.
- **CFW compatibility smoke**: the `pyrmts-cfw` test suite continues
  to pass with an S2-backed pyramid (no Node-only deps leak in).
  Verifies s2js doesn't break the Workers build.

(Removed from this section: BT mass-action, 13-SR tile,
direction-enum tests — those were H13-specific.)

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

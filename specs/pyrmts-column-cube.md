# Column-cube: pivot nested categorical axes into materialized rollup columns

Status: **proposed — tabled** (2026-09-13). Captured for later; not scheduled. Cross-consumer (crashes + ctbk). Companion thinking to [`ctbk-serve-time-canonicalization.md`](./ctbk-serve-time-canonicalization.md) — same monoid-rollup primitive, applied to the *column* axis instead of the identity/geo *row* axis.

## Motivation

Several consumers face a combinatorial blowout of categorical dimensions:

- **crashes**: crash severity × vehicle type × conditions × victim severity × age/sex × hit-and-run × … — not a full cartesian, more a *sum of products* (some dims cross, others don't).
- **ctbk**: ymrgtb-style blowups (analogous categorical fan-out).

Today these blow up the number of **rows**. A pyramid picks one (or a few) sort keys; parquet can only prune/index rows *in that sort order*. Geo adds still more rows (s2 rollups). And some columns cram a nested axis into a single cell as a **histogram monoid stored as stringified JSON**, parsed per row.

## The core duality

Parquet gives you **one sorted, prunable row axis** (the sort key — range-scan + RG-stats pruning) plus **free random access on the column axis** (fetch exactly the column chunks you want). And **s2 rows are already row-wise monoid rollups** — an s2 cell's row aggregates the stations/points under it.

So the design lever is: **pivot the axis you want selective random access on into columns; keep the axis you range-scan as the sort key; let s2 rows roll up geo.** Rollup *columns* are the column-axis analogue of s2 *rows*, and they are valid for the same reason: the aggregates are **monoids** (sum / count / histogram-merge), so a coarser column is the monoid-sum of finer columns.

## Design sketch

- **Pivot chosen high-cardinality categorical axes into columns.** Not the full cartesian (that *is* the blowup) — a hand-picked *sum of products*: e.g. crashes materializes `[h&r × {vehicle type}]`, `[!h&r × {vehicle type}]`, `[veh model × condition]`, plus denser top-rollup columns (e.g. per-crash / per-s2-row vehicle-condition totals).
- **Rollup columns are monoid sums of base columns** — same combine that builds s2 rows and coarser tiers (`cascade`/`monoids`). Leaf columns are sparse; top-rollup columns are dense.
- **Highest-value, lowest-risk first step — pivot the stringified-JSON histogram monoids into real columns** (per-bucket columns, or a proper list/nested column). Removes per-row JSON parse, enables bucket-level random access and cross-row bucket sums (rollups) via the existing monoid combine. This is a `monoids.py` + `writer.py` change and is worth doing on its own, independent of the larger cube.
- **Planner** picks columns at the rollup level suited to the query, random-accesses them, and fetches only the required row groups.

## Constraints / what to design around

- **No hard column limit in the parquet format.** The real ceiling is the **footer**: `FileMetaData` carries per-column-chunk metadata for *every* column × *every* row group, Thrift-encoded, and a reader must parse the whole footer to plan *any* query. Cost is **O(cols × RGs), paid per query** — brutal in a CFW (limited CPU/mem). 1,000 cols × 10 RGs ≈ 10k chunk entries → footer easily MB+.
- **Sparse data compresses; sparse *metadata* does not.** An all-null column's data is ~one tiny page, but its footer entry is a fixed cost regardless of density. So "sparse columns are nearly free" is true on-disk, **false for footer/plan cost** — that asymmetry is the binding constraint.
- **Column grouping is the partition knob.** "One file per column" is the decomposition extreme: max column selectivity + no footer bloat from unwanted columns, but it destroys **co-location** (shared RG boundaries that let a row-range predicate prune all columns together) and reintroduces cross-file alignment. Parquet's premise is co-location. The real design question is *which columns share a file* — cluster columns queried together, isolate wide-sparse rarely-hit rollup columns.
- **Fleet manifest** (the mitigation for footer cost + sequential round-trips): precompute footer/RG metadata (offsets, sort-key + pivot-col stats) into a central index — **D1 is already the shard registry**, so it is well-placed for this. A query then does 1–2 reads to the manifest, prunes across *all* files at once, and fans out RG byte-range GETs. Essentially Iceberg/Delta manifests. The manifest is a derived artifact that must stay consistent with the files — i.e. another DVX dirty→regen node.

## The hard problem: which cuboids to materialize

You can't materialize the full sum-of-products (that's the blowup you're escaping), so you hand-pick hot aggregates against the query workload and a storage budget. This is the classic OLAP **partial cube materialization** problem; automating it (from query logs) is genuinely hard. Start with a handful of hand-picked hot combos + the JSON-histogram pivot; automate cuboid selection later. Every materialized rollup column is also a new derived DVX node — the dep graph grows a lot, so the manifest + reactive-regen story must be solid first.

## Suggested sequencing

1. **JSON-histogram monoids → real columns** (`monoids.py`, `writer.py`) — standalone win, cross-consumer, no cube machinery required.
2. **Fleet manifest** (footer/RG index in D1) — de-risks wide schemas and multi-file queries generally; useful even without the cube.
3. **Hand-picked rollup columns + planner column-selection** — the cube proper, informed by real crashes + ctbk query patterns.

## Open questions

- Column-grouping policy: how to cluster columns into files given query co-access + the footer-cost asymmetry.
- Manifest granularity and consistency model (per-shard footer snapshot as a DVX output vs. a rebuilt global index).
- Do we need a per-query column-selection cost model in the planner, or is "pick the coarsest rollup column that covers the query" sufficient?
- Interaction with the sort-key axis: when does pivoting a dim to columns beat adding it to the sort key (or a second pyramid keyed differently)?

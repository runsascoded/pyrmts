# Multi-scan consolidation: fold a re-observation axis into shards for O(N)→O(#changes) archival

Status: **Phase 1 + Phase 2 landed (2026-09-21/22); (b) interval confirmed on cw-s3's real scans (~78× projected @ N=81). Storage driver + streaming fold + out-of-core DuckDB backend (byte-identical to the Python oracle) landed; `multiscan` CLI takes `--engine python|duckdb`. TS read primitives (`diffScans`/`seriesFor`/`extractScan`/`readMultiScan`) landed in `js/packages/pyrmts`. Phase 2c (routing manifest + verify-then-drop; JSONL + D1 read) and Phase 2d (declarative `multiScan:` policy + idempotent `multiscan seal`, `fixed` capped-K and `exponential` Bentley–Saxe schemes) landed — consolidation is usable end-to-end and automatable. **Phase 3 — the diff-index (flat-changeset events log + optional aligned dyadic levels): the changeset between ANY two scans with no snapshot read at query time; `DiffIndexStore` + `diffindex update|diff` + TS `diffOverSpan` — landed, then re-laid-out (sliding → aligned; the sliding layout was O(N) storage per level).** It is the audit / churn / index-build primitive; the diff-treemap engine is the consumer's rendering-bounded walk over random-access snapshots (individual shards or MS archives) — measured on cw's real 10M-row fleet pair: 48 GETs in 13 dependent rounds per fleet-root view with no per-pair precompute, **0.85 s wall over HTTP at 30 ms per request** with the TS engine `walkDiff` (0.4 s local), rows identical to a materialized diff's view (`bench diff` / `bench-walk.mjs`, see "Bake-off"). Cross-consumer, but narrower than the tier ladder (see "Generality"). Origin: marin-gcs-usage (`wt/cw-s3`, session `41e25a3f`) re-scans a GCS fleet on a 12h cadence and stores **one full pyramid per scan** — each scan ~a duplicate of the last. This spec captures the pyrmts-side capability to compress that redundancy. It is a **storage-layer optimization, opt-in**, orthogonal to tiers / monoids / cascade; the read path must "speak" it too.

**Phase 1 result (synthetic sweep, `multiscan-bench`, 50k keys × 30 scans, 0.5%/scan births):** the **interval encoder (b) wins the entire churn sweep** — 27.7× vs. the O(#scans) baseline at 0% value-churn, 22.4× @ 1%, 13.8× @ 5%, still 6.7× @ 20% — and beats densify (a) at every point (e.g. 725 KB vs. 1027 KB at 0%). This confirms the prior below (b > a).

**Real-scan operating point (cw-s3, 2026-09-21):** cw-s3 measured the change-count directly in DuckDB over 6 consecutive real path-indices (`(path)`-total granularity, ~6.18 M paths in the union). **Real churn is ~0.020%/12h scan** (~1,265 of 6.18 M paths move per scan) — an order of magnitude *below* the synthetic sweep's gentlest setting. Interval (b) therefore collapses **5.9× at N=6, ~78× projected @ today's N=81, ~335× @ N=365 (daily·1yr), ~626× @ N=730** — the compression *grows with scan count* on near-static data. This is the "compress across scans → afford much higher scan frequency" loop, quantified, and it decisively settles (b) on real data. Two method notes from cw-s3, both feeding Phase 2:
- **Row-count ≈ byte-ratio proxy:** they measured change-count (= interval rows) in DuckDB, not by running pyrmts's Python encoder at full N — because `consolidate_tables` builds a per-scan Python key-dict (`_scan_rows`), and 81 × 6.18 M keys OOMs one process. This is a **streaming / out-of-core** limit, *not* a slow-language one (pyrmts's algo is already polars/pyarrow-backed native). It is the concrete reason **production consolidation wants the DuckDB port (or chunked streaming), not a Python-at-full-N encoder** — noted for Phase 2. A byte-exact end-to-end run of the actual encoder is being done on node mgu (laptop-unsafe at this N) for the exact parquet sizes; the crossover that gates Phase 2 is already confirmed.
- **Snapshot indices have no event-time `binCol`:** cw-s3's path-index is a pure snapshot keyed `(path)`, with no created-date axis. This is *not* an API gap — the mapping is a **constant `binCol`** (a `dt=0`-style column, exactly as the unit tests construct their shards): consolidation keys on `(binCol, *dims)`, so a constant binCol reduces the logical key to `(*dims)` cleanly. Document this in the driver contract (snapshot → constant-bin pyramid); no code change.

Companion: [`pyrmts-column-cube.md`](./pyrmts-column-cube.md) covers the *other* redundancy (across-tier, within a scan) — different axis, different mechanism; see "Two redundancies" below. Prior art in the origin repo: `specs/storage.md` there raises cross-scan queries + "RLE on repeated paths" but never specs the compaction itself.

## Two redundancies — don't conflate them

A repeated-scan dataset has redundancy on **two axes with opposite futures**:

1. **Across tiers, within one scan** — coarse tiers ≈ fine tiers because most keys occupy one bin (write-once data). This is *redundant materialization*, and it **self-decays**: as long-lived keys accumulate events over time their bin support widens and coarse tiers stop being duplicates. The pyrmts-generic form is **sparse / lazy tier materialization** (don't materialize a coarse rung that's a trivial rollup of a finer shard already within read budget) — the same family as the column-cube's "partial cuboid materialization". Weaker, decaying, folded into that spec. **Not this spec.**
2. **Across scans** — each observation of the whole keyspace is ~identical to the last. This is the durable O(N) prize, and it needs a capability pyrmts does **not** have today. **This spec.**

## The reframing: a second (observation) time axis

pyrmts is a *single-temporal-axis* library: it privileges exactly one axis — the `binCol` — and builds the tier pyramid by monoid-rolling-up along it. A repeated-scan dataset is **bitemporal**:

- **event time** — the binCol the pyramid already rolls over (in the origin: an object's created-date / `binstart`).
- **observation time** — *when the snapshot was taken* (the scan timestamp). pyrmts has no model for this; today it is handled by *foldering* — one full pyramid per scan — which is the O(N) blowup.

The key consequence: **the scan axis is a stack, not a rollup.** You never combine two observations of the same key into a coarser observation (summing bytes across scans of the same object would double-count); every scan stays individually queryable (point-in-time reads; scan-vs-scan diffs). So:

- The monoid / cascade / tier machinery is **untouched** — MS does not add a tier ladder along scans.
- MS is a **physical shard-layout variant**: same logical pyramid, but each shard file grows the scan axis as an inner dimension, and the encoding exploits that consecutive observations of a key are near-identical.

## Where it sits vs. pyrmts's model

A **multi-scan (MS)** is a consolidation of a contiguous set of single-scan pyramids covering the same event-time/tier structure. Within an MS:

- **Event-time partitioning is preserved.** Still disjoint `(tier, period)` shards, exactly like a regular scan's pyramid — the tier ladder and shard periods are unchanged. (Confirmed: this was the origin session's explicit question; the answer is yes.)
- **The scan folder collapses inward.** `<scan>/<tier>/<period>.parquet` (one file per scan) → `<ms>/<tier>/<period>.parquet` (one file per MS, stacking every scan in the range as a `scan` column / interval). An MS is addressed by its scan-range, and carries the ordered list of member scan labels in KV-metadata.

**Precondition for clean folding: the binCol must be scan-invariant per key.** Created-date satisfies this (an object's creation bin never moves), so a key's `(tier, period)` placement is stable across scans and the shards align 1:1. A binCol that *can* move per scan (e.g. `mtime`, if an object is rewritten between scans) is still representable — the migration is just a value/interval change (see encoders) — but the aligner keys on the `(binCol, *dims)` tuple, so a moved key reads as one interval ending and another beginning, not as an in-place edit. State this in the API contract.

## The consolidation contract

Input: an ordered list of scans `[(scan_label, reader)]`, all sharing one pyramid config. For each `(tier, period)` tile: read each scan's shard, align rows by the full key `(binCol, *dims)` across scans, and encode the per-scan value-vector of the monoid **state columns** along the scan axis. Output: one MS shard per `(tier, period)`. Pure, storage-backend-agnostic (a scan is an opaque label + a way to read its shard for a tile). No monoid *combine* is used in consolidation — values are stacked, not aggregated; the monoid identity is only needed to represent "key absent in scan s" (an s2/rollup-free zero) for the encoders below.

### Encoder (a): densify + parquet RLE/delta

Materialize the full `key × scan` grid per tile, sorted **scan-innermost**, so a fixed key's state values across all scans form a contiguous run → parquet delta + RLE + dictionary crush the constant runs at the byte level. Absent `(key, scan)` pairs need an explicit zero/tombstone (so runs stay well-formed).

- Cost: O(#keys × #scans) rows before compression; compresses to ~O(#keys + #changes) *bytes*, but the row/metadata overhead is paid on the churny tail (every birth/death is a run boundary).
- Simple; leans entirely on the parquet writer; trivially lossless (every cell is stored).

### Encoder (b): change-interval rows (SCD-2 / temporal-table)

Store **one row per distinct value-run**: `(binCol, *dims, *state_cols, scan_lo, scan_hi)`, coalescing adjacent scans with equal state at write time. A key that is stable across all N scans is **one row**, not N cells RLE'd down.

- Cost: O(#changes), *not* O(#scans). Never materializes the constant cells.
- Degrades gracefully on hot keys (a changed value just splits an interval).
- Slightly more read machinery (interval-overlap predicate instead of an equality pick) — but that predicate also gives diffs directly.

**Prior belief to test:** for bursty filesystem-scan data dominated by a stable long tail, **(b) wins** on both storage and robustness. The benchmark decides.

## Round-trip fidelity — what "safely delete the originals" actually requires

The goal is **lossless, verifiable** recovery, so a user can consolidate `scans → MS`, delete the individual scans, and later `extract MS → scans` (or a single scan) with a guarantee nothing was lost. A fidelity ladder, weakest-sufficient first:

1. **Logical round-trip (the contract).** The extracted scan yields the *same rows* (same `(binCol, *dims) → state` set) as the original. Both encoders provide this. Make it **verifiable**: consolidation stores a per-scan **content digest** (hash of the canonical sorted-row content) in KV-metadata; `extract` recomputes and asserts it. This is what lets a user delete originals fearlessly — the guarantee is provable, not trusted.
2. **Byte-identical file round-trip.** The extracted parquet == the original bytes. Strictly stronger, and generally **not achievable across two writers** (codec, row-group sizing, KV-metadata, page/dictionary choices, writer version all differ). It holds **only when pyrmts owns a deterministic (RGIP) shard writer** — consistent with pyrmts's existing byte-for-byte idempotency posture (cf. `specs/done/*`, the canonicalize RGIP guarantee), but *not* available for foreign-writer shards (e.g. the origin's DuckDB-produced files). Do **not** promise this by default.
3. **Verbatim sidecar (escape hatch).** For a user who genuinely needs bit-exact original files back (or whose downstream is content-addressed — see below), an opt-in mode stores the original file bytes alongside the compressed form for flagged scans. This *forfeits the cross-scan win* for those scans, so it is a niche fallback, not the path.

**DVX / content-addressing caveat:** if a downstream system keys on the shard *file* hash (DVX outputs, CA storage), a logically-equal but byte-different extracted file reads as a change. Options: re-key the downstream dep on the stored **content digest** (level 1) rather than the file bytes, or use the verbatim sidecar (level 3). Name this explicitly in the CLI docs — it is the one place the clean "logical RT is enough" story breaks.

## Read path (built after a/b is chosen)

An MS reader that speaks the chosen encoding:

- **Point (as-of):** given a scan label, return that scan's rows. (a): filter `scan == label`. (b): rows whose `[scan_lo, scan_hi]` contains it.
- **Diff (scan_i vs scan_j):** per key, compare state at i and j. (a): join two scan slices. (b): compare the two covering intervals — often cheaper (a key unchanged across the window is a single interval, so its diff is trivially zero). Makes age-diff a **direct read**, subsuming today's two-scan fetch.
- **Range:** the scans within `[lo, hi]` — a stack, not an aggregate (no monoid combine; that would be semantically wrong for snapshots).

This is the "query/fetch code must speak it" half. It slots beside the existing serve planner; the planner's tier selection is unchanged (event-time axis), MS only adds the scan predicate.

## The observation axis, unified: one index, three payoffs

Consolidation (storage) is only the first use of the observation axis. Its representation — the per-`(key)` value **stream across scans**, stored as SCD-2 change-intervals — is the same object three otherwise-separate problems want. A cross-session synthesis (pyrmts + marin-gcs-usage `41e25a3f`, 2026-09-21):

1. **Storage** — fold old contiguous scans into interval-compressed archives (this spec's MS mode). O(#scans) → O(#changes).
2. **Serve latency (diff-indexing)** — a diff over `(a, b]` is a **range-scan of the interval boundaries in that window**: only keys whose run starts/ends inside `(a, b]` changed, so it reads **O(changes-in-span)**, not two full snapshots. That is the age-diff / diff-treemap (dTM) primitive.
3. **Ingest compute** — a per-scan changeset (objects added/removed since the previous scan) *is* the increment that maintains the interval store, so ingest becomes O(changes) per scan instead of rebuilding the whole pyramid (the origin's "1h base explode" cost). Same object again.

So "diff-indexing" is not a separate feature from MS — it is the **delta-view read path** of the same observation-axis index, and incremental ingest is its **write path**. Building the index once buys all three.

### Diff-indexing is always-on; MS archival is opt-in — decouple them

The origin session's worry is correct: MS consolidation is opt-in and covers only *old contiguous* ranges, so it can't be the *only* source of diffs (recent/hot scans stay per-scan, and some ranges cross the archive boundary). The resolution: the **diff-index is a first-class serve capability over *all* scans**, reading interval rows where a range is consolidated and falling back to two per-scan `extract`s (the existing 2-index diff) where it is not. Both produce the same changeset shape, so the dTM sees one uniform API. The universal primitive is `diff(state_a, state_b)` over *any* two scan states (raw or extracted); the interval store just makes it sparse where present.

### Invertibility splits the two views (the design crux)

- **Aggregate byte/object totals** are an additive **group** (invertible). A per-scan/per-subtree total telescopes, so `diff = state[b] − state[a]` is exact and O(1) in `d` (the span), and the fleet/subtree **over-time line is a prefix-sum** along the scan axis. No hierarchy needed for the aggregate.
- **The changeset** (which paths moved, and added-vs-removed churn) is a **non-invertible** set operation — remove-then-re-add across a span nets to zero but is real churn, so you cannot subtract two aggregates to recover it. It is well-defined only from the two **endpoint states** (or a monoid segment-tree of changesets, no subtraction). This is the part the interval-boundary scan serves sparsely.

### The diff-index: a flat-changeset events log with aligned dyadic levels (Phase 3) — built, then corrected

**Two corrections, in order.** (1) Storage consolidation does *not* give cheap flat diffs between arbitrary scans: an interval archive is key-sorted, so "rows whose `__scan_lo`/`__scan_hi` fall in `(i, j]`" is a full archive scan whether the pair is inside one archive or straddles a junction (RG min/max on the interval columns prune nothing — every row group spans all scans). The archive is a **snapshot store with per-key random access into any scan** (RG-prune by key, filter `__scan_lo ≤ s < __scan_hi`), not a diff index. (2) The diff-index is **not the diff-treemap engine**, and the first cut of this section overstated it as such. A treemap needs O(rendered) work: a best-first tandem walk that merge-joins the two listings of each expanded node and descends only where |Δ| is above screen resolution (disk-tree's `recursive_diff` / the cloud base's `buildDiff` — both already do this, over random-access snapshots; individual shards or MS archives both serve "children of P at scan s"). A **flat changeset** — every key whose state differs between scan i and j, no tree, no frontier — is O(changes-in-span), far more than the rendered cells over a long span of a big fleet. So the diff-index is the **audit / changelog primitive**: "what changed", gross churn (Σ|Δ_leaf|, which net rollups cannot give), and the *build input* for a materialized pairwise frontier on a non-adjacent pair (compose the L0 range instead of loading two full snapshots; unchanged-sibling context then comes from per-dir reads of scan j). What makes the diff-*treemap* fast is the walk's own levers (rendering-bounded stop criterion, small row groups, a decoded-footer cache, Arrow-native listings, batching expansions by RG) — tracked in disk-tree's `specs/pyrmts-adoption.md` §5.1.

**Structure.** A *changeset* is `{key → (before, after)}` over the keys that differ across a span (birth = identity→v, death = v→identity). Composition is associative but **non-invertible** (remove-then-re-add across a span nets to zero yet is real churn — you cannot subtract endpoints), so a span is covered by **disjoint** blocks. **Level 0 is the events log**: per scan, one **adjacency changeset** (scan k → k+1) computed from the two consecutive snapshots — the single O(fleet) step, **paid once per scan at ingest** — stored at `L0/{k}`. That is O(total changes) storage, strictly smaller than the interval archive (O(keys + changes)), and a range read of `(i, j]` returns O(changes-in-span). **Levels 1..L** (the index's `levels` cap, default 0) hold **aligned** power-of-2 nodes: `(level, start)` with `start` a multiple of `2^level` = the net change from scan `start` to `start + 2^level` — the segment-tree / Fenwick layout. `diff(a, b)` composes the disjoint blocks of `aligned_blocks(i, j, levels)`: greedy largest aligned block at each position, ≤ 2·log2(j−i)+1 blocks with enough levels, the j−i adjacency nodes at `levels = 0`, **with no snapshot read at query time**.

**Why aligned, not sliding (the correction that changed the layout).** The first cut stored a node at *every* `(level, i)` (`i = m − 2^level` on each append — a sparse-table / binary-lifting layout, popcount(j−i) reads). That is right when node values are O(1) scalars and wrong when they are span-sized changesets: level L has N−2^L nodes of up to 2^L·c entries, Σ ≈ c·N² before netting. At cw's operating point (c ≈ 20K changes/scan, N = 730 scans/yr, ~100M keys) that is ~9.3G entries/yr vs ~115M for the interval archive — an ~80× blowup. Aligned blocks give N/2^L nodes per level, each level bounded by L0's total, so the whole index is ≤ `(levels+1) ×` the events log (~146M/yr at 10 levels for cw, ~1.3× the archive) and netting only shrinks the higher levels. Higher levels pay off when the same keys churn repeatedly over long spans (a file edited daily: 365 L0 entries vs one in the year node — disk-tree-shaped); for monotone fleet churn they buy only fewer reads. Hence the cap, opt-in, default L0 only.

| layout | entries / yr (cw) | vs interval archive |
|---|---|---|
| interval archive (keys + changes) | ~115M | 1× |
| L0 only (adjacency events log) | ~15M | 0.13× |
| aligned dyadic, 10 levels | ~146M | 1.3× |
| sliding dyadic (first cut, removed) | ~9.3G | ~80× |

**Append-only.** Appending scan m writes the L0 node at `m−1` plus, for each level `L ≤ levels` with `2^L | m`, the aligned node at `m − 2^L` composed from two level-`L−1` nodes — ~2 node writes per scan amortized — and never touches an existing node, so persisted nodes are immutable and a from-scratch build equals an incremental one.

**Implementation.** `pyrmts.diffindex` (pure: `changeset_between` / `compose_changesets` / `changeset_to_table` / `changeset_from_table` / `aligned_blocks` / `SparseDiffIndex(levels=)` with `append` and `entries()`) — verified against the 2-snapshot oracle for every pair at caps 0/1/3 over a churny history incl. remove-then-re-add; blocks proven aligned, disjoint, capped and within the bound; per-level storage proven ≤ L0. `pyrmts_engine.diffindex_store.DiffIndexStore(…, levels=None)` persists it over any `Storage` (`{prefix}/index.json` = `{dataset, scans, levels}` + `{prefix}/L{level}/{start}.parquet`, standard changeset-table rows); the manifest's cap governs an existing index (a conflicting explicit cap is rejected); `update(scan_tables)` is the **idempotent ingest stage** (append every not-yet-indexed scan, in order; no-op when nothing is new — a cron fires it each cycle) and `diff(a, b)` fetches only the aligned blocks (proven: no snapshot read). CLI `pyrmts-engine diffindex update [-L levels] | diff`. TS twin in `js/packages/pyrmts/diffindex.ts`: `diffOverSpan(scans, schema, a, b, loadNode, levels)` + `alignedBlocks` / `composeChangesets` / `changesetBetween` / `readChangesetNode` / `parseDiffIndexManifest` (→ `{scans, levels}`), mirrored to the same node set and oracle. A reversed pair (`a` after `b`) swaps before/after — a *single* changeset reverses; only composition is non-invertible.

**How it relates to the storage layer.** Two orthogonal artifacts, both on by default: the **MS store** (fixed / exponential) bounds space and serves the **over-time line** (`seriesFor` / `seriesAcrossGroups`) *and* per-dir random access for the walk; the **diff-index** is the flat-changeset events log for audit / churn / index builds. Kept separate (they answer different queries); the exponential MS blocks and the aligned diff-index levels share the dyadic shape and could be unified later.

**Consumer wiring.** The diff-treemap is *one* widget in disk-tree's `cloud` base; cw/gcs inherit it by rebasing onto that base. Its engine is the walk; the diff-index's serve-side wiring (fetch `index.json` + aligned nodes → `diffOverSpan`) is for the audit / changelog view and the index build, and is DT's, once — not per-consumer.

### Bake-off: index-free diff walk vs. materialized pairwise diff (measured on real scans)

The question behind the diff-treemap engine: does it need any per-pair index, or is a **rendering-bounded best-first walk** over two random-access snapshots enough? `pyrmts-engine bench diff` (`pyrmts_engine.bench_diff`) runs both on real path-index scans with per-stage CPU, row-group reads, coalesced range GETs, bytes, and a modelled wall time (`CPU + dependent rounds × RTT`, a round = all GETs of one tree level, ⌈n / parallel⌉ trips each). The walk: expand every pending dir of the shallowest level in one round (a child can only be listed after its parent was; siblings are independent), list children on both sides (RG-pruned by `(depth, path)` stats, one contiguous run → one GET), merge-join, push changed subdirs; a dir whose size on both sides and |Δ| are all below the **render floor** (`root_bytes × cell_px² / canvas_px²`) is never expanded. Levers: decoded-footer cache, per-request RG cache, vectorized `filter` listing (vs `bisect` over per-RG sorted keys).

cw's real fleet pair (2026-09-15T12 → 09-16T00; 10.2M / 10.3M rows, 960 TB, 8192-row RGs), 1400×340 canvas, 4 px cells:

| view | walk: expansions / listings | RG reads → GETs / bytes | walk CPU | dependent rounds → modelled wall @30 ms / @80 ms RTT | materialize (once per pair) | view rows (walk = materialized) |
|---|---|---|---|---|---|---|
| fleet root (floor 32 GB) | 98 / 196 | 94 → 48 / 26 MB | 72–92 ms | 13 → 0.46 s / 1.1 s (best-first order: 26 → 0.88 s / 2.2 s) | 5.3 s (load 0.6 + join 4.6) → 87K diff rows; slice 7 ms | 121 = 121 |
| `…/tmp` (floor 5.1 GB) | 71 / 142 | 77 → 33 / 22 MB | 71–74 ms | 13 → 0.46 s / 1.1 s | same pair | 94 = 94 |
| `…/users` (floor 56 MB) | 1 / 2 | 2 → 2 / 0.6 MB | 17 ms | 1 → 0.05 s / 0.1 s | same pair | 0 = 0 |
| root, 2 px cells (floor 8 GB) | 122 / 244 | 102 / 28 MB | ~0.45 s (bisect listing) | | | |

disk-tree's repo pair (`/Users/ryan/c/disk-tree`, 2026-08-16 → 09-07; 91K / 130K rows, 1–2 RGs of 64K–130K rows): 171 expansions / 342 listings in 3 RG reads (the whole file), CPU 0.11 s (`bisect`) / 0.24 s (`filter`); materialize 54 ms; view rows 365 = 365.

Lever deltas at the cw root view: no RG cache → 406 RG reads / 113 MB (4.3×; sibling listings share RGs); `bisect` listing → CPU 0.4 s vs `filter` 0.08 s at 8K-row RGs (the per-RG key build dominates; `bisect` wins only at 64K+-row RGs); footer parse 7–15 ms per open, cached thereafter. `locate` is ~0 once the RG key ranges are bisected (built once per open from the footer stats — what a per-RG stats table in D1 would answer without the footer).

**The deployable engine, measured (TS, over HTTP).** `walkDiff` in `js/packages/pyrmts/src/walkdiff.ts` is the TS twin of the harness over `Storage` (row groups located by bisection over the footer's `(depth, path)` stats; one range GET per contiguous run of uncached row groups, then only the four walk columns decoded from memory; in-flight reads deduplicated so concurrent sibling listings share a row group; decoded-footer cache with `If-Match` on the warm path; level-synchronous rounds with `parallel` listings in flight). `scripts/bench-walk.mjs` runs it against any HTTP origin with Range support (`httpStorage`); `scripts/serve-range.mjs` serves a directory locally with an optional per-request latency. Same cw pair, fleet-root view, Node 26, measured wall:

| origin | order | RG reads → GETs / bytes | dependent rounds | decode CPU | wall |
|---|---|---|---|---|---|
| local HTTP (~0 ms) | level | 94 → 48 / 26 MB | 13 | 0.32 s | **0.39–0.55 s** |
| local HTTP + 30 ms per request | level, 8 in flight | 94 → 48 / 26 MB | 13 | 0.35 s | **0.85 s** |
| local HTTP + 30 ms | level, 32 in flight | same | 13 | 0.33 s | 0.83 s |
| local HTTP + 30 ms | best-first, one at a time | same | 26 | 0.34 s | 1.29 s |
| `…/tmp` view, local HTTP | level | 77 → 34 / 22 MB | 13 | 0.27 s | 0.34–0.44 s |

The model (`CPU + rounds × RTT` = 0.4 + 13 × 0.03 ≈ 0.8 s) matches the 30 ms measurement. The cross-row-group fetch coalescing the walk first did by hand (one GET per run of row groups, then decode from memory) now lives in the hyparquet fork (`runsascoded/hyparquet` ≥ `d999e10`, `specs/done/byte-range-coalescing.md` there: `maxOverfetchRatio` / `maxRunBytes`, format-agnostic, per call); pyrmts is pinned to its `dist` and `readRun` is a plain `parquetReadObjects({ columns, maxOverfetchRatio, maxRunBytes })` — same 48 GETs and rounds, 21.7 MB (hyparquet also trims a run's unselected leading/trailing chunks). Two things the Python harness could not show: (a) without in-flight deduplication the parallel rounds re-read shared row groups (290 RG reads / 85 MB instead of 94 / 26 MB), and (b) hyparquet's decode is the CPU floor: ~3.4 ms per 8192-row RG for four columns, ~7 ms for all eleven cw columns (2 s per view before projecting), so the row-group size and the shard's column count are the consumer-side levers left — 2048-row RGs would cut decoded rows per listing ~4× at a ~4× larger footer, which the footer cache absorbs.

**Reading.** (1) The walk needs **no per-pair precompute**: a fleet-root diff between any two scans is ~0.1 s CPU and ~50 range GETs in 13 dependent rounds, so ~0.5–1.1 s at cloud RTTs (level-synchronous; best-first order doubles the rounds); materializing the pair costs ~5 s once, then ~5 ms per view. Both are viable; the walk is the default because it works for *any* pair with nothing built, and the materialized index is the accelerator for a hot pair (adjacent scans) if a consumer wants sub-100 ms. (2) At the render floor the walk's rows **equal** the materialized view's (after collapsing rows under added/removed ancestors, which both represent as one row) — no blind spot showed on real data; the known one (change under a dir whose size *and* count are both unchanged, e.g. a same-size rename) needs an `mtime`/digest column in the descend trigger and is invisible to a materialized size-diff too. (3) Round trips and decode share the wall time remotely (13 × RTT ≈ 0.4 s vs 0.35 s of hyparquet decode at 30 ms RTT), and the dependency structure (parent listing before child) puts a floor of one round per expanded tree level under them: 13 of the 15 levels needed a GET at the fleet root. Below that floor the remaining levers are fewer levels (a coarser floor), a hot pre-listed top of the tree, or the decoded-footer + If-Match warm path (no `head`, no footer round trip per shard — landed in `fetchShardData`). (4) The 12 h diff is 87K changed rows out of 10M (0.85% of rows; the interval archive's 0.02%/12 h is per-*key* churn on objects, this counts every ancestor of every changed object).

### Over-time plot = the same index, state-view point read

The "over time" plot (scan × path total size) is a **per-key series along the observation axis** — exactly `series_for(key)`: expand that key's interval rows to one value per scan. The fleet/subtree over-time line is the prefix-sum aggregate above. So the two remaining mgu plots map cleanly onto **two indexed axes**, no third structure:

- **age histograms** → the **event-time** pyramid (built).
- **over-time line + diff treemap** → the **observation-axis** index (this spec): over-time = state-view point read; dTM = delta-view range read.

## Scan-location manifest (routing) — Phase 2c

Decoding a multi-scan shard is self-describing (the `pyrmts.multiscan` KV-metadata), but that only helps *once the reader holds the right shard*. A separate, prior problem is **routing**: "scan `S`'s tile for `(tier, period)` lives *where* — an individual per-scan shard, or folded into which MS archive (at which fold index)?" The reader must answer this **without footer-reading or listing every parquet** — i.e. from the manifest, exactly like the single-scan path.

**What exists.** pyrmts already has that manifest: `ShardIndex` (`shard_index.py` / `shard-index.ts`), with a D1-backed impl (`pyramid_shards(pyramid, tier, shard_dur, period_start, period_end, key, written_at)`, scoped by `pyramid`). A repeated-scan consumer stores each scan as its own `pyramid`, so an individual scan is already routable. **But the schema has no scan axis** — it cannot express "scans `s0..s9` for this tile are consolidated at key `K`, individuals dropped." So MS routing is *not* covered by the existing index.

**The overlay (additive, no migration of `pyramid_shards`).** A sibling manifest — D1 table `pyramid_multiscans(dataset, tier, shard_dur, period_start, period_end, key, scans, encoder, written_at)` (PK `(dataset, tier, shard_dur, period_start)`; `scans` a JSON array of the ordered member labels) — scoped by a `dataset` (the repeated-scan family). Routing for scan `S`, tile `(tier, shard, period)`:

1. Look up `pyramid_multiscans` for `(dataset, tier, shard_dur, period)`. If a row's `scans` contains `S` → route to that row's `key`, fold-index `= scans.indexOf(S)`; fetch once, `extract_scan` / `diff_scans` / `series_for`.
2. Else fall back to the existing single-scan `ShardIndex` (`pyramid = S`) → the individual key.

The overlay is orthogonal to `pyramid_shards` (which stays the live per-scan truth); consolidation *adds* an `pyramid_multiscans` row and, once verified, *removes* the now-covered individual shards + their `pyramid_shards` rows.

**The safety contract (why routing + drop must be one flow).** "Consolidate, then delete individual scans without unrecoverability" only holds if the order is: **write MS shard → digest-verify every member (`extract` == stored digest) → record the `pyramid_multiscans` row (reads now route to `K`) → only then drop the individual shards + rows.** A crash between any two steps is safe: before the manifest row, reads still hit individuals; after it, individuals are redundant. Dropping before verify+record is the only unsafe order, so the driver never does it.

**Division of labor.** pyrmts owns the record shape + the JSONL/`Storage` impl + the pure routing function (`resolveScan`), on both the Python (driver writes) and TS (reader routes) sides; the **D1 table is the consumer's backing store** (their glue), same split as `pyramid_shards`. The row shape above is D1-ready; the first impl is JSONL-through-`Storage` (mirrors `StorageJsonlShardIndex`), D1 a follow-up.

## CLI (in `pyrmts_engine`, mirroring `canonicalize`)

- `pyrmts-engine consolidate -r <scan-lo>/<scan-hi> [--group <stride>] [--encoder a|b] <config>` — consolidate a contiguous scan range into one MS, or, with `--group N`, into a sequence of MS archives each covering N scans (e.g. a year of daily scans grouped 10 at a time → ~36 archives). Storage-agnostic; the project supplies how scan labels map to shard locations.
- `pyrmts-engine extract [--scan <label>] <ms>` — reconstruct all member scans (default) or one, digest-verified (level 1). This is the recoverability guarantee that makes deleting originals safe.

Grouping/stride and the encoder are **project-tuned knobs** — pyrmts offers the pure orthogonal algo and takes no position on archival policy (how far back to consolidate, group size, when to delete originals). That is the "minimal prescription" design the origin session asked for.

## Benchmark plan (the gate)

The win is empirical — it hinges on the stable long tail dominating the churny dirs (tmp-ttl / ckpt change a lot; the long tail does not). Deliver, in order:

1. **Encode + extract for both (a) and (b)** (the pure algo; no serve path yet). ✓ `pyrmts/multiscan.py`.
2. **Synthetic-churn harness** in pyrmts: a generator with a tunable churn fraction, producing N aligned single-scan shard-sets; measure rows + bytes for (a) vs (b) vs the O(N) baseline across the churn sweep → the crossover curve. ✓ `pyrmts-engine multiscan-bench` (result in Status above — interval wins the sweep).
3. **Real operating point:** cw-s3 runs the same algo against the real scans (they hold the L2 files + the reader) → the actual ratio and the a/b verdict. pyrmts stays dependency-free of their storage. ✓ **Done (2026-09-21):** ~0.02%/scan real churn → ~78× projected @ N=81 (see Status). Byte-exact node-mgu encoder run in flight for exact sizes.
4. **Decide a/b**, then build the storage driver + read path. ✓ **(b) decided**; ✓ **streaming storage driver landed** (Phase 2a — `consolidate_scans` + `multiscan` CLI); read-path serve integration is the consumer's (cw-s3).

Measure: total bytes (the O(N) claim), row counts (metadata overhead on the tail), and extract-verify (logical RT holds for both).

## Deliverables / phasing

- **Phase 0 (this spec).** ✓ Committed.
- **Phase 1 (prototype, unblocks the decision).** ✓ **Done.** Pure algo (`pyrmts/multiscan.py`: `consolidate_tables` / `extract_table` / `scan_digest` / `MultiScan`, both encoders, exported from `pyrmts`), digest-verified logical RT, and the synthetic `multiscan-bench` harness (`pyrmts_engine`). 13 tests (`test_multiscan.py` + a `test_cli.py` case). cw-s3 wires `consolidate_tables` over their real scans for step 3 — no pyrmts dependency on their storage.
- **Phase 2a (storage driver + streaming fold).** ✓ **Done.** The two real-scan requirements are met:
  - **Streaming fold — `consolidate_scans(iterable, pyramid)`** (`pyrmts/multiscan.py`): folds one scan at a time over a lazily-yielded `(label, table)` iterator, maintaining only the open-run frontier + the current scan. Peak memory is **O(#keys per tile), not O(#keys × #scans)** — it removes the N factor that OOM'd the eager `consolidate_tables` at 81 × 6 M keys. Byte-identical to the eager interval encoder (tested). This is the **portable reference + correctness oracle**: dependency-light, fine to mid scale, but its working set is *Python objects* (an M-key `open_runs` dict is multiple GB), so it is not the fleet-scale path.
  - **DuckDB backend — `pyrmts_engine.multiscan_duckdb`** (optional `[duckdb]` extra): the same SCD-2 interval encoding as vectorized gaps-and-islands over `read_parquet` — columnar, out-of-core (spills to disk), far less memory and faster. `consolidate_parquet_duckdb` reads shards directly off disk (the real fleet-scale path); `consolidate_arrow_duckdb` is the in-memory twin. **Byte-identical to the Python oracle** — verified on the fixture, a histogram fixture, and a synthetic churn sweep. Selectable in the driver / CLI via `--engine duckdb` (currently FsStorage scans; S3 via DuckDB httpfs is a follow-up). This is the answer to "what does the DuckDB producer buy" — real, non-consumer-specific tradeoffs (out-of-core + native speed), so it belongs upstream, not in a consumer.
  - **Self-describing shards — `to_arrow` / `from_arrow`**: a written multi-scan shard carries its encoder / ordered member scans / per-scan content digests as parquet KV-metadata (`pyrmts.multiscan`), so extract is digest-verifiable with no sidecar.
  - **Storage driver — `pyrmts_engine.multiscan_driver` + the `multiscan` CLI group** (`consolidate` / `extract`): read a range of per-scan tiles from storage (each scan an opaque `(label, Storage)`; the reference CLI uses subdir-per-scan `<root>/<label>/`), stream-consolidate to multi-scan shards, and reconstruct any member scan back — digest-verified before an original is dropped. Dependency-free of any consumer's storage.
  - **Snapshot indices** (no event-time axis, e.g. cw-s3's `(path)`-keyed index) map to a **constant-`binCol` pyramid** (`dt=0`); no code change — the driver tests exercise exactly this shape.
- **Phase 2b (read primitives).** ✓ **Done.** TS read primitives landed in `js/packages/pyrmts` (`multiscan.ts`): `diffScans` / `diffTables` / `seriesFor` / `extractScan` + `readMultiScan`, mirrored to the Python fixture. A consumer's over-time / diff endpoints are thin wiring over these.
- **Phase 2c (scan-location manifest + safe drop + capped-K).** ✓ **Done.** The routing overlay end-to-end:
  - **Manifest:** `pyrmts_engine.multiscan_index` — `MultiScanRecord`, `MemMultiScanIndex` / `StorageJsonlMultiScanIndex`, `resolve_scan`, and the **D1 schema pyrmts owns**: `multiscan_d1_ddl` (`pyramid_multiscans`, PK `(dataset, key)`) + `multiscan_d1_row` (scans/digests JSON-encoded) — the consumer writes rows via its own CF-D1 path.
  - **Driver:** records a row per archive (`consolidate_range(ms_index=, dataset=)`), and `drop_consolidated_scans` verifies-then-deletes the individuals (safe tail).
  - **Capped-K:** `consolidate_groups` / CLI `--group-size K` seals every K consecutive scans into its own immutable archive (keyed by period + the group's first-scan label so groups don't collide), one manifest row each — the sealed-not-appended model that avoids the O(K²) rewrite. CLI: `multiscan consolidate --dataset --index --group-size --drop`.
  - **TS reader:** `js/packages/pyrmts` — `MultiScanIndexEntry`, `parseMultiScanIndex`, `resolveScan`, and `seriesAcrossGroups` (order a tile's groups by scan span, `seriesFor` per group, concat — cross-group over-time stitching, IO via a consumer `load` callback). `js/packages/pyrmts-cfw` — `MultiScanD1Index` (reads `pyramid_multiscans` from D1 → `MultiScanIndexEntry[]`, interchangeable with the JSONL reader) + `multiScanDdl`.
  - **Division:** pyrmts owns the DDL + row shape + read query + routing/stitch; the consumer owns the D1 instance + migration + the write (via its CF-D1 HTTP machinery, e.g. replaying the JSONL manifest into D1). Grouping *policy* (K, cadence, trigger) stays consumer-side (a lazy seal stage at the end of the scan-emit job), pyrmts stays pure mechanism.
- **Phase 2d (grouping policy + automatic sealing).** ✓ **Done.** A declarative `multiScan:` policy block on the pyramid config (`MultiScanPolicy`: `dataset`/`tier`/`shard`/`scheme`/`drop`, parsed by `yaml.py`, write-side only — the reader never needs it), plus `multiscan seal <config>`: an **idempotent** consolidation a consumer's cron/end-of-emit stage fires each cycle (pyrmts owns the *what*, the consumer owns the *when*). Two schemes:
  - **`fixed`** (`groupSize: K`) — seal every K not-yet-sealed scans into an immutable capped-K archive; skip already-sealed. Archive count O(N/K). Good for steady churn (cw). `seal_new_groups`.
  - **`exponential`** (`base: 2`) — the **logarithmic method (Bentley–Saxe / LSM leveling)**: reconcile archives to the dyadic decomposition of N — old scans coalesce into `base`-power blocks, recent scans stay small — so the archive count is **O(log N)**, not O(N/K). Best for low-churn data kept indefinitely (disk-tree's public demo), where a fixed scheme accretes unboundedly-many tiny archives. Costs O(log N) rewrites/scan as it climbs levels (write amplification — the right trade for a rarely-churning archive). `seal_dyadic` recomputes the target block set each run, (re)builds changed blocks by **merging their scans from wherever they currently live** (a smaller archive via `extract`, or the individual), drops superseded archives, drops now-covered individuals, and rewrites the manifest. Digests chain correctly through merges (extract is a faithful logical RT). This is the **same dyadic structure** as the deferred O(log d) diff-hierarchy, so it also sets that up.
  - **Consumer note:** the exponential scheme *deletes* superseded manifest rows (compaction), so a consumer replaying the JSONL manifest into D1 must handle **deletes**, not just inserts (`rewrite` semantics), and its CF-D1 sync must mirror the row set, not append-only.
- **Phase 3 (diff-index).** ✓ **Done, layout corrected.** The flat-changeset events log (L0) + optional aligned dyadic levels — see "The diff-index" above. `pyrmts.diffindex` (pure core, oracle-verified for all pairs at several caps; per-level storage ≤ L0) + `pyrmts_engine.diffindex_store.DiffIndexStore` (append-only immutable nodes, ~2 writes/scan; manifest carries `levels`; idempotent `update` ingest stage; `diff` reads only the aligned blocks — proven no snapshot read) + CLI `diffindex update [-L] | diff` + TS `diffOverSpan(…, levels)`. It is the audit / churn / index-build primitive, not the diff-treemap engine (that is the consumer's rendering-bounded walk over random-access snapshots). Serve-side wiring is DT's base's, once; cw/gcs inherit.

## Upstreaming boundary: kernels upstream, glue stays

cw-s3's obs-axis build (`specs/obs-axis-indexing.md`) names a "DuckDB producer" and a "CFW reader" as the consumer's. Both are **composites**, and the generic kernel of each belongs in pyrmts (the app-specific glue does not):

- **Producer** = [consumer's raw ingest → aligned per-scan tables] (**app territory** — pyrmts already draws this line via the `--source module:attr` hook) + [aligned tables → SCD-2 intervals] (**generic** — Python `consolidate_scans` + the DuckDB backend above, both landed here).
- **Reader** = [generic diff-over-span / over-time / point reads of the MS shard] (**generic** — Python `diff_scans`/`series_for`/`extract_table` landed; the **TS twins landed in `js/packages/pyrmts`** — `diffScans`/`diffTables`/`seriesFor`/`extractScan` + `readMultiScan` (parses rows + the `pyrmts.multiscan` KV-metadata a Python-written shard carries; int64 normalized to number like the fetch path). Pinned to the Python fixture behavior by mirrored tests) + [consumer's routes / R2 bindings / auth / API shape] (**app glue** — stays with the consumer; the `pyrmts-cfw` handler is thin wiring over these primitives).

With the TS primitives landed, the generic read + encode kernels are all upstream (Python + DuckDB encode; Python + TS read); only consumer-specific glue (raw ingest, HTTP routes/auth) stays out. The tradeoffs that motivate the DuckDB path (out-of-core, native speed) are **not consumer-specific** — every fleet-scale user wants them — which is why the backend is here, not in cw. **A consumer pins pyrmts at a SHA, so upstreaming/refactoring here cannot affect a consumer's in-flight build until they bump the pin** — the cost of upstreaming early is nil, and the benefit is that consumers build against the shared seam instead of hand-rolling then back-porting (cf. `pyrmts_engine/consolidate.py` absorbing ctbk's `lambda_exec.py` generic core — but done *ahead* of the consumer, not after).

## Open questions

- **Encoder:** ~~(a) densify+RLE vs (b) interval-rows~~ — **decided: (b)**, which wins the whole synthetic churn sweep *and* cw-s3's real scans (~78× @ N=81 on ~0.02%/scan real churn; see Status). Closed.
- ~~**Config surface:**~~ **Resolved.** Two distinct questions were conflated: (1) does the *reader* need a config block to decode/route? **No** — shards are self-describing + the manifest routes. (2) does *automatic grouping* want declarative config? **Yes** — a `multiScan:` policy block (Phase 2d) holds the grouping *parameters* (scheme/K/base/tier/dataset/drop) so `multiscan seal` is parameter-free; the *trigger/cadence* still stays consumer-side. The block is write-side only (engine), not on the TS `Pyramid`.
- **Digest definition:** canonical sorted-row hash over which columns, and how to make it writer-independent (so it survives a foreign-writer original) — the sorted logical tuples, not the file bytes.
- **Non-invariant binCol:** for `mtime`-style movable bins, is interval-splitting on key migration acceptable, or do some consumers want a key-identity notion that tracks a moved bin? Start with tuple-keying; revisit only if a consumer needs it.
- **Grouping ergonomics:** overlapping vs. strictly-partitioning groups; re-consolidating an MS with newer scans appended (incremental MS) vs. one-shot.

## Generality

Narrower than the tier ladder. The ladder generalizes to every timeseries consumer (ctbk, crashes); the **re-observation axis** is specific to **repeated full scans of a mutable keyspace** — bucket/filesystem usage, inventory snapshots, anything re-measured on a cadence. ctbk and crashes have event-time but no re-observation, so this is an **opt-in snapshot/archival index style**, not a default pyramid feature. Offered as a pure, orthogonal algorithm any such project can tune for itself — an O(N) storage win on frequently-scanned, infrequently-updated data.

# Multi-scan consolidation: fold a re-observation axis into shards for O(N)→O(#changes) archival

Status: **Phase 1 + Phase 2 landed (2026-09-21/22); (b) interval confirmed on cw-s3's real scans (~78× projected @ N=81). Storage driver + streaming fold + out-of-core DuckDB backend (byte-identical to the Python oracle) landed; `multiscan` CLI takes `--engine python|duckdb`. TS read primitives (`diffScans`/`seriesFor`/`extractScan`/`readMultiScan`) landed in `js/packages/pyrmts`. Phase 2c (routing manifest + verify-then-drop; JSONL + D1 read) and Phase 2d (declarative `multiScan:` policy + idempotent `multiscan seal`, `fixed` capped-K and `exponential` Bentley–Saxe schemes) landed — consolidation is usable end-to-end and automatable. **Phase 3 — the always-on diff-index (dyadic changeset-hierarchy): O(log N) diffs between ANY two scans, no snapshot read at query time; `DiffIndexStore` + `diffindex update|diff` + TS `diffOverSpan` — landed.** This corrects the earlier claim that storage consolidation gave cheap any-pair diffs (it doesn't: cross-archive junctions cost O(fleet)); the diff-index is the separate serve-side structure that does.** Cross-consumer, but narrower than the tier ladder (see "Generality"). Origin: marin-gcs-usage (`wt/cw-s3`, session `41e25a3f`) re-scans a GCS fleet on a 12h cadence and stores **one full pyramid per scan** — each scan ~a duplicate of the last. This spec captures the pyrmts-side capability to compress that redundancy. It is a **storage-layer optimization, opt-in**, orthogonal to tiers / monoids / cascade; the read path must "speak" it too.

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

### The always-on diff-index: a dyadic changeset-hierarchy (Phase 3) — built

**Correction to the earlier framing of this section (and of "one index, three payoffs").** Storage consolidation does *not* give cheap diffs between arbitrary scans. `diff_scans` is O(changes-in-span) only *within one archive*; across archives — any fixed-K or exponential layout — a pair straddling a **junction** has no stored delta between the two adjacent scans on either side, so detecting which keys changed there is a full-state comparison, O(fleet). Fixed-K makes a far-apart diff O(#groups) or O(fleet); exponential bounds the archive *count* but leaves the junction problem. Bounding *storage* and bounding *diff cost* are different problems, and this section had been treating the diff-index as a data-gated refinement. It is the **1C feature**: "an efficient diff between any two scans, on by default." It is a separate, serve-side structure, and it is now built.

**Structure.** A *changeset* is `{key → (before, after)}` over the keys that differ across a span (birth = identity→v, death = v→identity). Composition is associative but **non-invertible** (remove-then-re-add across a span nets to zero yet is real churn — you cannot subtract endpoints), so a span must be covered by **disjoint** blocks. Per scan, one **adjacency changeset** (scan k → k+1) is computed from the two consecutive snapshots — the single O(fleet) step, **paid once per scan at ingest**, moving that cost off every query. A **binary-lifting hierarchy** stores composed changesets over power-of-2 spans: node `(level, i)` = net change from scan `i` to `i + 2^level`. `diff(a, b)` composes the **popcount(|j−i|) = O(log)** disjoint nodes of `jumps(i, j)` → **O(log N + changes-in-span)** per query, for *any* pair, **with no snapshot read at query time**. (Names: sparse table / binary lifting, in the disjoint-block form because compose isn't idempotent; a Fenwick/BIT would do for the invertible aggregate totals but not the changeset.)

**Append-only.** Appending scan N creates *exactly one new node per level* `L` with `2^L ≤ #deltas` (each the composition of two existing nodes) and never touches an existing node — so persisted nodes are immutable and a from-scratch build equals an incremental one. Storage is O(N log N) changesets, each tiny on low-churn data.

**Implementation.** `pyrmts.diffindex` (pure: `changeset_between` / `compose_changesets` / `changeset_to_table` / `changeset_from_table` / `jumps` / `SparseDiffIndex` with `append`) — verified against the 2-snapshot oracle for every pair over a churny history incl. remove-then-re-add, with the O(log) block count asserted. `pyrmts_engine.diffindex_store.DiffIndexStore` persists it over any `Storage` (`{prefix}/index.json` + `{prefix}/L{level}/{i}.parquet`, standard changeset-table rows); `update(scan_tables)` is the **idempotent ingest stage** (append every not-yet-indexed scan, in order; no-op when nothing is new — a cron fires it each cycle) and `diff(a, b)` fetches only the jump nodes (proven: no snapshot read). CLI `pyrmts-engine diffindex update | diff`. TS twin in `js/packages/pyrmts/diffindex.ts`: `diffOverSpan(scans, schema, a, b, loadNode)` + `composeChangesets` / `changesetBetween` / `jumps` / `readChangesetNode` / `parseDiffIndexManifest`, mirrored to the same node set and oracle. A reversed pair (`a` after `b`) swaps before/after — a *single* changeset reverses; only composition is non-invertible.

**How it relates to the storage layer.** Two orthogonal indexes, both on by default: the **MS store** (fixed / exponential) bounds space and serves the **over-time line** (`seriesFor` / `seriesAcrossGroups`); the **diff-index** bounds diff cost and serves the **diff treemap** for any pair. The diff-TM belongs on the diff-index, not on the MS archives. Kept as separate artifacts for now (they answer different queries); the exponential MS blocks and the diff-index nodes share the dyadic shape and could be unified later.

**Consumer wiring.** The diff-treemap is *one* widget in disk-tree's `cloud` base; cw/gcs inherit it by rebasing onto that base. So the serve-side wiring (fetch `index.json` + jump nodes from R2/D1 → `diffOverSpan`) is DT's, once — not per-consumer.

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
- **Phase 3 (always-on diff-index).** ✓ **Done.** The dyadic changeset-hierarchy — see "The always-on diff-index" above. `pyrmts.diffindex` (pure core, oracle-verified for all pairs) + `pyrmts_engine.diffindex_store.DiffIndexStore` (append-only immutable nodes; idempotent `update` ingest stage; `diff` reads only the popcount jump nodes — proven no snapshot read) + CLI `diffindex update | diff` + TS `diffOverSpan` (mirrored node set + oracle). Orthogonal to the MS storage layer: MS serves the over-time line, the diff-index serves the diff-TM. Serve-side wiring is DT's base's, once; cw/gcs inherit.

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

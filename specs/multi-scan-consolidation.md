# Multi-scan consolidation: fold a re-observation axis into shards for O(N)→O(#changes) archival

Status: **Phase 1 + Phase 2 landed (2026-09-21/22); (b) interval confirmed on cw-s3's real scans (~78× projected @ N=81). Storage driver + streaming fold + out-of-core DuckDB backend (byte-identical to the Python oracle) landed; `multiscan` CLI takes `--engine python|duckdb`. TS read primitives (`diffScans`/`seriesFor`/`extractScan`/`readMultiScan`) landed in `js/packages/pyrmts`. All generic encode + read kernels are now upstream; only consumer glue (raw ingest, HTTP routes/auth) stays out. Remaining: config surface for the opt-in layout.** Cross-consumer, but narrower than the tier ladder (see "Generality"). Origin: marin-gcs-usage (`wt/cw-s3`, session `41e25a3f`) re-scans a GCS fleet on a 12h cadence and stores **one full pyramid per scan** — each scan ~a duplicate of the last. This spec captures the pyrmts-side capability to compress that redundancy. It is a **storage-layer optimization, opt-in**, orthogonal to tiers / monoids / cascade; the read path must "speak" it too.

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

### Optional: a dyadic delta-hierarchy to bound diffs at O(log d)

The interval-boundary scan is O(changes-in-span), which is spiky when a wide window touches a churny subtree. To bound it at **O(log d) regardless of churn density**, precompute changesets for power-of-2 scan spans and merge O(log d) of them for an arbitrary `(a, b]` — the classic **sparse-table / dyadic decomposition** (with the O(log) *disjoint*-block form, since changesets aren't idempotent; **Fenwick/BIT** for the invertible aggregate; **Bentley–Saxe** for the "merge into logarithmic power-of-2 levels" scheme itself). This is a **pure refinement of the serve layer**, worth building only if the O(changes-in-span) scan proves too spiky on real data — deferred, data-gated.

### Over-time plot = the same index, state-view point read

The "over time" plot (scan × path total size) is a **per-key series along the observation axis** — exactly `series_for(key)`: expand that key's interval rows to one value per scan. The fleet/subtree over-time line is the prefix-sum aggregate above. So the two remaining mgu plots map cleanly onto **two indexed axes**, no third structure:

- **age histograms** → the **event-time** pyramid (built).
- **over-time line + diff treemap** → the **observation-axis** index (this spec): over-time = state-view point read; dTM = delta-view range read.

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
- **Phase 2b (read primitives).** ✓ **Done.** TS read primitives landed in `js/packages/pyrmts` (`multiscan.ts`): `diffScans` / `diffTables` / `seriesFor` / `extractScan` + `readMultiScan`, mirrored to the Python fixture. A consumer's over-time / diff endpoints are thin wiring over these. The remaining pyrmts-side open item is the **config surface** for an opt-in MS layout (leaning driver-only + storage-key convention, no new `Pyramid` block — see Open questions).

## Upstreaming boundary: kernels upstream, glue stays

cw-s3's obs-axis build (`specs/obs-axis-indexing.md`) names a "DuckDB producer" and a "CFW reader" as the consumer's. Both are **composites**, and the generic kernel of each belongs in pyrmts (the app-specific glue does not):

- **Producer** = [consumer's raw ingest → aligned per-scan tables] (**app territory** — pyrmts already draws this line via the `--source module:attr` hook) + [aligned tables → SCD-2 intervals] (**generic** — Python `consolidate_scans` + the DuckDB backend above, both landed here).
- **Reader** = [generic diff-over-span / over-time / point reads of the MS shard] (**generic** — Python `diff_scans`/`series_for`/`extract_table` landed; the **TS twins landed in `js/packages/pyrmts`** — `diffScans`/`diffTables`/`seriesFor`/`extractScan` + `readMultiScan` (parses rows + the `pyrmts.multiscan` KV-metadata a Python-written shard carries; int64 normalized to number like the fetch path). Pinned to the Python fixture behavior by mirrored tests) + [consumer's routes / R2 bindings / auth / API shape] (**app glue** — stays with the consumer; the `pyrmts-cfw` handler is thin wiring over these primitives).

With the TS primitives landed, the generic read + encode kernels are all upstream (Python + DuckDB encode; Python + TS read); only consumer-specific glue (raw ingest, HTTP routes/auth) stays out. The tradeoffs that motivate the DuckDB path (out-of-core, native speed) are **not consumer-specific** — every fleet-scale user wants them — which is why the backend is here, not in cw. **A consumer pins pyrmts at a SHA, so upstreaming/refactoring here cannot affect a consumer's in-flight build until they bump the pin** — the cost of upstreaming early is nil, and the benefit is that consumers build against the shared seam instead of hand-rolling then back-porting (cf. `pyrmts_engine/consolidate.py` absorbing ctbk's `lambda_exec.py` generic core — but done *ahead* of the consumer, not after).

## Open questions

- **Encoder:** ~~(a) densify+RLE vs (b) interval-rows~~ — **decided: (b)**, which wins the whole synthetic churn sweep *and* cw-s3's real scans (~78× @ N=81 on ~0.02%/scan real churn; see Status). Closed.
- **Config surface:** does an MS layout need a new block on `Pyramid` (like `geo:` / `identityRollup:`), or is it purely a `pyrmts_engine` driver + storage-key convention with no pyramid-config change? Leaning driver-only (it changes physical layout, not the logical pyramid) — confirm against the read path's needs.
- **Digest definition:** canonical sorted-row hash over which columns, and how to make it writer-independent (so it survives a foreign-writer original) — the sorted logical tuples, not the file bytes.
- **Non-invariant binCol:** for `mtime`-style movable bins, is interval-splitting on key migration acceptable, or do some consumers want a key-identity notion that tracks a moved bin? Start with tuple-keying; revisit only if a consumer needs it.
- **Grouping ergonomics:** overlapping vs. strictly-partitioning groups; re-consolidating an MS with newer scans appended (incremental MS) vs. one-shot.

## Generality

Narrower than the tier ladder. The ladder generalizes to every timeseries consumer (ctbk, crashes); the **re-observation axis** is specific to **repeated full scans of a mutable keyspace** — bucket/filesystem usage, inventory snapshots, anything re-measured on a cadence. ctbk and crashes have event-time but no re-observation, so this is an **opt-in snapshot/archival index style**, not a default pyramid feature. Offered as a pure, orthogonal algorithm any such project can tune for itself — an O(N) storage win on frequently-scanned, infrequently-updated data.

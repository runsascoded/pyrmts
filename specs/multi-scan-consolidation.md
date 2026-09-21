# Multi-scan consolidation: fold a re-observation axis into shards for O(N)→O(#changes) archival

Status: **proposed — benchmark-gated** (2026-09-21). Cross-consumer, but narrower than the tier ladder (see "Generality"). Origin: marin-gcs-usage (`wt/cw-s3`, session `41e25a3f`) re-scans a GCS fleet on a 12h cadence and stores **one full pyramid per scan** — each scan ~a duplicate of the last. This spec captures the pyrmts-side capability to compress that redundancy. It is a **storage-layer optimization, opt-in**, orthogonal to tiers / monoids / cascade; the read path must "speak" it too. Not scheduled; the (a)-vs-(b) encoder choice is gated on a benchmark against real scans.

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

## CLI (in `pyrmts_engine`, mirroring `canonicalize`)

- `pyrmts-engine consolidate -r <scan-lo>/<scan-hi> [--group <stride>] [--encoder a|b] <config>` — consolidate a contiguous scan range into one MS, or, with `--group N`, into a sequence of MS archives each covering N scans (e.g. a year of daily scans grouped 10 at a time → ~36 archives). Storage-agnostic; the project supplies how scan labels map to shard locations.
- `pyrmts-engine extract [--scan <label>] <ms>` — reconstruct all member scans (default) or one, digest-verified (level 1). This is the recoverability guarantee that makes deleting originals safe.

Grouping/stride and the encoder are **project-tuned knobs** — pyrmts offers the pure orthogonal algo and takes no position on archival policy (how far back to consolidate, group size, when to delete originals). That is the "minimal prescription" design the origin session asked for.

## Benchmark plan (the gate)

The win is empirical — it hinges on the stable long tail dominating the churny dirs (tmp-ttl / ckpt change a lot; the long tail does not). Deliver, in order:

1. **Encode + extract for both (a) and (b)** (the pure algo; no serve path yet).
2. **Synthetic-churn harness** in pyrmts: a generator with a tunable churn fraction, producing N aligned single-scan shard-sets; measure rows + bytes for (a) vs (b) vs the O(N) baseline across the churn sweep → the crossover curve.
3. **Real operating point:** cw-s3 runs the same algo against the 79 real scans (they hold the L2 files + the reader) → the actual ratio and the a/b verdict. pyrmts stays dependency-free of their storage.
4. **Decide a/b**, then build the read path + finalize the CLI.

Measure: total bytes (the O(N) claim), row counts (metadata overhead on the tail), and extract-verify (logical RT holds for both).

## Deliverables / phasing

- **Phase 0 (this spec).** Commit; keep tabled pending the benchmark verdict, exactly as the column-cube waits on crashes' numbers.
- **Phase 1 (prototype, unblocks the decision).** `consolidate`/`extract` for both encoders + synthetic harness + digest-verified logical RT. Hand cw-s3 the algo for the real-data run.
- **Phase 2 (after a/b picked).** Read path (point / diff / range) + `pyrmts_engine` CLI hardening + config surface for the opt-in MS layout.

## Open questions

- **Encoder:** (a) densify+RLE vs (b) interval-rows — the benchmark's job. Prior: (b).
- **Config surface:** does an MS layout need a new block on `Pyramid` (like `geo:` / `identityRollup:`), or is it purely a `pyrmts_engine` driver + storage-key convention with no pyramid-config change? Leaning driver-only (it changes physical layout, not the logical pyramid) — confirm against the read path's needs.
- **Digest definition:** canonical sorted-row hash over which columns, and how to make it writer-independent (so it survives a foreign-writer original) — the sorted logical tuples, not the file bytes.
- **Non-invariant binCol:** for `mtime`-style movable bins, is interval-splitting on key migration acceptable, or do some consumers want a key-identity notion that tracks a moved bin? Start with tuple-keying; revisit only if a consumer needs it.
- **Grouping ergonomics:** overlapping vs. strictly-partitioning groups; re-consolidating an MS with newer scans appended (incremental MS) vs. one-shot.

## Generality

Narrower than the tier ladder. The ladder generalizes to every timeseries consumer (ctbk, crashes); the **re-observation axis** is specific to **repeated full scans of a mutable keyspace** — bucket/filesystem usage, inventory snapshots, anything re-measured on a cadence. ctbk and crashes have event-time but no re-observation, so this is an **opt-in snapshot/archival index style**, not a default pyramid feature. Offered as a pure, orthogonal algorithm any such project can tune for itself — an O(N) storage win on frequently-scanned, infrequently-updated data.

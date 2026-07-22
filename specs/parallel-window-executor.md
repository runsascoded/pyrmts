# pyrmts-engine: parallel window executor (queue/watermark) + merge closes + source coverage

Requested by the ctbk session (2026-07-21, relayed via transcript — no separate ctbk spec). Supersedes the earlier incremental asks (window pipelining / parallel per-tier rebins / async closes): those were pipeline-stage parallelism with a low ceiling; this is window-level *data* parallelism that scales with cores.

Measurements to beat (ctbk full-range avail validation runs, local + Batch):

- **~2.6-2.7 effective cores** (263 CPU-min / 100 wall-min), invariant across 8/16 vCPU — the executor is one sequential window loop with 2-deep read-ahead; everything else runs on the main thread.
- Wall **2h45m local / ~1h55m extrapolated Batch** for the full range (~205 12h windows, 15 tiers × ~10 rungs, ~5.5B source long rows).
- Worst single shard close: `/15m@32d`, 12.7M wide rows, **113 s** of dead air on the main thread (in-RAM combine), plus the close-time memory spike.

## 1. Parallel windowed walk

- N workers claim windows off a shared forward-marching cursor. Each claimed window is one task: read source → parse → rebin cascade for **all** tiers → route/append to spill. (The per-window rebin chain stays sequential within the task — tier N re-bins its predecessor's frame; the parallelism is across windows.)
- **Completion watermark gates closes**: a shard may close only when every window ≤ its `effective_end` has *completed* (not merely been claimed). The watermark is the largest window index `w` such that windows `0..w` have all finished.
- **max-in-flight knob `K`**: a worker may claim window `i` only if `i − watermark_index < K`. Bounds peak memory ≈ K × per-window frames (per-shard accumulators already live on disk via spill). Worst case is explicit and tunable against the container size.
- **Threads first**: the heavy work (parquet read, hist-JSON parse, polars rebins) is native code that releases the GIL. Process workers with partitioned spill are plan B, only if profiling forces it.
- Shard closes are tasks on the same pool (the old "async closes" ask folds in for free).

## 2. Sorted-run spill + merge close

- Workers append per-shard **runs** to spill, each run pre-sorted by the combine/group key. Out-of-order appends across workers are safe because close never depends on append order.
- Close = k-way heap-merge of the shard's runs + **streaming combine** (adjacent-equal group rows merge in the stream) + wide materialization + write. Replaces the current whole-shard in-RAM `group_by` collect — bounded-memory closes, no 113 s stall, no close spike.
- Detail to settle in impl: the *output* sort (`sort` kwarg, e.g. ctbk's `s2_cell,dt`) applies to wide rows, while combine groups long rows by `(*dims, bin, metric, state)`. Runs must be sorted by the full long-form group key for streaming merge-combine; the final wide frame then gets the output sort as today (inside `write_tier_parquet`). If those two orders share a prefix the merge output may already be mostly-ordered — nice-to-have, not load-bearing.
- **Scale note** (from the Spark digression): this is deliberately node-local external merge sort per shard. Largest shard ≈ 12.7M rows / ~1-2 GB in RAM; the whole pyramid is ~40 GB. If scale ever outgrows one node, partitioning the same sorted runs by key range across nodes is the distributed extension — extension, not rewrite.

## 3. Streaming-close fast path

When the sort prefix is the time axis (or no sort is configured), rows arrive in watermark order per shard — close is pure concat + combine of already-ordered runs, no gather/merge. Two paths, shared machinery, chosen off the sort spec.

## 4. Source coverage modeling

The `EmptySourceError` guard only catches the 100%-missing catastrophe; **partial** missing (filter typo, wrong metric name, half the keys GC'd) still builds a plausible-looking pyramid with quietly wrong numbers. Extend the read side to distinguish `None` (no such key) from affirmatively-empty — the output side already treats affirmative-empty as first-class (EMPTY placeholder shards):

- `WideShardSource` (and the `Source` protocol, optionally) surfaces per-window coverage: expected source shards vs present vs missing.
- The engine aggregates a missing-fraction; policy lives at the top: a configured genesis boundary makes pre-genesis misses expected; post-genesis misses are logged per window and error above a threshold (default strict; opt-out akin to `--allow-empty`).
- `EmptySourceError` remains as the degenerate (fraction = 1) case.
- API shape TBD in impl: keep plain `read_window` Sources working (coverage optional, `getattr`-style), so app raw-ingest Sources don't have to implement it.

## 5. Acceptance gate

- **Byte-identity under parallelism**: N-worker build == sequential build == the existing outputs, byte-for-byte (extend the window-invariance test matrix with a workers dimension; ctbk's compare harness covers the real-data side).
- Re-baseline wall + CPU before/after (the system Arrow memory pool baked at `59d9e85` may itself shift perf) — target: effective cores ≈ N instead of ~2.6.
- Existing invariants hold: outputs ≡ `list_expected_shards`, registration-after-PUT ordering, resume-from-manifest, spill cleanup.

## Also landed alongside this spec

- `batch push`/`bootstrap` repo creation now applies ctbk's ECR lifecycle policy by default (keep 4 most recent tags; expire untagged >7 days) so consumer accounts get self-maintaining pruning.

## Non-goals

- Raw-ingest Source (skips the hist-JSON parse tax, worth 30-50% of wall): ctbk-side app code via the existing `--source module:attr` hook; under this design the parse tax becomes exactly the CPU-heavy work that soaks worker parallelism, so parallelize first, eliminate later if the wall still warrants.
- Distributed executor / multi-node anything.
- Ladder redesign (avail-v4 is the baseline; the Graviton determinism compare needs it unchanged).

## Open questions

- Default N (`os.cpu_count()`? Fargate reports the vCPU count) and default K (2×N?).
- Whether `prefetch` survives as a knob or is subsumed by N/K.

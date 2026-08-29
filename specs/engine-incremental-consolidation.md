# Incremental builds: consolidation, not re-walk

Status: **open** (extracted 2026-08-28 from `specs/done/engine-e-iteration.md`, which is otherwise closed). None of this is implemented; it was lifted out so archiving that spec wouldn't bury a live roadmap. The measurements it cites were taken during the `e`-box iteration and are reproduced there in full.

## The problem

The engine builds every output shard from the source cascade, and resume re-walks all windows feeding any unfinished shard. So the day a large rung period completes (`30m@64d`, `2h@256d`; max-rung N is ~constant 3-4k bins across tiers by ladder design), the top-up run re-walks that whole span from source. A one-day extension can cost a twelve-day walk — measured, in the Lambda-envelope run: *"manifest-full4 truncated at 07-17 … the open `3d/12d` tile forces a resume from 07-06 — the amplification in action."*

That is an artifact of the implementation, not a necessity. Same-tier consolidation is the fix, and it is also why the daily top-up currently needs a Batch fallback rather than fitting Lambda's envelope on the worst-case boundary day.

## Direction 1 — same-tier consolidation as composable stream ops

A new large-dur shard is its tier's finer materialized shards over the same period, in rev-chron geometric pieces (`2m@2d` ⇐ `{…, 6h, 12h, 1d}`). Shard boundaries are bin-aligned, so **no (cell, bin) key spans inputs → no `group_by` at all**: it's a k-way merge of already-(s2,dt)-sorted parquet.

Sizing correction from the 2026-07-23 discussion, worth keeping prominent because the first estimate was wrong: this does **not** hold R full shards uncompressed. It's a heap merge of streams sharing the sort key, so resident = each input's frontier row-group + one output row-group ≈ **single-digit MB**, for R ≤ ~10 ladder pieces. Lambda-shaped.

Build it as composable stream ops (RG sources → merge/combine nodes → sinks), not bespoke loops — the same pieces are what `parallel-window-executor.md` §2 wanted for sorted-run merge closes. That spec is now closed with §2 superseded (chunked closes solved the memory constraint that motivated it), so if merge machinery is ever built, this is the reason to build it, and the two should share an implementation.

## Direction 2 — dt-major base rung

Highest value per unit complexity. The whole-shard parse cache exists because the `(s2_cell, dt)` sort defeats dt-based row-group pruning — every RG is a cell-range spanning the full shard period, so a window read can't prune. A dt-major base rung (or an engine-private dt-major mirror) makes `read_window` RG-streamable and **deletes the ~3 GB parse-cache floor**, which is the dominant steady-state term in the Lambda profile.

Note the rest of a dual-sorted pyramid buys little: cross-tier rebins need decode + `group_by` regardless, and same-tier consolidation is already merge-cheap.

## Direction 3 — s2-range shadow buckets

5-10 buckets, boundaries alignable to the ragged station-leaf s2 frontier, on **live/non-max shards only**. Lambda then sorts 5-10× smaller pieces; and because buckets are disjoint s2 ranges each internally `(s2, dt)`-sorted, the unsharded serving shard is a **zero-decode concat** of bucket shards in bucket order. Serving/CFW reads stay on the stitched unsharded form (live min-cover included); buckets are build-side artifacts, dropped once a max-rung shard stitches.

## Design card (not scheduled) — zero-decode concat

For dt-first-sorted pyramids, consolidation inputs cover disjoint key ranges, so the output is a literal concatenation of input row-groups: compressed-RG stitching via thrift metadata surgery, holding one *compressed* RG resident. pyarrow doesn't expose raw RG copy — this needs a low-level path or arrow-rs.

Caveat that decides whether it's acceptable: it inherits input RG boundaries (tail RGs below `rg_size`), so it is **not RGIP** against a from-source build. Adopting it means declaring the consolidated form canonical.

## Tests

Lambda-style workloads need first-class tests: single-additional-tick cascades (extend a built range by one base bin, resume, assert minimal windows + byte-identity), then profiling on those shapes. The first landed already — `pyrmts_engine/tests/test_engine.py::test_single_tick_incremental_resume` (`96b1659`), which also made no-op resumes exactly free.

## Relationship to what shipped

The immediate pain this addresses was worked around rather than solved: a cron `batch submit` (EventBridge → Fargate Spot, no 10 GB / 15 min caps, same hardened code path, ~cents/day) is the current default for top-ups, and the streaming per-batch parse (`b265673`) plus budget-gated readahead (`5020a3c`) brought the *typical* daily top-up inside Lambda's envelope (8.8 min / 9.7 GB, expected ~6.5 GB post-gating). What remains unfixed is worst-case amplification on a boundary day, which is what direction 1 exists for.

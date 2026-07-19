# pyrmts: fused long-form pyramid build engine

Requested by the ctbk session (2026-07-19); see
`ctbk/specs/drop-luc-station-keys.md` + `ctbk/specs/done/avail-v3-lambda-rebuild.md`
for the incidents motivating this.

## Problem: builds cost ~100× the layperson number

ctbk's avail pyramid aggregates **~6-8 GB of raw parquet** (~302M
observations over 100 days) into ~1.4B output rows across a 15-tier
ladder. A columnar engine should do that in tens of core-minutes.
Measured reality:

- Lambda fan-out (per-shard python materializers): ~81 Lambda-hrs
  estimated, ~$26-48, ~3.3 h wall at 48-wide.
- The best existing engine (ctbk `pyramid-cascade`, chunked-block
  Polars): 504 s per 7 days on a c7a.16xlarge → ~2.5-3.5 h full
  history (`ctbk/specs/cascade-perf-comparison.md`, 2026-06-21 —
  Polars beat parallel python-dict 2×, single-process 10×).
- Weeks of iteration have been spent waiting on multi-hour builds
  while experimenting with keying/algorithms. "Rebuilds are rare" is
  not an acceptable answer during development.

**Root cause: the wide histogram-JSON representation.** Serving shards
store metrics as JSON histogram strings (`{"0":12,"1":5,...}` per
(cell, dt, metric)). Every tier boundary therefore pays
parse → merge → re-serialize — row-wise in python paths, and even the
winning Polars engine pays a long↔wide pivot round-trip per tier. The
serialization tax at every internal edge of the DAG is the ~10-50×.

## Design

### 1. Long form is the canonical BUILD representation

Internal dataflow rows: `(key, dt, metric, state, count)` (for
histogram monoids; plain-sum monoids like rides omit `state`). Prior
art: ctbk `engine_longform.py` (branch
`e/feat/pyramid-cascade-longform` — long-form *storage* experiment,
off-path; this spec promotes the representation for *build-internal*
use only).

- Base ingest parses raw once → long form.
- EVERY cascade step is `group_by(key, floor(dt, bin), metric, state)
  .sum(count)` — pure columnar, no JSON anywhere inside the DAG.
- Wide hist-JSON is materialized exactly ONCE per final serving shard
  (vectorized pivot + string-build at write time). Serving format is
  unchanged — readers (CFW worker, pyrmts-js) never see long form.

### 2. Plan = explicit DAG; executors are pluggable

The ladder config compiles to a DAG: nodes = (tier, window) products
(+ base-ingest nodes over raw), edges = divisibility-predecessor
sourcing (`source = coarsest tier whose bin divides yours` — e.g.
ctbk's `/2m,/3m,/5m ← /1m` since 2,3,5 are pairwise coprime).

- **Fusion is free by construction**: all tiers sourcing the same
  window share one cached long-form frame; a node emits N sibling
  aggregations per scan (ctbk measured ~26 of 81 Lambda-hrs in
  exactly these re-reads).
- Executors:
  - **`local` (default)**: single node, windowed streaming outer loop
    (bounded memory: one window's long frame + per-tier WIP shard
    buffers), polars lazy/streaming inner parallelism. Max-rung WIP
    buffers accumulate across windows, so the LE's scaffold layers
    (~900 disposable shards — the dominant cost of a full re-key,
    ~$25-40 of ~$50; see ctbk `avail-v3-lambda-rebuild.md`) don't
    exist here at all. Target: full ctbk avail build **~15-45 min,
    ~$1-3** on a 16-core Batch/Fargate container (or any dev box).
  - **`lambda-fanout`**: the existing ctbk driver shape — per-node
    invocations for incremental self-heal / partial rebuilds where an
    always-on or on-demand node is wrong-sized.
  - **(slot) `distributed`**: Dask/Ray-shaped drop-in if data ever
    outgrows one node. Deliberately NOT built now — at ≤20 GB total,
    a distributed scheduler is overhead without need; within-node,
    polars' work-stealing engine already executes the per-window DAG.
- The DAG is also the observability surface: plan estimates
  (per-node class cost model), DOT export, and a progress feed
  (per-node status → JSON for a web UI) come from the same object.

### 3. Same kernels in the event-driven tick

The steady-state Lambda tick (5-min self-heal) imports the same
long-form kernels (pyarrow-expressible; no polars needed in the
bundle if size matters). Fixes the tick's own compute leak (~8,640
ticks/mo × ~30 s × 10 GB ≈ $40/mo today) — the venue (Lambda,
event-driven) is unchanged; only the inner loops go columnar.

### 4. Packaging

Container image (engine + configs), runnable identically on: a dev
box, AWS Batch/Fargate (spot), or any future node. No `e`-class
long-lived box required. ctbk supplies config + storage creds; the
engine is project-agnostic (rides is the second consumer — easier:
plain-sum monoids, already columnar sources).

## Validation

ctbk avail-v4 (frozen-vocabulary keying) is being built by the Lambda
fan-out right now — rebuild the SAME pyramid with this engine and
require content equality (canonicalized rows per shard; cross-tier
monoid rebin checks as in ctbk's 1h→6h probe). Wall/$ comparison on
the same build = the headline benchmark.

## Decisions (2026-07-18, pyrmts session)

1. **Package/language**: new `python/pyrmts_engine` package (sibling
   of `pyrmts` / `pyrmts_geo`), depending on `pyrmts` (types, axis,
   keys, `write_tier_parquet`, `list_expected_shards`) + polars.
   Keeps the base `pyrmts` install pyarrow-only. TS packages
   unaffected — serving format is unchanged, and CFW-side cascades
   (e.g. awair's) stay TS.
2. **Inner engine: polars.** ctbk `cascade-perf-comparison.md`
   settled polars-vs-python (H3, ~2× over the best python-dict
   parallelism); DuckDB's out-of-core group_by solves a spill
   problem that doesn't exist at ≤20 GB. No re-benchmark.
3. **Window = driver knob** (`--window`, default `1d`), constrained
   to a multiple of the base tier's bin — NOT derived from the
   ladder. Shard finalization is driven by boundary crossings of
   each tier's WIP buffer, so window size is purely a
   memory/throughput dial (matches the chunked-block engine's
   winning `task_size=1d`). Parallelism = blocks of windows across
   cores, ordered reduce at block boundaries. Memory calibration
   (avail): ~30 M long-form rows/day ≈ 1-1.5 GB per in-flight
   window frame → 16-way ≈ 16-24 GB + WIP buffers; shrink
   `--window` if tight.
4. **Output set / genesis**: the DAG's outputs are exactly
   `list_expected_shards(pyramid, range)`, which already implements
   genesis pruning + `effective{Start,End}` (`3f50c2d`). Raw reads
   clamp to genesis — outputs are a function of config, not of
   whatever raw exists.
5. **ShardIndex**: engine takes a `ShardIndex` protocol
   (`record_shard(...)`) with three impls — no-op (tests), local
   JSONL manifest (dev/dry-run), D1 REST (port of the LE's
   per-shard registration). Called immediately after each shard
   PUT: a crash leaves at worst an unregistered (invisible) R2
   object that a resumed run overwrites idempotently — never a
   registered-but-absent key. The manifest impl doubles as a
   "register later" mode if a consumer wants batch sync instead.
   Separately (ctbk-side, not this engine): `gc_sweep` should grow
   an mtime grace window ("never sweep objects younger than N min")
   — protects any transiently-unregistered fresh writes.
6. **Dtypes**: `state: int16`, `count: uint32`; no categoricals.
7. **Upload: single PUT per shard, no MPU.** Shards finalize as
   complete in-memory parquet buffers (footer is written last, so
   parquet can't stream anyway) at 5-100 MB — far under R2's
   ~5 GiB single-PUT limit. R2's MPU equal-part-size quirk (all
   parts but the last must match) is what bit us previously; it
   never comes into play here. Optional small upload thread pool
   overlaps PUTs with compute.

## Open questions

1. ~~WIP shard-buffer spill policy~~ — **resolved 2026-07-18: spill
   required and implemented.** ctbk's 4-day smoke hit ~40 GB holding
   open max-rung buffers in memory (full history extrapolated
   50-70 GB+). `SpillBuffer` now routes each window's rows to
   per-open-shard scratch parquet files (appended row-groups);
   shard close = streaming scan + group_by combine + delete. Peak
   memory ≈ one window's frames + one closing shard's long form.
2. Stale-source invalidation (`list_stale_shards`, or
   `list_missing_shards(include_stale=True)`) — surfaced by awair's
   raw-sync gap (raw `LastModified` > derived `written_at` goes
   undetected). Same discovery layer this engine plans from, but
   separable; track as its own spec.

## Implementation notes (2026-07-18)

Landed as `python/pyrmts_engine` (workspace member alongside `pyrmts`
/ `pyrmts_geo`): `longform.py`, `plan.py`, `source.py`,
`shard_index.py`, `engine.py`, `cli.py` (`pyrmts-engine plan|build`).
Deviations / refinements vs the design above:

- **Long form unifies monoid classes via the `metric` column**: for
  scalar monoids it holds the *state-column* name (`m_n`/`m_sum`/
  `m_sumsq` for sum; `m` for count), so a sum metric is three long
  rows and every cascade edge is the same
  `group_by(dims, bin, metric, state).sum(count)`.
- **Dtypes**: `state: Int32` (not the decided int16 — headroom for
  e.g. minute-valued histogram states, negligible cost);
  `count: Float64` (exact for integers ≤ 2^53; cast back to Int64 at
  wide-write for hist counts, count-monoid cols, and `_n`).
- **Sources are pluggable** (`Source.read_window → long frame`);
  `WideShardSource` reads an existing materialized rung (declares
  `provides` so the engine skips re-writing it). App-specific raw
  ingest (GBFS minutes, rides monthlies) implements `Source`
  consumer-side.
- **EMPTY-shard semantics**: zero-row expected shards are written and
  registered (cover-complete + zero rows — the ctbk outage-scar
  lesson). NB `cascade_tiers` *skips* these; the engine is the newer
  semantic.
- **Parallelism v0 is prefetch, not block-fanout**: a small thread
  pool reads source windows ahead while polars' own threads
  parallelize the group_bys. The ordered block-reduce across cores is
  deferred until a profile shows the outer loop CPU-bound.
- **Tests** (15): content conformance vs `cascade_tiers` on a
  3-tier / 3-monoid fixture, an independent hand-derived aggregate
  anchor, byte-identical output across window sizes (RGIP),
  exact expected-key-set (== `list_expected_shards`), registration
  records, EMPTY-shard write+register.

**Validation (in progress, ctbk-side —
`ctbk/specs/pyrmts-engine-validation.md`)**: 4-day smoke over real
avail data (`/1m@2d` source via `WideShardSource`, window 12h):
**content parity confirmed** — 8/13 shards EQUAL, 0 DIFF, across 8
tiers vs the independent Lambda python-dict materializer (incl. an
11.7M-row `/2m@4d` exact-key match); remaining 5 pending only on the
fan-out's coarse tail finishing. Spec stays in `specs/` until the
full-range build + compare passes.

Findings from that smoke, addressed 2026-07-18:

- **Spill** (finding 1): `SpillBuffer` as above; all engine tests run
  through the spill path, incl. byte-identical window invariance.
- **Source re-parse** (finding 2): `WideShardSource` now caches parsed
  shards across windows (thread-safe, evicted once the cursor passes
  their period) — each blob is fetched/parsed once regardless of
  window:shard_dur ratio.
- **`row_group_size`** (finding 3): plumbed through `build_local`
  (int for all tiers, or per-tier-name mapping — ctbk's 2048) and the
  CLI (`-g/--rg-size`).

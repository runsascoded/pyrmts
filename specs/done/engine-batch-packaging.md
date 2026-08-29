# pyrmts-engine: Batch/spot packaging + submit tooling

Requested by the ctbk session (2026-07-18), following up `specs/pyramid-build-engine.md` §Packaging. The validation build (ctbk `specs/pyrmts-engine-validation.md`) is running the full-history avail rebuild on a MacBook via the editable link — fine as a one-off correctness gate, but the target venue for real builds is ephemeral cloud compute with no standing infra. pyrmts should own the reusable pieces; apps (ctbk first) supply config + creds + a thin passthrough.

## Deliverables (pyrmts-side)

### 1. Container image

- Base image: engine + polars + pyrmts, entrypoint = `pyrmts-engine build <config> -r <range> [...]` — the CLI already has the right shape; config arrives via a mounted file, S3/R2 URL, or env; storage creds via env (`R2_*` / `AWS_*`).
- Published per dist rev (GHCR or ECR — GHCR probably simpler given the existing `npm-dist`-style GH publishing habits; ECR avoids cross-cloud pull latency/egress from Batch. Decide + document).
- **App-source plugin support**: `WideShardSource` runs from the base image alone, but real builds (raw-ingest Sources, rides) need app code inside the container. Two options; pick one early since it shapes the entrypoint:
  a. Base image supports `pip install <app>` at startup (env var, e.g. `ENGINE_PIP_INSTALL=ctbk @ git+...`), and the CLI grows a `--source module:factory` hook.
  b. Each app derives a small image (`FROM pyrmts-engine:<rev>` + `pip install .`) — more moving parts per app but reproducible and no cold-start pip.
  (b) is recommended: images are cheap to derive, and cold-start `pip install` of an app like ctbk (pandas, boto3, …) would dominate short builds.

### 2. Submit wrapper

- `pyrmts-engine batch bootstrap`: idempotent one-time creation of compute environment (spot), job queue, job definition, log group, and the minimal IAM role (no AWS data perms needed for ctbk — R2 creds ride as secrets/env; logs-only role, same shape as ctbk's `gbfs/lambda/deploy.py` role). Plain boto3, no CDK/Terraform.
- `pyrmts-engine batch submit -c <config-ref> -r <range> [-w window] [-g rg-size] [--vcpus N] [--memory M] [--watch]`: `submit-job` with overrides; `--watch` tails the CloudWatch stream and exits with the job's status. (An `--image` override was in the original ask, but Batch `containerOverrides` can't override the image — it's job-definition-only; re-run `bootstrap -i <ref>` to bump it, which re-registers the job def idempotently.)
- Secrets: accept `--secret NAME=arn` passthrough to the job def (Secrets Manager) OR document plain env injection via container overrides; ctbk's R2 creds are low-ceremony, don't over-engineer.

### 3. Sizing defaults (informed by ctbk measurements)

Post-spill (`3c79724`), the local executor is memory-bounded at ≈ one window's long frames + one closing shard's combine:

- ctbk avail full history (~102 d, ~4.5k keys × 5 hist metrics, `12h` window, `/1m@2d` wide source), measured on the full-range validation runs (ctbk spec, 2026-07-19): **RSS oscillates ~1-15 GB** (idle ~1 GB between shard closes; peak = one closing shard's combine — largest observed close `/15m@32d`, 12.7M wide rows, 113 s), **spill dir cycles 1-5 GB**, wall **~2h45m** single-process on an M-series laptop at only **~2.6-2.7 effective cores** (263 min CPU / 100 min wall). Content: 75/75 shards EQUAL vs the Lambda fan-out (tail tiles pending a rerun).
- Two implications: (1) 32 GB memory is comfortable (peak ~15 GB with headroom for bigger closes), 100 GB ephemeral is ample (spill ≤5 GB observed, budget 10× for bigger consumers); (2) **the wall-clock dial is NOT just vCPU count** — at ~2.6 effective cores, 16 vCPU won't get near 15-45 min without engine-side parallelism (window-level pipelining, or parallel per-tier rebins within a window). Also: this run paid the hist-JSON parse tax on ~5.5B source rows via `WideShardSource`; raw-ingest Sources skip it, which is worth maybe 30-50% of wall on its own.
- Job-def defaults of 8 vCPU / 32 GiB / 100 GiB (as landed) are fine to keep; more vCPUs are wasted until the executor parallelizes across windows.

## App-side (ctbk, for reference — not pyrmts work)

- Derived image with ctbk installed (once raw-ingest Source lands), R2 creds wiring, `ctbk gbfs engine submit` passthrough mirroring the existing `ctbk gbfs engine build` flags.
- Compare/acceptance stays app-side (`ctbk gbfs engine compare` + JSONL manifest; manifest can come back via S3/R2 instead of local disk — the engine's `JsonlShardIndex` may want an s3:// variant, cheap to add here if useful).

## Non-goals

- No distributed executor work (still deferred per the build-engine spec).
- No standing instances — that's the point. `e`-class boxes remain available but shouldn't be required for any pyramid build.
- No GPU, no multi-job orchestration; one build = one job.

## Implementation notes (pyrmts session, 2026-07-18)

Decisions on the spec's open choices:

- **Registry: ECR** (not GHCR). Batch pulls are IAM-native, same-region, free; GHCR would add registry-auth secret plumbing + cross-cloud pulls. Publishing starts as local `docker build` + push tagged with the git rev (no GHA until cadence demands); `bootstrap` creates the repo if missing.
- **App plugin: (b) derived images**, as recommended. The `--source module:attr` hook landed anyway (factory called as `factory(pyramid, filter) → Source`) so thin consumers can run raw-ingest builds from base-image + pip layer without their own driver CLI.
- **Compute: Fargate Spot** (16 vCPU / 120 GB / 200 GB-ephemeral ceiling; no AMI or instance-role management). EC2-spot CE is a follow-up if cost/wall demands. Networking = default-VPC subnets + default SG, public IP (needed for R2/ECR egress without NAT).
- Two entrypoint realities folded in: **config via `s3://` URL** (Batch has no bind mounts — "mounted file" isn't a thing there; the CLI now fetches `s3://bucket/key` through the standard `R2_*`/`AWS_*` env) and **`StorageJsonlShardIndex`** (manifest re-PUT through a pyrmts `Storage` every N records + at close — local JSONL dies with the container; `build_local` now calls `shard_index.close()` when present, `-m s3://...` wires it from the CLI).

Landed:

- `python/pyrmts_engine/Dockerfile` — build from repo root: `docker build -f python/pyrmts_engine/Dockerfile -t pyrmts-engine:$(git rev-parse --short HEAD) .`; entrypoint `pyrmts-engine`. Built + verified at `be489ec` (744 MB; containerized fixture build produced byte-identical shards to the host run — determinism holds across linux/arm64 vs macOS). ECR push remains (needs account creds; note Batch Fargate is amd64 — push with `docker build --platform linux/amd64` or buildx multi-arch). The amd64 image also builds locally (~3.5 min, wheels-only) but **cannot be runtime-verified under Rosetta**: polars' default x86_64 wheel segfaults on any compute kernel (AVX2+ instructions the emulator lacks — a bare `group_by` repros; imports are fine). Verify amd64 at the first real `batch submit --watch` smoke, or a GHA amd64 runner. (Fargate's Xeons have AVX2+, so this is emulation-only; if paranoid, `polars-lts-cpu` is the fallback knob.)
- `pyrmts_engine/batch.py` — pure spec builders (`job_definition_spec`, `compute_environment_spec`, `build_command`, `submit_overrides`; unit-tested exactly) + thin boto3 `bootstrap`/`submit` (+`--watch` log tailing). `pyrmts-engine batch bootstrap|submit` CLI. boto3 via the `[batch]` extra.
- Job-def defaults: 8 vCPU / 32 GiB / 100 GiB ephemeral / spot-retry ×2 — **provisional until ctbk's full-run numbers land** (§3 copy-numbers step still owed; adjust `bootstrap` defaults then).

Added 2026-07-19 (closing the "ctbk owns the AWS infra" gaps — everything below runs from ctbk's account/creds with no pyrmts-side AWS access):

- **`pyrmts-engine batch push <ecr-ref>`** — ECR-login docker (token via boto3), `docker build` (default `--platform linux/amd64`; `-B` to skip, `-c`/`-f` for context/Dockerfile), `docker push`. Creates the ECR repo if missing, so push can precede `bootstrap`.
- **`bootstrap --arch {X86_64,ARM64}`** — sets the job definition's Fargate `runtimePlatform`. **ARM64 is the low-risk path**: the linux/arm64 image is the one already runtime-verified (byte-identical to host builds); amd64 can't be runtime-verified locally (Rosetta/AVX2, above), and Graviton Fargate Spot is also ~20% cheaper. Pair `push -p linux/arm64` with `bootstrap -a ARM64`.

Consumer runbook (ctbk, one-time then per-build):

```bash
# 1. base or derived image → ECR (from pyrmts repo root for the base image)
pyrmts-engine batch push -p linux/arm64 -f python/pyrmts_engine/Dockerfile \
  <acct>.dkr.ecr.<region>.amazonaws.com/pyrmts-engine:<rev>
# 2. one-time infra (role, logs, CE, queue, job def; re-run to bump the job def)
pyrmts-engine batch bootstrap -a ARM64 -i <ecr-ref> -e R2_ENDPOINT_URL=... -e R2_ACCESS_KEY_ID=... -e R2_SECRET_ACCESS_KEY=...
# 3. per-build
pyrmts-engine batch submit -n avail -r <from>/<to> -w 12h -g 2048 -m s3://.../manifest.jsonl -W s3://.../config.yaml
```

Remaining: first real `push` + `bootstrap` + `submit --watch` smoke (ctbk-side; doubles as the clean wall benchmark and — if X86_64 is ever wanted — the amd64 runtime check). ctbk-side derived image once the raw-ingest Source exists. No teardown command on purpose: idle CE/queue/role/log-group cost nothing; ECR storage is pennies.

## First-smoke findings (ctbk session, 2026-07-20)

Infra path went clean end-to-end: `push -p linux/arm64` (base `652cee0` + a ctbk derived image `652cee0-ctbk.1`), `bootstrap -a ARM64` (all resources created first try), Graviton Fargate Spot capacity materialized in us-east-1 within ~1 min (SUBMITTED→STARTING; the region-availability caveat didn't bite). But the first submit produced a **silent all-empty build** — 204 windows, 0 source rows, 150 shards of headers, exit 0, `SUCCEEDED`, 43 min. Issues, roughly by severity:

1. **CLI default source rung is `tier.shards[0]` — the smallest rung — and a fully-absent source rung is indistinguishable from success.** ctbk's durable base rung is the *largest* rung (`/1m@2d`, the Lambda-cascade max; GC sweeps the small rungs), so the default read `1m/5min/*` (never existed), `WideShardSource` treated every miss as pre-genesis/outage-empty, and the build "succeeded". Two asks:
   - `build -t/--source-tier -d/--source-shard` passthrough to `WideShardSource` (the library ctor already takes them; only the CLI can't express them). Workaround used: derived image + `-x ctbk_engine_src:avail_1m_2d` — works, but a 2-file image per rung choice is heavy for what's one string flag.
   - **Zero-source-rows guard**: if the source produced 0 rows across the entire range, exit nonzero (or require an explicit `--allow-empty`). A 0-row full-range build is ~always misconfiguration; 43 min of Fargate writing empty parquet headers with exit 0 is the worst failure shape.
2. **Mis-sourced runs clobber seeded source data.** The engine's outputs include every rung of every tier except the source's `provides` — so when the source rung was mis-defaulted to `(1m, 5min)`, the run overwrote the `/1m@2d` shards ctbk had seeded into the scratch prefix with empty ones. Not a bug per se (provides-skip worked as designed), but worth a spec note: the *only* thing protecting the source rung from being overwritten by its own build is getting the source spec right.
3. **`submit --image` is documented in the spec but not implemented** (job def's image is the only source of truth). Re-running `bootstrap -i <new-ref>` to bump the job def works fine as the workaround; either land the override or fix the spec text.
4. Minor: the all-empty run's wall was **2610 s for zero data** — ~13 s/window of pure R2 key-probing + empty flushes from Fargate. Cross-cloud RTT per S3 op is evidently nontrivial; worth remembering when interpreting real-run wall numbers (and maybe batching/parallelizing the per-window existence probes).

Also confirmed on the plus side: config-fetch via `s3://` + `R2_*` env worked first try (inject *only* `R2_*` — pyrmts `S3Storage` prefers `AWS_*` when both are present, and 20-char AWS keys get rejected by R2); `StorageJsonlShardIndex` manifest re-PUTs worked; ARM64 image runs polars fine on Graviton. Real (factory-sourced) full-range run in flight as of this note.

5. **Allocator retention makes container memory grow ~monotonically with range progress — the 32 GiB default doesn't come close on the ctbk avail build** (the same build whose macOS RSS oscillated 1-15 GB). Measured ceiling→progress: 32 GiB → OOM at ~3% of the range (4.3 min); 60 GiB (`-M 61440`) → OOM at ~52% (58 min, 34 flushes, all row counts identical to the local run). Extrapolated full-range footprint ~90-120 GB against a ≤15 GB true working set. The reconciliation with the local numbers: macOS hid the retention in compressed memory/swap (~55 GB *footprint* at ~5 GB RSS); Fargate has no swap, so retained-but-idle allocator pages count fully against the cgroup. Consequences: (a) "allocator trim after shard close" is load-bearing for container viability, not a nicety; (b) until it lands, docs should say "size to macOS *footprint*, not RSS"; (c) ctbk is testing `_RJEM_MALLOC_CONF=dirty_decay_ms:1000,muzzy_decay_ms:1000` (py-polars' prefixed jemalloc env) as an env-only workaround at 16 vCPU / 120 GB — if it works, consider baking a decay default into the image/job-def.
6. **Intermittent instant SIGSEGV (~50% of container starts), now diagnosed via `PYTHONFAULTHANDLER=1`: native crash under concurrent `pq.read_table` in the prefetch pool.** Faulthandler shows the faulting thread with `<no Python frame>` (an Arrow-internal worker) while BOTH engine prefetch threads (`prefetch=2`) are simultaneously inside `WideShardSource._load` → `pyarrow.parquet.read_table` on cold start (5562-RG, 125 MB wide shards) — a startup race, which explains the lottery: identical resubmits sometimes run for an hour, sometimes die at ~30 s (observed 5×139 / 3 clean starts across 5 jobs; both 8 and 16 vCPU; with and without jemalloc-decay env; polars exonerated). Prime suspect is pyarrow's bundled jemalloc memory pool on aarch64; ctbk is testing `ARROW_DEFAULT_MEMORY_POOL=system` as the env-only mitigation. Engine-side options if that confirms: serialize the *first* window's reads, or a read-retry around `_load`, or default `pa.set_cpu_count(1)` in prefetch threads (read_table is internally parallel anyway, so per-call threading × prefetch threads is over-subscribed). Also: keep `PYTHONFAULTHANDLER=1` in the job-def env permanently — it's free and turned five mute 139s into a diagnosis.
7. Fargate Spot reclaims are real over multi-hour builds: TWO consecutive otherwise-healthy attempts (~45 min and ~66 min in) were killed with Batch's explicit "Your Spot Task was interrupted" reason (the mute-137 OOMs never carry that string, usefully disambiguating). With `retry ×2` often burned by the startup segv, a ~2 h build hasn't yet survived Spot. Wants, in rough priority order:
   - **Resume-from-manifest**: `build_local` always restarts from range start; shards are deterministic and the manifest names what's done, so a `--resume` that skips (or `stale_before`-style trusts) already-manifested shards would make Spot reclaims cost minutes, not runs. This is the fix that makes Spot *both* cheap and reliable.
   - **Manifest PUT cadence**: `StorageJsonlShardIndex` re-PUTs lagged badly — a reclaimed container left 25 records on R2 while ~100 shards were actually written (each is only re-discoverable by listing keys). A PUT per shard-close is noise next to the shard write itself; do that (it's also what makes resume-from-manifest trustworthy).
   - `bootstrap --on-demand` (or a second CE/queue): ~3.3× compute for guaranteed completion — still only ~$2-3 for this build; the right knob for "final" runs until resume lands.

## pyrmts response (2026-07-21) — findings 1-7 addressed

All landed engine/CLI/tooling-side; per-finding:

1. **`build -t/--source-tier -d/--source-shard`** now plumb to `WideShardSource` (also on `batch submit`; rejected when combined with `-x`). **Zero-source-rows guard**: `build_local` raises `EmptySourceError` when the source produced 0 rows over a non-empty range (the zero-row outputs are still written/registered first — they were flushed during the run); CLI exits **3** unless `-e/--allow-empty` (`-E` on `batch submit`). The exact footgun (durable rung = largest, default = smallest) is a regression test (`test_build_source_rung_flags`).
2. Noted, working as designed: the only protection for the source rung is a correct source spec — but finding 1's guard now converts the silent clobber-shaped run into a loud exit-3.
3. Spec §2 text fixed: Batch `containerOverrides` cannot override the image (job-def-only); `bootstrap -i <ref>` re-registration is the documented bump path.
4. Acknowledged, no code change: ~13 s/window was probe-RTT + empty flush + per-window manifest PUTs from Fargate→R2; real runs amortize it, and the guard kills the pathological all-empty case. Revisit (batched probes) if it shows up in real-run walls.
5. **Trim-after-close landed**: `build_local` calls `malloc_trim(0)` (glibc; no-op elsewhere) after every window that flushed ≥1 shard. Only effective for system-allocator memory, so the image now bakes **`ARROW_DEFAULT_MEMORY_POOL=system`** (routes Arrow buffers through glibc) plus **`_RJEM_MALLOC_CONF=dirty_decay_ms:1000,muzzy_decay_ms:1000`** (bounds polars' own jemalloc retention) — i.e. ctbk's two experimental env knobs are now image defaults (overridable via job-def/submit env). Rebuild + re-push the image to pick these up. If the retained footprint still grows after that, it's leak-shaped (live references), not retention — file what the RSS curve looks like.
6. **Cold-start segv**: two layers — `ARROW_DEFAULT_MEMORY_POOL=system` (above) avoids the suspect bundled-jemalloc pool entirely, and the engine now **serializes the very first window's read** before opening the prefetch pool (the observed race was both prefetch threads inside first-touch `pq.read_table`). `PYTHONFAULTHANDLER=1` is baked into the image per the ask.
7. **Resume + cadence + on-demand, all landed:**
   - `build -u/--resume` (and `batch submit -u`): skips shards already in the manifest (`existing_keys()` on the JSONL indexes; D1 not yet) and skips source windows that precede the earliest unfinished shard's `effective_start`. Shards are deterministic, so resumed outputs are byte-identical (regression-tested). Requires `-m`.
   - `StorageJsonlShardIndex` default `flush_every` **25 → 1** (a manifest PUT per shard close), and it now **loads an existing manifest on init** — records survive across attempts, which is what makes `--resume` against the same `-m s3://…` key work with zero ceremony: same submit command + `-u`.
   - `bootstrap -o/--on-demand` additionally creates a FARGATE (non-Spot) CE + queue `pyrmts-engine-od`; `submit -O/--on-demand` targets it. Spot + `-u` resubmits should usually beat it on cost now, but it's there for one-shot "final" runs.

Suggested first move ctbk-side: rebuild/push the image at this rev (picks up the env baking + trim + serialize-first-read), `bootstrap -i <new-ref>` to bump the job def, then resubmit the full range on Spot with `-t/-d` (or the factory) plus `-m s3://… -u` — reclaims then cost minutes. `-e` should NOT be passed.


## Closed (2026-08-28) — §3 numbers landed

The last literal open item was §3's own note: *"Job-def defaults … provisional until ctbk's full-run numbers land (§3 copy-numbers step still owed; adjust `bootstrap` defaults then)."* Those numbers landed in `specs/done/engine-e-iteration.md`; they are now in the defaults.

**vCPUs 8 → 16.** §3's reason for 8 was explicit — *"more vCPUs are wasted until the executor parallelizes across windows"* — and that is exactly what the `e`-box iteration then delivered. Every converged profile was measured at 16 vCPU, reaching **10.03 effective cores / 47.9 min** on the full ctbk range. The job definition was also asking for half of what the compute environment already allowed (`max_vcpus=16`), so a single job could never use the CE. Cost is roughly neutral: total CPU is ~constant (~8h) across every concurrency config — the engine is work-conserving, only overlap quality moves — so doubling vCPUs mostly buys wall-clock.

**Memory stays 32768 MiB**, now for a measured reason rather than a provisional one: the mem-tight profile (`-w 1h -b 20g -C 2 -c 1g`) peaks at **24.0 GB**, and the engine's default budget of 70% of the detected limit is ~22.4 GB on a 32 GiB container — i.e. the default dials reproduce the measured profile. The par-leaning and par-max profiles peak at 37.0 / 37.7 GB and need `-m 49152` or more; that trade is documented in `bootstrap`'s help.

**Ephemeral stays 100 GiB** — observed spill peaked ~22 GB, so this is 4× headroom.

The two profiles, for reference (full ctbk avail range, 16 vCPU box):

| profile | dials | wall | eff cores | peak RSS |
|---|---|---|---|---|
| mem-tight (default-shaped) | `-w 1h -b 20g -C 2 -c 1g` | 65.3 min | 7.36 | 24.0 GB |
| par-leaning | `-w 1h -b 24g -C 3 -c 2g` | 56.4 min | 8.86 | 37.0 GB |
| par-max | `-w 3h -b 36g -C 4` | 47.9 min | 10.03 | 37.7 GB |

The ctbk-side smoke this spec waited on is discharged by `specs/done/engine-fill-mode.md` — `batch submit` is a production path (`ctbk gbfs rides-v5-extend` calls `_engine_submit(..., fill=True)`).

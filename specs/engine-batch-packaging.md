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
- `pyrmts-engine batch submit -c <config-ref> -r <range> [-w window] [-g rg-size] [--image <ref>] [--vcpus N] [--memory M] [--watch]`: `submit-job` with overrides; `--watch` tails the CloudWatch stream and exits with the job's status.
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

Remaining: copy final ctbk measurements into §3; first image build + push; ctbk-side derived image + `submit` passthrough once the raw-ingest Source exists.

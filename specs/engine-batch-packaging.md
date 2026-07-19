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

- ctbk avail full history (~102 d, ~4.5k keys × 5 hist metrics, `12h` window, `/1m@2d` wide source): **RSS steady ~6-8 GB**, spill dir low-single-digit GB (cycles as shards close). (Final wall/peak numbers land in ctbk `specs/pyrmts-engine-validation.md` implementation notes when the run completes — copy them here before implementing.)
- Suggested job-def defaults: **8-16 vCPU, 32 GB, ≥50 GB scratch** (spill + source cache headroom for bigger consumers), spot. `m7a/c7a.4xlarge`-class or Fargate 16 vCPU/64 GB both fit; polars saturates cores during rebin/combine, so vCPU is the wall-clock dial.

## App-side (ctbk, for reference — not pyrmts work)

- Derived image with ctbk installed (once raw-ingest Source lands), R2 creds wiring, `ctbk gbfs engine submit` passthrough mirroring the existing `ctbk gbfs engine build` flags.
- Compare/acceptance stays app-side (`ctbk gbfs engine compare` + JSONL manifest; manifest can come back via S3/R2 instead of local disk — the engine's `JsonlShardIndex` may want an s3:// variant, cheap to add here if useful).

## Non-goals

- No distributed executor work (still deferred per the build-engine spec).
- No standing instances — that's the point. `e`-class boxes remain available but shouldn't be required for any pyramid build.
- No GPU, no multi-job orchestration; one build = one job.

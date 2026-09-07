# engine: default `mem_budget` misreads the container limit on Fargate

Status: **DONE** (2026-09-07). Both fixes landed:

- `batch.submit` adds `-b <70% × memory>m` to any `build` command that lacks an explicit `-b` — from `-M` when given, else the latest ACTIVE revision's MEMORY requirement (`job_definition_memory_mib`) — and logs `mem_budget: -b 22937m (70% of 32768 MiB)` at submit time (`with_default_mem_budget`, `python/pyrmts_engine/src/pyrmts_engine/batch.py`).
- `_detect_mem_limit` (engine.py) walks `/proc/self/cgroup` → `memory.max` at the process's cgroup and every ancestor, reads cgroup v1's root limit, and `ECS_CONTAINER_METADATA_URI_V4/task` `Limits.Memory`; the tightest candidate wins, `MemTotal` is the tagged last resort. The `build_local:` banner now ends `mem_budget=25.8GB (cgroup:/ecs/<task>)` / `(ecs-metadata)` / `(meminfo)` / `(explicit)`.
- Tests: `tests/test_mem_limit.py` (fake sysfs/proc trees + metadata dict), `tests/test_batch.py::test_submit_defaults_mem_budget_from_job_memory` (pass-through of explicit `-b`, `-M` beats the job-def lookup).

## Symptom (ctbk, 2026-09-07)

`pyrmts-engine batch submit` of a full `smg-v1` fill (ctbk; Fargate, job definition 8 vCPU / 32768 MiB) logged:

```
build_local: 307 windows × 12h … workers=8, max_inflight=16, mem_budget=46.3GB
…
progress: 96/307 windows, 8 shards written, 8 in-flight (cap 16), rss 21.8GB, …
```

and was then killed: exit 137, `OutOfMemoryError: container killed due to memory usage` (Batch job `fab6db18-62b4-43fc-8b79-275c5084b31d`, log group `/pyrmts-engine/batch`). No Python traceback — the cgroup OOM-killer took the process while it was under its own budget.

`46.3GB` is 70% of ~66 GB, i.e. `_detect_mem_bytes()` (`python/pyrmts_engine/engine.py`) fell through both cgroup paths and returned the **host's** `MemTotal`, not the 32 GiB task limit. The resubmit with an explicit `-b 24g -V 16 -M 49152` ran at `mem_budget=25.8GB` and completed.

## Cause

`_detect_mem_bytes` reads only the cgroup **root** files:

```
/sys/fs/cgroup/memory.max
/sys/fs/cgroup/memory/memory.limit_in_bytes
```

On Fargate (and on ECS-on-EC2 with cgroup v2) the container's `memory.max` at `/sys/fs/cgroup/memory.max` is `max` (the limit is applied on an ancestor cgroup that isn't mounted at the root of the container's namespace, or the root is unlimited and the task-level limit lives one level up). `max` fails `isdigit()`, the v1 path doesn't exist, so the `/proc/meminfo` fallback wins — and that is the host.

## Fix (either / both)

1. **Submitter knows the limit** — `batch submit` already takes the job memory (`-M`, or the job definition's `MEMORY` resource requirement). When `-b` isn't given, pass `-b <0.7 × memory MiB>` into the container command explicitly. Deterministic, no runtime detection needed; the detection stays as the fallback for non-Batch runs.
2. **Better detection** — in `_detect_mem_bytes`, before `/proc/meminfo`:
   - walk `/proc/self/cgroup` → `/sys/fs/cgroup/<path>/memory.max`, then each ancestor up to the root, taking the smallest numeric value;
   - if `ECS_CONTAINER_METADATA_URI_V4` is set, `GET $URI/task` → `Limits.Memory` (MiB) — authoritative on Fargate;
   - only then `MemTotal`.
   And log which source produced the number (`mem_budget=… (cgroup|ecs-metadata|meminfo)`) so a fallthrough is visible in the first line of the job log rather than 30 minutes later as exit 137.

(1) is the one that matters for ctbk's daily gap-fill steps; until it lands, ctbk passes `-b` on every `engine submit` (`gbfs-compact.yml`, `specs/avail-smg-pyramid.md` in ctbk).

## Verification

- Unit: `_detect_mem_bytes` with a fake `/sys/fs/cgroup` tree where the root says `max` and `/sys/fs/cgroup/ecs/<task>/memory.max` says `34359738368` → returns 32 GiB.
- Batch: a `-n`/dry-run of `batch submit -M 32768` shows `-b 22937m` (or equivalent) in the container command when `-b` is omitted; an explicit `-b` is passed through unchanged.
- Log line: first `build_local:` line names the budget source.

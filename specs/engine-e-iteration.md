# Engine executor iteration on `e` (ctbk avail workload)

Status: **open** (2026-07-22, written by the ctbk laptop session). Iterate `pyrmts-engine`'s parallel window executor on this box (m6g.4xlarge: 16× Graviton2, 61 GB, aarch64 — same arch family as the Fargate target) until the full-range ctbk avail build completes with bounded memory and a wall/effective-cores number worth reporting. Batch is parked until this converges; the final proof run goes back there (image rebuild → `bootstrap -i` → Spot + `-u` resume).

## Context

`6e2aebb` (parallel window executor + coverage modeling, this repo) went straight to AWS Batch and failed three ways in two runs — all diagnosable, none diagnosed *on* Batch comfortably. Hence this box: direct `build` CLI (no `batch submit` indirection), live `top`/py-spy/dmesg, no docker/ECR cycle per attempt.

### Batch findings 8-11 (2026-07-22, ctbk session — extends findings 1-7 in `specs/engine-batch-packaging.md`)

8. **`batch submit` lacks `-j/-K` passthrough** — the container is stuck with `workers=cpu_count`, `K=2N` defaults. Add passthrough (mind `-j` = `--job-name` collision in `submit`).
9. **Engine is mute until first flush** — two OOM'd attempts (64 GiB, ~115 s each) produced **zero** log lines. Print a startup banner (resolved workers/K, window count, range, source rung, config URL) before the first read.
10. **`K` bounds windows, but memory scales with distinct source shards ≈ `⌈K·window/source_shard_dur⌉` + `workers` rebin intermediates.** With 12h windows on a 2d source, `j=16/K=32` pins ~8 wide shards (cache eviction is watermark-driven, and the watermark can't advance before window 0 completes) plus 16 concurrent parse/rebin states: OOM at 64 GiB in ~115 s, twice, silent. Wanted: byte-aware admission (estimate per-shard frame bytes from the first load; gate claims on a budget), or at minimum scale default `K` by `window/shard_dur` and document the envelope.
11. **Cold-start SIGSEGV survives the finding-6 fix under N workers**: serializing only the *first* window's read doesn't help when 15 other workers then issue their first concurrent `pq.read_table` (120 GiB attempt died SIGSEGV ~2 min in, faulthandler showing two threads interleaved in `claim`/`build_local`). `ARROW_DEFAULT_MEMORY_POOL=system` was set (baked into the image env) and did not prevent it. Candidate fixes: serialize each *thread's* first read (per-thread first-touch barrier), a one-time process-wide warmup read before the pool opens, retry-on-crash is not catchable (it's a segv) — so prevention only; consider `pa.set_cpu_count(1)` inside worker threads (read_table's internal parallelism × 16 workers is oversubscribed anyway and may be the racing component).

## Environment (already set up on this box)

- `~/pyrmts` at `6e2aebb`, editable venv at `~/pyrmts/python/.venv` (`uv pip install -e './pyrmts[s3]' -e ./pyrmts_engine`).
- `~/e-engine-run.sh <label> <build args…>` — run wrapper: R2 creds from the `[cf]` profile in `~/.aws/credentials` + `CLOUDFLARE_ACCOUNT_ID` from `~/ctbk/.envrc`; exports `PYTHONFAULTHANDLER=1`, `ARROW_DEFAULT_MEMORY_POOL=system`, `_RJEM_MALLOC_CONF=dirty_decay_ms:1000,muzzy_decay_ms:1000`; logs to `~/engine-runs/<label>.log` and samples `RSS VSZ` (MB) every 10 s to `~/engine-runs/<label>.mem`.
- Scratch data (R2 bucket `ctbk`, prefix `avail-v4-engine-check/`): seeded `1m/2d/` source rung (51 shards, full range), `config.yaml` (keyTemplate rewritten into the scratch prefix — **cannot** touch prod `avail-v4/`), manifest at `avail-v4-engine-check/manifest.jsonl`.
- Full range: `2026-04-07T01:15/2026-07-18T00:00` (~205 12h windows). Reference numbers: sequential engine did it in 2h45m at ~2.6-2.7 effective cores on an M-series laptop; Fargate 16 vCPU paced ≈1h55m.
- Canonical invocation (2-week slice shown; drop the `-r` end to full range):

```
~/e-engine-run.sh <label> -n avail-v4-engine-check -r 2026-04-07T01:15/2026-04-21T00:00 \
  -w 12h -g 2048 -s s2_cell,dt -t 1m -d 2d -j 16 -K 8 -u \
  -m s3://ctbk/avail-v4-engine-check/manifest.jsonl s3://ctbk/avail-v4-engine-check/config.yaml
```

- Baseline datum from the first `j16/K8` 2-week slice (this box, 2026-07-22): **OOM-killed by the kernel at ~61 GB RSS ~3.5 min in** — mem log ramps 9.7 → 47 → 61 GB over ~2 min; zero flushes; **zero log lines** (finding 9 reproduced off-Batch). Implication for finding 10: the source-shard cache isn't the dominant term — with `K=8` only ~2 wide shards were resident, so ≥45 GB is **per-worker window-processing state** (~3-7 GB each: window frame slice + hist-JSON parse + long-form rebin intermediates across 15 tiers). `j=16` alone busts the box regardless of `K`. First cheap probe: `-j 4 -K 8`; the real fix is shrinking per-window state (stream/chunk the rebin cascade instead of materializing the full long-form per window). No cold-start segv on this launch (1/1 clean; keep counting toward acceptance #2 — note this venv uses pip pyarrow, not the Docker image's, if the segv won't reproduce here).

## Goal / acceptance

1. Full-range build **completes** on this box with peak RSS comfortably < 32 GB (the target Batch container is 16 vCPU / 32-64 GB once behaved).
2. No cold-start segv across ≥5 consecutive launches (finding 11 fixed, not dodged).
3. Wall + effective cores (CPU-time/wall) reported; target: meaningfully above 2.7 cores — ideally wall < 1h.
4. Byte-identity: `workers=N` output ≡ `workers=1` (the existing regression test), and the ctbk session will content-compare scratch vs the fan-out reference after a full run — don't delete scratch outputs.
5. Findings 8-9 landed (passthrough + banner); finding 10 landed at least as a documented default (`K` scaled by `window/shard_dur`) if byte-aware admission is deferred.

## Notes

- Iterate code here directly (this clone), commit on `main` as usual; the laptop sessions will `git fetch` from `e` / get pushed-back commits — coordinate via this spec's status line.
- Memory profile of every run is in `~/engine-runs/<label>.mem` — cite peak/steady numbers in the spec when closing it out.
- If close-time memory (not window concurrency) turns out to bind, that's the deferred phase-2 (sorted-run merge closes) trigger.

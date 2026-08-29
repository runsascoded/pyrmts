# Engine executor iteration on `e` (ctbk avail workload)

Status: **converged on e** (2026-07-23) — all five acceptance criteria met on this box (see Acceptance status); remaining work is ctbk-side (content-compare + Batch proof run). Design directions for incremental/Lambda builds captured below for follow-up specs. Iterate `pyrmts-engine`'s parallel window executor on this box (m6g.4xlarge: 16× Graviton2, 61 GB, aarch64 — same arch family as the Fargate target) until the full-range ctbk avail build completes with bounded memory and a wall/effective-cores number worth reporting. Batch is parked until this converges; the final proof run goes back there (image rebuild → `bootstrap -i` → Spot + `-u` resume).

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

## Iteration log (e session, 2026-07-22)

Measured ground truth (probe on one 12h window / one 2d source shard, pre-fix): source shard parses to a **3.6 GB** long frame (104.7M rows); a 12h window frame is **823 MB**; the tier cascade retained **~2 GB more** (all 15 tier frames held for the task's life); peak RSS for one window incl. parse transients ~14 GB. 8 in-flight × that ≈ the observed 61 GB OOM — fully explained.

Landed (each with its own commit):

- **Findings 8, 9, 11** (`53b37c7`): `batch submit` `--workers`/`-K`/`-b` passthrough; always-on stderr startup banner + ≤1/30s progress line (windows, shards, in-flight vs cap, RSS, cache); `_warmup_arrow()` main-thread pyarrow init + `pq.read_table(use_threads=False)` in `WideShardSource` (worker-level parallelism already saturates cores).
- **Finding 10, round 1** (`53b37c7`): estimate-based byte-aware admission — **insufficient**: claim-time-only accounting; 2nd OOM @62 GB. Round 2 (`e6e5aed`): RSS-feedback gate `rss + (inflight+1)×H×est ≤ budget` + base-tier rebin passthrough (the 1m→1m group_by was an identity copy: 823 MB + transients + ~4s CPU per 12h window) — **still insufficient** at `-w 3h`: 16 claims admitted in one cold-start pass at rss 7.7 GB; true per-task delta ≈ 5× retained-frame est; memcg-killed at 43.9 GB (the new `systemd-run MemoryMax=42G` wrapper confined it — no more box-wide OOM/thrash, which was also what kept killing the CC session). Round 3 (`e5a3dd4`): slow-start (≤2 claims/pass) + no-claims ceiling at 85% of budget (close-transient reserve) + headroom ×4.
- **`metric` Utf8 → Enum** (`21305a4`): −15% on every long frame (shard 3.6→3.05 GB, 12h window 823→703 MB).

Key structural insight: `-w` is the real per-task-memory dial (the spec's "stream/chunk the rebin cascade" ≡ smaller windows, since everything is monoid and spill-close combines partial bins). 12h windows → ~6.5 GB true per-task footprint → even correct admission can only run ~4-5 workers in 24 GB; 1h windows → ~0.5-0.7 GB/task → all 16 workers fit.

**First completing run** (`j16-2w-1h`, 2-week slice, `-w 1h -j 16 -b 24g`): wall 941 s, CPU 63.9 min → **4.08 effective cores**, walk-phase RSS 10-19 GB, 751M source rows, 26 shards, resume worked. Two residues: peak RSS 36.9 GB from *close-path* transients (a max-rung close materializes ~100M-row combined long + non-streaming pivot ≈ 10 GB, stacking with the walk), and the serial close-drain tail ate roughly half the wall (extrapolated full-range wall ≈ 1h55m ≈ Fargate pacing, not yet <1h).

- **Chunked closes** (`a7b6097`): closes over an estimated 2 GiB combined-long are split into disjoint bin-range chunks (combine+widen per chunk, concat wides, one global sort at write; ≤64 chunks). Byte-identity proven by uniqueness of wide rows per (dims, bin) + total writer sort; regression-tested with a forced 3-chunk build. This was the "close-time memory binds" trigger, taken at the lightweight end short of phase-2 sorted-run merges.

- **Readahead + parallel closes** (`614c234`): `WideShardSource.prefetch` (engine warms the shard the claim frontier will hit next; shard-boundary parses were stalling workers ~30 s each, ~25 min serialized over the full range) and a 3-worker close pool with ordered-`seq` registration (ShardIndex still sees the sequential-equivalent order; FIFO start order makes the barrier deadlock-free).

Full-range run 1 (`j16-full-1h`, pre-readahead/parallel-close code, fresh `manifest-full.jsonl`): walk finished 2447 windows in ~43 min, RSS 11-18 GB, admission cap never pinched below worker count for long — but the **single close thread saturated from ~minute 10**; at walk-end 72 of 99 closes were still queued at ~80-110 s each (2m/4d ≈ 85 s: combine 98M-row long, widen, PUT 180 MB). Killed rather than ride out a 1-2 h serial tail the already-committed close pool eliminates; its partial outputs/manifest remain (deterministic, so later runs' shards are byte-identical). Also: spill peaked ~22 GB on disk — plan ~25 GB scratch headroom for full-range runs.

Full-range run 2 (`j16-full2-1h`, readahead + parallel closes, fresh `manifest-full2.jsonl`): **wall 3382.5 s (56.4 min) — under the 1 h stretch — CPU 8h19m34s → 8.86 effective cores** (3.3× the 2.6-2.7 sequential reference), 2447 windows, 5.59B source rows, 99 shards (17.6 GB), zero errors. Peak sampled RSS **37.0 GB** though — over the "comfortably < 32" acceptance line; 3 concurrent close transients stacked on the walk (the close pool had slack: 92/99 closed before walk-end). Tuned `_CLOSE_WORKERS` 3→2 + close chunks 2→1 GiB (`e800312`), then promoted both to CLI dials `-C`/`-c` (`e18ebbe`).

Full-range run 3 (`j16-full3-1h`, `-b 20g -C 2 -c 1g`, fresh `manifest-full3.jsonl`): **wall 3917.3 s (65.3 min), CPU 8h00m → 7.36 effective cores, peak RSS 24.0 GB** — acceptance #1 met comfortably. Identical outputs (99 shards, byte counts match run 2). The par-vs-mem trade is now a clean two-profile story:

| profile | dials | wall | eff cores | peak RSS |
|---|---|---|---|---|
| par-leaning | `-w 1h -b 24g -C 3 -c 2g` | 56.4 min | 8.86 | 37.0 GB |
| mem-tight | `-w 1h -b 20g -C 2 -c 1g` | 65.3 min | 7.36 | 24.0 GB |
| par-max (this box) | `-w 3h -b 36g -C 4` | **47.9 min** | **10.03** | 37.7 GB |

Run 4 (`j16-full4-3h`, fresh `manifest-full4.jsonl`) answers the 8xl-resize question in favor of the resize: effective cores rose 8.86 → 10.03 with 3h windows **even though the budget throttled admission to 4-11 workers** (vs 16) — per-worker efficiency jumped with op size, so the walk is memory-limited here, not GIL-limited. On an 8xl (32 vCPU, 128 GB): `-w 6h -b ~90g -j 24-32 -C 4-6` plausibly lands mid-teens effective cores / ~25-35 min wall. GIL would presumably bind eventually; range-partitioned processes remain the next rung after that. CPU total is ~constant (~8h) across all three configs — the engine is work-conserving; only overlap quality moves.

All four full-range manifests/outputs produced identical total output bytes (17,563,471,257 across 99 shards, runs 2/3/4 under three different concurrency configs) — a strong cross-config RGIP signal ahead of the ctbk content-compare.

## Acceptance status (2026-07-23)

1. ✓ Full-range completes, peak RSS 24.0 GB (mem-tight profile, run 3) — comfortably < 32.
2. ✓ 7 consecutive clean cold starts, zero segv (caveat stands: venv pip pyarrow, not the Docker image's — Batch proof run still the real test).
3. ✓ Wall 47.9 min / 10.03 effective cores (best), 56-65 min on 1h-window profiles; all ≥2.7× ✓, best <1h ✓.
4. ✓ `workers=N ≡ workers=1` regression green throughout; scratch outputs + all manifests retained for the ctbk content-compare. Nothing deleted.
5. ✓ Findings 8-9 landed; 10 landed as full RSS-feedback byte-aware admission (stronger than the documented-default minimum); 11 landed as warmup + single-threaded reads.

Remaining (ctbk side): content-compare scratch vs fan-out reference; Batch proof run (image rebuild → `bootstrap -i` → Spot + `-u` resume).

## Lambda-envelope validation (2026-07-23)

Simulated a daily cron top-up: `manifest-full4` truncated at 07-17 (drops 13 tail shards; the open `3d/12d` tile forces a resume from 07-06 — the amplification in action: 1-day extension ⇒ 12-day walk), run under a 10 GB cgroup with `-j 2 -b 7g -C 1 -c 512m -w 1h`:

- **v1 memcg-killed at 10.4 GB ~30 s in — inside the first source-shard parse.** The whole-shard wide→long transient alone busts a 10 GB container. Fixed by streaming the parse per 128k-row record batch (`b265673`): transient bounded at one batch's explode intermediates; row-locality + downstream unordered group_bys + total writer sort keep bytes unreachable.
- **v2 completed: wall 525.5 s (8.8 min < 15), peak RSS 9.7 GB < 10** — but with only 3% headroom, caused by readahead pinning a second parsed shard. Readahead is now budget-gated (`5020a3c`: prefetch only when `budget − rss > 1.5 × cache`), which should put the profile near ~6.5 GB peak at the cost of boundary-stall wall (~+4-5 min at `-j 2`; still < 15 for this shape). Rebuilt shards matched run 4's (rows, KiB) exactly, 13/13.
- Verdict: the *daily top-up* shape fits Lambda post-streaming-parse. The unresolved risk is worst-case amplification (a 30m@64d / 2h@256d boundary day re-walks weeks — won't fit 15 min): needs same-tier consolidation (above) or a Batch fallback for boundary days. A cron `batch submit` (EventBridge → Fargate Spot, no 10 GB/15 min caps, same hardened code path, ~cents/day) remains the simpler default unless Lambda is specifically required.

Lambda profile datum: `-w 1h -j 2 -b 7g -C 1 -c 512m` → 8.8 min / 9.7 GB peak (pre-gating; expect ~6.5 GB with gated readahead).

## Incremental (cron/Lambda) builds → moved

This section became `specs/engine-incremental-consolidation.md` (2026-08-28). None of it was implemented, so it was lifted out rather than archived here: same-tier consolidation as composable stream ops, a dt-major base rung, s2-range shadow buckets, and the zero-decode concat design card. The measurements that motivate it stay above.

## Notes

- Iterate code here directly (this clone), commit on `main` as usual; the laptop sessions will `git fetch` from `e` / get pushed-back commits — coordinate via this spec's status line.
- Memory profile of every run is in `~/engine-runs/<label>.mem` — cite peak/steady numbers in the spec when closing it out.
- If close-time memory (not window concurrency) turns out to bind, that's the deferred phase-2 (sorted-run merge closes) trigger.


## Closed (2026-08-28)

Both ctbk-side residues named in the Acceptance status are discharged. The **Batch proof run**: `specs/done/engine-fill-mode.md:7` records ctbk standing up `avail-v5/` via an engine Batch backfill (genesis→07-18), and its 08-16 adoption note makes `batch submit -f` the production rides-v5 cadence — "the engine's proven 34 min / ~$2 for a full 3.4-month build at ~10 effective cores". The **content-compare**: `specs/done/engine-raw-ingest.md:80` records `compare_manifest` against prod avail-v5 returning **9/9 `equal`** on real data.

The "## Incremental (cron/Lambda) builds" section was **moved out** to `specs/engine-incremental-consolidation.md` before archiving — none of it is implemented, and it would have been buried here. This spec keeps the measurements that motivate it.

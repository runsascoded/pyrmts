# Engine `--fill` mode: declarative gap-fill (the Lambda contract, at Batch speed)

Status: **open** (2026-07-28, written by the ctbk session). Give `pyrmts-engine build` an optional mode with the same contract as ctbk's cascade Lambda — *inspect actual state, identify missing expected shards, build exactly those* — executed by the existing parallel window executor (maximally parallel, one node).

## Motivation (today's incident, ctbk avail-v5 burn-in)

ctbk stood up `avail-v5/` (engine Batch backfill genesis→07-18 + minutely cascade-Lambda tick). The tick then faced a cold 10-day hole (v4 dormancy) and **treadmilled**: gap discovery's min-cover demands `1m@2d` tiles whose pure raw ingest exceeds one 15-min invocation; per-shard commit granularity ⇒ zero durable progress, repeated forever, while the moving tip minted new small gaps. The fallback (Lambda fan-out rebuild driver, layered scaffolds→concats) works but is slow/expensive: est 87 min wall / 8.9 Lambda-hrs / ~$5.35 for 61 shards + 102 scaffolds — vs the engine's proven 34 min / ~$2 for a **full 3.4-month** build at ~10 effective cores. The engine should be the bulk gap-filler; today it can only rebuild an explicit range against a private JSONL manifest.

## Contract

`build --fill` (suggested: `-F/--fill`):

1. **Expected**: compute the expected min-cover shard set for the range (default: genesis → now) from the config ladder — `pyrmts.gap_discovery.list_expected_shards` already models this, including genesis-clipped tiles (`effective_{start,end}`) and largest-fitting-rung selection. Reuse it; do not re-implement.
2. **Actual**: LIST the target prefix on storage (same ground truth the cascade Lambda uses). Done-set = listing ∪ manifest records (manifest still appends per-write, so `-u` semantics compose; a later registry-driven mode can swap the listing for D1 rows — out of scope here).
3. **Diff → plan**: missing = expected − actual. Restrict the window walk to the minimal window set feeding any missing shard (this is a generalization of the existing resume logic, which already re-walks "windows feeding any unfinished shard" — the change is deriving done-ness from the listing instead of only the manifest).
4. **Clamp to source**: fillable = missing shards whose feeding windows are covered by the available source rung(s). Shards needing data past the source's coverage end (or below the source tier — e.g. the base tier itself, pre-raw-ingest) are **reported as unfillable and skipped**, mirroring the Lambda's "fillable gaps: X of Y" — not an error. Print both counts; exit 0 when all *fillable* gaps were filled.
5. **Write + record**: identical to today — write missing shards only, append manifest records (`md5`/`n_bytes` included). Registration stays app-side (ctbk `gbfs engine register` pushes the delta manifest to D1).

Result: "backfill", "resume", "extend", "catch-up" collapse into one operation — same declarative contract as the Lambda, two executors (Lambda incremental / engine bulk), per the steady-state doctrine (per-tier cron writers + one fsck-style backfill, no third shapes).

## Non-goals (this iteration)

- Same-tier consolidation (fill coarse tiles by merging finer, not re-walking source) — separate spec, already on the roadmap.
- Raw-ingest source (fills base-tier gaps; erases the Lambda fan-out's last exclusive capability) — separate spec.
- Registry(D1)-driven actual-state / CoW pyramids — needs ctbk-side discovery changes first.
- Verify mode (md5-check existing shards against manifest) — cheap to add later on the same diff machinery.

## Notes for implementation

- The walk restriction matters for wall time: scattered small gaps should not trigger a full-range walk. Window set = union of windows overlapping any missing shard's period (window grid unchanged, so spill/close determinism is unaffected; byte-identity of outputs vs a full rebuild should hold and is the key regression test).
- Listing is one paginated LIST per prefix (cheap); key→(tier, shard_dur, period) parse must tolerate foreign keys under the prefix (configs, manifests) by ignoring them.
- Tests (TFFP style): (a) full build → delete N random shards from storage → `--fill` rebuilds exactly those, byte-identical, walks only their windows; (b) extend: build [t0,t1), advance now to t2, `--fill` builds only the tail; (c) unfillable: expected shards past source coverage reported+skipped, exit 0; (d) `--fill` on a complete pyramid is a fast no-op (LIST + no walk).
- ctbk will wire `ctbk gbfs engine submit --fill` passthrough once the CLI flag exists; assume flag name `-F/--fill` unless something collides.

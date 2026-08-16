# Engine `--fill` mode: declarative gap-fill (the Lambda contract, at Batch speed)

Status: **implemented** (pyrmts session, 2026-07-28 — see Status section at bottom; flag is `-f/--fill`, since `-F` was taken by `--filter`). Written by the ctbk session (2026-07-28). Give `pyrmts-engine build` an optional mode with the same contract as ctbk's cascade Lambda — *inspect actual state, identify missing expected shards, build exactly those* — executed by the existing parallel window executor (maximally parallel, one node).

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

## Status (pyrmts session, 2026-07-28)

Implemented as `build_local(fill=True)` / `build -f/--fill` (`-F` collides with `-F/--filter`, on both `build` and `batch submit`); `batch submit -f` and `build_command(fill=True)` pass through. Notes against the contract:

1. **Expected**: `compile_plan` (which already wraps `list_expected_shards`) unchanged; fill diffs against `plan.outputs + plan.skipped_rungs` (the skipped source rung is part of the min-cover and reports separately, see 4).
2. **Actual**: one LIST of `commonprefix(expected keys)`; done-set = listing ∪ manifest `existing_keys()` (when the ShardIndex has them — so `-u` is subsumed; `-f` needs no `-m`). No key→(tier, shard, period) parsing at all: the diff is set-membership of *expected* keys against the listing, so foreign keys (configs, manifests, stale cover tiles from a narrower range) are ignored by construction.
3. **Diff → plan**: missing shards' `[effective_start, effective_end)` spans are merged; the window walk (grid unchanged) is restricted to windows overlapping a span — scattered gaps walk a sparse, non-contiguous window list. Byte-identity vs full rebuild is regression-tested (scattered multi-tier deletions, extend-the-range, truncated source).
4. **Clamp to source**: coverage end = max present source-tile end (source-rung keys come out of the same LIST). Unfillable = missing shards with `effective_end` > coverage end, plus shards on tiers finer than the source rung's bin, plus absent tiles of the source rung itself (raw-ingest territory) — all reported (stderr + `BuildResult.unfillable`/summary) and skipped, exit 0. Deliberate nuance: **mid-range** source holes are *not* clamped away — their windows walk, and the `max_missing_source` guard still fires (exit 4) unless `--max-missing` opts in; the clamp is for the tip, the guard for real holes.
5. **Write + record**: unchanged (md5/n_bytes in manifest records; registration app-side).

Also: the range stays `-r` (required) — "genesis → now" defaulting is driver-side (ctbk knows genesis; the engine doesn't). A complete pyramid is a no-op: LIST + 0 windows, no source reads (tested). Tests: `tests/test_fill.py` (6), CLI + `build_command` coverage in `test_cli.py`/`test_batch.py`.

Spec stays in `specs/` until ctbk wires the submit passthrough and burns it in on the avail-v5 hole (est. ~10 fillable-gap windows vs the Lambda fan-out's 87 min / $5.35).

## Adoption confirmed (2026-08-16, pyrmts session) — moving to `done/`

Both hold conditions are met in ctbk's checkout:

- **Submit passthrough wired** — `ctbk gbfs engine submit -f/--fill` (`gbfs_cli.py:1473`), whose help text cites this spec by name; `pyramid_cascade/fsck.py` documents `pyrmts_engine.build_local --fill` as the mechanism it delegates bulk gap-filling to.
- **Burned in, and now load-bearing** — fill is the production path for the rides-v5 monthly cadence: `ctbk gbfs rides-v5-extend` calls `_engine_submit(..., fill=True)` per anchor over an **uncapped** genesis→now range (ctbk `2662c347`, `49eaf98f`). That run surfaced two real refinements worth recording here:
  - **Don't cap the fill range.** Capping at month-end leaves coarse-rung holes — shards whose spans cross the cap (a `1d/64d` covering Jun 4–Aug 7) can't build inside it, and the monthly rebin then rides on the `1d` tier at serve time. Uncapped fill + a sweep of wholly-empty pure-future shards is the correct shape.
  - **`--max-missing` is the release valve for the in-progress period.** An uncapped fill's expected cover reaches `now`, so current-month shards want a source object that doesn't exist yet mid-month; ctbk runs `--max-missing 0.01` (1/~160 months ≈ 0.006), which tolerates exactly that while still failing on a real 2-month hole. This is the intended use of the §4 guard — the tip clamp handles the tip, the guard handles genuine holes, and `--max-missing` covers the one case that is structurally neither.

Not discharged by this: `specs/engine-min-cover-source.md` stays open — ctbk's engine wrapper still pins a source rung (`-s/--source` defaults to `1m@2d`), so the min-cover default is unexercised.

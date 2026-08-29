# Engine min-cover source: read the source tier as it's actually stored

Status: **implemented** (pyrmts session, 2026-07-28 — see Status section at bottom). Written by the ctbk session (2026-07-28). `WideShardSource` reads exactly one rung (`-t 1m -d 2d`); a maintained pyramid stores its source tier as a **min-cover mix** (large consolidated tiles for history + progressively finer tiles toward the live tip). Single-rung reading caps the engine's source coverage at the last complete large tile — on ctbk avail that's up to ~2 days behind now, which is exactly the freshest, most-queried span. This is the standing blocker for the engine owning tip-adjacent fills (`-f` currently marks that span unfillable and leaves it to the Lambda tick).

## Contract

`WideShardSource(pyramid, tier_name, shard_dur)` → `WideShardSource(pyramid, tier_name)` (+ optional `shard_dur` pin for back-compat / the seeded-scratch case):

1. **Discovery**: one LIST of the tier prefix (`<prefix>/<tier>/`) — all rungs. Parse `(shard_dur, period)` from keys (the engine's `-f` listing machinery already lists; reuse one LIST for both fill-diff and source discovery when source tier lives under the same prefix).
2. **Tile selection (min-cover, deterministic)**: for the requested range, choose the covering tile set preferring **largest shard_dur** at each instant; ties/overlaps resolved by (larger dur, earlier start) — the same preference `gap_discovery`'s expected-cover uses, so a healthy pyramid's actual min-cover matches the selection exactly and redundant stale tiles (not yet GC'd) are deterministically ignored. Selection must be a pure function of the LIST result — byte-determinism of builds depends on it. Overlapping-but-not-identical coverage (a 2d tile + a 12h tile inside it): the larger tile wins for its whole span; the finer tile is ignored (its content is a sub-aggregation of the same source rows — identical data — but *reading* a consistent tile set is what keeps window slices deterministic).
3. **Coverage model**: contiguous covered span from range start; `coverage()` / missing-source accounting per selected tile (a missing expected source tile mid-range still counts toward `max_missing_source`, as today). Coverage end = end of the last selected tile — i.e. the tip, minus only the currently-open finest bin.
4. **read_window / cache / prefetch**: unchanged shape — cache entries keyed by selected tile (not by fixed-dur period); watermark eviction uses tile end; readahead prefetches the next selected tile in walk order. Memory note: tip tiles are small (minutes–hours), so the byte-aware admission's per-tile estimates must come from each tile's own first load, not a single global estimate (mostly already true — verify the estimate isn't keyed to an assumed uniform shard size).
5. **CLI**: `-d/--source-shard` becomes optional; absent → min-cover mode. `batch submit` passthrough same. ctbk will drop `-d` from its wrapper default once this lands.

## Why now (concrete)

Today's avail-v5 catch-up used engine `-f` for tiers above 1m, but its source clamp stops at the last complete `1m@2d` (~07-27), leaving ~1-2 days × 14 tiers of dust to the Lambda tick. With min-cover source, `-f` fills to within minutes of now, the tick's steady-state job shrinks to the open-bin frontier, and daily cron `-f` runs stop having a structural blind spot.

## Determinism / tests

- Regression: build a range from a single-rung source vs the same data stored as a mixed min-cover (2d + 12h + 1h tiles) → **byte-identical outputs**. (Content per window is identical by construction; the test guards slicing/ordering bugs.)
- Redundant-tile ignoring: add a stale finer tile fully covered by a larger one → selection unchanged, bytes unchanged.
- Tip coverage: source stored as [2d…, 12h, 1h, 5min] → coverage end = end of the 5min tile; `-f` fills expected shards up to it; only the open bin reported unfillable.
- Mid-range hole in the min-cover (a 12h tile missing between 2d tiles) → counts as missing source (exit-4 guard preserved), not silently clamped.

## Non-goals

- Raw-ingest source (separate spec; after this, it's the only remaining Lambda-exclusive capability).
- Cross-prefix source selection via D1 registry (CoW world) — LIST-based is fine until discovery generally goes registry-driven.

## Status (pyrmts session, 2026-07-28)

Implemented: `WideShardSource(pyramid, tier_name)` defaults to min-cover; `shard_dur=` pins a single rung (back-compat / seeded-scratch — no LIST happens in pinned mode). Notes against the contract:

1. **Discovery**: one lazy LIST of the tier prefix (keyTemplate substituted through `{tier}` + filter, cut at the first unresolved placeholder), taken once per source instance (single-flight). No key *parsing*: candidate tiles are enumerated from the ladder's declared rungs via `shard_periods_covering` and checked for membership in the LIST set — foreign keys are ignored by construction. (The "reuse one LIST with `-f`" optimization was skipped: two prefix LISTs per run are noise; noted here in case it ever matters.)
2. **Selection** is pointwise and pure: at instant `t`, the largest rung whose grid-aligned tile is listed wins (ties impossible — one tile per rung covers `t`), which is exactly the greedy walk since the preference depends only on `t`. Redundant finer tiles under a present larger tile are never read (tested via `coverage()` period counts). Instants covered by **no** rung fall back to finest-rung reads whose misses feed the existing `max_missing_source` accounting — so mid-range holes trip the exit-4 guard exactly as in pinned mode (tested), and coverage-fraction semantics are unchanged.
3. **Coverage end** (fill clamp): computed engine-side as the max present tile end across **all** rungs (matching selection), from fill's own LIST. Deliberately max-end, not contiguous-from-start: a mid-range hole must reach the guard, not silently clamp fillability (consistent with `specs/engine-fill-mode.md` semantics).
4. **Cache/prefetch/eviction**: cache keys are `(dur, label)` tiles (labels alone collide across rungs — e.g. `2026-01-03` names both a 1d and a 4d tile); watermark eviction and readahead are otherwise unchanged. The byte-aware admission estimate was already max-observed-per-window (not uniform-shard-keyed), so small tip tiles need no change.
5. **CLI**: `-d` absent → min-cover (note: the *previous* default was the tier's smallest rung, so this is a behavior change for `-d`-less invocations, not just a new option); `batch submit -d` help updated. The finding-1/2 footgun is structurally gone: the no-flags build discovers whatever rung is seeded (CLI test updated to assert exactly that, keeping a pinned-wrong-rung leg for the guard).
6. **`provides` = `(tier, None)`** in min-cover mode, and the engine skips **every** rung of the source tier (the tier is externally owned; the engine writing a coarser source-tier tile from finer siblings would be same-tier consolidation — explicitly a separate spec — and would byte-diverge from Lambda-written tiles). Consequence for fill: absent source-tier cover tiles are reported as unfillable source-rung tiles, per the fill spec.

Tests: `tests/test_min_cover.py` — mixed-rung build byte-identical to a single-rung build's h/d outputs; redundant-tile ignoring (21 vs 24 tiles read, bytes unchanged); tip fill (coverage end = end of the finest tip tile, only later-data shards + absent source-tier tiles unfillable, no uncovered window read); mid-range hole → exact `SourceCoverageError`. Existing suite pins `shard_dur='6h'` (the seeded-scratch case) to keep exercising pinned semantics.

Spec stays in `specs/` until ctbk drops `-d` from its wrapper and burns min-cover in on avail-v5 (tip-adjacent `-f` fills).

## Closed 2026-08-29

The spec's own condition was *"stays in `specs/` until ctbk drops `-d` from its wrapper."* It has: `gbfs engine submit` — the production path, and what `rides-v5-extend` drives — defaults `-s 1m` (help: *"min-cover: read the tier as stored"*) and appends `-d` only when a `@shard_dur` is present, so an unqualified `-s` reaches the engine as `-t 1m` with no rung pin.

The `1m@2d` defaults that remain are on `gbfs engine build` and `gbfs engine seed`, and are correct there: both operate on the scratch prefix (`build` "never serving keys, never D1"; `seed` server-side-copies one rung into scratch), which is precisely the seeded-scratch case pinned mode exists for.

*(Recorded because an earlier read of this got it wrong — grepping `source_rung` finds `engine build`'s `1m@2d` first and it is easy to conclude from that line alone that production still pins. The distinguishing check is whether the wrapper appends `-d`, at `gbfs_cli.py:1454`, not what any one default string says.)*

# Engine min-cover source: read the source tier as it's actually stored

Status: **open** (2026-07-28, ctbk session). `WideShardSource` reads exactly one rung (`-t 1m -d 2d`); a maintained pyramid stores its source tier as a **min-cover mix** (large consolidated tiles for history + progressively finer tiles toward the live tip). Single-rung reading caps the engine's source coverage at the last complete large tile — on ctbk avail that's up to ~2 days behind now, which is exactly the freshest, most-queried span. This is the standing blocker for the engine owning tip-adjacent fills (`-f` currently marks that span unfillable and leaves it to the Lambda tick).

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

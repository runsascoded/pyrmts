# Inventory-driven read walk (min-cover-aware planner)

## Resolution

Landed:

- **Core planner** — `planQueryFromInventory(pyramid, input, registeredShards)` added to
  `js/packages/pyrmts/src/planner.ts`. Single-tier cursor walk and
  ragged-decomposition DP both consult the passed-in `RecordedShard[]`
  instead of synthesizing keys via `shardPeriodsCovering` /
  `shardKeys(...)`. Watermarks retain their trust roles
  (`authoritativeEnd`, `earliestPerShard` / per-tier earliest); only
  tile-existence moves to inventory.
- **Overlap tiebreak** — `pickBestCovering` implements largest
  `shardDur` → most-recent `periodStart` → most-recent `writtenAt`.
  `RecordedShard.writtenAt` is now an optional field on the inventory
  row; D1 was already storing `written_at`, the manifest impl now
  persists it too.
- **Windowed inventory** — `ShardIndex.listShards(pyramidName,
  { tier?, range? })` on both `D1ShardIndex` (WHERE-clause push-down
  using the existing `(pyramid, tier, period_start)` index) and
  `ManifestShardIndex` (post-parse filter). `CachedShardIndex` passes
  through.
- **Geo variant** — `planGeoQueryFromInventory` in
  `js/packages/pyrmts-geo/src/planner.ts`, delegating time-axis
  decisions to `planQueryFromInventory` (mirrors how `planGeoQuery`
  delegates to `planQuery`).
- **Serve wiring** — both `pyrmts-cfw/src/serve.ts` (`serveQuery`) and
  `pyrmts-geo/src/serve.ts` (`serveGeoQuery`) gain optional
  `{ pyramidName, shardIndex }`. When set, they prefetch
  `shardIndex.listShards(pyramidName, { range })` and use the
  inventory-driven planner; otherwise fall back to the watermark-only
  path (unchanged behavior for consumers not on min-cover). Passing
  `shardIndex` without `pyramidName` is a 400.

Tests added:

- `js/packages/pyrmts/src/planner-inventory.test.ts` — nine focused
  cases including the ctbk phantom-`1h@23:00` regression (bug demo
  against old `planQuery`, then clean pick from inventory /
  fall-through / earliestPerShard gate / ragged path), plus a
  stale-not-yet-GC'd overlap tiebreak.
- `pyrmts-cfw/src/serve.test.ts` — three integration cases: shardIndex
  path selects the right key, empty inventory beats a lying watermark
  (no phantom fetch), and `shardIndex` without `pyramidName` is a 400.
- `ShardIndex` conformance suite — windowed listShards (tier filter,
  range intersection), plus `writtenAt` presence on rows.

**Consumer rollout.** ctbk gbfs-api's avail/rides routes already hold a
`ShardIndex`; they pass it into `serveQuery` /
`serveGeoQuery` along with the pyramid name (wrap in
`CachedShardIndex` for per-isolate memoization if desired). The
`fetchShardData` object-not-found throw stays loud per the spec.

**Out of scope for this landing (call out for follow-up).** The Python
reader (no `ShardIndex` port yet) still uses watermark-only planning —
it inherits the phantom-key exposure the day a Python reader lands.
Serve-wrapper prefetch uses the raw `{ from, to }` range, not the
smoothing-extended window — smoothing edges could still under-cover
on min-cover pyramids; ctbk's avail-v3 doesn't use smoothing so this
isn't hot.

## Bug

The reader-side cursor walk synthesizes candidate shard keys from the
ladder + per-(tier, shardDur) watermarks, assuming each rung is dense
up to its watermark. Under min-cover maintenance that assumption is
false: a rung's tiles get superseded by larger-rung consolidation (and
GC'd), and the "last constituent" of every closing rung is never
materialized at all (it closes and is superseded in the same tick).

Observed in ctbk prod (2026-07-10, gbfs-api on pyrmts `f96532b`):

```
GET /api/avail-v3?cells=89c250b24&from=2026-07-03T02:15:00Z
    &to=2026-07-10T02:15:00Z&bin_budget=672&reducer=mean
→ 500 {"error":"fetchShardData: object not found:
       avail-v3/15m/1h/2026-07-09T23.parquet"}
```

`/15m@1h@2026-07-09T23:00` is not in the D1 `pyramid_shards` inventory
and never existed on R2 — at 00:00 it was superseded by `3h@21:00`
before any tick could list it as expected. The walk picked it because
the `15m@1h` *watermark* had advanced past midnight (today's 1h dust
writes) and the walk back-fills rung periods from the watermark.

This is the read-side twin of the ctbk cascade writer wedge (ctbk
`52a5d336`, "consolidate-the-dust"): both sides assumed per-rung full
history where min-cover only guarantees the *current cover*.

## Fix

Plan reads from the ShardIndex **inventory**, not synthesized
(ladder × watermark) keys. **Both planner paths**: the single-tier
cursor walk AND `planRagged` (which currently uses
`effectiveLargestShardWatermarks` and unconditionally emits keys via
`shardKeys(...)` at each tier's largest shardDur — same phantom-key
bug, same fix).

Within the query's resolved tier, tile the query window greedily from
**registered** shards:

1. At cursor `c`, among registered shards `(tier, *, periodStart ≤ c <
   periodEnd)`, pick the one covering `c`. Inventory rows are disjoint
   within a tier's live cover; stale not-yet-GC'd rows may overlap —
   tiebreak: largest `shardDur`, then most-recent `periodStart`, then
   most-recent `written_at`, so the choice is deterministic. Read it,
   clip rows to the query window, advance `c` to its `periodEnd`.
2. If no registered shard covers `c`, fall through to the next-finer
   tier for the residual (existing cross-tier fallthrough), or surface
   a gap.

Equivalently: the min-cover the writer maintains (ctbk
`planDustTiling` mirrors it) IS the read plan; the inventory is its
materialized form.

### Planner signature

Keep the planner pure/sync: new
`planQueryFromInventory(pyramid, input, registeredShards)` where the
caller pre-fetches `registeredShards` via a windowed
`listShards(pyramid, tier?, range?)` variant (D1 index on
`(pyramid, tier, period_start)` makes that cheap; `CachedShardIndex`
can memoize per isolate with a short TTL). Callers (ctbk gbfs-api
avail/rides routes) already hold a `ShardIndex`, so the prefetch is a
one-liner there. Preferred over making `planQuery` async + take the
index — planners stay unit-testable with literal inventories.

### Watermarks' residual role

Only "does this tile exist?" moves to inventory. Watermarks keep
their freshness/trust semantics unchanged:

- `authoritativeEnd` still derives from watermarks (raw-tier max
  effective end);
- `earliestPerShard` / per-tier earliest still gate which windows a
  rung is *trusted* for (partial-shards semantics);
- inventory then decides which concrete tiles supply the trusted
  window.

### Out of scope

`fetchShardData`'s object-not-found throw stays as-is (cf. the
earlier tolerate-missing-shards discussion): an inventory-planned
read should never 404 — keeping the throw loud is what surfaced this
bug, and papering over it would hide real index↔storage integrity
drift.

## Why now

- ctbk `/api/avail-v3` (StationDetail `availSrc=v3`, default since
  `#111`) 500s whenever a query window crosses a superseded-rung
  boundary — intermittent, data-dependent.
- Blocks ctbk #108 (cut `/api/totals?kind=availability` over to the
  pyramid): measured on Grove St PATH 7d/672-bins, legacy path is
  2.2–3.7 s; the pyramid path is 0.17–0.32 s when the plan happens to
  hit live tiles — a ~10× win sitting behind this bug.

## Notes

- The Python reader (if/when one lands) needs the same rule; only the
  TS planner is deployed today.
- Test shape: register a min-cover-shaped inventory (max tiles + dust,
  with the last-constituent holes), advance watermarks past the holes,
  and assert the plan reads exactly the registered tiles — never a
  synthesized key. The ctbk incident above is the fixture to encode.

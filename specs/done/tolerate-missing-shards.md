# Tolerate missing shards: per-consumer earliest-data + 404 fallthrough

## Problem

The planner currently assumes every shard whose period overlaps the query
range exists. For pyramids with heterogeneous start times across dim values
(e.g. awair's 4 devices each starting on a different month), wide queries
that pre-date some dims' data emit segment keys for shards that don't exist
and `fetchShardData` throws `object not found: <key>` on the 404 — which
`serveQuery` propagates as HTTP 400.

Reproed in awair: `t=-186d` with two devices, one of which only has 95
days of S3 history. Worker 400s; chart aborts the query.

## Two complementary fixes

### A. `tolerate404` on the fetch path (immediate correctness)

Add an option to `fetchShardData` / `fetchSegmentRows`:

```ts
export interface FetchOptions {
  binCol?: string
  range?: { from: Date; to: Date }
  initialFetchSize?: number
  /**
   * If true, missing objects (Storage.head returns null) are treated as
   * empty shards (return []) instead of throwing. Useful for pyramids
   * with heterogeneous dim coverage where some shards pre-date a dim's
   * data start.
   */
  tolerate404?: boolean
}
```

And in `serveQuery`:

```ts
plan.segments.map(seg => fetchSegmentRows(pyramid.storage, seg.keys, {
  binCol: pyramid.binCol,
  range: { from: seg.from, to: seg.to },
  tolerate404: opts.tolerateMissingShards ?? false,  // new opt on ServeOptions
}))
```

`ServeOptions.tolerateMissingShards` defaults `false` — pyramids with
guaranteed dense shards keep failing loudly (good for catching real
config bugs). Consumers with sparse / dim-heterogeneous pyramids opt in.

### B. `earliestWatermarks` on `planQuery` (optimization)

Symmetric to `watermarks`. Per-tier instant: "no data exists in this tier
before X." Planner clamps each segment's `from` to ≥ earliest watermark,
omitting segments entirely below it. Avoids the wasted HEAD-then-skip
round trip that (A) alone still incurs.

```ts
export interface PlanQueryInput {
  range: { from: Date; to: Date }
  binBudget: number
  watermarks?: Record<string, Date>           // existing — latest complete bin
  earliestWatermarks?: Record<string, Date>   // new      — earliest available bin
  filter?: Record<string, string | number>
}
```

Like `watermarks`, consumer-supplied. For awair, the worker resolves it
similarly: `R2.list(prefix=pyramid/awair-{device_id}/{tier}/)` → sort keys
→ pick first period → convert to instant. (Cacheable per-deploy since
earliest-data only grows monotonically.)

For pyramids where earliest is uniform across dims, a single global
earliest also works. For dim-heterogeneous pyramids (awair: one device
started 95d ago, another 188d ago), consumers compute per-active-dim
earliest and `min()` across the dims being queried.

`serveQuery` should accept a parallel `earliestWatermarks` callback:

```ts
earliestWatermarks?:
  | Record<string, Date>
  | ((req: Request) => Promise<Record<string, Date>> | Record<string, Date>)
```

## Why both

- (A) alone is sufficient for correctness — never errors on missing shards.
- (B) alone is insufficient — pyramids without precomputed earliest still
  hit 404s on dim-sparse views.
- (A) + (B) gives correctness *and* avoids unnecessary HEADs for pre-data
  ranges.

Land (A) first as the unblocker. Awair will use it via a one-line
`tolerateMissingShards: true` in its `ServeOptions`. Then (B) as a
follow-up perf optimization once consumers wire earliest-data.

## Backwards compatibility

Both opts default to off / undefined. No existing test or consumer needs
changes. Awair would just add `tolerateMissingShards: true` to its
`serveQuery` call and drop a temporary local fork (described in awair's
`cfw/serve/src/index.ts` until this lands).

## awair-side temporary workaround

Until (A) lands in pyrmts, awair forks `serveQuery` locally in
`cfw/serve/src/index.ts` to filter segment keys via `storage.head()`
before calling `fetchSegmentRows`. ~30 LOC. Removed once `tolerate404`
is published.

## Resolution

Both parts landed together. New options:

- `FetchOptions.tolerate404` — `fetchShardData` returns `[]` on missing
  objects instead of throwing.
- `PlanQueryInput.earliestWatermarks` — per-tier earliest-available-bin
  instants. Segment `from` is clamped (or the whole segment dropped if
  earliest exceeds the segment end).
- `ServeOptions.tolerateMissingShards` + `ServeOptions.earliestWatermarks`
  in both `pyrmts-cfw/serve.ts` and `pyrmts-geo/serve.ts` (the geo
  version threads the same options through `planGeoQuery`).

### `earliestWatermarks` propagation

Symmetric to `watermarks` but in the opposite direction. Coarser tiers
are built **from** finer ones, so a coarser tier's data range is a
subset of the finer's; in particular, `earliest[coarser] >=
earliest[finer]`. The planner walks **finest → coarsest** and computes
`effective[tier] = max(declared[tier], finerBound)`, propagating a
finer tier's bound upward. Net effect for awair-style consumers:
declare `earliest[raw]` once and the coarser tiers inherit it
automatically. A coarser tier with its own declared earliest > the
finer-propagated bound wins (modeling "we didn't backfill mo1 for the
oldest months").

### Test additions (118 → 132)

- `fetch.test.ts`: 5 new tests for `tolerate404` on both `fetchShardData`
  and `fetchSegmentRows`.
- `planner.test.ts`: 5 new tests for `earliestWatermarks` (clamping,
  whole-segment drop, finest→coarsest propagation, declared-coarser
  override, no-op when query starts after earliest).
- `pyrmts-cfw/serve.test.ts`: 3 new tests for `tolerateMissingShards`,
  `earliestWatermarks` direct, and `earliestWatermarks` callback.
- `pyrmts-geo/serve.test.ts`: 1 combined test exercising both options.

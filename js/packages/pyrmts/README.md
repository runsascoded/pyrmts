# `pyrmts`

> Multi-scale timeseries pyramids — planner, types, stitcher, frontend hook.

`pyrmts` pre-computes (shard × bin)-tier aggregates of time-series data once,
then serves any range × bin-budget query in O(log) bins from edge. Storage
is parquet (R2 / S3) or D1; the row-level `StorageBackend` abstraction lets
consumers swap byte-store + table-store interchangeably without changing the
planner or stitcher.

Spatial extension: see [`pyrmts-geo`](../pyrmts-geo).
CFW serving helpers: see [`pyrmts-cfw`](../pyrmts-cfw).

## Quick usage

```ts
import {
  parquetBackend,
  planQuery,
  stitch,
  type Pyramid,
} from 'pyrmts'
import { r2Storage } from 'pyrmts-cfw'
```

### Declaring a pyramid

```ts
const awair: Pyramid = {
  storage: parquetBackend(r2Storage(env.PYRAMID_BUCKET), 'awair-{device_id}/{tier}/{period}.parquet'),
  keyTemplate: 'awair-{device_id}/{tier}/{period}.parquet',
  axis: 'time',
  binCol: 'ts',
  dims: [{ name: 'device_id', type: 'int' }],
  metrics: [
    { name: 'temp',  monoid: 'sum' },
    { name: 'co2',   monoid: 'sum' },
    { name: 'pm25',  monoid: 'sum' },
  ],
  tiers: [
    { name: 'raw', bin: '1min', shards: ['1mo'] },   // finest first
    { name: 'h1',  bin: '1h',   shards: ['1mo'] },
    { name: 'd1',  bin: '1d',   shards: ['1y']  },
    { name: 'mo1', bin: '1mo',  shards: ['1y']  },
  ],
}
```

### Bin-budget query

```ts
const plan = planQuery(awair, {
  range: { from: new Date('2026-05-01'), to: new Date('2026-06-01') },
  binBudget: 256,
  filter: { device_id: 17617 },
})
// → planner picks the finest tier whose `binsInRange ≤ binBudget`,
//   emits one segment per tier (walking down from the chosen tier to raw,
//   covering each tier's pre-watermark range)
const rows = await Promise.all(plan.segments.map(s => awair.storage.fetchSegment(s)))
const records = stitch({ pyramid: awair, plan, shardRows: rows })
```

### Caller-specified bin width (ragged decomposition)

When the caller wants an exact output bin width that doesn't match any
stored tier (e.g. `/5min` against a `{1min, 1h, ...}` pyramid), pass
`targetBin`:

```ts
const plan = planQuery(awair, {
  range: { from: new Date('2026-05-01'), to: new Date('2026-05-02') },
  binBudget: 1024,
  targetBin: '5min',                  // not in tiers — DP packs each /5min from finer atoms
  filter: { device_id: 17617 },
})
```

Each output bin is packed with a minimum-item set of finer-tier atoms via
DP (shortest-path on tier-bin-aligned positions, strict-equality alignment);
adjacent same-tier atoms coalesce into segments. See
[`specs/done/multi-tier-bin-packing.md`](../../../specs/done/multi-tier-bin-packing.md)
for the design + 8 deviations from the originally-proposed spec.

Throws if `targetBin` is calendar-variable (`mo`/`y`) or if no integer
linear combination of fixed-width tier bins equals `targetBin`.

### Multi-shard-size ladders for fresh-data tails

For pyramids needing interactive "Latest · N" UX, declare a per-tier
**shard-duration ladder** — every entry is a real shard size, smallest to
largest. The compactor writes at `shards[0]` (e.g. 10-min cadence) and
promotes up the ladder at each boundary (10min → 30min → 1h → … → 15d).
At any moment, the timeline is tiled by shards of varying sizes:
smaller-near-present, larger-further-back (subject to retention).

```ts
const avail: Pyramid = {
  storage: parquetBackend(r2Storage(env.BUCKET), 'avail/{tier}/{shard}/{period}.parquet'),
  keyTemplate: 'avail/{tier}/{shard}/{period}.parquet',
  axis: 'time',
  binCol: 'ts',
  dims: [{ name: 'station_id', type: 'int' }],
  metrics: [{ name: 'n_bikes', monoid: 'sum' }],
  tiers: [
    { name: '15m', bin: '15min', shards: ['10min', '30min', '1h', '3h', '12h', '1d', '3d', '15d'] },
    { name: '1h',  bin: '1h',    shards: ['1h', '3h', '12h', '1d', '3d', '1mo'] },
  ],
}
```

Pass watermarks keyed by `${tier}@${shardDur}` to `planQuery`. The planner
walks the output tier's ladder LARGEST-first at each cursor position;
where the largest isn't sealed for the containing period, it falls to the
next smaller; if no shard at the output tier covers, it falls to the next
finer tier (`reaggregate: true`). Adjacent same-`(tier, shardDur)`
segments coalesce post-walk:

```ts
const plan = planQuery(avail, {
  range: { from, to },
  binBudget: 256,
  watermarks: {
    '15m@15d':  last15mShard15dEnd,
    '15m@1d':   last15mShard1dEnd,
    '15m@1h':   last15mShard1hEnd,
    '15m@10min': last15mShard10minEnd,
  },
})
// → segments: [
//     { tier: '15m', shardDur: '15d', ... },   // largest shard for backfilled history
//     { tier: '15m', shardDur: '1d',  ... },   // falls to /1d where /15d not sealed
//     { tier: '15m', shardDur: '1h',  ... },   // ... and to /1h closer to present
//     { tier: '15m', shardDur: '10min', ... }, // smallest for live tail
//   ]
```

Use a `ShardIndex` impl to source watermarks at query time:

```ts
import { CachedShardIndex } from 'pyrmts'
import { D1ShardIndex } from 'pyrmts-cfw'

const shardIndex = new CachedShardIndex(new D1ShardIndex(env.DB), { ttlMs: 60_000 })
const watermarks = Object.fromEntries(await shardIndex.getWatermarks('avail'))
const plan = planQuery(avail, { range, binBudget, watermarks })
```

`D1ShardIndex` is the recommended impl for CFW consumers (concurrent writers,
atomic upserts). `ManifestShardIndex` (JSON blob over any `Storage`) is a
fallback for single-writer / non-CF deploys. See
[`specs/done/unified-shard-ladder.md`](../../../specs/done/unified-shard-ladder.md) for
the design (cursor-aware-largest-first planner walk; per-tier shard
ladders; promotion-aware watermark propagation).

### Server-side rolling-window smoothing

```ts
const plan = planQuery(awair, {
  range: { from, to },
  binBudget: 256,
  smoothing: '4h',                    // or { auto: true, multiplier: 50 }
  smoothMode: 'centered',             // or 'trailing'
})
```

Plan extends the query window outward by the smoothing buffer so the rolling
pass has full context at the visible edges; the stitcher trims back to
`visibleRange` after smoothing. See
[`specs/done/server-side-smoothing.md`](../../../specs/done/server-side-smoothing.md).

### CFW serve handler

```ts
import { serveQuery } from 'pyrmts-cfw'

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    return serveQuery({ pyramid: awair, request: req })
  },
}
```

The handler parses `?from=&to=&bin_budget=&smooth=&smooth_mode=&<filter>=…`,
runs the planner + fetch + stitcher, returns
`{ records, plan: { outputTier?, outputBin, segments, smoothing, … } }`.

## API surface

### Types

- `Pyramid` — `{ storage, keyTemplate, axis, binCol, dims, metrics, tiers, geo? }`.
- `Tier` — `{ name, bin, shards }`. `shards` is the ascending shard-duration
  ladder (smallest to largest; smallest ≥ `bin`; each divides the next).
- `Dim` — `{ name, type: 'int' | 'string' | 'h3' | 'geohash' }`.
- `Metric` — `{ name, monoid, config? }`.
- `Bin` — `Duration | StepCount` (time-axis or step-axis units).
- `Duration` — `${number}${'min' | 'h' | 'd' | 'mo' | 'y'}`.
- `PlanQueryInput`, `QueryPlan`, `PlanSegment` — planner I/O.
- `Row` — `Record<string, unknown>`.

### Planner

- `planQuery(pyramid, input): QueryPlan` — pure planner; emits segment list +
  output tier (when one matches) + smoothing resolution.
  - `input.binBudget` (required) — max output bins; planner picks finest
    tier whose `binsInRange ≤ binBudget`.
  - `input.targetBin?` — caller-specified output width; ragged decomposition
    via per-bin DP when no tier matches exactly.
  - `input.range`, `input.watermarks`, `input.earliestWatermarks`,
    `input.earliestPerShard`, `input.filter`, `input.smoothing`,
    `input.smoothMode`.
  - `earliestPerShard` — per-`(tier, shardDur)` earliest-available-bin
    gate keyed uniformly `${tier}@${shardDur}`. Per-entry only, no
    propagation up the tier ladder; complements `earliestWatermarks`
    (which propagates). Use for shard durations with forward-only
    coverage from a cron deploy date. See
    [`specs/done/unified-shard-ladder.md`](../../../specs/done/unified-shard-ladder.md).
- `binsInRange(from, to, bin): number`, `parseDuration`, `floorToSpan`,
  `addSpan`, `shardPeriodsCovering`, `formatPeriod` — duration helpers.

### Stitcher

- `stitch({ pyramid, plan, shardRows }): Row[]` — groups rows by output bin
  via `floorToSpan(row[binCol], plan.outputBin)`, monoid-combines across
  segments, applies smoothing if `plan.smoothing` is set, trims to
  `plan.visibleRange`.
- `pivotTallToHistogram(rows, opts): Row[]` — for histogram monoids,
  pivot one-row-per-(key, value) into one-row-per-key with a
  `value → count` map.

### Storage backends

- `StorageBackend` — row-level abstraction: `fetchSegment(segment, opts?)`.
- `parquetBackend(byteStorage, keyTemplate)` — fetch parquet over an
  underlying byte-level `Storage`, row-group-prune by `binCol` / `filters`,
  decode rows.
- `d1Backend(db, tableTemplate)` (in `pyrmts-cfw`) — Cloudflare D1 / SQLite.
- `r2Storage(bucket)` (in `pyrmts-cfw`) — R2 byte-store implementing the
  byte-level `Storage` interface, wraps with `parquetBackend`.
- `memStorage()` — in-memory; dev / tests.

### Monoids

- `getMonoid(name): Monoid` — `'sum' | 'count' | 'histogram' | 'topk' | 'botk' | 'hll' | 'tdigest'`.
- `stateColumns(monoid, name): string[]` — per-monoid state column names
  (e.g. `sum` → `[name_n, name_sum, name_sumSq]`).

### YAML

- `parsePyramidYaml(yaml): PyramidConfig` — parse a YAML doc into a config.
- `pyramidFromConfig(config, storage): Pyramid` — build a `Pyramid` from a
  parsed config + supplied storage. YAML configs are one constructor; apps
  with runtime-dynamic pyramids build `Pyramid` directly.

### `ShardIndex` (watermark grid backend)

- `ShardIndex` — `{ getWatermarks(name) → Map<key, Date>, recordShard(input) }`.
  Key encoding: uniformly `${tier}@${shardDur}` for every entry on the
  per-tier shard ladder.
- `CachedShardIndex` — TTL wrapper around any `ShardIndex` (default 60s);
  dedupes concurrent in-flight reads; `recordShard` invalidates the pyramid.
- `ManifestShardIndex` — JSON-blob impl over `Storage` (R2, memStorage, fs).
  Single-writer per pyramid; defensive parser (missing / bad blob → empty Map).
- `D1ShardIndex` (in `pyrmts-cfw`) — Cloudflare D1 impl; atomic upsert per
  shard; static `schemaSql()` for wrangler migrations. Recommended for
  cascading-cron workloads.
- `assertShardIndexConformance(factory)` — shared vitest suite that pins both
  impls to identical observable semantics. Import from `pyrmts/test-utils`.
- `encodeWatermarkKey(tier, shardDur)` / `decodeWatermarkKey(key)` — codec
  helpers, mirror the `Map<key, Date>` shape.

### FE hook

- `usePyramid({ pyramidUrl, filter, range, binBudget, … }): { records, isLoading, error, plan }` — React hook around `fetchPyramidQuery`.
- `fetchPyramidQuery(input): Promise<PyramidQueryResult>` — plain async; no React.
- `buildQueryUrl(input): string` — URL builder shared by the hook and direct callers.

## Consumers

- **[`ctbk`](https://github.com/ryan-williams/ctbk)** — bike-share rides +
  station availability. Time + space (via `pyrmts-geo`); S2 multi-resolution
  station-set covers; targetBin for FE-driven `/5min` queries.
- **[`awair`](https://github.com/runsascoded/awair)** — environmental sensor
  data (temp / CO2 / PM). Time-only.

## See also

- [`pyrmts-geo`](../pyrmts-geo) — spatial extension (H3 + S2 backends, joint time × space planner).
- [`pyrmts-cfw`](../pyrmts-cfw) — Cloudflare Worker serving helpers + D1 / R2 storage.
- [`SPEC.md`](../../../SPEC.md) — design rationale + decision history.
- [`specs/done/`](../../../specs/done/) — architectural specs (per-feature). Notable:
  - `pluggable-spatial-backend.md` — `SpatialIndex` interface + S2 / H3 / H13 / T4 analysis
  - `multi-tier-bin-packing.md` — `targetBin` + ragged decomposition DP
  - `partial-shards.md` — multi-cadence sub-shards, `ShardIndex`, planner grid walk
  - `server-side-smoothing.md` — planner-driven rolling-window smoothing
  - `plan-geo-query-precomputed-cover.md` — `outputCells` short-circuit to skip `pickResolution`

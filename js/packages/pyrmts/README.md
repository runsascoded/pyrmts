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
    { name: 'raw', bin: '1min', shard: '1mo' },   // finest first
    { name: 'h1',  bin: '1h',   shard: '1mo' },
    { name: 'd1',  bin: '1d',   shard: '1y'  },
    { name: 'mo1', bin: '1mo',  shard: '1y'  },
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
- `Tier` — `{ name, bin, shard }`.
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
    `input.filter`, `input.smoothing`, `input.smoothMode`.
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
  - `server-side-smoothing.md` — planner-driven rolling-window smoothing
  - `plan-geo-query-precomputed-cover.md` — `outputCells` short-circuit to skip `pickResolution`

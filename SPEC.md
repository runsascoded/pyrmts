# `pyrmts` — multi-scale timeseries pyramids

> **PYR**amid + **M**ulti-scale + **T**ime**S**eries

A polyglot library for building, serving, and consuming pre-aggregated time-series pyramids. Pre-compute (shard × bin)-tier shards once; serve any range × bin-budget query in O(log) bins, with monoid-stitched aggregates. Python builders, TypeScript serving (CFW + browser), shared YAML config.

This document is the first artifact. Implementation deferred until the design holds up against all four target consumers.

---

## Motivation

Each of the projects below needs the same thing: bin/aggregate raw time-series data at multiple temporal scales, serve from edge with the minimum bytes/rows needed for a given viewport. Today each project rebuilds the machinery — `ctbk` is furthest along, the others either bin client-side at request time (`awair`) or haven't started (`tomat`, `crashes`).

The shared substrate:
- Tiered storage shards keyed by `(tier, period[, dim…])`
- Monoid aggregation primitives that compose at every scale
- A query planner that picks the coarsest tier such that `returned_bins ≤ bin_budget`
- A frontend hook that turns `(range, container_px_width)` into rendered data

## Consumers

| Project | Schema (dims × metrics) | Geo? | Status |
|---|---|---|---|
| **awair** | `(ts, device_id) × (temp, co2, pm10, pm25, humid, voc)` | no | binning client-side at request time |
| **tomat** | `(step, run_id) × (loss, lr, …)` (WB-style training viz) | no | not started |
| **ctbk** | `(dt, station_id, region, gender, …) × (count, duration_s, duration_s_sq)` and `(dt, station, metric, state) × minutes` | yes | builders (`avail_agg.py`, `trips_agg.py`) + CFW (`gbfs/api/`) **live in production** |
| **crashes** | `(ts, lat, lon, …) × (…)` | yes | not started |

`ctbk` is the reference implementation; `pyrmts` will absorb its patterns. The other three drive abstraction pressure.

## Repo layout

```
pyrmts/
├── SPEC.md                     # this doc
├── README.md
├── python/
│   ├── pyproject.toml          # uv workspace root
│   ├── pyrmts/                 # PyPI: pyrmts          (build, CLI, core types)
│   └── pyrmts_geo/             # PyPI: pyrmts-geo      (spatial extension)
└── js/
    ├── package.json            # pnpm workspace root
    ├── pnpm-workspace.yaml
    └── packages/
        ├── pyrmts/             # npm: pyrmts           (planner, types, FE hook)
        ├── pyrmts-cfw/         # npm: pyrmts-cfw       (CFW serving helpers)
        └── pyrmts-geo/         # npm: pyrmts-geo       (spatial extension)
```

(Scoped vs. unscoped npm names is [open](#open-questions); `@pyrmts/core`, `@pyrmts/geo`, etc. is the alternative.)

## Core concepts

- **Pyramid** — the full (tier × dim × metric × storage) configuration for one dataset. Constructed from YAML or in code.
- **Tier** — one level of the pyramid. Has a **bin** (the granularity within a row) and a **shard** (the period covered by one file). Cartesian product of bins × shards gives the tier set; conventional naming below.
- **Monoid** — composable aggregation primitive (sum, count, top-k, hll, tdigest, …). Every metric declares one. Tier rebucketing = re-applying the monoid.
- **Planner** — given `(range, bin_budget)`, picks the coarsest tier such that returned bins ≤ budget, then computes the list of shard keys to fetch.
- **Stitcher** — merges shard reads into a single result, re-applying the monoid where range edges fall mid-bin in coarser tiers (fetch one finer tier at the edges).

## Data model

Pseudocode (real Python/TS shapes will follow the same shape):

```ts
type Pyramid = {
  storage: Storage           // s3 | r2 | fs | duckdb
  key_template: string       // e.g. '{dataset}/{tier}/{period}.parquet'
  dims: Dim[]                // sort cols, the "primary key" of a row
  metrics: Metric[]          // aggregated cols
  tiers: Tier[]              // sorted coarsest → finest (or vice versa)
}

type Tier = {
  name: string               // 'raw' | 'h1' | 'd1' | 'mo1' | …
  bin:   Duration            // '1min' | '1h' | '1d' | '1mo'
  shard: Duration            // '1mo' | '1y' | 'all'
}

type Dim = {
  name: string
  type: 'int' | 'string' | 'h3' | 'geohash' | …
}

type Metric = {
  name: string
  monoid: 'sum' | 'count' | 'topk' | 'histogram' | 'hll' | 'tdigest' | …
  // monoid-specific config (k for topk, precision for hll, …)
}
```

## YAML schema

The YAML is one constructor; the SDK's `Pyramid` type is the source of truth. Apps with runtime-dynamic pyramids skip YAML and build `Pyramid` directly.

### `awair` example

```yaml
storage:
  type: s3
  bucket: 380nwk
  key: 'awair-{device_id}/{tier}/{period}.parquet'

dims:
  - { name: device_id, type: int }

metrics:
  - { name: temp,  monoid: sum }   # → (n, sum, sum_sq) under the hood; gives mean ± std at any tier
  - { name: co2,   monoid: sum }
  - { name: pm10,  monoid: sum }
  - { name: pm25,  monoid: sum }
  - { name: humid, monoid: sum }
  - { name: voc,   monoid: sum }

tiers:
  - { name: raw, bin: 1min, shard: 1mo }
  - { name: h1,  bin: 1h,   shard: 1mo }
  - { name: d1,  bin: 1d,   shard: 1y  }
  - { name: mo1, bin: 1mo,  shard: 1y  }
```

### `ctbk` example (uses `pyrmts-geo`)

```yaml
storage:
  type: s3
  bucket: ctbk
  key: 'trips/{tier}/{period}.parquet'

dims:
  - { name: station_id, type: string }
  - { name: region,     type: string }
  - { name: gender,     type: string }
  - { name: rideable,   type: string }

metrics:
  - { name: count,         monoid: count }
  - { name: duration_s,    monoid: sum }
  - { name: duration_s_sq, monoid: sum }

tiers:
  - { name: raw, bin: 1min, shard: 1h }   # per-hour bundle of per-minute trips
  - { name: n1,  bin: 1min, shard: 1mo }
  - { name: h1,  bin: 1h,   shard: 1y  }
  - { name: d1,  bin: 1d,   shard: all }
  - { name: mo1, bin: 1mo,  shard: all }

# Optional spatial dim (consumed by pyrmts-geo)
geo:
  index: h3
  resolutions: [5, 7, 9]
  lat_col: start_lat
  lon_col: start_lon
```

## CLI

```bash
# Build a tier from raw (or coarsen from a finer tier)
pyrmts build --config pyramid.yml --tier h1 --period 2026-05

# Build all tiers from a source
pyrmts build --config pyramid.yml --from-raw raw/*.parquet

# Inspect a pyramid: tier layout, expected key counts at a sample query
pyrmts inspect pyramid.yml --range 1y --bin-budget 1024

# Serve locally (for dev) — same code path as the CFW deployment
pyrmts serve --config pyramid.yml --port 8787
```

## SDK

### Python (build side)

```python
from pyrmts import Pyramid

p = Pyramid.from_yaml('pyramid.yml')

# Aggregate raw rows into the configured tier shards
p.build_from_raw(raw_df, tier='h1', period='2026-05')

# Coarsen one tier from another
p.coarsen(from_tier='h1', to_tier='d1', period='2026')
```

### TypeScript (serve side, CFW)

```ts
import { Pyramid, planQuery, stitch } from 'pyrmts'
import { fetchTierShard } from 'pyrmts-cfw'

const p = await Pyramid.fromYamlUrl(env.PYRAMID_URL)
const plan = planQuery(p, { range, binBudget })   // → { tier, keys[] }
const shards = await Promise.all(plan.keys.map(k => fetchTierShard(p.storage, k)))
const result = stitch(plan, shards)               // monoid-aware merge + edge refinement
```

### TypeScript (FE hook)

```ts
import { usePyramid } from 'pyrmts'

const { records, isLoading } = usePyramid({
  pyramidUrl: '/pyramid.yml',
  filter: { device_id: 17617 },
  range,
  binBudget: chartContainerWidth,
})
```

## Monoid catalog

| Monoid | Underlying state | Use cases |
|---|---|---|
| `sum` | `(n, sum, sum_sq)` | mean ± std at any tier (awair sensors, ctbk trip durations) |
| `count` | `n` | row counts (ctbk trip counts) |
| `histogram` | `dim_value → count` | ctbk availability (state × minutes) |
| `topk` | sorted `[value, key][:k]` | top-K things over arbitrary windows |
| `botk` | sorted `[value, key][:k]` reversed | min-K |
| `hll` | HyperLogLog sketch | approximate distinct counts |
| `tdigest` | t-digest | approximate quantiles, distribution shape |

Initial release ships `sum`, `count`, `histogram`. `topk`/`hll`/`tdigest` follow as needed.

## Geo extension (`pyrmts-geo`)

Adds a spatial dim type (h3 / geohash / s2), spatial-tier picker (resolution levels), and joint time-×-space planning. Optional; `pyrmts` itself ships no spatial deps.

Time-only consumers (`awair`, `tomat`) skip this package entirely.

## Storage backends

- **s3** — HEAD + Range Requests, parquet shards. Primary for awair, ctbk.
- **r2** — same shape via s3-compat.
- **fs** — local files; dev/tests.
- **duckdb** — single-file store; small/embedded cases.

All backends share one `Storage` interface — `head(key)`, `get_range(key, start, end)`, `put(key, bytes)`, `list(prefix)`.

## Open questions

- **Calendar-aligned vs fixed-width tiers.** ctbk uses calendar (1mo, 1y). Fixed-width (e.g. 30d, 365d) is simpler but breaks DST/month-length semantics. Lean calendar.
- **Row-group sizing.** ctbk computes RG size dynamically based on n_stations. Library should expose this as a knob with a sensible default.
- **In-progress-tier stitching.** The current bin/shard is incomplete. Two paths: (a) re-aggregate the in-progress tier on every serve from a finer tier; (b) cache it with a short TTL. ctbk's `gbfs/api/` does (a) for some endpoints.
- **Sort order.** ctbk sorts `(station_id, dt, metric, state)` for RG predicate pushdown. Library convention should be `(dims…, bin)` but apps may override.
- **Sketch ownership.** `hll`/`tdigest`/`topk` add deps + complexity. Likely deferred until a consumer needs them.
- **Scoped vs. unscoped npm.** `pyrmts` (unscoped) is symmetric with PyPI but `@pyrmts/core` + `@pyrmts/geo` + `@pyrmts/cfw` reads cleaner as a family. Decide at first publish.
- **Schema evolution.** What happens when a metric or dim is added/removed from an existing pyramid? Versioning in YAML? Migration command? Likely punt to "rebuild the affected tiers."

## Cross-references

- `~/c/hccs/ctbk/ctbk/avail_agg.py`, `trips_agg.py` — Python build references
- `~/c/hccs/ctbk/gbfs/api/src/planQuery.ts` + tests — planner reference
- `~/c/hccs/ctbk/gbfs/api/src/index.ts` — CFW serving reference
- `~/c/hccs/ctbk/specs/multiscale-timeseries-v2.md` — ctbk's design doc
- `~/c/hccs/ctbk/specs/multi-scale-ts-library.md` — ctbk's earlier extraction sketch
- `~/c/awair/www/src/services/dataSources/hyparquetSource.ts` — current client-side binning (what `pyrmts` replaces)
- `~/c/awair/cfw/monitor/` — awair's first CFW; serving worker will live alongside as `cfw/serve/`

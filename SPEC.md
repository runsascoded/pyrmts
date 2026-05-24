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
| **tomat** | `(run_id) × (loss, lr, nmae, nemd, mfu, …)` on **step axis** (not time) | no | not started; reads from W&B on the fly today |
| **ctbk** | `(dt, station_id, region, gender, …) × (count, duration_s, duration_s_sq)` and `(dt, station, metric, state) × minutes` | yes | builders (`avail_agg.py`, `trips_agg.py`) + CFW (`gbfs/api/`) **live in production** |
| **crashes** | `(ts, lat, lon, …) × (…)` — **schema TBD, verify with $c/crashes** | yes | not started |

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
- **Planner** — given `(range, bin_budget)`, picks the coarsest tier such that returned bins ≤ budget, then computes the list of shard keys to fetch. Respects per-tier **watermarks**: shards past the watermark aren't requested.
- **Stitcher** — merges shard reads into a single result. Two responsibilities:
  - **Edge refinement**: re-apply the monoid where range edges fall mid-bin in coarser tiers (fetch one finer tier at the edges).
  - **In-progress coarsening**: re-aggregate the in-progress coarse-tier bin (e.g. this month's `d1` shard, before the builder has emitted it) from the next finer tier on the fly.
- **Watermark** — per-tier `latest_complete_bin(tier) → instant`. Defines where the pyramid's authoritative coverage ends. Anything past the raw-tier watermark is *live tail* — the consumer's CFW serves it from a hot path (D1 / KV / live R2 prefix), not from pyrmts shards. ctbk's per-station 1-min unwieldiness lives entirely below this line; pyrmts only sees "raw shards exist up through T."

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
  name: string               // 'raw' | 'h1' | 'd1' | 'mo1' | 's100' | …
  bin:   Bin                 // span on the pyramid's axis
  shard: Shard               // span (or boundary) on the pyramid's axis
}

// Initial supported axes; pluggable.
type Bin   = Duration | StepCount                   // '1min' | '1h' | '100steps' | …
type Shard = Duration | RunBoundary | 'all'         // '1mo' | '1y' | '1run' | 'all'

// Bin/Shard variants on a single pyramid must come from the same axis —
// don't mix time-axis bins with step-axis shards.

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

Each pyramid commits to one **axis** — wall-clock time (awair, ctbk, crashes) or training-step (tomat). Bin/shard values on a single pyramid must come from the same axis.

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

### `tomat` example (step axis)

Bins and shards are training steps rather than wall-clock durations. All other concepts unchanged.

```yaml
storage:
  type: r2
  bucket: tomat-runs
  key: 'runs/{run_id}/{tier}.parquet'

dims:
  - { name: run_id, type: string }

metrics:
  - { name: train_loss, monoid: sum }
  - { name: val_loss,   monoid: sum }
  - { name: val_nmae,   monoid: sum }
  - { name: lr,         monoid: sum }
  - { name: mfu,        monoid: sum }

tiers:
  - { name: raw,  bin: 1step,    shard: 1run }
  - { name: s100, bin: 100steps, shard: 1run }
  - { name: s1k,  bin: 1ksteps,  shard: 1run }
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

The CLI is a thin wrapper over public library functions — no CLI-only code paths. Each config file describes one pyramid; consumers with multiple pyramids (e.g. ctbk's `avail` + `trips`) have multiple configs, invoked separately.

Consumers with project-specific ingest (e.g. ctbk's per-station 1-min consolidation, where raw bins can't be written naively) wrap the library directly in their own CLI subcmds (`ctbk avail-agg-h1 …`), calling `pyrmts.build_tier(…)` for the standard flow once their bespoke step is done.

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

## Sequencing

Greenfield lib, not extraction-from-ctbk: ctbk's working code (`gbfs/api/`, `avail_agg.py`, `trips_agg.py`) is the *reference* but the lib starts from a blank page so the abstractions aren't shaped by any one consumer's accidents. Build the read side first; package extraction follows ad-hoc builders, not the other way round.

**Consumer milestones** (in order):

1. **awair v0.1** — first consumer; proves the read-side API. Ad-hoc Python builder, `pyrmts` CFW + FE hook serve the chart. Goal: parity-or-better latency vs. today's `hyparquetSource.ts`.
2. **ctbk trips** — second ts-only consumer; ports `trips_agg.py` + `gbfs/api/` trips endpoints to pyrmts. No geo dependency (trips dims are all strings). Validates the "N=2 ts-only" abstraction.
3. **tomat** — third ts-only consumer; exercises the step axis (`Bin = StepCount`, `Shard = RunBoundary`). Replaces today's on-the-fly W&B fetches with stored tier shards (a generally interesting "ML-training-run storage + viz" use case in its own right).
4. **crashes + `pyrmts-geo`** — first geo consumer; drives the spatial extension.
5. **ctbk avail (phase 2)** — `avail_agg` + spatial overlays land on top of `pyrmts-geo`, completing ctbk's migration.

**Library milestones** (interleaved with consumers):

- **Read side** (TS): `Pyramid` type, `planQuery`, `stitch`, `pyrmts-cfw`, FE hook — built for #1.
- **Build side** (Python): per-consumer ad-hoc builders for #1–#3; library extraction once awair + ctbk both have working builders to compare against (the rule-of-three threshold).
- **`pyrmts-geo`**: built for #4.

**Publishing**: dist-SHA branches (`npm-dist` pattern) until the API has been beaten on by ≥2 consumers. Promote to npm/PyPI after #2 or #3.

## Decision history

Captured here so future sessions don't re-litigate:

- **Lib-first over copy-first**: with 4 imminent consumers (ctbk live, awair next, tomat + crashes following), the rule-of-three threshold is met. Copy-first risks the extraction never landing.
- **Greenfield over extract-from-ctbk**: extractions usually require greenfield-shaped work anyway, while also constraining the design to not break the source. Cleaner to design fresh with ctbk as reference.
- **Polyglot monorepo over split repos**: YAML schema + data model must evolve atomically between Python (build) and TS (serve). uv + pnpm workspaces side-by-side, polars/ruff/uv-style.
- **Geo as separate package, not core**: H3/S2 are non-trivial deps; time-only consumers (awair, tomat) shouldn't carry them. `pyrmts-geo` depends on `pyrmts`.
- **Name**: `pyrmts` chosen over `mts` (npm-taken), `pyramts` (more pronounceable but `pyrmts` is shorter and the tagline carries explanation), `pyrami.ts` (cute but biases against Python sibling).
- **Generalized bin/shard axis**: `bin: Duration` → `bin: Bin = Duration | StepCount`; `shard` similarly. tomat needs step-based bins (`100steps`, `1run`); baking time in at the type level forces a retrofit later. Each pyramid commits to one axis.
- **Watermark over end-to-end ingest**: pyrmts owns coarser-tier in-progress stitching (re-aggregate from finer tier when the shard isn't built yet) but *not* raw-tier live tail. Each consumer supplies a raw-tier watermark; everything past it is the consumer's hot-path problem (their CFW, D1, KV — pyrmts doesn't see it). Keeps ctbk's per-station-per-minute consolidation out of the lib.
- **CLI = thin wrapper over lib**: simple consumers (awair) drive `pyrmts build/serve` directly; complex consumers (ctbk's per-station 1-min consolidation) wrap `pyrmts.build_tier(…)` from their own CLI. Configs are 1:1 with pyramids — ctbk has `avail.yml` + `trips.yml`, not one multi-pyramid YAML.
- **ctbk migration split**: trips pyramid (time-only, all-string dims) ports in the "N=2 ts-only" phase; avail + spatial overlays are ctbk phase 2 alongside `pyrmts-geo`. Lets ctbk's first port proceed without blocking on the geo package.

## Open questions

- **Calendar-aligned vs fixed-width tiers.** ctbk uses calendar (1mo, 1y). Fixed-width (e.g. 30d, 365d) is simpler but breaks DST/month-length semantics. Lean calendar.
- **Row-group sizing.** ctbk computes RG size dynamically based on n_stations. Library should expose this as a knob with a sensible default.
- **In-progress-tier caching.** Resolved in principle (re-aggregate from the next finer tier; see Stitcher above), but whether to add a CFW-side TTL cache over those re-aggregations is a per-consumer call, not a lib decision. ctbk's `gbfs/api/` re-aggregates on every serve today.
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

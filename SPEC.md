# `pyrmts` — design rationale

> **PYR**amid + **M**ulti-scale + **T**ime**S**eries

A polyglot library for building, serving, and consuming pre-aggregated
time-series pyramids. Pre-compute (shard × bin)-tier shards once; serve any
range × bin-budget query in O(log) bins, with monoid-stitched aggregates.

This doc holds the *why* — motivation, the data-model shape, key decisions.
The *how* (current APIs, types, storage backends, monoid catalog) lives in
package READMEs, which travel with the code as it changes:

- [`js/packages/pyrmts/README.md`](./js/packages/pyrmts/README.md) — core (planner, stitcher, types, FE hook)
- [`js/packages/pyrmts-cfw/README.md`](./js/packages/pyrmts-cfw/README.md) — CFW serving helpers + D1 / R2 backends
- [`js/packages/pyrmts-geo/README.md`](./js/packages/pyrmts-geo/README.md) — spatial extension (H3 + S2)
- [`specs/done/`](./specs/done/) — per-feature architectural specs

---

## Motivation

Each of the target consumers needs the same shared substrate: bin/aggregate
raw time-series data at multiple temporal scales, serve from edge with the
minimum bytes/rows needed for a given viewport. Today each project rebuilds
it.

The shared substrate:
- Tiered storage shards keyed by `(tier, period[, dim…])`
- Monoid aggregation primitives that compose at every scale
- A query planner that picks the coarsest tier such that `returned_bins ≤ bin_budget` (or, with `targetBin`, packs sub-bin atoms across multiple finer tiers)
- A frontend hook that turns `(range, container_px_width)` into rendered data

## Core concepts

- **Pyramid** — the full (tier × dim × metric × storage) configuration for one dataset. Constructed from YAML or in code.
- **Tier** — one level of the pyramid. Has a **bin** (the granularity within a row) and a **shard** (the period covered by one file). Cartesian product of bins × shards gives the tier set; conventional naming below.
- **Monoid** — composable aggregation primitive (sum, count, top-k, hll, tdigest, …). Every metric declares one. Tier rebucketing = re-applying the monoid.
- **Planner** — given `(range, bin_budget)`, picks the coarsest tier such that returned bins ≤ budget, then computes the list of shard keys to fetch. Respects per-tier **watermarks**: shards past the watermark aren't requested. Given `targetBin` instead, packs each output bin from a minimum-item set of finer-tier atoms (DP).
- **Stitcher** — merges shard reads into a single result. Two responsibilities:
  - **Edge refinement**: re-apply the monoid where range edges fall mid-bin in coarser tiers (fetch one finer tier at the edges).
  - **In-progress coarsening**: re-aggregate the in-progress coarse-tier bin (e.g. this month's `d1` shard, before the builder has emitted it) from the next finer tier on the fly.
- **Watermark** — per-tier `latest_complete_bin(tier) → instant`. Defines where the pyramid's authoritative coverage ends. Anything past the raw-tier watermark is *live tail* — the consumer's CFW serves it from a hot path (D1 / KV / live R2 prefix), not from pyrmts shards.

## Data model (sketch)

The concrete TypeScript types live in [`js/packages/pyrmts/src/types.ts`](./js/packages/pyrmts/src/types.ts) and the JSDoc in [`planner.ts`](./js/packages/pyrmts/src/planner.ts); this is the shape, not the spec:

```ts
type Pyramid = {
  storage: StorageBackend                 // row-level: parquetBackend | d1Backend | …
  keyTemplate: string                     // e.g. 'awair-{device_id}/{tier}/{period}.parquet'
  axis: 'time' | 'step'
  binCol: string                          // 'ts' | 'dt' | 'bin'
  dims: Dim[]                             // sort cols
  metrics: Metric[]                       // aggregated cols
  tiers: Tier[]                           // finest first
  geo?: { cellCol, resolutions, index }   // optional, consumed by pyrmts-geo
}

type Tier = { name: string; bin: Bin; shard: Shard }
type Bin   = `${number}${'min' | 'h' | 'd' | 'mo' | 'y'}` | `${number}${'step' | 'steps' | 'ksteps' | 'msteps'}`
type Shard = Bin | '1run' | 'all'
```

Bin/shard variants on a single pyramid must come from the same axis — don't mix time-axis bins with step-axis shards. Each pyramid commits to one axis.

## Decision history

Captured here so future sessions don't re-litigate:

- **Lib-first over copy-first.** With 4 imminent consumers (ctbk live, awair next, tomat + crashes following), the rule-of-three threshold is met. Copy-first risks the extraction never landing.
- **Greenfield over extract-from-ctbk.** Extractions usually require greenfield-shaped work anyway, while also constraining the design to not break the source. Cleaner to design fresh with ctbk as reference.
- **Polyglot monorepo over split repos.** YAML schema + data model must evolve atomically between Python (build) and TS (serve). uv + pnpm workspaces side-by-side.
- **Geo as separate package.** H3/S2 are non-trivial deps; time-only consumers (awair, tomat) shouldn't carry them. `pyrmts-geo` depends on `pyrmts`.
- **S2 over H3 for new geo pyramids.** S2's exact lineage (`cellToParent(cellAt(L,r), r-1) === cellAt(L,r-1)` for every L) and clean 4-way quadtree make multi-resolution covers exact and `minimalCover` optimal. H3 stays supported for fixed-level / legacy use. See [`specs/done/pluggable-spatial-backend.md`](./specs/done/pluggable-spatial-backend.md) for the H3 → H13 → T4 → S2 pivot rationale.
- **Name.** `pyrmts` chosen over `mts` (npm-taken), `pyramts` (more pronounceable but `pyrmts` is shorter and the tagline carries explanation), `pyrami.ts` (cute but biases against Python sibling).
- **Generalized bin/shard axis.** `bin: Duration` → `bin: Bin = Duration | StepCount`; `shard` similarly. tomat needs step-based bins (`100steps`, `1run`); baking time in at the type level forces a retrofit later. Each pyramid commits to one axis.
- **Watermark over end-to-end ingest.** pyrmts owns coarser-tier in-progress stitching (re-aggregate from finer tier when the shard isn't built yet) but *not* raw-tier live tail. Each consumer supplies a raw-tier watermark; everything past it is the consumer's hot-path problem (their CFW, D1, KV — pyrmts doesn't see it). Keeps ctbk's per-station-per-minute consolidation out of the lib.
- **CLI = thin wrapper over lib.** Simple consumers (awair) drive `pyrmts build/serve` directly; complex consumers (ctbk's per-station 1-min consolidation) wrap `pyrmts.build_tier(…)` from their own CLI. Configs are 1:1 with pyramids — ctbk has `avail.yml` + `trips.yml`, not one multi-pyramid YAML.
- **Row-level `StorageBackend` abstraction.** Originally one byte-level `Storage` interface (HEAD + Range Requests over parquet). Adding D1 forced a row-level abstraction (`fetchSegment(segment) → Row[]`) that subsumes parquet-on-R2 *and* SQL-on-D1; consumers swap backends without changing the planner / stitcher. See [`specs/done/cascade-tiers-and-geo-materializer.md`](./specs/done/cascade-tiers-and-geo-materializer.md).
- **`targetBin` for arbitrary output widths.** Caller-specified bin width packed via per-output-bin DP across finer tiers. Unlocks `/5min` queries on a `{1min, 1h}` pyramid without forcing a 5× read cost or pyramid changes. See [`specs/done/multi-tier-bin-packing.md`](./specs/done/multi-tier-bin-packing.md).
- **Server-side smoothing in the planner.** Rolling-window monoid combine runs in the stitcher; the planner extends segments outward by the smoothing buffer so the rolling pass has full context at the visible edges. See [`specs/done/server-side-smoothing.md`](./specs/done/server-side-smoothing.md).

## Open questions

- **Row-group sizing.** ctbk computes RG size dynamically based on n_stations. Library should expose this as a knob with a sensible default.
- **In-progress-tier caching.** Resolved in principle (re-aggregate from the next finer tier; see Stitcher above), but whether to add a CFW-side TTL cache over those re-aggregations is a per-consumer call, not a lib decision.
- **Sort order.** ctbk sorts `(station_id, dt, metric, state)` for RG predicate pushdown. Library convention should be `(dims…, bin)` but apps may override.
- **Sketch ownership.** `hll` / `tdigest` / `topk` add deps + complexity. Likely deferred until a consumer needs them.
- **Schema evolution.** What happens when a metric or dim is added/removed from an existing pyramid? Versioning in YAML? Migration command? Likely punt to "rebuild the affected tiers."

## Cross-references

- `~/c/hccs/ctbk/ctbk/avail_agg.py`, `trips_agg.py` — Python build references
- `~/c/awair/www/src/services/dataSources/hyparquetSource.ts` — pre-pyrmts client-side binning (what `pyrmts` replaces for awair)
- `~/c/awair/cfw/monitor/` — awair's first CFW; serving worker will live alongside as `cfw/serve/`

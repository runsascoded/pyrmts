# `pyrmts-geo`

> Spatial extension for [`pyrmts`](../pyrmts) — S2 cell addressing over pyrmts pyramids.

`pyrmts-geo` adds spatial-dim addressing to pyrmts pyramids, plus a query
planner that picks an appropriate resolution at read time and filters rows
by either a bbox or a mixed-resolution cell cover.

## Backends

`s2Index` is the backend. The `SpatialIndex` interface stays pluggable — the
planner never calls a backend directly — but S2 is the only implementation
shipped.

| Backend | Branching | Mixed-resolution | Status |
|---|---|---|---|
| **`s2Index`** | 4 (quadtree) | exact | shipped |
| **`h3Index`** | 7 (hex) | approximate | **test-only**, not exported |

### `s2Index` — the backend

S2's cube-face Hilbert-curve quadtree gives:

- **Exact lineage**: `cellToParent(latLngToCell(L, r), r-1) === latLngToCell(L, r-1)` for every L. No edge-case mismatches at level transitions.
- **Clean 4-way subdivision**: parents partition into 4 children perfectly. `minimalCover` reduces to a tree DP that's optimal for the |ops| objective.
- **Library**: [`s2js`](https://github.com/missinglink/s2js) (pure-TS port of golang/geo, Cloudflare-Workers compatible, no Node-only deps).

### `h3Index` — test-only, not exported

Uber's H3 hexagons are fine for single-resolution work, but multi-resolution gets messy because of **Boundary Triangles**: at every level transition, the 7 children of a parent cover the parent's area + 6 small slivers extending outward and miss 6 corresponding slivers from sibling parents. For ~7% of points at each (r → r-1) transition, `cellToParent(latLngToCell(L, r), r-1) ≠ latLngToCell(L, r-1)`. Exact multi-resolution aggregation is therefore unachievable on H3 — which is why the last H3-keyed pyramids were retired downstream.

`h3Index` remains in the source tree as the **second implementation** the conformance suite runs the `SpatialIndex` contract against. That's the only thing keeping the interface honest — with a single backend it would quietly become "whatever S2 does" — and H13/T4 are deferred indefinitely, so there's no other candidate.

It is not exported from the package index, and `h3-js` is a devDependency. Both are deliberate: `h3-js` declares no `sideEffects`, so a single reachable reference pins ~195 KB (minified) into every consumer bundle. An export-surface test guards against re-adding it.

## Quick usage

```ts
import {
  buildQueryUrl,
  fetchPyramidGeoQuery,
  filterCellsByCover,
  isCellInCover,
  minimalCover,
  planGeoQuery,
  s2Index,
  serveGeoQuery,
  type GeoPyramid,
  type SpatialSet,
} from 'pyrmts-geo'
```

### Declaring a pyramid

```ts
const ridesPyramid: GeoPyramid = {
  storage: r2Storage(env.PYRAMID_BUCKET),
  keyTemplate: 'rides-v3/{tier}/{period}.parquet',
  axis: 'time',
  binCol: 'ts',
  dims: [{ name: 'station_id', type: 'string' }],
  metrics: [{ name: 'count', monoid: 'count' }],
  tiers: [
    { name: 'h1',  bin: '1h',  shards: ['1mo'] },
    { name: 'd1',  bin: '1d',  shards: ['1y']  },
    { name: 'mo1', bin: '1mo', shards: ['1y']  },
  ],
  geo: {
    cellCol: 's2_cell',
    resolutions: [14, 12, 10, 8],  // finest → coarsest
    index: s2Index,                 // required — no default backend
  },
}
```

### Bbox query (single-resolution at read time)

```ts
const plan = planGeoQuery(pyramid, {
  range: { from: new Date('2026-05-01'), to: new Date('2026-05-31') },
  binBudget: 256,
  bbox: { minLat: 40.70, maxLat: 40.78, minLng: -74.02, maxLng: -73.96 },
  cellBudget: 1024,
})
// → plan.outputRes, plan.outputCells, plan.segments
```

The planner picks the finest materialized resolution whose `bboxToCells` count fits the budget.

### Pre-computed cover (skip `pickResolution`)

When the caller already has a cell list — e.g., the FE ran `minimalCover` and serialized the cover in the query — pass `outputCells` instead of `bbox` + `cellBudget`. The planner skips `pickResolution` entirely; downstream still uses the cell list for predicate pushdown.

```ts
const plan = planGeoQuery(pyramid, {
  range: { from, to },
  binBudget: 256,
  outputCells: { res: 12, cells: precomputedCells },        // single-level
  // or, for a mixed-level cover (S2 `minimalCover` output):
  // outputCells: { res: -1, cells: [...cover.include, ...cover.exclude] },
})
```

The `bbox + cellBudget` and `outputCells` forms are mutually exclusive; pass exactly one. `pickResolution` (and its `RegionCoverer` allocation tail) only runs in the `bbox` form, so consumers that already have a cover get a real CPU + heap win — relevant for Cloudflare Workers where V8 GC compounds at the next async safepoint (e.g., `await fetchSegment(...)`).

### Station-set query (multi-resolution cover)

```ts
const cover: SpatialSet = minimalCover(
  s2Index,
  stationCellsOfInterest,    // e.g., S2 leaf cells for stations matching a filter
  allSystemStationCells,
)
// → { include: ['89c2594...', ...], exclude: ['89c2594c4...'] }
//   (mixed levels, lineage-disjoint)

// at fetch time:
const rows = await fetchSegmentRows(...)
const filtered = filterCellsByCover(rows, pyramid.geo.cellCol, plan.outputRes, cover, s2Index)
```

`minimalCover`'s DP is backend-agnostic — it runs on any `SpatialIndex` (the conformance suite exercises it on the test-only `h3Index` too, BT caveats and all).

### Membership check

```ts
isCellInCover(s2Index, '89c2594c4', cover)  // true if cover.include covers the cell and no cover.exclude is more specific
```

## API surface

### Types

- `SpatialIndex<C extends string = string>` — the pluggable backend interface (`latLngToCell`, `cellLevel`, `cellToParent`, `bboxToCells`, `cellInSet`, `minimalCover`).
- `BBox` — `{ minLat, maxLat, minLng, maxLng }`.
- `SpatialSet<C>` — `{ include: C[]; exclude: C[] }`.
- `GeoPyramid` — `Pyramid` with optional `geo.index: SpatialIndex`.
- `GeoQueryPlan`, `GeoPlanSegment`, `PlanGeoQueryInput` — query planner types.

### Backends

- `s2Index: SpatialIndex`.
- `getSpatialIndex(pyramid): SpatialIndex` — resolves the pyramid's `geo.index`. Throws if unset; there is no default backend.

### Helpers

- `minimalCover(index, include, system, opts?)` — backend-agnostic DP. Optimal |ops|. `opts.allowSubtraction = false` returns pure-union (no excludes).
- `isCellInCover(index, cell, cover)` — lineage-walk membership check.
- `filterCellsAndRes(rows, cellCol, level, allowedCells, index?)` — single-resolution row filter.
- `filterCellsByCover(rows, cellCol, level, cover, index)` — mixed-resolution row filter.
- `planGeoQuery(pyramid, input)` — joint time × space planner.
- `serveGeoQuery(opts)` — HTTP handler (CFW-compatible).

### S2 range predicates (`s2-range.ts`)

Pure-bigint S2 cell-id math (no `s2js` dep at runtime): the base-level descendants of any S2 cell form one contiguous numeric id range, so a cover becomes a handful of `[lo, hi]` predicates for parquet row-group pruning or SQL `cellid BETWEEN lo AND hi` (works on TEXT token columns too — trailing-zero-stripped tokens preserve numeric order under lex compare). Upstreamed from [nj-crashes](https://github.com/hudcostreets/nj-crashes) `cells-api`, where it drives both consumers in prod.

- `s2RangesForCells(cells, baseLevel)` — cover tokens (mixed levels OK) → merged disjoint `S2CellRange[]`.
- `s2RangeForCell(id, baseLevel)` / `s2RangeForCellToken(token, baseLevel)` — single-cell range, bigint or token flavored.
- `mergeRanges(ranges)` / `intersectRanges(a, b)` — grid-agnostic `{lo, hi}` bigint set ops.
- `s2TokenToId` / `s2IdToToken` / `s2LevelOf` / `s2Parent` / `s2LsbForLevel` / `S2_LEAF_LEVEL` — token ↔ id, level extraction, ancestor walk, marker-bit math.

### Conformance suite

- `assertSpatialIndex(index, opts)` (exported from `spatial-index-conformance.ts` for tests) — every backend must pass this.

## Consumers

- **[`ctbk`](https://github.com/ryan-williams/ctbk)** — bike-share rides + station availability. Uses `s2Index` for multi-resolution station-set covers.
- **[`nj-crashes`](https://github.com/hudcostreets/nj-crashes)** — NJ State Police fatal + NJDOT crash data, with a Hudson County map view. On S2 since 2026-08-23 (own H3→S2 migration; prod serves S2 `l4..l21`, worker speaks only S2). Its cell-id range math (`cells-api/src/s2-range.ts`) is upstreamed here as `s2-range.ts`; adoption of the cover/planner half is specced in nj-crashes `specs/pyrmts-geo-adoption.md`.

## See also

- [`SPEC.md`](../../../SPEC.md) at the repo root — design rationale.
- [`specs/done/pluggable-spatial-backend.md`](../../../specs/done/pluggable-spatial-backend.md) — architectural spec for this package's backends + the H13/T4 candidates that were considered and deferred.

# `pyrmts-geo`

> Spatial extension for [`pyrmts`](../pyrmts) — pluggable backends for H3 and S2.

`pyrmts-geo` adds spatial-dim addressing to pyrmts pyramids, plus a query
planner that picks an appropriate resolution at read time and filters rows
by either a bbox or a mixed-resolution cell cover.

## Backends

| Backend | Branching | Mixed-resolution | Recommended for |
|---|---|---|---|
| **`s2Index`** | 4 (quadtree) | exact | new pyramids; multi-resolution queries; `minimalCover`-driven station filters |
| **`h3Index`** | 7 (hex) | approximate | legacy pyramids (rides-v1, avail-v2 et al.); single-resolution queries |

### `s2Index` — production primary

S2's cube-face Hilbert-curve quadtree gives:

- **Exact lineage**: `cellToParent(latLngToCell(L, r), r-1) === latLngToCell(L, r-1)` for every L. No edge-case mismatches at level transitions.
- **Clean 4-way subdivision**: parents partition into 4 children perfectly. `minimalCover` reduces to a tree DP that's optimal for the |ops| objective.
- **Library**: [`s2js`](https://github.com/missinglink/s2js) (pure-TS port of golang/geo, Cloudflare-Workers compatible, no Node-only deps).

### `h3Index` — fixed-level / legacy

Uber's H3 hexagons are excellent for single-resolution work. Multi-resolution gets messy because of **Boundary Triangles**: at every level transition, the 7 children of a parent cover the parent's area + 6 small slivers extending outward and miss 6 corresponding slivers from sibling parents. For ~7% of points at each (r → r-1) transition, `cellToParent(latLngToCell(L, r), r-1) ≠ latLngToCell(L, r-1)`.

`h3Index.minimalCover` runs the same tree DP as `s2Index` but the lineage walks are BT-affected. The result is self-consistent (a station's cover-membership matches what the DP encoded) but approximate against geographically-defined covers (a bbox cell at level r-1 may BT-exclude some points whose H3 lineage puts them in a sibling).

If your queries are single-resolution (pick a level, filter by bbox cells at that level), `h3Index` is fine. For mixed-resolution station-set filtering, use `s2Index`.

## Quick usage

```ts
import {
  buildQueryUrl,
  fetchPyramidGeoQuery,
  filterCellsByCover,
  h3Index,
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
    { name: 'h1',  bin: '1h',  shard: '1mo' },
    { name: 'd1',  bin: '1d',  shard: '1y'  },
    { name: 'mo1', bin: '1mo', shard: '1y'  },
  ],
  geo: {
    cellCol: 's2_cell',
    resolutions: [14, 12, 10, 8],  // finest → coarsest
    index: s2Index,                 // default is `h3Index`
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

`minimalCover`'s DP is backend-agnostic — works on `h3Index` too (with the BT caveats noted above).

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

- `h3Index: SpatialIndex` (default when `geo.index` is unset).
- `s2Index: SpatialIndex`.
- `getSpatialIndex(pyramid): SpatialIndex` — resolves to the pyramid's index or the H3 default.

### Helpers

- `minimalCover(index, include, system, opts?)` — backend-agnostic DP. Optimal |ops|. `opts.allowSubtraction = false` returns pure-union (no excludes).
- `isCellInCover(index, cell, cover)` — lineage-walk membership check.
- `bboxToCells(bbox, level)` — h3-style bbox → cells (back-compat wrapper for `h3Index.bboxToCells`).
- `filterCellsAndRes(rows, cellCol, level, allowedCells, index?)` — single-resolution row filter.
- `filterCellsByCover(rows, cellCol, level, cover, index)` — mixed-resolution row filter.
- `planGeoQuery(pyramid, input)` — joint time × space planner.
- `serveGeoQuery(opts)` — HTTP handler (CFW-compatible).

### Conformance suite

- `assertSpatialIndex(index, opts)` (exported from `spatial-index-conformance.ts` for tests) — every backend must pass this.

## Consumers

- **[`ctbk`](https://github.com/ryan-williams/ctbk)** — bike-share rides + station availability. Uses `s2Index` for multi-resolution station-set covers.
- **[`crashes`](https://github.com/runsascoded/crashes)** — NYC vehicle-crash data. Uses `h3Index` for fixed-level viz.

## See also

- [`SPEC.md`](../../../SPEC.md) at the repo root — design rationale.
- [`specs/done/pluggable-spatial-backend.md`](../../../specs/done/pluggable-spatial-backend.md) — architectural spec for this package's backends + the H13/T4 candidates that were considered and deferred.

# `planGeoQuery`: accept pre-computed `outputCells`

`packages/pyrmts-geo/src/planner.ts:64` (`planGeoQuery`) currently always
runs `pickResolution(index, bbox, resolutions, cellBudget)` to derive
`outputRes` + `outputCells`. When the caller already has a precomputed
cover (e.g. `minimalCover` output for an I/E station set), this work is
wasted — the result is dropped by `userCells ?? plan.outputCells`-style
checks at the call site.

The waste isn't free. `pickResolution` iterates all of
`pyramid.geo.resolutions` (finest-first), and at every level calls
`index.bboxToCells(bbox, res)`. For S2, that constructs an
`s2.RegionCoverer({ maxCells: 1_000_000, … })` and runs `.covering(rect)`.
The coverer's internal queue + candidate-cell allocation is sized to
explore that fineness even when the bbox×level intersection produces
millions of candidate cells; for NYC bbox at S2 level 15 ≈ 30k cells,
level 14 ≈ 25k, … walking down 6 levels × heavy allocation each =
substantial heap pressure.

The CPU time itself is bounded (`pickResolution` returns in <1ms wall
per `performance.now`), so the cost is invisible to inline timing. V8
defers GC to the next async safepoint — and for `planGeoQuery`'s
callers, that's the immediately-following `await
pyramid.storage.fetchSegment(...)`.

In CFW that's where it shows up. ctbk's `/api/rides-v3` pays a
**3-5 second tax** on every request whose handler chain is
`planGeoQuery(…) → await d1Backend.fetchSegment(…) → await
stmt.all()`. D1's `meta.sql_duration_ms` reports the actual query
finished in 7-15ms; the rest is V8 GC pausing the runtime between the
RPC reply arriving and the `await` resuming. Repros:

```
fetch wall  | meta.sql_duration_ms
------------+---------------------
5944 ms     | 9.4 ms
4699 ms     | 6.0 ms
5234 ms     | 6.1 ms
```

Same SQL/binds/session called from a *different* call frame (one whose
`planGeoQuery` step is skipped or short-circuited) returns in 30-200ms
on the same isolate. Empirically diagnosed by passing `bbox` as an
array literal `[minLat, minLng, maxLat, maxLng]` instead of the typed
`BBox` object — `pickResolution`'s `bbox.minLat` access then yields
`undefined`, `bboxToCells` allocates a coverer but the rect has NaN
coords so `covering()` returns empty, falls through to the cheap
centroid fallback. **That gross workaround is what ctbk currently
ships pending this spec; remove it once this lands.**

## Fix: pre-computed cover option

Extend `PlanGeoQueryInput` (line 36) so callers with their own cover
can hand it in and skip `pickResolution`:

```ts
export interface PlanGeoQueryInput {
  range: { from: Date; to: Date }
  binBudget: number
  // EITHER bbox + cellBudget (planner derives outputCells from bbox)
  // OR outputCells (caller supplies; planner skips pickResolution).
  // Provide exactly one form.
  bbox?: BBox
  cellBudget?: number
  // Pre-computed cover; bypasses pickResolution. `res` is the cover's
  // resolution if single-level (mirrors what pickResolution would
  // return); pass -1 for mixed-resolution covers (S2 minimalCover
  // output with parent+leaf entries) — downstream uses the cell list
  // for predicate pushdown and the value of `res` doesn't matter past
  // that.
  outputCells?: { res: number; cells: readonly string[] }
  // ...existing fields...
}
```

`planGeoQuery` enforces the XOR and either runs `pickResolution` or
trusts the caller:

```ts
export function planGeoQuery(pyramid, input): GeoQueryPlan {
  if (input.outputCells === undefined && input.bbox === undefined) {
    throw new Error('planGeoQuery: pass either `bbox` or `outputCells`')
  }
  if (input.outputCells !== undefined && input.bbox !== undefined) {
    throw new Error('planGeoQuery: pass `bbox` xor `outputCells`, not both')
  }

  // ...existing planQuery delegation...

  const { outputRes, outputCells } = input.outputCells !== undefined
    ? input.outputCells
    : pickResolution(index, input.bbox!, resolutions, input.cellBudget!)

  // ...rest unchanged...
}
```

`GeoQueryPlan`'s shape doesn't change. `segments[].cells` keeps holding
`outputCells` either way (cheap; callers can ignore it).

ctbk's `serveRidesReduced` then becomes:

```ts
const outputCellsArg = userCells !== null
  ? { res: userCoverIsMixed ? -1 : userCoverLevels[0]!, cells: userCells }
  : undefined
const plan = planGeoQuery(pyramid, {
  range: { from, to }, binBudget,
  ...(outputCellsArg ? { outputCells: outputCellsArg } : { bbox, cellBudget }),
})
```

`bbox` becomes optional in the URL contract too, but we keep it required
on the ctbk side because some queries (no-region rollup) still go through
the bbox path. Server picks which `planGeoQuery` call form to use.

## Bench

The existing `pickResolution`-level perf test (if any) wouldn't catch
this — `performance.now()` between `pickResolution` start and finish
shows it as instant. The cost only shows up at the next async
safepoint.

Add a `bench/plan-geo-query-allocation.test.ts` (or extend an existing
e2e in `pyrmts-cfw/src/serve.test.ts` if miniflare-flavored e2e is the
right venue) that:

1. Plans a query that triggers full `pickResolution` walk (NYC bbox,
   `cellBudget = 16`, v3-equivalent resolutions `[15, 14, 13, 12, 11, 10]`).
2. Immediately `await`s a no-op `fetchSegment` (a memStorage-backed
   parquet shard returning ~5k rows, or a stub).
3. Asserts that the wall time of the await stays under a reasonable
   bound (e.g. 200ms, vs the observed 4000-7000ms with the bug).
4. Adds a second variant calling `planGeoQuery` with `outputCells` set —
   confirms the await stays under 50ms.

The CFW path is where the symptom manifested — Node's V8 GC scheduling
may differ. Run the bench under miniflare/`wrangler dev` so heap behavior
matches production. A Node-only bench may not reproduce.

Also expose `pickResolution`'s cell count and `bboxToCells` call count
in the bench output (log only) so future regressions in the algorithm
side are visible.

## Out of scope here, but related

`pickResolution`'s walk order is finest→coarsest, returning the first
resolution that fits `cellBudget`. For region-cover queries this is
wasted: the user-supplied cover is what's used downstream; the only
reason to walk finer levels is to find a better fit than the coarsest
one. A future improvement: walk coarsest→finest, stop as soon as a
level *exceeds* `cellBudget`. For NYC at cellBudget=16 only one
coverer (level 10) would ever be constructed, instead of six. Lower
priority once this spec's API change is in — most ctbk traffic
provides `outputCells` and skips `pickResolution` entirely.

Independently: pyrmts-geo already has the optimal-cover algorithm the
ctbk FE uses for I/E set covers — `minimalCover` in
`spatial-index-cover.ts`, the DP that emits the `+`/`-` ops. That's
unchanged; this spec is just about not paying for redundant cover
computation server-side when the FE already ran `minimalCover`.

## Resolution

Shipped in `pyrmts-geo` with the API shape proposed above, plus one
runtime guard not called out in the original spec:

```ts
export interface PlanGeoQueryInput {
  range: { from: Date; to: Date }
  binBudget: number
  bbox?: BBox
  cellBudget?: number
  outputCells?: { res: number; cells: readonly string[] }
  // ...watermarks / earliestWatermarks / filter / smoothing / smoothMode (unchanged)
}
```

`planGeoQuery` enforces three guards before the planner runs:

1. `bbox` xor `outputCells` — exactly one must be supplied.
2. `bbox` requires `cellBudget` to also be supplied (paired-arg sanity).
3. `pyramid.geo` / `resolutions` checks are unchanged.

When `outputCells` is supplied the planner skips `pickResolution`
entirely, including the wrapping `getSpatialIndex(...).bboxToCells(...)`
call. `GeoQueryPlan.outputRes` is set to the caller-supplied `res`
(including the conventional `-1` sentinel for mixed-resolution covers);
`GeoQueryPlan.outputCells` is a defensive copy of `outputCells.cells` so
the caller's `readonly` input stays read-only.

### Deviations from the proposed spec

- **Added guard (3) above** — `cellBudget` is now a required companion
  to `bbox` (previously implicit because both were required at the type
  level). Saves a confusing downstream `NaN` cellBudget surprise.
- **Defensive copy on `outputCells.cells`** — `planGeoQuery` spreads the
  caller's `readonly string[]` into a fresh `string[]` for
  `GeoQueryPlan.outputCells`. Cheap (cells already in memory) and keeps
  the existing mutable-array shape of `GeoQueryPlan`. Asserted by a
  test that mutates the plan's `outputCells` and confirms the caller's
  input is unchanged.
- **Comment-only updates to `GeoQueryPlan` / `GeoPlanSegment` docstrings**
  — call out that `outputRes === -1` means mixed-resolution + downstream
  must use `filterCellsByCover`, not `filterCellsAndRes`.
- **No CFW perf bench landed in this commit.** The proposed `bench/`
  test would need miniflare to reproduce the CFW V8 GC tail; the
  behavior fix is covered by Node-level unit tests instead
  (`planner.test.ts > planGeoQuery: pre-computed outputCells`). A
  miniflare-flavored regression bench is left as a follow-up if ctbk's
  post-migration timing isn't where we want it.
- **`pickResolution` walk-order tweak (out-of-scope note in spec)** —
  not done in this commit. The spec correctly notes most ctbk traffic
  will provide `outputCells` and skip `pickResolution` entirely, so the
  walk-order win mostly disappears post-migration.

### ctbk migration

`packages/ctbk-cfw/src/serve-rides-reduced.ts` (or wherever
`planGeoQuery` is called for the v3 endpoint) can drop the bbox
workaround mentioned in the spec ("passing `bbox` as an array literal
to trip up `pickResolution`"). New call shape:

```ts
const outputCellsArg = userCells !== null
  ? { res: userCoverIsMixed ? -1 : userCoverLevels[0]!, cells: userCells }
  : undefined
const plan = planGeoQuery(pyramid, {
  range: { from, to }, binBudget,
  ...(outputCellsArg
    ? { outputCells: outputCellsArg }
    : { bbox, cellBudget }),
})
```

Downstream filtering must use `filterCellsByCover` (not
`filterCellsAndRes`) when `outputRes === -1` — `filterCellsAndRes`
matches against a single resolution and will reject every row of a
mixed-resolution cover.

### Tests

`packages/pyrmts-geo/src/planner.test.ts > planGeoQuery: pre-computed
outputCells` covers:

- Honors caller-supplied cells verbatim + reports the supplied `res`.
- Wrapping `SpatialIndex` confirms `bboxToCells` is called 0 times in
  the `outputCells` form (regression guard against re-introducing the
  RegionCoverer allocation).
- `res: -1` for mixed-resolution passthrough.
- Defensive-copy assertion on the returned `outputCells`.
- Three `expect(...).toThrow(...)` cases for the XOR + cellBudget
  guards.

`packages/pyrmts-geo/src/planner.test.ts > planGeoQuery: resolution
selection` (the pre-existing bbox-path tests) still pass unmodified — the
bbox form's behavior is unchanged.

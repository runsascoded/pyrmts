// Joint time-×-space query planner. Layers spatial-index cell selection on
// top of pyrmts core's time-axis planner.
//
// Storage model (shared with the core planner): one shard per (time_tier,
// period). Geo resolutions are not separate shards — every materialized
// resolution lives inside every shard, sorted by the spatial cell column
// (cell IDs encode resolution so the sort naturally clusters rows by
// resolution-then-spatial-locality). Predicate pushdown handles the
// spatial filter at read time.
//
// The planner consumes `Pyramid.geo` via the `SpatialIndex` abstraction
// (`spatial-index.ts`); pyramids must set `geo.index` explicitly.

import {
  planQuery,
  planQueryFromInventory,
  type Duration,
  type PlanLimits,
  type PlanSegment,
  type QueryPlan,
  type RecordedShard,
  type SmoothingSpec,
  type SmoothMode,
  type Tier,
} from 'pyrmts'
import { getSpatialIndex } from './spatial-index.js'
import type { BBox, GeoPyramid, SpatialIndex } from './spatial-index.js'

export type { BBox } from './spatial-index.js'

export interface PlanGeoQueryInput {
  range: { from: Date; to: Date }
  // Max output time bins (same as core planQuery).
  binBudget: number
  // Exactly one of {bbox + cellBudget, outputCells} must be provided.
  //
  // `bbox` + `cellBudget`: planner runs `pickResolution` to pick the finest
  // materialized resolution whose `bboxToCells` count fits `cellBudget`,
  // and uses that cell list. The classic geo-query path.
  //
  // `outputCells`: caller supplies a pre-computed cover (e.g.,
  // `minimalCover` output for an I/E station set) and the planner skips
  // `pickResolution` entirely. `RegionCoverer` allocates aggressively
  // (esp. on S2 at fine levels); for callers that already have a cover
  // this short-circuits the redundant work and the V8 GC tail it triggers
  // at the next async safepoint (see `specs/done/plan-geo-query-precomputed-cover.md`).
  bbox?: BBox
  cellBudget?: number
  // Pre-computed cover; bypasses `pickResolution`. `res` is the cover's
  // resolution if single-level (mirrors what `pickResolution` would
  // return); pass `-1` for mixed-resolution covers (e.g., S2 `minimalCover`
  // output with parent + leaf entries). Downstream `filterCellsAndRes`
  // checks against `outputRes` and only works for single-level covers;
  // mixed-resolution covers must be filtered via `filterCellsByCover`.
  outputCells?: { res: number; cells: readonly string[] }
  watermarks?: Record<string, Date>
  earliestWatermarks?: Record<string, Date>
  // See `pyrmts.PlanQueryInput.earliestPerShard`. Keyed by encoded
  // `(tier, shardDur)` (e.g. `'1m@5min'`). Per-entry gate, no propagation.
  earliestPerShard?: Record<string, Date>
  filter?: Record<string, string | number>
  // Caller-requested output bin width, delegated to the core planner
  // (`specs/calendar-composition-and-query-limits.md` §4). Same semantics
  // as `pyrmts.PlanQueryInput.targetBin`: an exact tier-bin match makes
  // that tier the output tier, anything else decomposes raggedly. Geo
  // metadata (`outputRes`, per-segment cells) is resolved the same way on
  // either path.
  targetBin?: Duration
  // Query cost ceilings, delegated to the core planner. See
  // `pyrmts.PlanLimits`; violations throw `PlanLimitError`.
  limits?: PlanLimits
  // Server-side rolling-window smoothing (see `pyrmts` `PlanQueryInput`).
  smoothing?: import('pyrmts').SmoothingSpec
  smoothMode?: import('pyrmts').SmoothMode
}

export interface GeoPlanSegment extends PlanSegment {
  // Spatial cells (at `outputRes`) for the segment — same list per segment;
  // present here for read-time predicate pushdown convenience. Source is
  // either `pickResolution(bbox)` or a caller-supplied `outputCells` cover.
  cells: string[]
}

export interface GeoQueryPlan extends Omit<QueryPlan, 'segments'> {
  // Spatial resolution chosen by `pickResolution`, or `outputCells.res`
  // when the caller supplied a pre-computed cover. `-1` is conventional
  // for mixed-resolution covers (see `PlanGeoQueryInput.outputCells`).
  outputRes: number
  // Cell list to push down at read time. Single-resolution unless
  // `outputRes === -1`, in which case use `filterCellsByCover` downstream.
  outputCells: string[]
  segments: GeoPlanSegment[]
}

export function planGeoQuery(
  pyramid: GeoPyramid,
  input: PlanGeoQueryInput,
): GeoQueryPlan {
  if (pyramid.geo === undefined) {
    throw new Error('planGeoQuery: pyramid has no `geo` config (use planQuery for time-only)')
  }
  const resolutions = pyramid.geo.resolutions
  if (resolutions.length === 0) {
    throw new Error('planGeoQuery: pyramid.geo.resolutions is empty')
  }
  const haveBBox = input.bbox !== undefined
  const haveCells = input.outputCells !== undefined
  if (!haveBBox && !haveCells) {
    throw new Error('planGeoQuery: pass either `bbox` or `outputCells`')
  }
  if (haveBBox && haveCells) {
    throw new Error('planGeoQuery: pass `bbox` xor `outputCells`, not both')
  }
  if (haveBBox && input.cellBudget === undefined) {
    throw new Error('planGeoQuery: `cellBudget` required when `bbox` is provided')
  }
  const index = getSpatialIndex(pyramid)

  // Delegate the time-axis decisions to the core planner.
  const timePlan = planQuery(pyramid, {
    range: input.range,
    binBudget: input.binBudget,
    ...(input.watermarks !== undefined ? { watermarks: input.watermarks } : {}),
    ...(input.earliestWatermarks !== undefined ? { earliestWatermarks: input.earliestWatermarks } : {}),
    ...(input.earliestPerShard !== undefined ? { earliestPerShard: input.earliestPerShard } : {}),
    ...(input.filter !== undefined ? { filter: input.filter } : {}),
    ...(input.targetBin !== undefined ? { targetBin: input.targetBin } : {}),
    ...(input.limits !== undefined ? { limits: input.limits } : {}),
    ...(input.smoothing !== undefined ? { smoothing: input.smoothing } : {}),
    ...(input.smoothMode !== undefined ? { smoothMode: input.smoothMode } : {}),
  })

  // Pick the finest materialized resolution whose cells-in-bbox fits the
  // cell budget. `resolutions` is finest-first. Skipped when the caller
  // supplied `outputCells` directly.
  const { outputRes, outputCells } = input.outputCells !== undefined
    ? { outputRes: input.outputCells.res, outputCells: [...input.outputCells.cells] }
    : pickResolution(index, input.bbox!, resolutions, input.cellBudget!)

  // Each segment carries the same cell list — every materialized resolution
  // lives in every shard, so the cell predicate is the same regardless of
  // which (finer) time tier the segment reads from.
  const segments: GeoPlanSegment[] = timePlan.segments.map(seg => ({
    from: seg.from,
    to: seg.to,
    shardTier: seg.shardTier,
    shardDur: seg.shardDur,
    keys: seg.keys,
    reaggregate: seg.reaggregate,
    cells: outputCells,
  }))

  return {
    ...(timePlan.outputTier !== undefined ? { outputTier: timePlan.outputTier } : {}),
    outputBin: timePlan.outputBin,
    outputRes,
    outputCells,
    segments,
    authoritativeEnd: timePlan.authoritativeEnd,
    visibleRange: timePlan.visibleRange,
    smoothing: timePlan.smoothing,
    atomCount: timePlan.atomCount,
  }
}

// Min-cover-aware variant: takes a snapshot of registered shards
// (`shardIndex.listShards(...)`) and plans reads against it instead of
// synthesizing keys from the ladder × watermark grid. Same shape as
// `planGeoQuery` otherwise. See
// `specs/done/inventory-driven-read-walk.md`.
export function planGeoQueryFromInventory(
  pyramid: GeoPyramid,
  input: PlanGeoQueryInput,
  registeredShards: RecordedShard[],
): GeoQueryPlan {
  if (pyramid.geo === undefined) {
    throw new Error('planGeoQueryFromInventory: pyramid has no `geo` config (use planQueryFromInventory for time-only)')
  }
  const resolutions = pyramid.geo.resolutions
  if (resolutions.length === 0) {
    throw new Error('planGeoQueryFromInventory: pyramid.geo.resolutions is empty')
  }
  const haveBBox = input.bbox !== undefined
  const haveCells = input.outputCells !== undefined
  if (!haveBBox && !haveCells) {
    throw new Error('planGeoQueryFromInventory: pass either `bbox` or `outputCells`')
  }
  if (haveBBox && haveCells) {
    throw new Error('planGeoQueryFromInventory: pass `bbox` xor `outputCells`, not both')
  }
  if (haveBBox && input.cellBudget === undefined) {
    throw new Error('planGeoQueryFromInventory: `cellBudget` required when `bbox` is provided')
  }
  const index = getSpatialIndex(pyramid)

  const timePlan = planQueryFromInventory(
    pyramid,
    {
      range: input.range,
      binBudget: input.binBudget,
      ...(input.watermarks !== undefined ? { watermarks: input.watermarks } : {}),
      ...(input.earliestWatermarks !== undefined ? { earliestWatermarks: input.earliestWatermarks } : {}),
      ...(input.earliestPerShard !== undefined ? { earliestPerShard: input.earliestPerShard } : {}),
      ...(input.filter !== undefined ? { filter: input.filter } : {}),
      ...(input.targetBin !== undefined ? { targetBin: input.targetBin } : {}),
      ...(input.limits !== undefined ? { limits: input.limits } : {}),
      ...(input.smoothing !== undefined ? { smoothing: input.smoothing } : {}),
      ...(input.smoothMode !== undefined ? { smoothMode: input.smoothMode } : {}),
    },
    registeredShards,
  )

  const { outputRes, outputCells } = input.outputCells !== undefined
    ? { outputRes: input.outputCells.res, outputCells: [...input.outputCells.cells] }
    : pickResolution(index, input.bbox!, resolutions, input.cellBudget!)

  const segments: GeoPlanSegment[] = timePlan.segments.map(seg => ({
    from: seg.from,
    to: seg.to,
    shardTier: seg.shardTier,
    shardDur: seg.shardDur,
    keys: seg.keys,
    reaggregate: seg.reaggregate,
    cells: outputCells,
  }))

  return {
    ...(timePlan.outputTier !== undefined ? { outputTier: timePlan.outputTier } : {}),
    outputBin: timePlan.outputBin,
    outputRes,
    outputCells,
    segments,
    authoritativeEnd: timePlan.authoritativeEnd,
    visibleRange: timePlan.visibleRange,
    smoothing: timePlan.smoothing,
    atomCount: timePlan.atomCount,
  }
}

function pickResolution(
  index: SpatialIndex,
  bbox: BBox,
  resolutions: number[],
  cellBudget: number,
): { outputRes: number; outputCells: string[] } {
  // Try finest → coarsest (resolutions is finest-first); pick first that
  // yields a non-empty cell list within the budget. An empty result at some
  // resolution means the bbox is smaller than that resolution's centroid
  // spacing — skip; coarser will catch it.
  let lastNonEmpty: { res: number; cells: string[] } | undefined
  for (const res of resolutions) {
    const cells = index.bboxToCells(bbox, res)
    if (cells.length === 0) continue
    if (cells.length <= cellBudget) {
      return { outputRes: res, outputCells: cells }
    }
    lastNonEmpty = { res, cells }
  }
  if (lastNonEmpty !== undefined) {
    // None fit the budget; return the coarsest that yielded data.
    return { outputRes: lastNonEmpty.res, outputCells: lastNonEmpty.cells }
  }
  // No resolution yielded any cells (bbox smaller than every materialized
  // cell). Fall back to the single cell containing the bbox center at the
  // coarsest resolution.
  const coarsest = resolutions[resolutions.length - 1]!
  const centerLat = (bbox.minLat + bbox.maxLat) / 2
  const centerLng = (bbox.minLng + bbox.maxLng) / 2
  return {
    outputRes: coarsest,
    outputCells: [index.latLngToCell(centerLat, centerLng, coarsest)],
  }
}

// Filter fetched rows to a chosen geo resolution + allowed cell set. Drops
// rows whose cell is at a different resolution (every shard has multiple)
// or whose cell isn't in the bbox-covering set. Stitch consumes the result.
// `index` is required — it used to default to `h3Index`, which is what
// pinned `h3-js` into every consumer bundle (see `spatial-index.ts`).
export function filterCellsAndRes(
  rows: Array<Record<string, unknown>>,
  cellCol: string,
  outputRes: number,
  allowedCells: string[],
  index: SpatialIndex,
): Array<Record<string, unknown>> {
  const allowed = new Set(allowedCells)
  return rows.filter(r => {
    const cell = r[cellCol]
    if (typeof cell !== 'string') return false
    if (index.cellLevel(cell) !== outputRes) return false
    return allowed.has(cell)
  })
}

// Filter fetched rows by a mixed-resolution `SpatialSet` cover (e.g.,
// `minimalCover` output). Each row's cell is checked against the cover
// via lineage walks (`SpatialIndex.cellInSet`): include cells "cover"
// their descendants; exclude cells (more specific in the lineage) win
// over ancestor include cells.
//
// Use this when the query's spatial filter is defined by a station set
// (run through `minimalCover` to compact) rather than a bbox.
export function filterCellsByCover(
  rows: Array<Record<string, unknown>>,
  cellCol: string,
  level: number,
  cover: import('./spatial-index.js').SpatialSet,
  index: SpatialIndex,
): Array<Record<string, unknown>> {
  return rows.filter(r => {
    const cell = r[cellCol]
    if (typeof cell !== 'string') return false
    return index.cellInSet(cell, level, cover)
  })
}

// Re-export the time-axis Tier type for convenience.
export type { Tier }

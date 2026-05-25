// Joint time-×-space query planner. Layers h3 cell selection on top of
// pyrmts core's time-axis planner.
//
// Storage model (shared with the core planner): one shard per (time_tier,
// period). Geo resolutions are not separate shards — every materialized
// resolution lives inside every shard, sorted by `h3_cell` (h3 cell IDs
// encode resolution in their high bits, so the sort naturally clusters
// rows by resolution-then-spatial-locality). Predicate pushdown handles
// the spatial filter at read time.

import { getResolution, latLngToCell, polygonToCells } from 'h3-js'
import {
  planQuery,
  type PlanSegment,
  type Pyramid,
  type QueryPlan,
  type Tier,
} from 'pyrmts'

export interface BBox {
  minLat: number
  maxLat: number
  minLng: number
  maxLng: number
}

export interface PlanGeoQueryInput {
  range: { from: Date; to: Date }
  // Max output time bins (same as core planQuery).
  binBudget: number
  bbox: BBox
  // Max output h3 cells. Planner picks the finest materialized resolution
  // whose cell count in `bbox` fits this budget.
  cellBudget: number
  watermarks?: Record<string, Date>
  filter?: Record<string, string | number>
}

export interface GeoPlanSegment extends PlanSegment {
  // h3 cells (at outputRes) overlapping `bbox`. Same list per segment —
  // present here for read-time predicate pushdown convenience.
  cells: string[]
}

export interface GeoQueryPlan extends Omit<QueryPlan, 'segments'> {
  outputRes: number                  // h3 resolution chosen by the planner
  outputCells: string[]              // h3 cells (at outputRes) covering bbox
  segments: GeoPlanSegment[]
}

export function planGeoQuery(
  pyramid: Pyramid,
  input: PlanGeoQueryInput,
): GeoQueryPlan {
  if (pyramid.geo === undefined) {
    throw new Error('planGeoQuery: pyramid has no `geo` config (use planQuery for time-only)')
  }
  const resolutions = pyramid.geo.resolutions
  if (resolutions.length === 0) {
    throw new Error('planGeoQuery: pyramid.geo.resolutions is empty')
  }

  // Delegate the time-axis decisions to the core planner.
  const timePlan = planQuery(pyramid, {
    range: input.range,
    binBudget: input.binBudget,
    ...(input.watermarks !== undefined ? { watermarks: input.watermarks } : {}),
    ...(input.filter !== undefined ? { filter: input.filter } : {}),
  })

  // Pick the finest materialized resolution whose cells-in-bbox fits the
  // cell budget. `resolutions` is finest-first.
  const { outputRes, outputCells } = pickResolution(input.bbox, resolutions, input.cellBudget)

  // Each segment carries the same cell list — every materialized resolution
  // lives in every shard, so the cell predicate is the same regardless of
  // which (finer) time tier the segment reads from.
  const segments: GeoPlanSegment[] = timePlan.segments.map(seg => ({
    from: seg.from,
    to: seg.to,
    shardTier: seg.shardTier,
    keys: seg.keys,
    reaggregate: seg.reaggregate,
    cells: outputCells,
  }))

  return {
    outputTier: timePlan.outputTier,
    outputBin: timePlan.outputBin,
    outputRes,
    outputCells,
    segments,
    authoritativeEnd: timePlan.authoritativeEnd,
  }
}

function pickResolution(
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
    const cells = bboxToCells(bbox, res)
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
    outputCells: [latLngToCell(centerLat, centerLng, coarsest)],
  }
}

// Convert a bbox to the h3 cells covering it at the given resolution.
// h3-js `polygonToCells` takes a closed polygon in [lat, lng] order by
// default.
export function bboxToCells(bbox: BBox, res: number): string[] {
  const polygon: number[][] = [
    [bbox.minLat, bbox.minLng],
    [bbox.minLat, bbox.maxLng],
    [bbox.maxLat, bbox.maxLng],
    [bbox.maxLat, bbox.minLng],
    [bbox.minLat, bbox.minLng],
  ]
  return polygonToCells(polygon, res)
}

// Filter fetched rows to a chosen geo resolution + allowed cell set. Drops
// rows whose cell is at a different resolution (every shard has multiple)
// or whose cell isn't in the bbox-covering set. Stitch consumes the result.
export function filterCellsAndRes(
  rows: Array<Record<string, unknown>>,
  cellCol: string,
  outputRes: number,
  allowedCells: string[],
): Array<Record<string, unknown>> {
  const allowed = new Set(allowedCells)
  return rows.filter(r => {
    const cell = r[cellCol]
    if (typeof cell !== 'string') return false
    if (getResolution(cell) !== outputRes) return false
    return allowed.has(cell)
  })
}

// Re-export the time-axis Tier type for convenience.
export type { Tier }

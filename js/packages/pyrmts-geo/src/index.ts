// pyrmts-geo — spatial extension for pyrmts.
// See ../../../../SPEC.md.

export {
  bboxToCells,
  filterCellsAndRes,
  planGeoQuery,
} from './planner.js'
export type {
  BBox,
  GeoPlanSegment,
  GeoQueryPlan,
  PlanGeoQueryInput,
} from './planner.js'

export { serveGeoQuery } from './serve.js'
export type { ServeGeoOptions } from './serve.js'

export { buildGeoQueryUrl, fetchPyramidGeoQuery } from './query.js'
export type {
  FetchPyramidGeoQueryInput,
  GeoPlanMeta,
  PyramidGeoQueryResult,
} from './query.js'

export { usePyramidGeo } from './use-pyramid-geo.js'
export type {
  UsePyramidGeoInput,
  UsePyramidGeoResult,
} from './use-pyramid-geo.js'

export const VERSION = '0.0.0'

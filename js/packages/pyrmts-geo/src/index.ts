// pyrmts-geo — spatial extension for pyrmts.
// See ../../../../SPEC.md.

export {
  filterCellsAndRes,
  filterCellsByCover,
  planGeoQuery,
  planGeoQueryFromInventory,
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

// `h3Index` is deliberately NOT re-exported — it is a test-only backend,
// and exporting it pins `h3-js` into every consumer bundle. See
// `h3-index.ts`.
export { s2Index } from './s2-index.js'
export {
  S2_LEAF_LEVEL,
  intersectRanges,
  mergeRanges,
  s2IdToToken,
  s2LevelOf,
  s2LsbForLevel,
  s2Parent,
  s2RangeForCell,
  s2RangeForCellToken,
  s2RangesForCells,
  s2TokenToId,
} from './s2-range.js'
export type { S2CellRange } from './s2-range.js'
export { isCellInCover, minimalCover } from './spatial-index-cover.js'
export { buildVocabGraph, vocabCover } from './vocab-cover.js'
export type {
  VocabCoverOpts,
  VocabGraph,
  VocabLeaf,
  VocabNode,
} from './vocab-cover.js'
export { getSpatialIndex } from './spatial-index.js'
export type {
  GeoPyramid,
  GeoSpecWithIndex,
  MinimalCoverOpts,
  SpatialIndex,
  SpatialSet,
} from './spatial-index.js'

export const VERSION = '0.0.0'

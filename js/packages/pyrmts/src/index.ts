// pyrmts — multi-scale timeseries pyramids.
// See ../../../../SPEC.md.

export type {
  Axis,
  Bin,
  Dim,
  Duration,
  Metric,
  MonoidName,
  Pyramid,
  RunBoundary,
  Shard,
  StepCount,
  StepUnit,
  Storage,
  Tier,
  TimeUnit,
} from './types.js'

export {
  addSpan,
  binsInRange,
  floorToSpan,
  formatPeriod,
  parseDuration,
  shardPeriodsCovering,
} from './axis.js'

export type { ParsedTimeSpan } from './axis.js'

export { planQuery } from './planner.js'
export type {
  PlanQueryInput,
  PlanSegment,
  QueryPlan,
} from './planner.js'

export { getMonoid, stateColumns } from './monoids.js'
export type { Monoid, Row } from './monoids.js'

export { stitch } from './stitch.js'
export type { StitchInput } from './stitch.js'

export { fetchShardData, fetchSegmentRows } from './fetch.js'

export { memStorage } from './storage.js'

export { parsePyramidYaml, pyramidFromConfig } from './yaml.js'
export type { PyramidConfig } from './yaml.js'

export { buildQueryUrl, fetchPyramidQuery } from './query.js'
export type {
  FetchPyramidQueryInput,
  PlanMeta,
  PyramidQueryResult,
} from './query.js'

export { usePyramid } from './use-pyramid.js'
export type { UsePyramidInput, UsePyramidResult } from './use-pyramid.js'

export const VERSION = '0.0.0'

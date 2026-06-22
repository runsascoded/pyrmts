// pyrmts — multi-scale timeseries pyramids.
// See ../../../../SPEC.md.

export type {
  Axis,
  Bin,
  ColumnFilter,
  Dim,
  Duration,
  FetchOptionsBase,
  FetchSegment,
  GeoSpec,
  Metric,
  MonoidName,
  Pyramid,
  Row,
  RunBoundary,
  Shard,
  StepCount,
  StepUnit,
  Storage,
  StorageBackend,
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

export { DEFAULT_AUTO_MULTIPLIER, planQuery } from './planner.js'
export type {
  PlanQueryInput,
  PlanSegment,
  QueryPlan,
  SmoothMode,
  SmoothingSpec,
} from './planner.js'

export { getMonoid, stateColumns } from './monoids.js'
export type { Monoid } from './monoids.js'

export { stitch } from './stitch.js'
export type { StitchInput } from './stitch.js'

export { pivotTallToHistogram } from './pivot.js'
export type { PivotTallToHistogramOptions } from './pivot.js'

export { fetchShardData, parquetBackend } from './fetch.js'
export type { FetchOptions, FetchTrace } from './fetch.js'

export { validatePartials } from './partials.js'
export type { ValidatedPartials } from './partials.js'

export {
  CachedShardIndex,
  WATERMARK_KEY_SEPARATOR,
  decodeWatermarkKey,
  encodeWatermarkKey,
} from './shard-index.js'
export type {
  CachedShardIndexOptions,
  DecodedWatermarkKey,
  RecordShardInput,
  ShardIndex,
} from './shard-index.js'

export { memStorage } from './storage.js'

export { parsePyramidYaml, pyramidFromConfig } from './yaml.js'
export type { PyramidConfig } from './yaml.js'

export { buildQueryUrl, fetchPyramidQuery } from './query.js'
export type {
  FetchPyramidQueryInput,
  PlanMeta,
  PyramidQueryResult,
} from './query.js'

export const VERSION = '0.0.0'

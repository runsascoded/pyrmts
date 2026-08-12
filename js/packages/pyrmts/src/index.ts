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

export { EtagConflict, NotSupported } from './types.js'

export {
  addSpan,
  binsInRange,
  ceilToSpan,
  floorToSpan,
  formatPeriod,
  parseDuration,
  shardPeriodsCovering,
} from './axis.js'

export { shardBuildableAt, sourceTierFor } from './cascade-source.js'

export type { ParsedTimeSpan } from './axis.js'

export { DEFAULT_AUTO_MULTIPLIER, planQuery, planQueryFromInventory } from './planner.js'
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

export { validateLadders } from './ladder.js'
export type { ValidatedTierLadder } from './ladder.js'

export {
  CachedShardIndex,
  WATERMARK_KEY_SEPARATOR,
  decodeWatermarkKey,
  encodeWatermarkKey,
} from './shard-index.js'
export type {
  CachedShardIndexOptions,
  DecodedWatermarkKey,
  ListShardsFilter,
  RecordShardInput,
  RecordedShard,
  ShardIndex,
} from './shard-index.js'

export { listExpectedShards, listMissingShards } from './gap-discovery.js'
export type { ExpectedShard } from './gap-discovery.js'

export {
  CAS_ATTEMPTS,
  JOURNAL_BASENAME,
  invalidate,
  journalKey,
  listExistingWithMtime,
  loadInvalidations,
  overlaps,
  pruneSpent,
  staleKeysFor,
} from './invalidation.js'
export type { Invalidation } from './invalidation.js'

export { substituteKey } from './keys.js'

export { ManifestShardIndex } from './manifest-shard-index.js'
export type { ManifestShardIndexOptions } from './manifest-shard-index.js'

// `assertShardIndexConformance` is exported separately as
// `pyrmts/test-utils` so consumers importing the main module don't pull
// vitest into their runtime bundle.

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

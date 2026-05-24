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

export const VERSION = '0.0.0'

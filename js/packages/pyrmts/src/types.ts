// Core type definitions for pyrmts. See ../../../../SPEC.md.

// Pyramid axis. Currently only `time` is implemented by the planner; `step` is
// reserved for tomat (see SPEC.md §Sequencing).
export type Axis = 'time' | 'step'

// Time-axis units.
export type TimeUnit = 'min' | 'h' | 'd' | 'mo' | 'y'
export type Duration = `${number}${TimeUnit}`

// Step-axis units (reserved; not yet handled by the planner).
export type StepUnit = 'step' | 'steps' | 'ksteps' | 'msteps'
export type StepCount = `${number}${StepUnit}`
export type RunBoundary = '1run'

// Span on the pyramid's axis. Variants on a single pyramid must come from
// the same axis — don't mix time-axis bins with step-axis shards.
export type Bin = Duration | StepCount
export type Shard = Duration | RunBoundary | 'all'

export interface Tier {
  name: string
  bin: Bin
  shard: Shard
}

export interface Dim {
  name: string
  type: 'int' | 'string' | 'h3' | 'geohash'
}

export type MonoidName =
  | 'sum'
  | 'count'
  | 'histogram'
  | 'topk'
  | 'botk'
  | 'hll'
  | 'tdigest'

export interface Metric {
  name: string
  monoid: MonoidName
  config?: Record<string, unknown>
}

export interface Storage {
  head(key: string): Promise<{ size: number; etag?: string } | null>
  getRange(key: string, start: number, end: number): Promise<Uint8Array>
  get(key: string): Promise<Uint8Array | null>
  put(key: string, bytes: Uint8Array): Promise<void>
  list(prefix: string): AsyncIterable<string>
}

export interface Pyramid {
  storage: Storage
  // Substitution template for shard keys. `{tier}` and `{period}` are required.
  // Additional `{dim_name}` placeholders are allowed for dim-sharded pyramids
  // (e.g. `awair-{device_id}/{tier}/{period}.parquet`).
  keyTemplate: string
  axis: Axis
  dims: Dim[]
  metrics: Metric[]
  // Canonical order: finest → coarsest. Planner iterates coarsest-first
  // (reverse) when picking the tier that fits a bin budget.
  tiers: Tier[]
}

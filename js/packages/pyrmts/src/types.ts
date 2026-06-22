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

// Byte-level storage interface. Wraps a blob store (R2, in-memory, FS, ...).
// Used internally by `parquetBackend` to fetch parquet bytes / row-group
// ranges; not used directly by the planner or stitch.
export interface Storage {
  head(key: string): Promise<{ size: number; etag?: string } | null>
  // Fetch the half-open byte range [start, end). Returns exactly `end - start`
  // bytes; throws if the object doesn't exist or the range is out of bounds.
  getRange(key: string, start: number, end: number): Promise<Uint8Array>
  get(key: string): Promise<Uint8Array | null>
  put(key: string, bytes: Uint8Array): Promise<void>
  list(prefix: string): AsyncIterable<string>
}

// Generic row type returned by storage backends. Kept here (not in monoids.ts)
// so `StorageBackend` doesn't create a types→monoids→types cycle.
export type Row = Record<string, unknown>

// Minimal segment shape consumed by a `StorageBackend`. The planner emits a
// `PlanSegment` (a superset of this shape), so callers can pass `PlanSegment`
// directly via structural typing.
export interface FetchSegment {
  from: Date
  to: Date
  shardTier: Tier
  keys: readonly string[]
}

// Row-level storage abstraction. Implementations include:
// - `parquetBackend(byteStorage, keyTemplate)`: fetch parquet shards over a
//   byte-level `Storage`, RG-prune by `binCol`/`filters`, decode rows.
// - `d1Backend(db, tableTemplate)` (pyrmts-cfw): emit `SELECT … WHERE dt
//   BETWEEN ? AND ? AND col IN (?,…)` against a Cloudflare D1 table.
// Backends share `FetchOptionsBase`; parquet adds parquet-specific opts.
export interface StorageBackend<Opts = FetchOptionsBase> {
  readonly name: string
  fetchSegment(segment: FetchSegment, opts?: Opts): Promise<Row[]>
}

// Shared filter shape understood by all backends.
export type ColumnFilter =
  | { col: string; values: readonly string[] | readonly number[] }
  | { col: string; range: { min: number; max: number } }

// Options common to all storage backends. Backend-specific options (parquet
// `initialFetchSize` / `trace`) extend this in their own modules.
export interface FetchOptionsBase {
  // Column name to range-filter on. For time-axis pyramids, the bin column
  // (`pyramid.binCol`).
  binCol?: string
  // Range to filter to. `[from, to)`.
  range?: { from: Date; to: Date }
  // AND-combined column filters (set membership or closed numeric range).
  filters?: ColumnFilter[]
  // Treat missing shards as empty (parquet-only — D1 has no "missing shard"
  // concept since rows live in tables, not files).
  tolerate404?: boolean
}

export interface Pyramid {
  storage: StorageBackend
  // Substitution template for canonical-shard keys. `{tier}` and `{period}`
  // are required. Additional `{dim_name}` placeholders are allowed for dim-
  // sharded pyramids (e.g. `awair-{device_id}/{tier}/{period}.parquet`).
  keyTemplate: string
  // Substitution template for partial-shard keys. Required when `partials`
  // is non-empty; ignored otherwise. Adds a `{shard}` placeholder (the
  // cadence label, e.g. `1h`) on top of `keyTemplate`'s placeholders. The
  // canonical convention is to prefix the cadence with `p` so the segment
  // can't collide with a tier name — e.g.
  // `avail-v3/{tier}/p{shard}/{period}.parquet`.
  partialKey?: string
  axis: Axis
  // Name of the bin column in each shard. For time-axis pyramids, this is
  // an int64 UTC millisecond timestamp; for step-axis, an int step count.
  // Conventional names: 'ts', 'bin', 'dt'.
  binCol: string
  dims: Dim[]
  metrics: Metric[]
  // Canonical order: finest → coarsest. Planner iterates coarsest-first
  // (reverse) when picking the tier that fits a bin budget.
  tiers: Tier[]
  // Pyramid-wide sub-shard cadence ladder. Each cadence applies to every
  // tier whose canonical `shard > cadence` AND whose `bin` divides
  // `cadence` (so each sub-shard contains a whole number of bins).
  //
  // Cadences MUST be fixed-duration (`min`/`h`/`d`). Calendar (`mo`/`y`)
  // is canonical-shard-only — sub-shards exist to serve fresh-data queries
  // on a regular cron, and calendar boundaries don't divide cleanly.
  //
  // For the cascading sub-shard builder's "break early" optimization (a
  // /5m cron firing only the cadences that align with `now`), each cadence
  // must divide all coarser cadences in the ladder. Validated at
  // `planQuery` entry when `partials` is set.
  //
  // See `specs/partial-shards.md`.
  partials?: Duration[]
  // Optional geo (h3) extension. When present, every shard contains rows
  // at all materialized resolutions; the geo planner (in `pyrmts-geo`)
  // picks a resolution at query time + filters by cell list.
  geo?: GeoSpec
}

export interface GeoSpec {
  // Parquet column holding the h3 cell ID (string form, e.g. '892a100...').
  cellCol: string
  // h3 resolutions materialized inside every shard. Finest first.
  resolutions: number[]
}

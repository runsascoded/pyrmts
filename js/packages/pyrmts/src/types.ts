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
export type Shard = Duration | RunBoundary

export interface Tier {
  name: string
  bin: Bin
  // Per-tier shard-duration ladder, sorted ascending. `shards[0]` is the
  // smallest (must be ≥ `bin`); `shards.at(-1)` is the largest. Each entry
  // must divide the next (clean promotion via row concat). Single-element
  // ladders are legal (collapses to one shard duration at this tier).
  // See `specs/done/unified-shard-ladder.md`.
  shards: Shard[]
}

export interface Dim {
  name: string
  type: 'int' | 'string' | 'h3' | 'geohash' | 's2'
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
  // ---- Optional: CAS + mtime primitives (invalidation.ts) ----
  // `Storage` implementations that back mutable state (invalidation
  // journals, watermarks, other coordination docs) should implement
  // these. Read-only backends (a fetch-only worker on a bucket the
  // build side owns) don't need them. Backends without them throw
  // `NotSupported` when consumers reach for CAS or mtime data.

  // `(content, etag)` for a subsequent `putIfMatch`. Etag opaque to the
  // caller; round-trip verbatim. `null` etag when the object doesn't
  // exist (paired with `null` content — the create-only case).
  getWithEtag?(key: string): Promise<[Uint8Array | null, string | null]>
  // Conditional put. `etag` from `getWithEtag`, or `null` = create-only
  // (If-None-Match:* semantics). Throws `EtagConflict` on precondition
  // failure — the retry contract callers rely on.
  putIfMatch?(key: string, bytes: Uint8Array, etag: string | null): Promise<void>
  // Async iterator over `[key, mtime]` under `prefix` — mtime is the
  // backend's "last modified" (R2 `uploaded`, S3 `LastModified`, FS
  // `mtime`, MemStorage `clock`). `null` = mtime unknown, treated as
  // fresh by invalidation staleness (backends that can't report mtimes
  // shouldn't trigger rebuilds).
  listWithMtime?(prefix: string): AsyncIterable<[string, Date | null]>
}

// Raised when a `putIfMatch` precondition fails (object changed since
// the etag was read, or existed when create-only was requested).
// Callers retry — see `invalidate` / `pruneSpent` for the pattern.
export class EtagConflict extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'EtagConflict'
  }
}

// Raised by consumers that reach for optional `Storage` methods on a
// backend that doesn't implement them (e.g. `invalidate` on a read-only
// fetch-only backend). Kept distinct from `EtagConflict` so callers
// can distinguish "backend can't do this" from "you lost the race".
export class NotSupported extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'NotSupported'
  }
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
  // Substitution template for shard keys. `{tier}`, `{shard}`, and
  // `{period}` are required. Additional `{dim_name}` placeholders are
  // allowed for dim-sharded pyramids. `{shard}` substitutes the shard's
  // duration label (e.g. `1h`, `1d`); the canonical convention is uniform
  // `<tier>/<shard>/<period>` paths — e.g.
  // `avail-v3/{tier}/{shard}/{period}.parquet`.
  keyTemplate: string
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

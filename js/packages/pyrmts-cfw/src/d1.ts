// D1 (Cloudflare SQLite) StorageBackend for pyrmts.
//
// Each tier maps to one D1 table. The backend's `fetchSegment` builds a
// single SELECT per call:
//
//   SELECT <cols> FROM <table>
//   WHERE dt BETWEEN ? AND ?
//     [ AND <dim> IN (?,?,…) ]*
//
// `<table>` is derived from the tier name via the constructor's
// `tableTemplate`, e.g. `'rides_v3_start_{tier}'` → `'rides_v3_start_1mo'`
// for a 1mo-tier segment.
//
// D1 has a hard limit of 100 bind parameters per query. The `cell IN (…)`
// filter typically dominates (a 200-cell neighborhood query). We chunk
// `cells` into batches and UNION-ALL the per-chunk results inside one
// statement; D1 happily executes a few-hundred-row UNION-ALL. For dim
// filters (gender, user_type, bike_type — each has 2-4 distinct values)
// chunking is unnecessary.
//
// Returned rows are plain `Record<string, unknown>` — D1's
// `.all<T>()` returns `{ results: T[] }`. We forward `results` directly;
// downstream `stitch()` sees the same row shape as the parquet backend.

import type { FetchOptionsBase, FetchSegment, Row, StorageBackend, Tier } from 'pyrmts'

// Subset of the D1Database surface we depend on. Declared structurally
// (rather than importing `@cloudflare/workers-types`'s D1Database type)
// so this module compiles in environments without the workers-types
// dependency loaded.
export interface D1Like {
  prepare(query: string): D1PreparedStatement
  // Atomically execute multiple prepared statements in one round-trip.
  // Used by `D1ShardIndex.recordShard` to write the inventory + bump the
  // watermark in a single transaction. Optional because `d1Backend`
  // (read-only) doesn't need it; impls used for writes must provide it.
  batch?(statements: D1PreparedStatement[]): Promise<unknown[]>
}

export interface D1PreparedStatement {
  bind(...values: unknown[]): D1PreparedStatement
  all<T = Record<string, unknown>>(): Promise<{ results: T[]; success?: boolean; meta?: unknown }>
  // Execute a statement that doesn't return rows (INSERT / UPDATE /
  // DELETE / DDL). Optional — `d1Backend` only uses `all()`.
  run?(): Promise<{ success?: boolean; meta?: unknown }>
}

// Columns to select. Empty / undefined → SELECT * (caller responsible
// for matching the parquet schema row shape that stitch expects).
export interface D1BackendOptions {
  // Per-shard table-name template. `{tier}` is substituted with the
  // tier's `name` (e.g. `'1mo'`). Other `{X}` placeholders are taken
  // verbatim from each `Tier` field (e.g. `'{tier}_{shard}'`).
  tableTemplate: string
  // Optional list of columns to SELECT explicitly. Defaults to '*'.
  selectCols?: readonly string[]
  // Max bind params per chunk (D1's hard cap is 100; default 90 for
  // safety since dt range adds 2 + each dim filter adds N).
  chunkSize?: number
}

const DEFAULT_CHUNK_SIZE = 90

// Materialize `tableTemplate` against `tier`. Substitutes `{tier}` →
// `tier.name`. Extra `{X}` placeholders pull from `tier[X]` for fields
// the spec-defined Tier type carries (bin, shard).
function resolveTable(template: string, tier: Tier): string {
  return template.replace(/\{(\w+)\}/g, (_, k) => {
    if (k === 'tier') return tier.name
    if (k === 'bin') return String(tier.bin)
    if (k === 'shard') return String(tier.shard)
    throw new Error(`d1Backend: unknown placeholder '{${k}}' in tableTemplate '${template}'`)
  })
}

// Quote a SQLite identifier (table or column). SQLite uses "" for ident
// quoting; embedded quotes are escaped by doubling.
function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`
}

// Build the WHERE clause + bind params from `opts`. Returns the SQL
// fragment (without leading WHERE) and the bind values in order. Returns
// `null` if no filters apply.
function buildWhere(opts: FetchOptionsBase | undefined): { sql: string; binds: unknown[] } | null {
  const binds: unknown[] = []
  const conds: string[] = []

  if (opts?.binCol !== undefined && opts.range !== undefined) {
    conds.push(`${quoteIdent(opts.binCol)} >= ? AND ${quoteIdent(opts.binCol)} < ?`)
    binds.push(opts.range.from.getTime(), opts.range.to.getTime())
  }

  for (const f of opts?.filters ?? []) {
    if ('values' in f) {
      if (f.values.length === 0) {
        // Empty IN set → no rows match. Short-circuit with a contradiction.
        return { sql: '1 = 0', binds: [] }
      }
      const placeholders = f.values.map(() => '?').join(',')
      conds.push(`${quoteIdent(f.col)} IN (${placeholders})`)
      binds.push(...f.values)
    } else {
      conds.push(`${quoteIdent(f.col)} >= ? AND ${quoteIdent(f.col)} <= ?`)
      binds.push(f.range.min, f.range.max)
    }
  }

  if (conds.length === 0) return null
  return { sql: conds.join(' AND '), binds }
}

// Split filters with large IN sets into a list of FetchOptionsBase, one
// per chunk. The dt-range + small-set filters are duplicated across all
// chunks; the large-IN filter splits.
function chunkFilters(
  opts: FetchOptionsBase | undefined,
  chunkSize: number,
): (FetchOptionsBase | undefined)[] {
  if (opts === undefined || opts.filters === undefined) return [opts]
  // Find the IN filter with the most values (typically the cells set).
  let maxIdx = -1
  let maxLen = 0
  for (let i = 0; i < opts.filters.length; i++) {
    const f = opts.filters[i]!
    if ('values' in f && f.values.length > maxLen) {
      maxIdx = i
      maxLen = f.values.length
    }
  }
  if (maxIdx === -1 || maxLen <= chunkSize) return [opts]
  // Budget: chunkSize - (dt range = 2) - (other small-set + range filters).
  const otherBindCount = (opts.binCol !== undefined && opts.range !== undefined ? 2 : 0)
    + opts.filters.reduce((sum, f, i) => {
      if (i === maxIdx) return sum
      if ('values' in f) return sum + f.values.length
      return sum + 2
    }, 0)
  const perChunk = Math.max(1, chunkSize - otherBindCount)

  const bigFilter = opts.filters[maxIdx]!
  if (!('values' in bigFilter)) return [opts]  // shouldn't happen — guarded above
  const out: FetchOptionsBase[] = []
  for (let off = 0; off < bigFilter.values.length; off += perChunk) {
    const slice = bigFilter.values.slice(off, off + perChunk)
    const newFilters = opts.filters.map((f, i) => i === maxIdx
      ? { col: bigFilter.col, values: slice }
      : f)
    out.push({ ...opts, filters: newFilters })
  }
  return out
}

export function d1Backend(db: D1Like, options: D1BackendOptions): StorageBackend<FetchOptionsBase> {
  const chunkSize = options.chunkSize ?? DEFAULT_CHUNK_SIZE
  const selectClause = options.selectCols !== undefined
    ? options.selectCols.map(quoteIdent).join(', ')
    : '*'

  return {
    name: 'd1',
    async fetchSegment(segment: FetchSegment, opts?: FetchOptionsBase): Promise<Row[]> {
      const table = resolveTable(options.tableTemplate, segment.shardTier)
      const tableQ = quoteIdent(table)

      const chunks = chunkFilters(opts, chunkSize)
      const allRows: Row[] = []
      for (const chunkOpts of chunks) {
        const where = buildWhere(chunkOpts)
        const sql = where === null
          ? `SELECT ${selectClause} FROM ${tableQ}`
          : `SELECT ${selectClause} FROM ${tableQ} WHERE ${where.sql}`
        const stmt = where === null
          ? db.prepare(sql)
          : db.prepare(sql).bind(...where.binds)
        const res = await stmt.all<Row>()
        allRows.push(...res.results)
      }
      return allRows
    },
  }
}

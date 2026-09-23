// Index-free diff walk — the TS twin of `pyrmts_engine.bench_diff` and the
// deployable engine for a diff treemap between ANY two scans. See
// ../../../../specs/multi-scan-consolidation.md ("Bake-off").
//
// Two snapshots, each a `(depth, path)`-sorted path-index parquet (dir rows
// carry rolled-up size/count), read over `Storage`. Expanding a directory =
// listing its children on both sides (row groups located by bisection over the
// footer's `(depth, path)` min/max, read once per request via the RG cache),
// merge-joining by name, and queueing the changed subdirs. A dir whose size on
// both sides and |Δ| are all below the render `floor` (bytes) is never
// expanded — that is the bound; `budget` is a backstop.
//
// `order: 'level'` (default) expands every pending dir of the shallowest level
// in one round, all listings in flight together (a child can only be listed
// after its parent's listing returned; siblings are independent), so the
// dependent round trips equal the depth of the expanded tree. Every range
// request hyparquet issues is counted (`stats.gets`, `stats.bytes`) and timed
// per round (`stats.roundMs`), so a run over a real network measures itself.

import { parquetMetadataAsync, parquetReadObjects, type AsyncBuffer, type FileMetaData, type RowGroup, type SchemaElement } from 'hyparquet'
import type { MetadataCache } from './fetch.js'
import { EtagConflict, type Row, type Storage } from './types.js'

export interface WalkCols {
  path: string
  depth: string
  size: string
  count: string
}

/** pyrmts / cw path indexes: `b` bytes, `o` objects. disk-tree: `size`, `n_desc`. */
export const DEFAULT_WALK_COLS: WalkCols = { path: 'path', depth: 'depth', size: 'b', count: 'o' }

export interface WalkStats {
  /** Row groups read (RG granularity). */
  requests: number
  /** Data range GETs actually issued (hyparquet slices, excluding footer reads). */
  gets: number
  /** Footer range GETs (cold opens only). */
  footerGets: number
  /** Bytes moved by those GETs. */
  bytes: number
  rgDecodes: number
  rgCacheHits: number
  footerParses: number
  listings: number
  expansions: number
  /** GETs per dependent round (level-synchronous walk). */
  rounds: number[]
  /** Measured wall ms per round. */
  roundMs: number[]
  /** Per-stage ms summed over listings. `fetch` overlaps across concurrent
   * listings (network wait); `decode` includes hyparquet's planning and the
   * fetch wait inside it, so it is an upper bound on decode CPU. */
  ms: { footer: number; locate: number; fetch: number; decode: number; post: number }
}

export function newWalkStats(): WalkStats {
  return {
    requests: 0, gets: 0, footerGets: 0, bytes: 0, rgDecodes: 0, rgCacheHits: 0, footerParses: 0,
    listings: 0, expansions: 0, rounds: [], roundMs: [],
    ms: { footer: 0, locate: 0, fetch: 0, decode: 0, post: 0 },
  }
}

/** CPU-ish ms: everything except network wait. */
export function cpuMs(s: WalkStats): number {
  return s.ms.footer + s.ms.locate + s.ms.decode + s.ms.post
}

/** Rounds that issued at least one GET. */
export function roundTrips(s: WalkStats): number {
  return s.rounds.filter(n => n > 0).length
}

/** Modelled wall: CPU + ⌈GETs / parallel⌉ trips per dependent round × RTT. */
export function wallModel(s: WalkStats, rttMs: number, parallel = 8): number {
  const trips = s.rounds.reduce((acc, n) => acc + Math.ceil(n / Math.max(1, parallel)), 0)
  return cpuMs(s) + trips * rttMs
}

export interface NodeState {
  size: number
  count: number
}

export type Listing = Map<string, NodeState>

/** What locating a listing needs per row group: its row range and the
 * `(depth, path)` min/max from the footer statistics. JSON-able: a consumer
 * stores these in D1 / a manifest blob so the edge never parses the footer. */
export interface RowGroupSummary {
  rowStart: number
  numRows: number
  depthMin: number
  depthMax: number
  pathMin: string
  pathMax: string
}

/** A pre-computed row-group index in place of the parquet footer. `rowGroup(i)`
 * returns hyparquet's per-group metadata (column-chunk offsets / sizes / codec
 * / encodings — what decoding group `i` needs), served however the consumer
 * likes: all at once from a manifest, or one group at a time from D1. */
export interface RowGroupIndex {
  size: number
  etag?: string
  schema: SchemaElement[]
  groups: RowGroupSummary[]
  rowGroup(i: number): RowGroup | Promise<RowGroup>
}

/** The per-group summaries from a parsed footer — the producer side of
 * `RowGroupIndex.groups`. Requires `(depth, path)` statistics. */
export function rowGroupSummaries(metadata: FileMetaData, cols: WalkCols = DEFAULT_WALK_COLS): RowGroupSummary[] {
  const first = metadata.row_groups[0]
  if (!first) return []
  const idx = (name: string) => {
    const i = first.columns.findIndex(c => c.meta_data?.path_in_schema.join('.') === name)
    if (i < 0) throw new Error(`rowGroupSummaries: column '${name}' not in the file`)
    return i
  }
  const di = idx(cols.depth)
  const pi = idx(cols.path)
  const out: RowGroupSummary[] = []
  let cursor = 0
  for (const rg of metadata.row_groups) {
    const ds = rg.columns[di]?.meta_data?.statistics
    const ps = rg.columns[pi]?.meta_data?.statistics
    if (!ds || !ps || ds.min_value === undefined || ps.min_value === undefined) {
      throw new Error('rowGroupSummaries: the file lacks (depth, path) row-group statistics')
    }
    const numRows = Number(rg.num_rows)
    out.push({
      rowStart: cursor, numRows,
      depthMin: num(ds.min_value), depthMax: num(ds.max_value),
      pathMin: str(ps.min_value), pathMax: str(ps.max_value),
    })
    cursor += numRows
  }
  return out
}

/** A `RowGroupIndex` over an already-parsed footer (in-memory `rowGroup`),
 * e.g. to build what a producer persists. */
export function rowGroupIndexFromMetadata(
  metadata: FileMetaData,
  size: number,
  etag?: string,
  cols: WalkCols = DEFAULT_WALK_COLS,
): RowGroupIndex {
  return {
    size,
    ...(etag !== undefined ? { etag } : {}),
    schema: metadata.schema,
    groups: rowGroupSummaries(metadata, cols),
    rowGroup: (i: number) => metadata.row_groups[i]!,
  }
}

export interface SnapshotReaderOptions {
  cols?: WalkCols
  stats?: WalkStats
  /** Decoded-footer cache (see `fetchShardData`); on a hit no `head`, no
   * footer read, and data ranges carry `If-Match`. */
  metadataCache?: MetadataCache
  /** Keep decoded row groups for this reader's life (one request). Default true. */
  rgCache?: boolean
  /** Initial bytes-from-EOF for the footer read. */
  initialFetchSize?: number
  /** Pre-supplied row-group index: no `head`, no footer read or parse; the
   * per-group metadata comes from `rowGroups.rowGroup(i)` on demand. */
  rowGroups?: RowGroupIndex
  /** hyparquet's fetch coalescing budget for a run of row groups: the max
   * share of a fetch that no selected column chunk needs, and the max bytes
   * per fetch. Default `{ 1, Infinity }` = one GET per run regardless of the
   * unselected columns' share; lower the ratio to trade GETs for bytes. */
  maxOverfetchRatio?: number
  maxRunBytes?: number
}

function num(v: unknown): number {
  if (typeof v === 'bigint') return Number(v)
  if (typeof v === 'number') return v
  return 0
}

function str(v: unknown): string {
  if (typeof v === 'string') return v
  if (v instanceof Uint8Array) return new TextDecoder().decode(v)
  return String(v)
}

function cmpKey(a: [number, string], b: [number, string]): number {
  if (a[0] !== b[0]) return a[0] - b[0]
  return a[1] < b[1] ? -1 : a[1] > b[1] ? 1 : 0
}

/** First index `i` in sorted `arr` with `arr[i] >= key`. */
function bisectLeft(arr: [number, string][], key: [number, string]): number {
  let lo = 0
  let hi = arr.length
  while (lo < hi) {
    const mid = (lo + hi) >> 1
    if (cmpKey(arr[mid]!, key) < 0) lo = mid + 1
    else hi = mid
  }
  return lo
}

/** Per-directory children reader over one snapshot parquet. */
export class SnapshotReader {
  readonly cols: WalkCols
  readonly stats: WalkStats
  private schema: SchemaElement[] = []
  private size = 0
  private etag: string | undefined
  private readonly index: RowGroupIndex | undefined
  private readonly groupMeta = new Map<number, Promise<RowGroup>>()
  private rgLo: [number, string][] = []
  private rgHi: [number, string][] = []
  private rgRowStart: number[] = []
  private rgRows: number[] = []
  private readonly rgCache: Map<number, Promise<Row[]>> | null
  private readonly metadataCache: MetadataCache | undefined
  private readonly initialFetchSize: number
  private readonly maxOverfetchRatio: number
  private readonly maxRunBytes: number
  private opened = false

  constructor(readonly storage: Storage, readonly key: string, opts: SnapshotReaderOptions = {}) {
    this.cols = opts.cols ?? DEFAULT_WALK_COLS
    this.stats = opts.stats ?? newWalkStats()
    this.rgCache = opts.rgCache === false ? null : new Map()
    this.metadataCache = opts.metadataCache
    this.initialFetchSize = opts.initialFetchSize ?? 64 * 1024
    this.index = opts.rowGroups
    this.maxOverfetchRatio = opts.maxOverfetchRatio ?? 1
    this.maxRunBytes = opts.maxRunBytes ?? Infinity
  }

  /** Build the RG key ranges from the pre-supplied index, the cached footer,
   * or a footer read. */
  async open(): Promise<this> {
    if (this.opened) return this
    const t0 = performance.now()
    let summaries: RowGroupSummary[]
    if (this.index !== undefined) {
      this.size = this.index.size
      this.etag = this.index.etag
      this.schema = this.index.schema
      summaries = this.index.groups
    } else {
      let metadata: FileMetaData
      const cached = this.metadataCache?.get(this.key)
      if (cached !== undefined) {
        metadata = cached.metadata
        this.size = cached.size
        this.etag = cached.etag
      } else {
        const head = await this.storage.head(this.key)
        if (head === null) throw new Error(`SnapshotReader: object not found: ${this.key}`)
        this.size = head.size
        metadata = await parquetMetadataAsync(this.file(false), { initialFetchSize: this.initialFetchSize })
        this.stats.footerParses++
        if (head.etag !== undefined) {
          this.etag = head.etag
          this.metadataCache?.set(this.key, { etag: head.etag, size: head.size, metadata })
        }
      }
      this.schema = metadata.schema
      summaries = rowGroupSummaries(metadata, this.cols)
      metadata.row_groups.forEach((rg, i) => this.groupMeta.set(i, Promise.resolve(rg)))
    }
    if (summaries.length === 0) throw new Error(`SnapshotReader: ${this.key} has no row groups`)
    for (const g of summaries) {
      this.rgLo.push([g.depthMin, g.pathMin])
      this.rgHi.push([g.depthMax, g.pathMax])
      this.rgRowStart.push(g.rowStart)
      this.rgRows.push(g.numRows)
    }
    this.stats.ms.footer += performance.now() - t0
    this.opened = true
    return this
  }

  /** Per-group metadata: from the parsed footer, or `rowGroups.rowGroup(i)` (memoized). */
  private rowGroupMeta(i: number): Promise<RowGroup> {
    let p = this.groupMeta.get(i)
    if (p === undefined) {
      if (this.index === undefined) throw new Error(`SnapshotReader: no metadata for row group ${i}`)
      p = Promise.resolve(this.index.rowGroup(i))
      this.groupMeta.set(i, p)
    }
    return p
  }

  // An AsyncBuffer over Storage: every slice is one range GET, counted as a
  // footer GET (`guard` false, cold open) or a data GET; data reads carry
  // If-Match when the etag is known, so a rewrite since the footer / index was
  // taken fails loudly.
  private file(guard: boolean): AsyncBuffer {
    const { storage, key, size, stats } = this
    const opts = guard && this.etag !== undefined ? { ifMatch: this.etag } : undefined
    return {
      byteLength: size,
      async slice(start: number, end?: number): Promise<ArrayBuffer> {
        const e = end ?? size
        if (guard) stats.gets++
        else stats.footerGets++
        stats.bytes += e - start
        const t0 = performance.now()
        const bytes = await storage.getRange(key, start, e, opts)
        if (guard) stats.ms.fetch += performance.now() - t0
        return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer
      },
    }
  }

  /** Row groups whose key range intersects `[(depth, lo), (depth, hi))`, as `[first, last)`. */
  private locate(depth: number, lo: string, hi: string): [number, number] {
    const first = bisectLeft(this.rgHi, [depth, lo])
    const last = bisectLeft(this.rgLo, [depth, hi])
    return [first, Math.max(first, last)]
  }

  // Read row groups `[i, j)`: hyparquet plans the fetches over a synthetic
  // footer holding just this run's groups and coalesces them across row groups
  // under `maxOverfetchRatio` / `maxRunBytes` (one GET per run at the
  // defaults), reading only the four walk columns. Returns one row array per RG.
  private async readRun(i: number, j: number): Promise<Row[][]> {
    const c = this.cols
    const groups = await Promise.all(Array.from({ length: j - i }, (_, k) => this.rowGroupMeta(i + k)))
    const numRows = groups.reduce((n, g) => n + Number(g.num_rows), 0)
    const metadata: FileMetaData = {
      version: 2, schema: this.schema, num_rows: BigInt(numRows), row_groups: groups, metadata_length: 0,
    }
    const t0 = performance.now()
    const rows = await parquetReadObjects({
      file: this.file(true), metadata, rowStart: 0, rowEnd: numRows,
      columns: [c.path, c.depth, c.size, c.count],
      maxOverfetchRatio: this.maxOverfetchRatio, maxRunBytes: this.maxRunBytes,
    })
    this.stats.ms.decode += performance.now() - t0
    const out: Row[][] = []
    let off = 0
    for (let k = i; k < j; k++) {
      const n = this.rgRows[k]!
      out.push(rows.slice(off, off + n))
      off += n
      this.stats.requests++
      this.stats.rgDecodes++
    }
    return out
  }

  private async readRgs(first: number, last: number): Promise<Row[][]> {
    const pending: Promise<Row[]>[] = []
    let i = first
    while (i < last) {
      const cached = this.rgCache?.get(i)
      if (cached !== undefined) {
        this.stats.rgCacheHits++
        pending.push(cached)
        i++
        continue
      }
      // Contiguous uncached run → one GET; register every RG's promise before
      // awaiting so concurrent listings of the same row groups share it.
      let j = i + 1
      while (j < last && !this.rgCache?.has(j)) j++
      const base = i
      const run = this.readRun(base, j)
      for (let k = base; k < j; k++) {
        const p = run.then(parts => parts[k - base]!)
        this.rgCache?.set(k, p)
        pending.push(p)
      }
      i = j
    }
    return Promise.all(pending)
  }

  private async list(depth: number, lo: string, hi: string): Promise<Listing> {
    await this.open()
    const c = this.cols
    const t0 = performance.now()
    const [first, last] = this.locate(depth, lo, hi)
    const t1 = performance.now()
    const groups = await this.readRgs(first, last)
    const t2 = performance.now()
    const out: Listing = new Map()
    for (const rows of groups) {
      for (const r of rows) {
        if (num(r[c.depth]) !== depth) continue
        const p = str(r[c.path])
        if (p < lo || p >= hi) continue
        out.set(p, { size: num(r[c.size]), count: num(r[c.count]) })
      }
    }
    const t3 = performance.now()
    this.stats.ms.locate += t1 - t0
    this.stats.ms.post += t3 - t2
    return out
  }

  /** `{size, count}` of one node, or null if absent. */
  async node(path: string): Promise<NodeState | null> {
    const depth = path === '' || path === '.' ? 0 : path.split('/').length
    const l = await this.list(depth, path, path + '\0')
    return l.get(path) ?? null
  }

  /** Direct children of `prefix` (a node at `depth`). */
  async children(prefix: string, depth: number): Promise<Listing> {
    this.stats.listings++
    const lo = prefix ? `${prefix}/` : ''
    const hi = prefix ? `${prefix}0` : '\x7f'
    return this.list(depth + 1, lo, hi)
  }
}

export interface DeltaRow {
  path: string
  /** Levels below the page path (1 = direct child). */
  depth: number
  status: 'added' | 'removed' | 'changed'
  sizeA: number
  sizeB: number
  countA: number
  countB: number
  /** A dir we descended into. */
  expanded: boolean
  /** Differing, not expanded (render floor or budget): change may hide below. */
  pruned: boolean
}

export interface WalkResult {
  /** Changed rows met, sorted by |Δ| desc. */
  rows: DeltaRow[]
  expansions: number
  truncated: boolean
}

export interface WalkOptions {
  /** Render floor in bytes (see `renderFloor`). Default 0 = expand every change. */
  floor?: number
  /** Expansion backstop. */
  budget?: number
  order?: 'level' | 'bestfirst'
  /** Expansions in flight per round (each is two listings). */
  parallel?: number
}

/** Smallest subtree (bytes) that can be drawn: a node's area ≈ its share of
 * the page root × the canvas, so `rootSize × cellPx² / (width × height)`. */
export function renderFloor(rootSize: number, widthPx: number, heightPx: number, minCellPx = 4): number {
  return Math.floor(rootSize * (minCellPx * minCellPx) / Math.max(1, widthPx * heightPx))
}

interface Pending {
  prio: number   // -|Δ|
  depth: number
  seq: number
  path: string
}

async function mapPool<T, R>(items: T[], parallel: number, fn: (t: T) => Promise<R>): Promise<R[]> {
  const out: R[] = new Array(items.length)
  let next = 0
  const workers = Array.from({ length: Math.max(1, Math.min(parallel, items.length)) }, async () => {
    while (next < items.length) {
      const i = next++
      out[i] = await fn(items[i]!)
    }
  })
  await Promise.all(workers)
  return out
}

/** Pruned recursive diff between two snapshots under `pagePath`. */
export async function walkDiff(
  ra: SnapshotReader,
  rb: SnapshotReader,
  pagePath = '',
  opts: WalkOptions = {},
): Promise<WalkResult> {
  const floor = opts.floor ?? 0
  const budget = opts.budget ?? 10_000
  const order = opts.order ?? 'level'
  const parallel = opts.parallel ?? 8
  const stats = ra.stats
  const getsOf = () => stats.gets + (rb.stats === stats ? 0 : rb.stats.gets)
  const pageDepth = pagePath === '' || pagePath === '.' ? 0 : pagePath.split('/').length
  const rows: DeltaRow[] = []
  const byPath = new Map<string, DeltaRow>()
  let queue: Pending[] = [{ prio: 0, depth: pageDepth, seq: 0, path: pagePath }]
  let seq = 0
  let expansions = 0
  let truncated = false
  stats.rounds = []
  stats.roundMs = []
  await Promise.all([ra.open(), rb.open()])

  while (queue.length) {
    if (expansions >= budget) {
      truncated = true
      break
    }
    let batch: Pending[]
    if (order === 'level') {
      const level = Math.min(...queue.map(q => q.depth))
      const atLevel = queue.filter(q => q.depth === level)
      batch = atLevel.slice(0, budget - expansions)
      queue = queue.filter(q => q.depth !== level).concat(atLevel.slice(batch.length))
    } else {
      queue.sort((x, y) => x.prio - y.prio || x.seq - y.seq)
      batch = [queue.shift()!]
    }
    const gets0 = getsOf()
    const t0 = performance.now()
    const listings = await mapPool(batch, parallel, async q => {
      const [ca, cb] = await Promise.all([ra.children(q.path, q.depth), rb.children(q.path, q.depth)])
      return { q, ca, cb }
    })
    stats.roundMs.push(performance.now() - t0)
    stats.rounds.push(getsOf() - gets0)
    for (const { q, ca, cb } of listings) {
      expansions++
      stats.expansions++
      const self = byPath.get(q.path)
      if (self) self.expanded = true
      const names = [...new Set([...ca.keys(), ...cb.keys()])].sort()
      for (const name of names) {
        const a = ca.get(name)
        const b = cb.get(name)
        const sa = a?.size ?? 0, na = a?.count ?? 0
        const sb = b?.size ?? 0, nb = b?.count ?? 0
        let status: DeltaRow['status'] | 'unchanged'
        if (!a) status = 'added'
        else if (!b) status = 'removed'
        else if (sa !== sb || na !== nb) status = 'changed'
        else status = 'unchanged'
        if (status === 'unchanged') continue
        const row: DeltaRow = {
          path: name, depth: q.depth + 1 - pageDepth, status,
          sizeA: sa, sizeB: sb, countA: na, countB: nb, expanded: false, pruned: false,
        }
        byPath.set(name, row)
        rows.push(row)
        if (status === 'changed') {
          // Only a changed node present on both sides can hide change below
          // it; added/removed rows already tell the whole story.
          if (Math.max(sa, sb) < floor && Math.abs(sb - sa) < floor) {
            row.pruned = true
          } else {
            // No `kind` column in general (a prefix with one object and the
            // object itself both carry count 1): a leaf shows as an empty listing.
            seq++
            queue.push({ prio: -Math.abs(sb - sa), depth: q.depth + 1, seq, path: name })
          }
        }
      }
    }
  }
  for (const q of queue) {
    const r = byPath.get(q.path)
    if (r) {
      r.pruned = true
      truncated = true
    }
  }
  rows.sort((x, y) => Math.abs(y.sizeB - y.sizeA) - Math.abs(x.sizeB - x.sizeA) || (x.path < y.path ? -1 : 1))
  return { rows, expansions, truncated }
}

export { EtagConflict }

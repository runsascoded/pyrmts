// Flat-changeset diff-index — the TS reader for `pyrmts.diffindex` /
// `DiffIndexStore`: the changeset between ANY two scans, composed from disjoint
// aligned dyadic nodes. See ../../../../specs/multi-scan-consolidation.md
// (Phase 3).
//
// A changeset is `{key → (before, after)}` over the keys that differ across a
// span (birth = identity→v, death = v→identity). Composition is associative but
// NOT invertible (remove-then-re-add nets to zero yet is real churn), so a span
// is covered by *disjoint* blocks. Level 0 is the events log: one adjacency
// node per scan pair. Levels 1..L (the index's `levels` cap, in its manifest)
// hold aligned power-of-2 nodes: `(level, start)` with `start` a multiple of
// `2^level` = the net change from scan `start` to `start + 2^level`.
// `diffOverSpan` fetches only the nodes named by `alignedBlocks` (≤ 2·log2(span)+1
// with enough levels, `span` adjacency nodes at `levels = 0`) and composes them —
// no snapshot is read at query time. Node rows are the standard changeset shape
// (`key_cols` + `{c}__a`/`{c}__b`), identical to `diffTables` output.
//
// This is the audit / changelog / gross-churn primitive, not the diff-treemap
// engine: a treemap needs O(rendered) work (a best-first tandem walk over two
// random-access snapshots), while a flat changeset is O(changes-in-span).

import { parquetMetadata, parquetReadObjects, type AsyncBuffer } from 'hyparquet'
import { identities, keyStateCols, type State } from './multiscan.js'
import type { Pyramid, Row } from './types.js'

type Schema = Pick<Pyramid, 'binCol' | 'dims' | 'metrics'>

export interface ChangeEntry {
  keyRow: Row
  sa: State
  sb: State
}

/** A changeset keyed by the JSON-encoded key tuple. */
export type Changeset = Map<string, ChangeEntry>

function keyStr(row: Row, keyCols: string[]): string {
  return JSON.stringify(keyCols.map(c => row[c] ?? null))
}

function stateEq(a: State, b: State): boolean {
  if (a.length !== b.length) return false
  for (let i = 0; i < a.length; i++) if (!Object.is(a[i], b[i])) return false
  return true
}

function cmpVal(a: unknown, b: unknown): number {
  if (a === b) return 0
  if (a === null || a === undefined) return -1
  if (b === null || b === undefined) return 1
  return a < b ? -1 : a > b ? 1 : 0
}

/** Parse changeset rows (`key_cols` + `{c}__a`/`{c}__b`) into a `Changeset`. */
export function changesetFromRows(rows: Row[], schema: Schema): Changeset {
  const { keyCols, stateCols } = keyStateCols(schema)
  const out: Changeset = new Map()
  for (const r of rows) {
    const keyRow: Row = {}
    for (const c of keyCols) keyRow[c] = r[c]
    out.set(keyStr(r, keyCols), {
      keyRow,
      sa: stateCols.map(c => r[`${c}__a`] ?? null),
      sb: stateCols.map(c => r[`${c}__b`] ?? null),
    })
  }
  return out
}

/** Materialize a `Changeset` as rows, sorted `(*dims, binCol)` — the same
 * shape `diffTables` / `diffScans` return. */
export function changesetToRows(cs: Changeset, schema: Schema): Row[] {
  const { keyCols, stateCols } = keyStateCols(schema)
  const order = [...schema.dims.map(d => d.name), schema.binCol]
  const rows: Row[] = []
  for (const { keyRow, sa, sb } of cs.values()) {
    const row: Row = {}
    for (const c of keyCols) row[c] = keyRow[c]
    stateCols.forEach((c, i) => {
      row[`${c}__a`] = sa[i]
      row[`${c}__b`] = sb[i]
    })
    rows.push(row)
  }
  return rows.sort((r1, r2) => {
    for (const c of order) {
      const d = cmpVal(r1[c], r2[c])
      if (d !== 0) return d
    }
    return 0
  })
}

/** The changeset from `rowsA` to `rowsB` (two scan states) as a `Changeset` —
 * the dict twin of `diffTables`; absent → monoid identity. */
export function changesetBetween(rowsA: Row[], rowsB: Row[], schema: Schema): Changeset {
  const { keyCols, stateCols } = keyStateCols(schema)
  const ids = identities(schema)
  const id: State = stateCols.map(c => ids[c])
  const a = new Map<string, { keyRow: Row; state: State }>()
  const b = new Map<string, { keyRow: Row; state: State }>()
  for (const r of rowsA) a.set(keyStr(r, keyCols), { keyRow: r, state: stateCols.map(c => r[c] ?? null) })
  for (const r of rowsB) b.set(keyStr(r, keyCols), { keyRow: r, state: stateCols.map(c => r[c] ?? null) })
  const out: Changeset = new Map()
  for (const k of new Set([...a.keys(), ...b.keys()])) {
    const sa = a.get(k)?.state ?? id
    const sb = b.get(k)?.state ?? id
    if (!stateEq(sa, sb)) out.set(k, { keyRow: (a.get(k) ?? b.get(k))!.keyRow, sa, sb })
  }
  return out
}

/** Compose `left` over `(a, m]` with `right` over `(m, b]` → net over `(a, b]`.
 * A key in both chains `left.before → right.after` (dropped if equal — churn
 * that cancels); a key in one passes through. Not invertible: compose only
 * disjoint spans. Mirrors Python `compose_changesets`. */
export function composeChangesets(left: Changeset, right: Changeset): Changeset {
  const out: Changeset = new Map(left)
  for (const [k, { keyRow, sa: mid, sb }] of right) {
    const prev = out.get(k)
    if (prev) {
      if (stateEq(prev.sa, sb)) out.delete(k)
      else out.set(k, { keyRow: prev.keyRow, sa: prev.sa, sb })
    } else {
      out.set(k, { keyRow, sa: mid, sb })
    }
  }
  return out
}

/** The disjoint aligned dyadic blocks covering `(i, j]` as `[level, start]`
 * pairs — block `(level, start)`, `start` a multiple of `2^level`, `level ≤
 * levels` = net change from scan `start` to `start + 2^level`. Greedy largest
 * aligned block at each position: ≤ 2·log2(j−i)+1 blocks when the cap allows,
 * the `j−i` adjacency blocks at `levels = 0`. Mirrors Python `aligned_blocks`. */
export function alignedBlocks(i: number, j: number, levels = 0): Array<[number, number]> {
  if (levels < 0) throw new Error(`alignedBlocks: levels must be ≥ 0, got ${levels}`)
  const out: Array<[number, number]> = []
  let pos = i
  while (pos < j) {
    let level = 0
    while (level < levels && pos % (1 << (level + 1)) === 0 && pos + (1 << (level + 1)) <= j) level++
    out.push([level, pos])
    pos += 1 << level
  }
  return out
}

/** Diff between any two scans, composed from only the aligned nodes covering
 * the span. `scans` and `levels` come from the index manifest
 * (`parseDiffIndexManifest`); `loadNode` fetches node `(level, start)` as
 * changeset rows (the consumer's storage read). If `a` is after `b`, the forward
 * changeset is computed and each before/after swapped (a single changeset
 * reverses; only composition is non-invertible). */
export async function diffOverSpan(
  scans: string[],
  schema: Schema,
  a: string,
  b: string,
  loadNode: (level: number, start: number) => Promise<Row[]>,
  levels = 0,
): Promise<Row[]> {
  let i = scans.indexOf(a)
  let j = scans.indexOf(b)
  if (i < 0) throw new Error(`diffOverSpan: '${a}' not in the index`)
  if (j < 0) throw new Error(`diffOverSpan: '${b}' not in the index`)
  const reverse = i > j
  if (reverse) [i, j] = [j, i]
  let result: Changeset = new Map()
  for (const [level, start] of alignedBlocks(i, j, levels)) {
    result = composeChangesets(result, changesetFromRows(await loadNode(level, start), schema))
  }
  if (reverse) {
    for (const [k, e] of result) result.set(k, { keyRow: e.keyRow, sa: e.sb, sb: e.sa })
  }
  return changesetToRows(result, schema)
}

function toArrayBuffer(bytes: Uint8Array): ArrayBuffer {
  const ab = new ArrayBuffer(bytes.byteLength)
  new Uint8Array(ab).set(bytes)
  return ab
}

function asyncBufferFromBytes(bytes: Uint8Array): AsyncBuffer {
  const ab = toArrayBuffer(bytes)
  return { byteLength: ab.byteLength, slice: (start: number, end?: number) => ab.slice(start, end) }
}

/** Parse a persisted node parquet (as `DiffIndexStore` writes it) into
 * changeset rows, int64 normalized to number like the fetch path. */
export async function readChangesetNode(bytes: Uint8Array): Promise<Row[]> {
  const metadata = parquetMetadata(toArrayBuffer(bytes))
  const raw = await parquetReadObjects({ file: asyncBufferFromBytes(bytes), metadata })
  return raw.map(row => {
    const out: Row = {}
    for (const k in row) {
      const v = row[k]
      out[k] = typeof v === 'bigint' ? Number(v) : v
    }
    return out
  })
}

export interface DiffIndexManifest {
  /** Ordered scan labels; position = scan index. */
  scans: string[]
  /** Hierarchy cap: aligned nodes exist for levels `1..levels` (0 = events log only). */
  levels: number
}

/** Parse the store's `index.json` → ordered scan labels + hierarchy cap. */
export function parseDiffIndexManifest(bytes: Uint8Array, dataset?: string): DiffIndexManifest {
  const meta = JSON.parse(new TextDecoder().decode(bytes)) as { dataset: string; scans: string[]; levels: number }
  if (dataset !== undefined && meta.dataset !== dataset) {
    throw new Error(`parseDiffIndexManifest: manifest is for dataset '${meta.dataset}', not '${dataset}'`)
  }
  return { scans: meta.scans, levels: meta.levels }
}

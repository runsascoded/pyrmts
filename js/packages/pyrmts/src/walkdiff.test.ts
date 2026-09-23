// Index-free diff walk — the TS twin of `pyrmts_engine.bench_diff` over the
// same fixture as `test_bench_diff.py`: a deep change, a birth, a death, a
// net-zero rename (the walk's known blind spot), 4-row row groups.

import { parquetWriteBuffer } from 'hyparquet-writer'
import { describe, expect, test } from 'vitest'
import type { CachedMetadata } from './fetch.js'
import { memStorage } from './storage.js'
import type { Storage } from './types.js'
import { SnapshotReader, newWalkStats, renderFloor, roundTrips, walkDiff, wallModel } from './walkdiff.js'

type R = [string, number, number, number]   // path, depth, b, o

const TREE_A: R[] = [
  ['r', 1, 1000, 6], ['r/a', 2, 400, 2], ['r/b', 2, 600, 2], ['r/z', 2, 0, 0],
  ['r/a/f1', 3, 100, 1], ['r/a/f2', 3, 300, 1], ['r/b/sub', 3, 600, 1],
  ['r/b/sub/big.bin', 4, 600, 1],
]
const TREE_B: R[] = [
  ['r', 1, 2010, 7], ['r/a', 2, 400, 2], ['r/b', 2, 1600, 2], ['r/z', 2, 10, 1],
  ['r/a/f1', 3, 100, 1], ['r/a/f3', 3, 300, 1],
  ['r/b/sub', 3, 1600, 1], ['r/z/new', 3, 10, 1],
  ['r/b/sub/big.bin', 4, 1600, 1],
]

function parquetOf(rows: R[], rowGroupSize = 4): Uint8Array {
  const sorted = [...rows].sort((x, y) => x[1] - y[1] || (x[0] < y[0] ? -1 : x[0] > y[0] ? 1 : 0))
  return new Uint8Array(parquetWriteBuffer({
    columnData: [
      { name: 'path', type: 'STRING', data: sorted.map(r => r[0]) },
      { name: 'depth', type: 'INT64', data: sorted.map(r => BigInt(r[1])) },
      { name: 'b', type: 'INT64', data: sorted.map(r => BigInt(r[2])) },
      { name: 'o', type: 'INT64', data: sorted.map(r => BigInt(r[3])) },
    ],
    rowGroupSize,
  }))
}

async function pair(): Promise<Storage> {
  const s = memStorage()
  await s.put('a.parquet', parquetOf(TREE_A))
  await s.put('b.parquet', parquetOf(TREE_B))
  return s
}

const key = (rows: { path: string; status: string; sizeA: number; sizeB: number }[]) =>
  rows.map(r => [r.path, r.status, r.sizeA, r.sizeB] as const).sort((x, y) => (x[0] < y[0] ? -1 : 1))

describe('walkDiff', () => {
  test('finds the changed spines and misses the net-zero rename (the known blind spot)', async () => {
    const s = await pair()
    const stats = newWalkStats()
    const res = await walkDiff(new SnapshotReader(s, 'a.parquet', { stats }), new SnapshotReader(s, 'b.parquet', { stats }), '')
    expect(key(res.rows)).toEqual([
      ['r', 'changed', 1000, 2010],
      ['r/b', 'changed', 600, 1600],
      ['r/b/sub', 'changed', 600, 1600],
      ['r/b/sub/big.bin', 'changed', 600, 1600],
      ['r/z', 'changed', 0, 10],
      ['r/z/new', 'added', 0, 10],
    ])
    expect(res.rows.some(r => r.path === 'r/a')).toBe(false)
    // '', r, r/b + r/z, r/b/sub, big.bin (a leaf: empty listing) → 6 expansions in 5 rounds.
    expect(res.expansions).toBe(6)
    expect(res.truncated).toBe(false)
    expect(stats.listings).toBe(12)
    expect(stats.rounds).toHaveLength(5)
    expect(stats.roundMs).toHaveLength(5)
    expect(stats.rounds.reduce((a, b) => a + b, 0)).toBe(stats.gets)
    expect(stats.footerParses).toBe(2)
    expect(stats.footerGets).toBe(2)
    expect(stats.rgCacheHits).toBeGreaterThan(0)
    expect(res.rows[0]!.path).toBe('r')                       // sorted by |Δ| desc
  })

  test('render floor stops expansion and marks pruned', async () => {
    const s = await pair()
    const res = await walkDiff(new SnapshotReader(s, 'a.parquet'), new SnapshotReader(s, 'b.parquet'), '', { floor: 100 })
    const by = new Map(res.rows.map(r => [r.path, r]))
    expect(by.get('r/z')!.pruned).toBe(true)
    expect(by.get('r/z')!.expanded).toBe(false)
    expect(by.has('r/z/new')).toBe(false)
    expect(by.get('r/b')!.expanded && by.get('r/b/sub')!.expanded).toBe(true)
    expect(res.expansions).toBe(5)
    expect(renderFloor(1_000_000, 1000, 100, 10)).toBe(1000)
  })

  test('a non-root page path walks only that subtree', async () => {
    const s = await pair()
    const res = await walkDiff(new SnapshotReader(s, 'a.parquet'), new SnapshotReader(s, 'b.parquet'), 'r/b')
    expect(key(res.rows)).toEqual([
      ['r/b/sub', 'changed', 600, 1600],
      ['r/b/sub/big.bin', 'changed', 600, 1600],
    ])
    expect(res.rows.map(r => r.depth)).toEqual([1, 2])
  })

  test('best-first order gives the same rows with one expansion per round', async () => {
    const s = await pair()
    const sl = newWalkStats()
    const rl = await walkDiff(new SnapshotReader(s, 'a.parquet', { stats: sl }), new SnapshotReader(s, 'b.parquet', { stats: sl }), '')
    const sb = newWalkStats()
    const rbf = await walkDiff(new SnapshotReader(s, 'a.parquet', { stats: sb }), new SnapshotReader(s, 'b.parquet', { stats: sb }), '', { order: 'bestfirst' })
    expect(key(rl.rows)).toEqual(key(rbf.rows))
    expect(sb.rounds).toHaveLength(6)
    expect(roundTrips(sl)).toBeLessThanOrEqual(roundTrips(sb))
    expect(wallModel(sl, 30)).toBeGreaterThanOrEqual(0)
  })

  test('no RG cache re-reads shared row groups', async () => {
    const s = await pair()
    const on = newWalkStats()
    await walkDiff(new SnapshotReader(s, 'a.parquet', { stats: on }), new SnapshotReader(s, 'b.parquet', { stats: on }), '')
    const off = newWalkStats()
    await walkDiff(new SnapshotReader(s, 'a.parquet', { stats: off, rgCache: false }), new SnapshotReader(s, 'b.parquet', { stats: off, rgCache: false }), '')
    expect(off.rgCacheHits).toBe(0)
    expect(off.requests).toBe(on.requests + on.rgCacheHits)
    expect(off.gets).toBeGreaterThan(on.gets)
  })

  test('a warm metadata cache skips head + footer and guards ranges; a rewrite falls back', async () => {
    const s = await pair()
    let heads = 0
    const ifMatch: (string | undefined)[] = []
    const counting: Storage = {
      ...s,
      async head(k) { heads++; return s.head(k) },
      async getRange(k, a, b, o) { ifMatch.push(o?.ifMatch); return s.getRange(k, a, b, o) },
    }
    const metadataCache = new Map<string, CachedMetadata>()
    const s1 = newWalkStats()
    await walkDiff(new SnapshotReader(counting, 'a.parquet', { stats: s1, metadataCache }), new SnapshotReader(counting, 'b.parquet', { stats: s1, metadataCache }), '')
    expect(heads).toBe(2)
    expect(s1.footerParses).toBe(2)
    const s2 = newWalkStats()
    ifMatch.length = 0
    await walkDiff(new SnapshotReader(counting, 'a.parquet', { stats: s2, metadataCache }), new SnapshotReader(counting, 'b.parquet', { stats: s2, metadataCache }), '')
    expect(heads).toBe(2)                                       // no head on the warm path
    expect(s2.footerParses).toBe(0)
    expect(ifMatch.length).toBeGreaterThan(0)
    expect(ifMatch.every(m => m !== undefined)).toBe(true)      // every data range guarded
    await s.put('b.parquet', parquetOf(TREE_B.concat([['r/late', 2, 1, 1]])))
    const stale = new SnapshotReader(counting, 'b.parquet', { metadataCache })
    await expect(stale.children('r', 1)).rejects.toBeInstanceOf(Error)   // EtagConflict surfaces
  })

  test('node lookup', async () => {
    const s = await pair()
    const b = new SnapshotReader(s, 'b.parquet')
    expect(await b.node('r/b/sub')).toEqual({ size: 1600, count: 1 })
    expect(await b.node('nope')).toBeNull()
    expect([...(await b.children('r', 1)).entries()]).toEqual([
      ['r/a', { size: 400, count: 2 }], ['r/b', { size: 1600, count: 2 }], ['r/z', { size: 10, count: 1 }],
    ])
  })
})

describe('SnapshotReader over a pre-supplied RowGroupIndex (no footer on the edge)', () => {
  test('same listings and walk, zero head / footer traffic, per-group metadata fetched on demand', async () => {
    const { parquetMetadata } = await import('hyparquet')
    const { rowGroupIndexFromMetadata } = await import('./walkdiff.js')
    const s = await pair()
    let heads = 0
    const counting: Storage = { ...s, async head(k) { heads++; return s.head(k) } }
    const indexOf = async (key: string, served: number[]) => {
      const bytes = (await s.get(key))!
      const md = parquetMetadata(bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer)
      const full = rowGroupIndexFromMetadata(md, bytes.byteLength, (await s.head(key))!.etag)
      // Serve group metadata lazily, as a D1 lookup would, recording which groups were asked for.
      return { ...full, rowGroup: async (i: number) => { served.push(i); return full.rowGroup(i) } }
    }
    const servedA: number[] = []
    const servedB: number[] = []
    const ia = await indexOf('a.parquet', servedA)
    const ib = await indexOf('b.parquet', servedB)
    heads = 0
    const stats = newWalkStats()
    const res = await walkDiff(
      new SnapshotReader(counting, 'a.parquet', { stats, rowGroups: ia }),
      new SnapshotReader(counting, 'b.parquet', { stats, rowGroups: ib }),
      '',
    )
    const ref = await walkDiff(new SnapshotReader(s, 'a.parquet'), new SnapshotReader(s, 'b.parquet'), '')
    expect(key(res.rows)).toEqual(key(ref.rows))
    expect(heads).toBe(0)
    expect(stats.footerParses).toBe(0)
    expect(stats.footerGets).toBe(0)
    expect(stats.gets).toBeGreaterThan(0)
    expect(ia.groups).toHaveLength(2)                                   // 8 rows / 4 per group
    expect(servedA.sort()).toEqual([0, 1])                              // only touched groups, once each
    expect(new Set(servedB).size).toBe(servedB.length)
    expect(ia.groups[0]).toEqual({ rowStart: 0, numRows: 4, depthMin: 1, depthMax: 2, pathMin: 'r', pathMax: 'r/z' })
  })
})

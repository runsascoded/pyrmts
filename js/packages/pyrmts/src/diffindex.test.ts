// Flat-changeset diff-index reader — the TS twin of `pyrmts.diffindex` /
// `DiffIndexStore`. Builds the same aligned node set the Python store persists
// (level 0 = every adjacency; level L = aligned starts only), then proves
// `diffOverSpan` equals the direct 2-snapshot `changesetBetween` for EVERY pair
// at several hierarchy caps while loading only the aligned blocks covering the
// span; plus a parquet round-trip of a node and the manifest parse.

import { parquetWriteBuffer } from 'hyparquet-writer'
import { describe, expect, test } from 'vitest'
import {
  alignedBlocks,
  changesetBetween,
  changesetFromRows,
  changesetToRows,
  composeChangesets,
  diffOverSpan,
  parseDiffIndexManifest,
  readChangesetNode,
  type Changeset,
} from './diffindex.js'
import type { Metric, Pyramid, Row } from './types.js'

const SCHEMA: Pick<Pyramid, 'binCol' | 'dims' | 'metrics'> = {
  binCol: 'dt',
  dims: [{ name: 'path', type: 'string' }],
  metrics: [{ name: 'b', monoid: 'count' } as Metric, { name: 'o', monoid: 'count' } as Metric],
}

// Same churny 11-scan history as the Python tests: constant, ramp, blink
// (even scans only — remove-then-re-add), late birth, early death.
function history(): Row[][] {
  const scans: Row[][] = []
  for (let k = 0; k < 11; k++) {
    const rows: Row[] = [{ dt: 0, path: 'const', b: 100, o: 1 }, { dt: 0, path: 'ramp', b: k * 10, o: k }]
    if (k % 2 === 0) rows.push({ dt: 0, path: 'blink', b: 5, o: 1 })
    if (k >= 4) rows.push({ dt: 0, path: 'late', b: 7, o: 2 })
    if (k < 7) rows.push({ dt: 0, path: 'early', b: 3, o: 1 })
    scans.push(rows)
  }
  return scans
}

// Build the store's node set the way `DiffIndexStore.append_scan` does:
// level 0 = adjacency changesets; level L = compose of the two level L−1
// nodes at aligned starts (multiples of 2^L) only.
function buildNodes(scans: Row[][], levels: number): Map<string, Changeset> {
  const nodes = new Map<string, Changeset>()
  const n = scans.length - 1
  for (let k = 0; k < n; k++) nodes.set(`0/${k}`, changesetBetween(scans[k]!, scans[k + 1]!, SCHEMA))
  for (let level = 1; level <= levels; level++) {
    const width = 1 << level
    for (let start = 0; start + width <= n; start += width) {
      nodes.set(`${level}/${start}`, composeChangesets(nodes.get(`${level - 1}/${start}`)!, nodes.get(`${level - 1}/${start + width / 2}`)!))
    }
  }
  return nodes
}

const rowsOf = (cs: Changeset) => changesetToRows(cs, SCHEMA)

describe('alignedBlocks', () => {
  test('aligned, disjoint, capped', () => {
    expect(alignedBlocks(0, 8, 3)).toEqual([[3, 0]])
    expect(alignedBlocks(0, 7, 3)).toEqual([[2, 0], [1, 4], [0, 6]])
    expect(alignedBlocks(3, 10, 3)).toEqual([[0, 3], [2, 4], [1, 8]])
    expect(alignedBlocks(0, 8, 1)).toEqual([[1, 0], [1, 2], [1, 4], [1, 6]])
    expect(alignedBlocks(3, 10, 0)).toEqual([3, 4, 5, 6, 7, 8, 9].map(k => [0, k]))
    expect(alignedBlocks(4, 4, 3)).toEqual([])
    for (const levels of [0, 1, 2, 3, 4]) {
      for (let i = 0; i < 20; i++) {
        for (let j = i; j < 20; j++) {
          let pos = i
          for (const [level, start] of alignedBlocks(i, j, levels)) {
            expect(start).toBe(pos)
            expect(start % (1 << level)).toBe(0)
            expect(level).toBeLessThanOrEqual(levels)
            pos += 1 << level
          }
          expect(pos).toBe(j)
        }
      }
    }
  })
})

describe('diffOverSpan', () => {
  const scans = history()
  const labels = scans.map((_, k) => `s${k}`)

  for (const levels of [0, 1, 3]) {
    test(`equals the 2-snapshot diff for every pair, both directions (levels=${levels})`, async () => {
      const nodes = buildNodes(scans, levels)
      const loadNode = async (level: number, start: number) => changesetToRows(nodes.get(`${level}/${start}`)!, SCHEMA)
      for (let i = 0; i < scans.length; i++) {
        for (let j = 0; j < scans.length; j++) {
          const got = await diffOverSpan(labels, SCHEMA, labels[i]!, labels[j]!, loadNode, levels)
          expect(got).toEqual(rowsOf(changesetBetween(scans[i]!, scans[j]!, SCHEMA)))
        }
      }
    })
  }

  test('loads only the aligned blocks covering the span', async () => {
    const nodes = buildNodes(scans, 3)
    const loads: string[] = []
    const loadNode = async (level: number, start: number) => {
      loads.push(`${level}/${start}`)
      return changesetToRows(nodes.get(`${level}/${start}`)!, SCHEMA)
    }
    await diffOverSpan(labels, SCHEMA, 's0', 's8', loadNode, 3)
    expect(loads).toEqual(['3/0'])                       // one aligned 2^3 node
    loads.length = 0
    await diffOverSpan(labels, SCHEMA, 's3', 's10', loadNode, 3)
    expect(loads).toEqual(['0/3', '2/4', '1/8'])         // 1 + 4 + 2, aligned + disjoint
    loads.length = 0
    await diffOverSpan(labels, SCHEMA, 's3', 's7', loadNode, 0)
    expect(loads).toEqual(['0/3', '0/4', '0/5', '0/6'])  // events log only: the span's adjacencies
  })

  test('remove-then-re-add nets out across an even→even span', async () => {
    const nodes = buildNodes(scans, 3)
    const loadNode = async (level: number, start: number) => changesetToRows(nodes.get(`${level}/${start}`)!, SCHEMA)
    const d02 = await diffOverSpan(labels, SCHEMA, 's0', 's2', loadNode, 3)
    expect(d02.some(r => r.path === 'blink')).toBe(false)
    const d01 = await diffOverSpan(labels, SCHEMA, 's0', 's1', loadNode, 3)
    expect(d01.some(r => r.path === 'blink')).toBe(true) // present → absent: a death
  })

  test('rejects unknown scans', async () => {
    const loadNode = async () => []
    await expect(diffOverSpan(labels, SCHEMA, 's0', 'nope', loadNode)).rejects.toThrow(/not in the index/)
  })
})

describe('node parquet + manifest', () => {
  test('a persisted node round-trips through parquet with int64 → number', async () => {
    const cs = changesetBetween(history()[0]!, history()[3]!, SCHEMA)
    const rows = rowsOf(cs)
    const buf = parquetWriteBuffer({
      columnData: [
        { name: 'dt', type: 'INT64', data: rows.map(r => BigInt(r.dt as number)) },
        { name: 'path', type: 'STRING', data: rows.map(r => r.path as string) },
        { name: 'b__a', type: 'INT64', data: rows.map(r => BigInt(r.b__a as number)) },
        { name: 'o__a', type: 'INT64', data: rows.map(r => BigInt(r.o__a as number)) },
        { name: 'b__b', type: 'INT64', data: rows.map(r => BigInt(r.b__b as number)) },
        { name: 'o__b', type: 'INT64', data: rows.map(r => BigInt(r.o__b as number)) },
      ],
    })
    const back = await readChangesetNode(new Uint8Array(buf))
    expect(rowsOf(changesetFromRows(back, SCHEMA))).toEqual(rows)
  })

  test('parses index.json (scans + levels) and checks the dataset scope', () => {
    const bytes = new TextEncoder().encode(JSON.stringify({ dataset: 'dt', scans: ['s0', 's1'], levels: 2 }))
    expect(parseDiffIndexManifest(bytes, 'dt')).toEqual({ scans: ['s0', 's1'], levels: 2 })
    expect(() => parseDiffIndexManifest(bytes, 'other')).toThrow(/not 'other'/)
  })
})

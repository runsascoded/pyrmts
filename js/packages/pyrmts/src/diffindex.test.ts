// Dyadic diff-index reader — the TS twin of `pyrmts.diffindex` / `DiffIndexStore`.
// Builds the same immutable node set the Python store persists (one node per
// (level, i), composed per level), then proves `diffOverSpan` equals the direct
// 2-snapshot `changesetBetween` for EVERY pair while loading only popcount(j−i)
// nodes; plus a parquet round-trip of a node and the manifest parse.

import { parquetWriteBuffer } from 'hyparquet-writer'
import { describe, expect, test } from 'vitest'
import {
  changesetBetween,
  changesetFromRows,
  changesetToRows,
  composeChangesets,
  diffOverSpan,
  jumps,
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
// level 0 = adjacency changesets, level L = compose of two level L−1 nodes.
function buildNodes(scans: Row[][]): Map<string, Changeset> {
  const nodes = new Map<string, Changeset>()
  const deltas: Changeset[] = []
  for (let k = 0; k + 1 < scans.length; k++) deltas.push(changesetBetween(scans[k]!, scans[k + 1]!, SCHEMA))
  let level = 0
  let cur = deltas
  while (cur.length) {
    cur.forEach((cs, i) => nodes.set(`${level}/${i}`, cs))
    const width = 1 << level
    const next: Changeset[] = []
    for (let i = 0; i + 2 * width <= deltas.length; i++) next.push(composeChangesets(cur[i]!, cur[i + width]!))
    cur = next
    level++
  }
  return nodes
}

const rowsOf = (cs: Changeset) => changesetToRows(cs, SCHEMA)

describe('diffOverSpan', () => {
  const scans = history()
  const labels = scans.map((_, k) => `s${k}`)
  const nodes = buildNodes(scans)
  const loads: string[] = []
  const loadNode = async (level: number, i: number) => {
    loads.push(`${level}/${i}`)
    return changesetToRows(nodes.get(`${level}/${i}`)!, SCHEMA)
  }

  test('equals the 2-snapshot diff for every pair, both directions', async () => {
    for (let i = 0; i < scans.length; i++) {
      for (let j = 0; j < scans.length; j++) {
        const got = await diffOverSpan(labels, SCHEMA, labels[i]!, labels[j]!, loadNode)
        expect(got).toEqual(rowsOf(changesetBetween(scans[i]!, scans[j]!, SCHEMA)))
      }
    }
  })

  test('loads only the popcount(j−i) jump nodes', async () => {
    loads.length = 0
    await diffOverSpan(labels, SCHEMA, 's0', 's8', loadNode)
    expect(loads).toEqual(['3/0'])                       // one 2^3 node
    loads.length = 0
    await diffOverSpan(labels, SCHEMA, 's3', 's10', loadNode)
    expect(loads).toEqual(['0/3', '1/4', '2/6'])         // 7 = 1+2+4, disjoint
    expect(jumps(0, 7)).toEqual([[0, 0], [1, 1], [2, 3]])
  })

  test('remove-then-re-add nets out across an even→even span', async () => {
    const d02 = await diffOverSpan(labels, SCHEMA, 's0', 's2', loadNode)
    expect(d02.some(r => r.path === 'blink')).toBe(false)
    const d01 = await diffOverSpan(labels, SCHEMA, 's0', 's1', loadNode)
    expect(d01.some(r => r.path === 'blink')).toBe(true) // present → absent: a death
  })

  test('rejects unknown scans', async () => {
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

  test('parses index.json and checks the dataset scope', () => {
    const bytes = new TextEncoder().encode(JSON.stringify({ dataset: 'dt', scans: ['s0', 's1'] }))
    expect(parseDiffIndexManifest(bytes, 'dt')).toEqual(['s0', 's1'])
    expect(() => parseDiffIndexManifest(bytes, 'other')).toThrow(/not 'other'/)
  })
})

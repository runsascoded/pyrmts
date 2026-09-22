// Multi-scan read primitives — the TS twin of `pyrmts.multiscan`'s read path.
// The expected values mirror the Python `test_multiscan.py` fixture exactly
// (three scans: `a` constant, `b` changes then vanishes, `c` appears late), so
// the two implementations are pinned to the same behavior. A parquet round-trip
// (write with `pyrmts.multiscan` KV-metadata, read via `readMultiScan`) proves
// the self-describing on-disk contract the Python writer produces.

import { parquetWriteBuffer } from 'hyparquet-writer'
import { describe, expect, test } from 'vitest'
import {
  diffScans,
  diffTables,
  extractScan,
  parseMultiScanIndex,
  readMultiScan,
  resolveScan,
  seriesAcrossGroups,
  seriesFor,
  type MultiScan,
  type MultiScanIndexEntry,
} from './multiscan.js'
import type { Metric, Pyramid, Row } from './types.js'

const SCHEMA: Pick<Pyramid, 'binCol' | 'dims' | 'metrics'> = {
  binCol: 'dt',
  dims: [{ name: 'path', type: 'string' }],
  metrics: [
    { name: 'b', monoid: 'count' } as Metric,
    { name: 'o', monoid: 'count' } as Metric,
  ],
}

// The interval-encoded MultiScan for the shared fixture: `a` spans [0,2]; `b`
// splits at its change then ends before s2; `c` only s2 (O(#changes) rows).
const MS: MultiScan = {
  scans: ['s0', 's1', 's2'],
  encoder: 'interval',
  rows: [
    { dt: 0, path: 'a', b: 10, o: 1, __scan_lo: 0, __scan_hi: 2 },
    { dt: 0, path: 'b', b: 20, o: 2, __scan_lo: 0, __scan_hi: 0 },
    { dt: 0, path: 'b', b: 30, o: 3, __scan_lo: 1, __scan_hi: 1 },
    { dt: 0, path: 'c', b: 5, o: 1, __scan_lo: 2, __scan_hi: 2 },
  ],
}

const rows = (rs: Row[]): Row[] =>
  [...rs].sort((x, y) => `${x.path}${x.dt}`.localeCompare(`${y.path}${y.dt}`))

describe('extractScan', () => {
  test('reconstructs each member scan', () => {
    expect(rows(extractScan(MS, SCHEMA, 's0'))).toEqual([
      { dt: 0, path: 'a', b: 10, o: 1 },
      { dt: 0, path: 'b', b: 20, o: 2 },
    ])
    expect(rows(extractScan(MS, SCHEMA, 's1'))).toEqual([
      { dt: 0, path: 'a', b: 10, o: 1 },
      { dt: 0, path: 'b', b: 30, o: 3 },
    ])
    expect(rows(extractScan(MS, SCHEMA, 's2'))).toEqual([
      { dt: 0, path: 'a', b: 10, o: 1 },
      { dt: 0, path: 'c', b: 5, o: 1 },
    ])
  })

  test('rejects a non-member scan', () => {
    expect(() => extractScan(MS, SCHEMA, 's9')).toThrow(/not a member scan/)
  })
})

describe('seriesFor (over-time line)', () => {
  test('constant, dies, born-late', () => {
    const line = (path: string) => seriesFor(MS, SCHEMA, { dt: 0, path }).map(p => [p.scan, p.state.b, p.state.o])
    // `a` constant; `b` present then absent (→ identity 0); `c` absent then born.
    expect(line('a')).toEqual([['s0', 10, 1], ['s1', 10, 1], ['s2', 10, 1]])
    expect(line('b')).toEqual([['s0', 20, 2], ['s1', 30, 3], ['s2', 0, 0]])
    expect(line('c')).toEqual([['s0', 0, 0], ['s1', 0, 0], ['s2', 5, 1]])
  })
})

describe('diff', () => {
  test('diffTables changeset: births and deaths', () => {
    const s0 = extractScan(MS, SCHEMA, 's0')
    const s2 = extractScan(MS, SCHEMA, 's2')
    expect(diffTables(s0, s2, SCHEMA)).toEqual([
      { dt: 0, path: 'b', b__a: 20, o__a: 2, b__b: 0, o__b: 0 }, // death
      { dt: 0, path: 'c', b__a: 0, o__a: 0, b__b: 5, o__b: 1 }, // birth
    ])
  })

  test('diffScans matches extract-diff for every pair', () => {
    for (const a of MS.scans) {
      for (const b of MS.scans) {
        const viaScans = diffScans(MS, SCHEMA, a, b)
        const viaExtract = diffTables(extractScan(MS, SCHEMA, a), extractScan(MS, SCHEMA, b), SCHEMA)
        expect(viaScans).toEqual(viaExtract)
      }
    }
  })

  test('diffScans reads only changed keys in the span', () => {
    // s0→s1: only `b` changed (20,2)→(30,3); `a` constant is excluded.
    expect(diffScans(MS, SCHEMA, 's0', 's1')).toEqual([
      { dt: 0, path: 'b', b__a: 20, o__a: 2, b__b: 30, o__b: 3 },
    ])
  })
})

describe('readMultiScan', () => {
  // Write the interval table with the same columns + `pyrmts.multiscan`
  // KV-metadata the Python writer attaches, then read it back.
  function shard(): Uint8Array {
    const col = (name: string, type: 'INT64', data: unknown[]) => ({ name, type, data })
    const buf = parquetWriteBuffer({
      columnData: [
        { name: 'dt', type: 'INT64', data: MS.rows.map(r => BigInt(r.dt as number)) },
        { name: 'path', type: 'STRING', data: MS.rows.map(r => r.path as string) },
        { name: 'b', type: 'INT64', data: MS.rows.map(r => BigInt(r.b as number)) },
        { name: 'o', type: 'INT64', data: MS.rows.map(r => BigInt(r.o as number)) },
        col('__scan_lo', 'INT64', MS.rows.map(r => BigInt(r.__scan_lo as number))),
        col('__scan_hi', 'INT64', MS.rows.map(r => BigInt(r.__scan_hi as number))),
      ],
      kvMetadata: [
        {
          key: 'pyrmts.multiscan',
          value: JSON.stringify({ encoder: 'interval', scans: MS.scans, digests: { s0: 'x', s1: 'y', s2: 'z' } }),
        },
      ],
    })
    return new Uint8Array(buf)
  }

  test('round-trips rows + metadata, and the primitives work off it', async () => {
    const ms = await readMultiScan(shard())
    expect(ms.encoder).toBe('interval')
    expect(ms.scans).toEqual(['s0', 's1', 's2'])
    expect(ms.digests).toEqual({ s0: 'x', s1: 'y', s2: 'z' })
    expect(ms.rows.length).toBe(4)
    // int64 normalized to number, so primitives behave identically to in-memory.
    expect(rows(extractScan(ms, SCHEMA, 's1'))).toEqual([
      { dt: 0, path: 'a', b: 10, o: 1 },
      { dt: 0, path: 'b', b: 30, o: 3 },
    ])
    expect(diffScans(ms, SCHEMA, 's0', 's1')).toEqual([
      { dt: 0, path: 'b', b__a: 20, o__a: 2, b__b: 30, o__b: 3 },
    ])
  })

  test('rejects a parquet with no multiscan metadata', async () => {
    const buf = parquetWriteBuffer({ columnData: [{ name: 'dt', type: 'INT64', data: [0n] }] })
    await expect(readMultiScan(new Uint8Array(buf))).rejects.toThrow(/no pyrmts.multiscan metadata/)
  })
})

describe('routing manifest', () => {
  // The exact JSONL row shape `pyrmts_engine.StorageJsonlMultiScanIndex` writes
  // (snake_case keys), so the two sides speak the same on-disk manifest.
  const jsonl = [
    JSON.stringify({
      dataset: 'usage', tier: 'base', shard_dur: '1mo',
      period_start: 0, period_end: 1, key: 'p/base/1mo/2026-01.parquet',
      scans: ['s0', 's1', 's2'], encoder: 'interval', written_at: 7,
      digests: { s0: 'd0', s1: 'd1', s2: 'd2' },
    }),
    JSON.stringify({
      dataset: 'other', tier: 'base', shard_dur: '1mo',
      period_start: 0, period_end: 1, key: 'k2', scans: ['x'], encoder: 'interval', written_at: 8,
    }),
  ].join('\n') + '\n'

  test('parses + dataset-scopes the manifest', () => {
    const all = parseMultiScanIndex(new TextEncoder().encode(jsonl))
    expect(all.map(e => e.dataset)).toEqual(['usage', 'other'])
    const usage = parseMultiScanIndex(new TextEncoder().encode(jsonl), 'usage')
    expect(usage).toHaveLength(1)
    expect(usage[0]).toMatchObject({ shardDur: '1mo', periodStart: 0, key: 'p/base/1mo/2026-01.parquet' })
  })

  test('resolveScan routes to the archive with fold index, else null', () => {
    const usage = parseMultiScanIndex(new TextEncoder().encode(jsonl), 'usage')
    expect(resolveScan(usage, 's2')).toEqual({
      key: 'p/base/1mo/2026-01.parquet', foldIndex: 2, encoder: 'interval',
    })
    expect(resolveScan(usage, 's9')).toBeNull() // not consolidated → caller falls back
  })
})

describe('seriesAcrossGroups (capped-K stitching)', () => {
  // Two sealed groups covering a path 'a' over four scans.
  const groupA: MultiScan = {
    scans: ['s0', 's1'], encoder: 'interval',
    rows: [{ dt: 0, path: 'a', b: 10, o: 1, __scan_lo: 0, __scan_hi: 1 }],
  }
  const groupB: MultiScan = {
    scans: ['s2', 's3'], encoder: 'interval',
    rows: [
      { dt: 0, path: 'a', b: 20, o: 2, __scan_lo: 0, __scan_hi: 0 },
      { dt: 0, path: 'a', b: 30, o: 3, __scan_lo: 1, __scan_hi: 1 },
    ],
  }
  const entry = (key: string, periodStart: number, scans: string[]): MultiScanIndexEntry => ({
    dataset: 'ot', tier: 'base', shardDur: '1mo', periodStart, periodEnd: periodStart + 1,
    key, scans, encoder: 'interval', writtenAt: 0,
  })

  test('orders groups by scan span and concatenates each group series', async () => {
    // Entries deliberately out of order — seriesAcrossGroups sorts by periodStart.
    const entries = [entry('B', 100, ['s2', 's3']), entry('A', 0, ['s0', 's1'])]
    const load = async (k: string) => (k === 'A' ? groupA : groupB)
    const line = await seriesAcrossGroups(entries, SCHEMA, { dt: 0, path: 'a' }, load)
    expect(line.map(p => [p.scan, p.state.b, p.state.o])).toEqual([
      ['s0', 10, 1], ['s1', 10, 1], ['s2', 20, 2], ['s3', 30, 3],
    ])
  })

  test('a key-filtered PARTIAL MultiScan yields the same series (footer-pruned load)', () => {
    // A group holding many paths; the pruned load returns only 'a'-key rows +
    // the full scans list. seriesFor must be identical to the full-group read.
    const full: MultiScan = {
      scans: ['s0', 's1'], encoder: 'interval',
      rows: [
        { dt: 0, path: 'a', b: 10, o: 1, __scan_lo: 0, __scan_hi: 1 },
        { dt: 0, path: 'zzz', b: 99, o: 9, __scan_lo: 0, __scan_hi: 1 }, // other keys
        { dt: 0, path: 'mmm', b: 77, o: 7, __scan_lo: 0, __scan_hi: 0 },
      ],
    }
    const partial: MultiScan = {
      scans: ['s0', 's1'], encoder: 'interval',
      rows: [{ dt: 0, path: 'a', b: 10, o: 1, __scan_lo: 0, __scan_hi: 1 }], // only 'a'
    }
    expect(seriesFor(partial, SCHEMA, { dt: 0, path: 'a' }))
      .toEqual(seriesFor(full, SCHEMA, { dt: 0, path: 'a' }))
  })
})

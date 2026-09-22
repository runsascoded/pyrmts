// MultiScanD1Index — reads the `pyramid_multiscans` routing manifest from D1
// and maps rows to `MultiScanIndexEntry` (parsing the JSON scans/digests), so
// `resolveScan` (from pyrmts) routes over D1 exactly as over the JSONL manifest.

import { resolveScan } from 'pyrmts'
import { describe, expect, test } from 'vitest'
import type { D1Like, D1PreparedStatement } from './d1.js'
import { MultiScanD1Index, multiScanDdl, multiScanEntryFromRow } from './multiscan-index.js'

interface Call { sql: string; binds: unknown[] }

// Minimal D1Like returning preset rows, capturing the last (sql, binds).
function mockD1(rows: unknown[]): { db: D1Like; calls: Call[] } {
  const calls: Call[] = []
  const db: D1Like = {
    prepare(sql: string): D1PreparedStatement {
      const call: Call = { sql, binds: [] }
      const stmt: D1PreparedStatement = {
        bind(...values: unknown[]) { call.binds = values; return stmt },
        async all<T>() { calls.push(call); return { results: rows as T[] } },
      }
      return stmt
    },
  }
  return { db, calls }
}

const ROW = {
  dataset: 'over-time', tier: 'base', shard_dur: '1mo',
  period_start: 0, period_end: 100, key: 'p/base/1mo/2026-01--s0.parquet',
  scans: '["s0","s1"]', encoder: 'interval', digests: '{"s0":"d0","s1":"d1"}', written_at: 7,
}

describe('multiScanDdl', () => {
  test('creates pyramid_multiscans with the (dataset, key) PK', () => {
    const ddl = multiScanDdl()
    expect(ddl.startsWith('CREATE TABLE IF NOT EXISTS "pyramid_multiscans"')).toBe(true)
    expect(ddl).toContain('PRIMARY KEY (dataset, key)')
  })
})

describe('multiScanEntryFromRow', () => {
  test('parses JSON scans + digests', () => {
    expect(multiScanEntryFromRow(ROW)).toEqual({
      dataset: 'over-time', tier: 'base', shardDur: '1mo', periodStart: 0, periodEnd: 100,
      key: 'p/base/1mo/2026-01--s0.parquet', scans: ['s0', 's1'], encoder: 'interval',
      writtenAt: 7, digests: { s0: 'd0', s1: 'd1' },
    })
  })

  test('null digests → omitted', () => {
    const e = multiScanEntryFromRow({ ...ROW, digests: null })
    expect('digests' in e).toBe(false)
  })
})

describe('MultiScanD1Index.listMultiScans', () => {
  test('scopes by dataset and yields resolvable entries', async () => {
    const { db, calls } = mockD1([ROW])
    const entries = await new MultiScanD1Index(db).listMultiScans('over-time')
    expect(calls[0].sql).toContain('FROM "pyramid_multiscans" WHERE dataset = ?')
    expect(calls[0].binds).toEqual(['over-time'])
    // The entries route via pyrmts' resolveScan — s1 → this archive at fold 1.
    expect(resolveScan(entries, 's1')).toEqual({
      key: 'p/base/1mo/2026-01--s0.parquet', foldIndex: 1, encoder: 'interval',
    })
  })

  test('applies tier + range filter', async () => {
    const { db, calls } = mockD1([])
    await new MultiScanD1Index(db).listMultiScans('over-time', { tier: 'base', range: { from: 10, to: 20 } })
    expect(calls[0].sql).toContain('tier = ?')
    expect(calls[0].sql).toContain('period_end > ?')
    expect(calls[0].binds).toEqual(['over-time', 'base', 10, 20])
  })
})

// D1ShardIndex unit tests. Mocks `D1Like` and records every (sql, binds)
// tuple so tests can assert on the wire format directly.
//
// `mockD1WithStore` (bottom of file) goes a step further — it actually
// executes the two SQL templates `D1ShardIndex` emits, so the shared
// `assertShardIndexConformance` suite verifies real semantics (monotonic
// upserts, watermark visibility), not just SQL strings.

import type { RecordShardInput } from 'pyrmts'
import { assertShardIndexConformance } from 'pyrmts/test-utils'
import { describe, expect, test } from 'vitest'
import type { D1Like, D1PreparedStatement } from './d1.js'
import { D1ShardIndex } from './shard-index.js'

interface PrepareCall {
  sql: string
  binds: unknown[]
  method: 'all' | 'run' | 'batch'
}

interface MockD1Options {
  // Rows the next `all()` returns. Pop-front; if empty, returns [].
  rowsQueue?: Record<string, unknown>[][]
  // Provide `batch` on the mock (default true). Some tests verify the
  // fallback path when `batch` is undefined.
  withBatch?: boolean
}

function mockD1(opts: MockD1Options = {}): { db: D1Like; calls: PrepareCall[] } {
  const calls: PrepareCall[] = []
  const rowsQueue = opts.rowsQueue ?? []

  function makeStmt(sql: string): D1PreparedStatement {
    const call: PrepareCall = { sql, binds: [], method: 'all' }
    const stmt: D1PreparedStatement = {
      bind(...values: unknown[]) {
        call.binds = values
        return stmt
      },
      async all() {
        call.method = 'all'
        calls.push(call)
        const rows = rowsQueue.shift() ?? []
        return { results: rows as never[] }
      },
      async run() {
        call.method = 'run'
        calls.push(call)
        return { success: true }
      },
    }
    // Stash the call ref so batch() can finalize it without going through
    // bind/all/run directly.
    ;(stmt as unknown as { __call: PrepareCall }).__call = call
    return stmt
  }

  const db: D1Like = {
    prepare(sql: string) {
      return makeStmt(sql)
    },
  }
  if ((opts.withBatch ?? true)) {
    db.batch = async (statements: D1PreparedStatement[]) => {
      for (const s of statements) {
        const c = (s as unknown as { __call: PrepareCall }).__call
        c.method = 'batch'
        calls.push(c)
      }
      return statements.map(() => ({ success: true }))
    }
  }
  return { db, calls }
}

function makeRecordInput(overrides: Partial<RecordShardInput> = {}): RecordShardInput {
  return {
    pyramidName: 'avail',
    tier: '15m',
    shardDur: '1h',
    periodStart: new Date('2026-06-21T14:00:00Z'),
    periodEnd: new Date('2026-06-21T15:00:00Z'),
    key: 'avail/15m/p1h/2026-06-21T14.parquet',
    ...overrides,
  }
}

describe('D1ShardIndex.getWatermarks', () => {
  test('emits a single SELECT bound to pyramidName', async () => {
    const { db, calls } = mockD1({ rowsQueue: [[]] })
    const idx = new D1ShardIndex(db, { now: () => 0 })
    const result = await idx.getWatermarks('avail')
    expect(calls).toEqual([{
      sql: 'SELECT tier, shard_dur, latest_period_end FROM "pyramid_watermarks" WHERE pyramid = ?',
      binds: ['avail'],
      method: 'all',
    }])
    expect(result).toEqual(new Map())
  })

  test('decodes (tier, shard_dur) rows into uniform `${tier}@${shardDur}` keys', async () => {
    const { db } = mockD1({
      rowsQueue: [[
        { tier: '15m', shard_dur: '1h', latest_period_end: 1_700_000_000_000 },
        { tier: '15m', shard_dur: '1d', latest_period_end: 1_700_000_000_000 },
        { tier: '2m', shard_dur: '10min', latest_period_end: 1_700_000_000_000 },
      ]],
    })
    const idx = new D1ShardIndex(db, { now: () => 0 })
    const result = await idx.getWatermarks('avail')
    expect(Array.from(result.keys()).sort()).toEqual(['15m@1d', '15m@1h', '2m@10min'])
  })

  test('multiple (tier, shardDur) rows coexist in one map', async () => {
    const { db } = mockD1({
      rowsQueue: [[
        { tier: '15m', shard_dur: '15m', latest_period_end: 100 },
        { tier: '15m', shard_dur: '1h', latest_period_end: 200 },
        { tier: '15m', shard_dur: '1d', latest_period_end: 300 },
      ]],
    })
    const idx = new D1ShardIndex(db, { now: () => 0 })
    const result = await idx.getWatermarks('avail')
    expect(result).toEqual(new Map<string, Date>([
      ['15m@15m', new Date(100)],
      ['15m@1h', new Date(200)],
      ['15m@1d', new Date(300)],
    ]))
  })

  test('custom watermarksTable propagates into SQL', async () => {
    const { db, calls } = mockD1({ rowsQueue: [[]] })
    const idx = new D1ShardIndex(db, { watermarksTable: 'tenant1_watermarks', now: () => 0 })
    await idx.getWatermarks('avail')
    expect(calls[0]!.sql).toBe(
      'SELECT tier, shard_dur, latest_period_end FROM "tenant1_watermarks" WHERE pyramid = ?',
    )
  })
})

describe('D1ShardIndex.recordShard', () => {
  test('shardDur binds as-is into the upsert', async () => {
    const { db, calls } = mockD1()
    const idx = new D1ShardIndex(db, { now: () => 999 })
    await idx.recordShard(makeRecordInput({ shardDur: '1h' }))
    expect(calls).toHaveLength(2)
    // First: watermark upsert.
    expect(calls[0]!.binds).toEqual([
      'avail', '15m', '1h',
      new Date('2026-06-21T15:00:00Z').getTime(),
      999,
    ])
    // Second: shards inventory.
    expect(calls[1]!.binds).toEqual([
      'avail', '15m', '1h',
      new Date('2026-06-21T14:00:00Z').getTime(),
      new Date('2026-06-21T15:00:00Z').getTime(),
      'avail/15m/p1h/2026-06-21T14.parquet',
      999,
    ])
  })

  test('shardDur defaults via makeRecordInput bind into first 3 positions', async () => {
    const { db, calls } = mockD1()
    const idx = new D1ShardIndex(db, { now: () => 0 })
    await idx.recordShard(makeRecordInput({ shardDur: '1d' }))
    expect(calls[0]!.binds.slice(0, 3)).toEqual(['avail', '15m', '1d'])
    expect(calls[1]!.binds.slice(0, 3)).toEqual(['avail', '15m', '1d'])
  })

  test('watermark upsert uses MAX(existing, new) — monotonic guard', async () => {
    const { db, calls } = mockD1()
    const idx = new D1ShardIndex(db, { now: () => 0 })
    await idx.recordShard(makeRecordInput())
    expect(calls[0]!.sql).toBe(
      'INSERT INTO "pyramid_watermarks" ' +
      '(pyramid, tier, shard_dur, latest_period_end, updated_at) VALUES (?, ?, ?, ?, ?) ' +
      'ON CONFLICT(pyramid, tier, shard_dur) DO UPDATE SET ' +
      'latest_period_end = MAX(excluded.latest_period_end, "pyramid_watermarks".latest_period_end), ' +
      'updated_at = excluded.updated_at',
    )
  })

  test('inventory upsert overwrites period_end / key / written_at on conflict', async () => {
    const { db, calls } = mockD1()
    const idx = new D1ShardIndex(db, { now: () => 0 })
    await idx.recordShard(makeRecordInput())
    expect(calls[1]!.sql).toBe(
      'INSERT INTO "pyramid_shards" ' +
      '(pyramid, tier, shard_dur, period_start, period_end, key, written_at) VALUES (?, ?, ?, ?, ?, ?, ?) ' +
      'ON CONFLICT(pyramid, tier, shard_dur, period_start) DO UPDATE SET ' +
      'period_end = excluded.period_end, key = excluded.key, written_at = excluded.written_at',
    )
  })

  test('uses db.batch when available — atomic 2-statement transaction', async () => {
    const { db, calls } = mockD1({ withBatch: true })
    const idx = new D1ShardIndex(db, { now: () => 0 })
    await idx.recordShard(makeRecordInput())
    expect(calls.map(c => c.method)).toEqual(['batch', 'batch'])
  })

  test('falls back to sequential run() when db.batch is undefined', async () => {
    const { db, calls } = mockD1({ withBatch: false })
    const idx = new D1ShardIndex(db, { now: () => 0 })
    await idx.recordShard(makeRecordInput())
    expect(calls.map(c => c.method)).toEqual(['run', 'run'])
  })

  test('skipInventory=true: only writes the watermark upsert', async () => {
    const { db, calls } = mockD1({ withBatch: true })
    const idx = new D1ShardIndex(db, { skipInventory: true, now: () => 0 })
    await idx.recordShard(makeRecordInput())
    expect(calls).toHaveLength(1)
    expect(calls[0]!.sql).toContain('"pyramid_watermarks"')
  })

  test('custom shardsTable propagates into the inventory upsert', async () => {
    const { db, calls } = mockD1()
    const idx = new D1ShardIndex(db, { shardsTable: 'tenant1_shards', now: () => 0 })
    await idx.recordShard(makeRecordInput())
    expect(calls[1]!.sql).toBe(
      'INSERT INTO "tenant1_shards" ' +
      '(pyramid, tier, shard_dur, period_start, period_end, key, written_at) VALUES (?, ?, ?, ?, ?, ?, ?) ' +
      'ON CONFLICT(pyramid, tier, shard_dur, period_start) DO UPDATE SET ' +
      'period_end = excluded.period_end, key = excluded.key, written_at = excluded.written_at',
    )
  })

  test('now() drives both updated_at and written_at to the same value', async () => {
    const { db, calls } = mockD1()
    const idx = new D1ShardIndex(db, { now: () => 12345 })
    await idx.recordShard(makeRecordInput())
    expect(calls[0]!.binds[4]).toBe(12345)
    expect(calls[1]!.binds[6]).toBe(12345)
  })
})

describe('D1ShardIndex.schemaSql', () => {
  test('default schema returns watermarks + shards DDL', () => {
    expect(D1ShardIndex.schemaSql()).toEqual([
      'CREATE TABLE IF NOT EXISTS "pyramid_watermarks" (\n' +
      '  pyramid TEXT NOT NULL,\n' +
      '  tier TEXT NOT NULL,\n' +
      '  shard_dur TEXT NOT NULL,\n' +
      '  latest_period_end INTEGER NOT NULL,\n' +
      '  updated_at INTEGER NOT NULL,\n' +
      '  PRIMARY KEY (pyramid, tier, shard_dur)\n' +
      ') WITHOUT ROWID',
      'CREATE TABLE IF NOT EXISTS "pyramid_shards" (\n' +
      '  pyramid TEXT NOT NULL,\n' +
      '  tier TEXT NOT NULL,\n' +
      '  shard_dur TEXT NOT NULL,\n' +
      '  period_start INTEGER NOT NULL,\n' +
      '  period_end INTEGER NOT NULL,\n' +
      '  key TEXT NOT NULL,\n' +
      '  written_at INTEGER NOT NULL,\n' +
      '  PRIMARY KEY (pyramid, tier, shard_dur, period_start)\n' +
      ') WITHOUT ROWID',
    ])
  })

  test('skipInventory omits the shards DDL', () => {
    const stmts = D1ShardIndex.schemaSql({ skipInventory: true })
    expect(stmts).toHaveLength(1)
    expect(stmts[0]).toContain('"pyramid_watermarks"')
  })

  test('custom table names propagate', () => {
    const stmts = D1ShardIndex.schemaSql({
      watermarksTable: 't1_watermarks',
      shardsTable: 't1_shards',
    })
    expect(stmts[0]).toContain('"t1_watermarks"')
    expect(stmts[1]).toContain('"t1_shards"')
  })

  test('quotes identifiers with embedded double-quotes', () => {
    const stmts = D1ShardIndex.schemaSql({
      watermarksTable: 'w"ater',
      skipInventory: true,
    })
    expect(stmts[0]).toContain('"w""ater"')
  })
})

// In-memory `D1Like` that actually executes the two SQL templates
// `D1ShardIndex` emits. Lets the shared `assertShardIndexConformance`
// verify monotonic upsert behavior end-to-end, not just SQL strings.
function mockD1WithStore(): { db: D1Like } {
  interface WatermarkRow {
    pyramid: string
    tier: string
    shard_dur: string
    latest_period_end: number
    updated_at: number
  }
  interface ShardRow {
    pyramid: string
    tier: string
    shard_dur: string
    period_start: number
    period_end: number
    key: string
    written_at: number
  }
  const watermarks = new Map<string, WatermarkRow>()
  const shards = new Map<string, ShardRow>()
  const wmKey = (p: string, t: string, s: string): string => `${p}|${t}|${s}`
  const shardKey = (p: string, t: string, s: string, ps: number): string =>
    `${p}|${t}|${s}|${ps}`

  function makeStmt(sql: string): D1PreparedStatement {
    let binds: unknown[] = []
    const exec = (): { results: unknown[] } => {
      if (sql.startsWith('SELECT tier, shard_dur, latest_period_end')) {
        const pyramid = binds[0] as string
        const results: WatermarkRow[] = []
        for (const row of watermarks.values()) {
          if (row.pyramid === pyramid) results.push(row)
        }
        return { results }
      }
      if (sql.startsWith('SELECT tier, shard_dur, period_start, period_end, key')) {
        // Order of binds follows the WHERE clauses emitted by
        // `D1ShardIndex.listShards`: pyramid, then optional tier, then
        // optional (period_end > from, period_start < to). Detect from SQL
        // substrings to stay in step with the real impl.
        let i = 0
        const pyramid = binds[i++] as string
        const tierFilter = sql.includes('tier = ?') ? binds[i++] as string : null
        const rangeFrom = sql.includes('period_end > ?') ? binds[i++] as number : null
        const rangeTo = sql.includes('period_start < ?') ? binds[i++] as number : null
        const results: ShardRow[] = []
        for (const row of shards.values()) {
          if (row.pyramid !== pyramid) continue
          if (tierFilter !== null && row.tier !== tierFilter) continue
          if (rangeFrom !== null && row.period_end <= rangeFrom) continue
          if (rangeTo !== null && row.period_start >= rangeTo) continue
          results.push(row)
        }
        return { results }
      }
      if (sql.includes('"pyramid_watermarks"') && sql.startsWith('INSERT INTO')) {
        const [pyramid, tier, shard_dur, latest_period_end, updated_at] =
          binds as [string, string, string, number, number]
        const key = wmKey(pyramid, tier, shard_dur)
        const existing = watermarks.get(key)
        const newEnd = existing !== undefined
          ? Math.max(existing.latest_period_end, latest_period_end)
          : latest_period_end
        watermarks.set(key, { pyramid, tier, shard_dur, latest_period_end: newEnd, updated_at })
        return { results: [] }
      }
      if (sql.includes('"pyramid_shards"') && sql.startsWith('INSERT INTO')) {
        const [pyramid, tier, shard_dur, period_start, period_end, key, written_at] =
          binds as [string, string, string, number, number, string, number]
        shards.set(shardKey(pyramid, tier, shard_dur, period_start), {
          pyramid, tier, shard_dur, period_start, period_end, key, written_at,
        })
        return { results: [] }
      }
      return { results: [] }
    }
    const stmt: D1PreparedStatement = {
      bind(...values: unknown[]) {
        binds = values
        return stmt
      },
      async all() {
        return exec() as { results: never[] }
      },
      async run() {
        exec()
        return { success: true }
      },
    }
    return stmt
  }

  const db: D1Like = {
    prepare(sql: string) {
      return makeStmt(sql)
    },
    async batch(stmts: D1PreparedStatement[]) {
      const results: unknown[] = []
      for (const s of stmts) {
        if (s.run !== undefined) results.push(await s.run())
      }
      return results
    },
  }
  return { db }
}

describe('D1ShardIndex (inventory on)', () => {
  assertShardIndexConformance(
    () => {
      const { db } = mockD1WithStore()
      return new D1ShardIndex(db, { now: () => 0 })
    },
    { inventory: true },
  )
})

describe('D1ShardIndex (inventory off)', () => {
  assertShardIndexConformance(
    () => {
      const { db } = mockD1WithStore()
      return new D1ShardIndex(db, { now: () => 0, skipInventory: true })
    },
    { inventory: false },
  )
})

// D1ShardIndex against a real SQLite engine (`node:sqlite`), exercising
// the actual DDL from `schemaSql()` — including the `(pyramid, period_end)`
// secondary index — and asserting on `EXPLAIN QUERY PLAN` output.
//
// The mock-based tests in `shard-index.test.ts` verify the wire format;
// these verify the plan. The failure mode this file exists to prevent is a
// windowed `listShards` regressing to a partition scan: `PRIMARY KEY
// (pyramid, tier, shard_dur, period_start)` can only seek on `pyramid`
// when neither `tier` nor `shard_dur` is pinned, reading O(shards in
// pyramid) rows per call (see `specs/d1-shard-index-temporal.md`).

import type { DatabaseSync } from 'node:sqlite'
import type { RecordedShard } from 'pyrmts'

// `node:sqlite` is a prefix-only builtin; Vite's ssr resolver strips the
// prefix and fails to resolve it, so load it via `process.getBuiltinModule`
// (which bypasses the module graph entirely) instead of a static import.
const { DatabaseSync: SqliteDatabase } = process.getBuiltinModule('node:sqlite')
import { afterEach, beforeEach, describe, expect, test } from 'vitest'
import type { D1Like, D1PreparedStatement } from './d1.js'
import { D1ShardIndex } from './shard-index.js'

// Minimal `D1Like` over `node:sqlite`, recording each SELECT's (sql, binds)
// so tests can re-run the exact query under `EXPLAIN QUERY PLAN`.
function sqliteD1(db: DatabaseSync): { d1: D1Like; selects: { sql: string; binds: unknown[] }[] } {
  const selects: { sql: string; binds: unknown[] }[] = []
  const d1: D1Like = {
    prepare(sql: string) {
      let binds: (string | number)[] = []
      const stmt: D1PreparedStatement = {
        bind(...values: unknown[]) {
          binds = values as (string | number)[]
          return stmt
        },
        async all<T>() {
          selects.push({ sql, binds })
          const results = db.prepare(sql).all(...binds) as T[]
          return { results }
        },
        async run() {
          db.prepare(sql).run(...binds)
          return { success: true }
        },
      }
      return stmt
    },
    async batch(statements: D1PreparedStatement[]) {
      const results: unknown[] = []
      for (const s of statements) {
        results.push(await s.run!())
      }
      return results
    },
  }
  return { d1, selects }
}

function planFor(db: DatabaseSync, sql: string, binds: unknown[]): string[] {
  const rows = db.prepare(`EXPLAIN QUERY PLAN ${sql}`).all(...binds as (string | number)[]) as {
    detail: string
  }[]
  return rows.map(r => r.detail)
}

const HOUR = 3_600_000

// 3 tiers × 200 hourly shards spread across ~200h of history, so a narrow
// window matches a small fraction and a scan-vs-seek plan difference is
// semantically meaningful (not just two ways to read 3 rows).
async function seed(idx: D1ShardIndex): Promise<void> {
  for (const tier of ['15m', '1h', '1d']) {
    for (let i = 0; i < 200; i++) {
      await idx.recordShard({
        pyramidName: 'avail',
        tier,
        shardDur: '1h',
        periodStart: new Date(i * HOUR),
        periodEnd: new Date((i + 1) * HOUR),
        key: `avail/${tier}/p1h/${i}.parquet`,
      })
    }
  }
  // A second pyramid, to confirm the leading `pyramid` column isolates it.
  await idx.recordShard({
    pyramidName: 'rides',
    tier: '1h',
    shardDur: '1h',
    periodStart: new Date(0),
    periodEnd: new Date(HOUR),
    key: 'rides/1h/p1h/0.parquet',
  })
}

function sortShards(shards: RecordedShard[]): RecordedShard[] {
  return [...shards].sort((a, b) =>
    a.tier.localeCompare(b.tier) || a.periodStart.getTime() - b.periodStart.getTime(),
  )
}

describe('D1ShardIndex on real SQLite', () => {
  let db: DatabaseSync
  let idx: D1ShardIndex
  let selects: { sql: string; binds: unknown[] }[]

  beforeEach(async () => {
    db = new SqliteDatabase(':memory:')
    for (const stmt of D1ShardIndex.schemaSql()) db.exec(stmt)
    const wrapped = sqliteD1(db)
    selects = wrapped.selects
    idx = new D1ShardIndex(wrapped.d1, { now: () => 0 })
    await seed(idx)
  })

  afterEach(() => {
    db.close()
  })

  test('windowed listShards seeks via the period index, not the PK', async () => {
    await idx.listShards('avail', {
      range: { from: new Date(150 * HOUR), to: new Date(151 * HOUR) },
    })
    const { sql, binds } = selects[selects.length - 1]!
    expect(planFor(db, sql, binds)).toEqual([
      'SEARCH pyramid_shards USING INDEX pyramid_shards_period (pyramid=? AND period_end>?)',
    ])
  })

  test('narrow window returns exactly the intersecting shards, identically to a no-index table', async () => {
    // Intersection semantics: period_end > from AND period_start < to, so a
    // [150h, 152h) window matches hours 150 and 151 in each of 3 tiers.
    const windowed = await idx.listShards('avail', {
      range: { from: new Date(150 * HOUR), to: new Date(152 * HOUR) },
    })
    expect(sortShards(windowed)).toEqual([
      ...['15m', '1d', '1h'].flatMap(tier => [150, 151].map(i => ({
        tier,
        shardDur: '1h',
        periodStart: new Date(i * HOUR),
        periodEnd: new Date((i + 1) * HOUR),
        key: `avail/${tier}/p1h/${i}.parquet`,
        writtenAt: new Date(0),
      }))),
    ])
    // The index must change only the plan: drop it and re-run the same query.
    db.exec('DROP INDEX "pyramid_shards_period"')
    const unindexed = await idx.listShards('avail', {
      range: { from: new Date(150 * HOUR), to: new Date(152 * HOUR) },
    })
    expect(sortShards(unindexed)).toEqual(sortShards(windowed))
  })

  test('tier-pinned windowed query returns the same rows regardless of chosen index', async () => {
    const rows = await idx.listShards('avail', {
      tier: '1h',
      range: { from: new Date(10 * HOUR), to: new Date(12 * HOUR) },
    })
    expect(sortShards(rows)).toEqual([10, 11].map(i => ({
      tier: '1h',
      shardDur: '1h',
      periodStart: new Date(i * HOUR),
      periodEnd: new Date((i + 1) * HOUR),
      key: `avail/1h/p1h/${i}.parquet`,
      writtenAt: new Date(0),
    })))
  })

  test('whole-history window returns every shard in the pyramid (degenerate case)', async () => {
    const rows = await idx.listShards('avail', {
      range: { from: new Date(0), to: new Date(1000 * HOUR) },
    })
    expect(rows).toHaveLength(600)
    const unwindowed = await idx.listShards('avail')
    expect(sortShards(rows)).toEqual(sortShards(unwindowed))
  })

  test('schemaSql() re-applied to a populated database is a no-op migration', async () => {
    for (const stmt of D1ShardIndex.schemaSql()) db.exec(stmt)
    const rows = await idx.listShards('avail')
    expect(rows).toHaveLength(600)
  })
})

describe('D1ShardIndex.verifySchema on real SQLite', () => {
  // Real SQLite here rather than a mock, because the whole value of
  // `verifySchema` is agreeing with what SQLite actually reports through
  // `sqlite_master` / `PRAGMA table_info` / `PRAGMA index_info`.
  let db: DatabaseSync
  let d1: D1Like

  beforeEach(() => {
    db = new SqliteDatabase(':memory:')
    d1 = sqliteD1(db).d1
  })

  afterEach(() => {
    db.close()
  })

  function applySchema(): void {
    for (const stmt of D1ShardIndex.schemaSql()) db.exec(stmt)
  }

  test('a database built from schemaSql() verifies clean', async () => {
    applySchema()
    expect(await D1ShardIndex.verifySchema(d1)).toEqual({
      ok: true, missing: [], mismatched: [],
    })
  })

  test('an empty database reports every object missing', async () => {
    expect(await D1ShardIndex.verifySchema(d1)).toEqual({
      ok: false,
      missing: ['pyramid_watermarks', 'pyramid_shards', 'pyramid_shards_period'],
      mismatched: [],
    })
  })

  test('a pre-index deployment reports exactly the missing index', async () => {
    // The state both consumers were in: tables provisioned before
    // `pyramid_shards_period` was added to the DDL.
    applySchema()
    db.exec('DROP INDEX "pyramid_shards_period"')
    expect(await D1ShardIndex.verifySchema(d1)).toEqual({
      ok: false, missing: ['pyramid_shards_period'], mismatched: [],
    })
  })

  test('an index on the wrong columns is mismatched, not ok', async () => {
    // `(period_end, pyramid)` cannot serve the windowed seek that
    // `(pyramid, period_end)` does, so it must not read as equivalent.
    db.exec(D1ShardIndex.schemaSql()[0]!)
    db.exec(D1ShardIndex.schemaSql()[1]!)
    db.exec('CREATE INDEX "pyramid_shards_period" ON "pyramid_shards" (period_end, pyramid)')
    expect(await D1ShardIndex.verifySchema(d1)).toEqual({
      ok: false,
      missing: [],
      mismatched: ['pyramid_shards_period: expected=["pyramid","period_end"] actual=["period_end","pyramid"]'],
    })
  })

  test('a table missing a column is mismatched', async () => {
    db.exec(
      'CREATE TABLE "pyramid_watermarks" (pyramid TEXT NOT NULL, tier TEXT NOT NULL, ' +
      'shard_dur TEXT NOT NULL, latest_period_end INTEGER NOT NULL, ' +
      'PRIMARY KEY (pyramid, tier, shard_dur)) WITHOUT ROWID',
    )
    db.exec(D1ShardIndex.schemaSql()[1]!)
    db.exec(D1ShardIndex.schemaSql()[2]!)
    expect(await D1ShardIndex.verifySchema(d1)).toEqual({
      ok: false,
      missing: [],
      mismatched: ['pyramid_watermarks: expected=["latest_period_end","pyramid","shard_dur","tier","updated_at"] actual=["latest_period_end","pyramid","shard_dur","tier"]'],
    })
  })

  test('skipInventory verifies only the watermarks table', async () => {
    db.exec(D1ShardIndex.schemaSql({ skipInventory: true })[0]!)
    expect(await D1ShardIndex.verifySchema(d1, { skipInventory: true })).toEqual({
      ok: true, missing: [], mismatched: [],
    })
    // …while the full expectation still flags the absent inventory objects.
    expect((await D1ShardIndex.verifySchema(d1)).missing).toEqual([
      'pyramid_shards', 'pyramid_shards_period',
    ])
  })

  test('custom table names are verified under their own names', async () => {
    const opts = { shardsTable: 't1_shards' }
    for (const stmt of D1ShardIndex.schemaSql(opts)) db.exec(stmt)
    expect(await D1ShardIndex.verifySchema(d1, opts)).toEqual({
      ok: true, missing: [], mismatched: [],
    })
  })
})

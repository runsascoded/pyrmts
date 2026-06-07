// d1Backend unit tests: SQL generation, parameter binding, chunking.
//
// We mock the D1Database surface with an in-memory implementation that
// records every prepared SQL + bound parameters, so tests can assert on
// the wire format directly. No real D1 is exercised.

import type { FetchSegment, Tier } from 'pyrmts'
import { describe, expect, test } from 'vitest'
import { d1Backend, type D1Like, type D1PreparedStatement } from './d1.js'

interface Call {
  sql: string
  binds: unknown[]
}

interface MockResults {
  // SQL substring → rows to return. Matches by `sql.includes(key)`.
  byContains?: { match: string; rows: Record<string, unknown>[] }[]
  // Default rows if nothing matches.
  defaultRows?: Record<string, unknown>[]
}

function mockD1(results: MockResults = {}): { db: D1Like; calls: Call[] } {
  const calls: Call[] = []
  const db: D1Like = {
    prepare(sql: string): D1PreparedStatement {
      const call: Call = { sql, binds: [] }
      const stmt: D1PreparedStatement = {
        bind(...values: unknown[]) {
          call.binds = values
          return stmt
        },
        async all() {
          calls.push(call)
          const matchedRows = results.byContains?.find(r => call.sql.includes(r.match))?.rows
          return { results: (matchedRows ?? results.defaultRows ?? []) as never[] }
        },
      }
      return stmt
    },
  }
  return { db, calls }
}

const TIER_1MO: Tier = { name: '1mo', bin: '1mo', shard: 'all' }
const TIER_1D: Tier = { name: '1d', bin: '1d', shard: 'all' }

function seg(tier: Tier, from = '2024-01-01T00:00:00Z', to = '2024-12-31T23:59:59Z'): FetchSegment {
  return { from: new Date(from), to: new Date(to), shardTier: tier, keys: [] }
}

describe('d1Backend: basic SQL', () => {
  test('SELECT * FROM <table> when no opts', async () => {
    const { db, calls } = mockD1({ defaultRows: [{ cell: 'abc', dt: 1 }] })
    const backend = d1Backend(db, { tableTemplate: 'rides_start_{tier}' })
    const rows = await backend.fetchSegment(seg(TIER_1MO))
    expect(calls).toHaveLength(1)
    expect(calls[0]!.sql).toBe('SELECT * FROM "rides_start_1mo"')
    expect(calls[0]!.binds).toEqual([])
    expect(rows).toEqual([{ cell: 'abc', dt: 1 }])
  })

  test('binCol + range emits BETWEEN clause with ms bind values', async () => {
    const { db, calls } = mockD1()
    const backend = d1Backend(db, { tableTemplate: 'rides_{tier}' })
    await backend.fetchSegment(seg(TIER_1D), {
      binCol: 'dt',
      range: { from: new Date('2024-06-01T00:00:00Z'), to: new Date('2024-07-01T00:00:00Z') },
    })
    expect(calls[0]!.sql).toBe('SELECT * FROM "rides_1d" WHERE "dt" >= ? AND "dt" < ?')
    expect(calls[0]!.binds).toEqual([
      new Date('2024-06-01T00:00:00Z').getTime(),
      new Date('2024-07-01T00:00:00Z').getTime(),
    ])
  })

  test('selectCols restricts columns', async () => {
    const { db, calls } = mockD1()
    const backend = d1Backend(db, {
      tableTemplate: 'rides_{tier}',
      selectCols: ['cell', 'dt', 'count_n'],
    })
    await backend.fetchSegment(seg(TIER_1MO))
    expect(calls[0]!.sql).toBe('SELECT "cell", "dt", "count_n" FROM "rides_1mo"')
  })

  test('values filter emits IN clause', async () => {
    const { db, calls } = mockD1()
    const backend = d1Backend(db, { tableTemplate: 'rides_{tier}' })
    await backend.fetchSegment(seg(TIER_1MO), {
      filters: [{ col: 'gender', values: [0, 1, 2] }],
    })
    expect(calls[0]!.sql).toBe('SELECT * FROM "rides_1mo" WHERE "gender" IN (?,?,?)')
    expect(calls[0]!.binds).toEqual([0, 1, 2])
  })

  test('range filter emits closed-range clause', async () => {
    const { db, calls } = mockD1()
    const backend = d1Backend(db, { tableTemplate: 'rides_{tier}' })
    await backend.fetchSegment(seg(TIER_1MO), {
      filters: [{ col: 'count_n', range: { min: 1, max: 100 } }],
    })
    expect(calls[0]!.sql).toBe('SELECT * FROM "rides_1mo" WHERE "count_n" >= ? AND "count_n" <= ?')
    expect(calls[0]!.binds).toEqual([1, 100])
  })

  test('empty IN set short-circuits to 1=0 (no rows)', async () => {
    const { db, calls } = mockD1({ defaultRows: [{ cell: 'a' }] })
    const backend = d1Backend(db, { tableTemplate: 'rides_{tier}' })
    const rows = await backend.fetchSegment(seg(TIER_1MO), {
      filters: [{ col: 'cell', values: [] }],
    })
    expect(calls[0]!.sql).toContain('WHERE 1 = 0')
    // mockD1 returns defaultRows regardless; the real D1 wouldn't.
    // The point of the assertion is the SQL form.
    expect(rows).toEqual([{ cell: 'a' }])
  })

  test('multiple filters AND together', async () => {
    const { db, calls } = mockD1()
    const backend = d1Backend(db, { tableTemplate: 'rides_{tier}' })
    await backend.fetchSegment(seg(TIER_1MO), {
      binCol: 'dt',
      range: { from: new Date('2024-01-01Z'), to: new Date('2024-02-01Z') },
      filters: [
        { col: 'cell', values: ['c1', 'c2'] },
        { col: 'gender', values: [1] },
      ],
    })
    const expectedSql = 'SELECT * FROM "rides_1mo" '
      + 'WHERE "dt" >= ? AND "dt" < ? '
      + 'AND "cell" IN (?,?) AND "gender" IN (?)'
    expect(calls[0]!.sql).toBe(expectedSql)
    expect(calls[0]!.binds).toEqual([
      new Date('2024-01-01Z').getTime(),
      new Date('2024-02-01Z').getTime(),
      'c1', 'c2', 1,
    ])
  })
})

describe('d1Backend: chunking large IN sets', () => {
  test('splits >chunkSize cells across multiple queries; concats results', async () => {
    const cells = Array.from({ length: 250 }, (_, i) => `cell_${i}`)
    const { db, calls } = mockD1({ defaultRows: [{ cell: 'x', dt: 0 }] })
    const backend = d1Backend(db, {
      tableTemplate: 'rides_{tier}',
      chunkSize: 90,
    })
    const rows = await backend.fetchSegment(seg(TIER_1MO), {
      binCol: 'dt',
      range: { from: new Date('2024-01-01Z'), to: new Date('2024-02-01Z') },
      filters: [{ col: 'cell', values: cells }],
    })
    // chunkSize=90, dt range eats 2 binds → 88 cells per chunk.
    // 250 cells → ceil(250/88) = 3 chunks.
    expect(calls).toHaveLength(3)
    expect(rows).toHaveLength(3)  // 3 defaultRows concatenated
    // Verify total cells bound = 250 (no drops).
    let totalCells = 0
    for (const c of calls) {
      totalCells += c.binds.filter(b => typeof b === 'string' && b.startsWith('cell_')).length
    }
    expect(totalCells).toBe(250)
  })

  test('no chunking when cells fit in chunkSize', async () => {
    const cells = Array.from({ length: 50 }, (_, i) => `cell_${i}`)
    const { db, calls } = mockD1()
    const backend = d1Backend(db, { tableTemplate: 'rides_{tier}' })
    await backend.fetchSegment(seg(TIER_1MO), {
      filters: [{ col: 'cell', values: cells }],
    })
    expect(calls).toHaveLength(1)
  })
})

describe('d1Backend: tableTemplate substitution', () => {
  test('{tier} substitutes tier.name', async () => {
    const { db, calls } = mockD1()
    const backend = d1Backend(db, { tableTemplate: 'rides_v3_start_{tier}' })
    await backend.fetchSegment(seg(TIER_1MO))
    expect(calls[0]!.sql).toContain('"rides_v3_start_1mo"')
  })

  test('{bin} and {shard} substitute Tier fields', async () => {
    const { db, calls } = mockD1()
    const backend = d1Backend(db, { tableTemplate: 'tbl_{tier}_{bin}_{shard}' })
    await backend.fetchSegment(seg(TIER_1D))
    expect(calls[0]!.sql).toContain('"tbl_1d_1d_all"')
  })

  test('unknown placeholder throws', async () => {
    const { db } = mockD1()
    const backend = d1Backend(db, { tableTemplate: 'tbl_{tier}_{nope}' })
    await expect(backend.fetchSegment(seg(TIER_1MO)))
      .rejects.toThrow(/unknown placeholder '\{nope\}'/)
  })

  test('identifiers with embedded double-quote are properly escaped', async () => {
    const { db, calls } = mockD1()
    const backend = d1Backend(db, { tableTemplate: 'q"uote' })
    await backend.fetchSegment(seg(TIER_1MO))
    expect(calls[0]!.sql).toBe('SELECT * FROM "q""uote"')
  })
})

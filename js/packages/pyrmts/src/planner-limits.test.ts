// Query cost ceilings (`specs/calendar-composition-and-query-limits.md` §3).

import { describe, expect, test } from 'vitest'

import { planQuery, planQueryFromInventory } from './planner.js'
import { PlanLimitError, type Pyramid } from './types.js'
import type { RecordedShard } from './shard-index.js'

const d = (s: string) => new Date(s)
const mockStorage = { fetch: async () => { throw new Error('not used') } } as Pyramid['storage']

const days: Pyramid = {
  storage: mockStorage,
  keyTemplate: 'toy/{tier}/{shard}/{period}.parquet',
  axis: 'time',
  binCol: 'ts',
  dims: [],
  metrics: [{ name: 'v', monoid: 'sum' }],
  tiers: [
    { name: 'h1', bin: '1h', shards: ['1d'] },
    { name: 'd1', bin: '1d', shards: ['1mo'] },
  ],
}

// `PlanLimitError` fields, not just the message — callers switch on `limit`
// to pick a status code.
function limitOf(fn: () => unknown): { limit: string; requested: number; allowed: number; message: string } {
  try {
    fn()
  } catch (e) {
    if (!(e instanceof PlanLimitError)) throw e
    return { limit: e.limit, requested: e.requested, allowed: e.allowed, message: e.message }
  }
  throw new Error('expected PlanLimitError')
}

describe('PlanLimits: maxOutputBins', () => {
  test('throws on a targetBin query that plans too many bins', () => {
    expect(limitOf(() => planQuery(days, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-05T00:00:00Z') },
      binBudget: 10_000,
      targetBin: '1h',
      limits: { maxOutputBins: 50 },
    }))).toEqual({
      limit: 'bins',
      requested: 96,
      allowed: 50,
      message: 'planQuery: bins limit exceeded (96 > 50)',
    })
  })

  test('passes at exactly the ceiling', () => {
    const plan = planQuery(days, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-05T00:00:00Z') },
      binBudget: 10_000,
      targetBin: '1h',
      limits: { maxOutputBins: 96 },
    })
    expect(plan.outputBin).toBe('1h')
  })

  test('binBudget stands in for maxOutputBins when unset', () => {
    expect(limitOf(() => planQuery(days, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-05T00:00:00Z') },
      binBudget: 24,
      targetBin: '1h',
    }))).toEqual({
      limit: 'bins',
      requested: 96,
      allowed: 24,
      message: 'planQuery: bins limit exceeded (96 > 24)',
    })
  })

  test('explicit maxOutputBins overrides binBudget', () => {
    const plan = planQuery(days, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-05T00:00:00Z') },
      binBudget: 24,
      targetBin: '1h',
      limits: { maxOutputBins: 1000 },
    })
    expect(plan.outputBin).toBe('1h')
  })

  test('calendar targets are checked too', () => {
    expect(limitOf(() => planQuery(days, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2027-01-01T00:00:00Z') },
      binBudget: 10_000,
      targetBin: '1mo',
      limits: { maxOutputBins: 6 },
    }))).toEqual({
      limit: 'bins',
      requested: 12,
      allowed: 6,
      message: 'planQuery: bins limit exceeded (12 > 6)',
    })
  })
})

describe('PlanLimits: maxAtoms and maxKeys', () => {
  // 4 days at 1d bins, packed from the h1 tier when d1 isn't sealed: the
  // atom count is the pre-coalesce packing count, `atomCount` on the plan.
  const range = { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-05T00:00:00Z') }

  test('plan reports atomCount; maxAtoms throws above it', () => {
    // 48 `2h` bins over the 4 days, each packed from 2 h1 atoms.
    const plan = planQuery(days, { range, binBudget: 10_000, targetBin: '2h' })
    expect(plan.atomCount).toBe(96)
    expect(limitOf(() => planQuery(days, {
      range, binBudget: 10_000, targetBin: '2h', limits: { maxAtoms: 95 },
    }))).toEqual({
      limit: 'atoms',
      requested: 96,
      allowed: 95,
      message: 'planQuery: atoms limit exceeded (96 > 95)',
    })
    expect(planQuery(days, {
      range, binBudget: 10_000, targetBin: '2h', limits: { maxAtoms: 96 },
    }).atomCount).toBe(96)
  })

  test('maxKeys counts distinct shard keys across segments', () => {
    // 4 days spans 4 daily h1 shards.
    const plan = planQuery(days, { range, binBudget: 10_000, targetBin: '2h' })
    const keys = new Set(plan.segments.flatMap(s => s.keys))
    expect([...keys].sort()).toEqual([
      'toy/h1/1d/2026-01-01.parquet',
      'toy/h1/1d/2026-01-02.parquet',
      'toy/h1/1d/2026-01-03.parquet',
      'toy/h1/1d/2026-01-04.parquet',
    ])
    expect(limitOf(() => planQuery(days, {
      range, binBudget: 10_000, targetBin: '2h', limits: { maxKeys: 3 },
    }))).toEqual({
      limit: 'keys',
      requested: 4,
      allowed: 3,
      message: 'planQuery: keys limit exceeded (4 > 3)',
    })
  })
})

describe('PlanLimits: sourcing and non-targetBin paths', () => {
  test('pyramid.limits supplies defaults; input.limits overrides wholesale', () => {
    const capped: Pyramid = { ...days, limits: { maxOutputBins: 10 } }
    const range = { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-05T00:00:00Z') }
    expect(limitOf(() => planQuery(capped, { range, binBudget: 10_000, targetBin: '1h' })))
      .toEqual({
        limit: 'bins',
        requested: 96,
        allowed: 10,
        message: 'planQuery: bins limit exceeded (96 > 10)',
      })
    // Override replaces the pyramid default entirely (not merged): the
    // input's maxKeys-only limits leave bins bounded by binBudget alone.
    expect(planQuery(capped, {
      range, binBudget: 10_000, targetBin: '1h', limits: { maxKeys: 10 },
    }).outputBin).toBe('1h')
  })

  test('binBudget-driven (no targetBin) queries are capped too', () => {
    // pickTier picks h1 (96 bins ≤ budget), then maxKeys rejects the plan.
    expect(limitOf(() => planQuery(days, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-05T00:00:00Z') },
      binBudget: 10_000,
      limits: { maxKeys: 2 },
    }))).toEqual({
      limit: 'keys',
      requested: 4,
      allowed: 2,
      message: 'planQuery: keys limit exceeded (4 > 2)',
    })
  })

  test('unset limits leave behavior unchanged', () => {
    const plan = planQuery(days, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-05T00:00:00Z') },
      binBudget: 10_000,
    })
    expect(plan.outputTier?.name).toBe('h1')
    // atomCount is the PRE-coalesce walk count (one per daily h1 shard
    // period); the four collapse into a single segment.
    expect(plan.atomCount).toBe(4)
    expect(plan.segments.length).toBe(1)
  })

  test('planQueryFromInventory enforces the same ceilings', () => {
    const shards: RecordedShard[] = [
      { tier: 'h1', shardDur: '1d', periodStart: d('2026-01-01T00:00:00Z'), periodEnd: d('2026-01-02T00:00:00Z'), key: 'toy/h1/1d/2026-01-01.parquet' },
      { tier: 'h1', shardDur: '1d', periodStart: d('2026-01-02T00:00:00Z'), periodEnd: d('2026-01-03T00:00:00Z'), key: 'toy/h1/1d/2026-01-02.parquet' },
    ]
    expect(limitOf(() => planQueryFromInventory(days, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-03T00:00:00Z') },
      binBudget: 10_000,
      targetBin: '1h',
      limits: { maxOutputBins: 12 },
    }, shards))).toEqual({
      limit: 'bins',
      requested: 48,
      allowed: 12,
      message: 'planQuery: bins limit exceeded (48 > 12)',
    })
    expect(limitOf(() => planQueryFromInventory(days, {
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-03T00:00:00Z') },
      binBudget: 10_000,
      targetBin: '1h',
      limits: { maxKeys: 1 },
    }, shards))).toEqual({
      limit: 'keys',
      requested: 2,
      allowed: 1,
      message: 'planQuery: keys limit exceeded (2 > 1)',
    })
  })
})

// Calendar het-tiling exactness (`specs/calendar-units.md` acceptance #2):
// for a `{1d, 3d, 7d, 14d}` pyramid with deterministic sum-monoid data,
// `targetBin` = `1mo`/`3mo`/`1y` het-tiled plan+stitch results exactly
// equal a brute-force 1d→calendar groupby, across ranges straddling
// month/year boundaries and (leap) February. Any atom straddling a
// calendar boundary, any gap, or any double-count breaks the equality —
// this is the independent check on the greedy packer.

import { describe, expect, test } from 'vitest'

import { floorToSpan, parseDuration } from './axis.js'
import { planQuery } from './planner.js'
import { stitch, type Row } from './stitch.js'
import type { Duration, Pyramid, Storage } from './types.js'

const mockStorage: Storage = {
  head: async () => null,
  getRange: async () => new Uint8Array(),
  get: async () => null,
  put: async () => {},
  list: async function* () {},
}

const days: Pyramid = {
  storage: mockStorage,
  keyTemplate: 'toy/{tier}/{shard}/{period}.parquet',
  axis: 'time',
  binCol: 'ts',
  dims: [],
  metrics: [{ name: 'v', monoid: 'sum' }],
  tiers: [
    { name: 'd1', bin: '1d', shards: ['1y'] },
    { name: 'd3', bin: '3d', shards: ['1y'] },
    { name: 'd7', bin: '7d', shards: ['1y'] },
    { name: 'd14', bin: '14d', shards: ['1y'] },
  ],
}

const DAY_MS = 24 * 60 * 60_000
const DATA_FROM = Date.UTC(2023, 0, 1)
const DATA_TO = Date.UTC(2025, 0, 1)  // two full years, incl. leap 2024

const d = (iso: string): Date => new Date(iso)
const ms = (iso: string): number => new Date(iso).getTime()

// One row per day, LCG-valued — integers, so sums stay float-exact.
function dayRows(): Row[] {
  const rows: Row[] = []
  let s = 42
  for (let ts = DATA_FROM; ts < DATA_TO; ts += DAY_MS) {
    s = (s * 1103515245 + 12345) % 2147483648
    const v = s % 1000
    rows.push({ ts, v_n: 1, v_sum: v, v_sumsq: v * v })
  }
  return rows
}

function combineInto(acc: Row, r: Row): void {
  acc.v_n = (acc.v_n as number) + (r.v_n as number)
  acc.v_sum = (acc.v_sum as number) + (r.v_sum as number)
  acc.v_sumsq = (acc.v_sumsq as number) + (r.v_sumsq as number)
}

// Aggregate day rows onto a fixed tier's epoch-aligned grid — what that
// tier's shards would contain.
function tierRows(daily: Row[], binMs: number): Row[] {
  const byBin = new Map<number, Row>()
  for (const r of daily) {
    const b = Math.floor((r.ts as number) / binMs) * binMs
    const acc = byBin.get(b)
    if (acc === undefined) {
      byBin.set(b, { ...r, ts: b })
    } else {
      combineInto(acc, r)
    }
  }
  return [...byBin.values()].sort((a, b) => (a.ts as number) - (b.ts as number))
}

// Independent reference: group day rows straight into calendar bins.
function bruteForce(daily: Row[], fromMs: number, toMs: number, targetBin: Duration): Row[] {
  const span = parseDuration(targetBin)
  const byBin = new Map<number, Row>()
  for (const r of daily) {
    const ts = r.ts as number
    if (ts < fromMs || ts >= toMs) continue
    const b = floorToSpan(new Date(ts), span).getTime()
    const acc = byBin.get(b)
    if (acc === undefined) {
      byBin.set(b, { ...r, ts: b })
    } else {
      combineInto(acc, r)
    }
  }
  return [...byBin.values()].sort((a, b) => (a.ts as number) - (b.ts as number))
}

// Plan + stitch a calendar target over the toy pyramid, feeding each
// segment its tier's full row set (stitch clips to segment bounds).
function serve(from: string, to: string, targetBin: Duration): Row[] {
  const daily = dayRows()
  const rowsByTier: Record<string, Row[]> = {
    d1: daily,
    d3: tierRows(daily, 3 * DAY_MS),
    d7: tierRows(daily, 7 * DAY_MS),
    d14: tierRows(daily, 14 * DAY_MS),
  }
  const plan = planQuery(days, {
    range: { from: d(from), to: d(to) },
    binBudget: 1000,
    targetBin,
  })
  const shardRows = plan.segments.map(s => rowsByTier[s.shardTier.name]!)
  return stitch({ pyramid: days, plan, shardRows })
}

describe('calendar het-tiling exactness vs brute-force groupby', () => {
  test('1mo across year boundary and leap February', () => {
    const from = '2023-11-01T00:00:00Z'
    const to = '2024-04-01T00:00:00Z'
    const out = serve(from, to, '1mo')
    expect(out.map(r => r.ts)).toEqual([
      ms('2023-11-01T00:00:00Z'),
      ms('2023-12-01T00:00:00Z'),
      ms('2024-01-01T00:00:00Z'),
      ms('2024-02-01T00:00:00Z'),
      ms('2024-03-01T00:00:00Z'),
    ])
    expect(out).toEqual(bruteForce(dayRows(), ms(from), ms(to), '1mo'))
  })

  test('3mo (Gregorian quarters) across the year boundary', () => {
    const from = '2023-10-01T00:00:00Z'
    const to = '2024-04-01T00:00:00Z'
    const out = serve(from, to, '3mo')
    expect(out.map(r => r.ts)).toEqual([
      ms('2023-10-01T00:00:00Z'),
      ms('2024-01-01T00:00:00Z'),
    ])
    expect(out).toEqual(bruteForce(dayRows(), ms(from), ms(to), '3mo'))
  })

  test('1y over two full years (one leap)', () => {
    const from = '2023-01-01T00:00:00Z'
    const to = '2025-01-01T00:00:00Z'
    const out = serve(from, to, '1y')
    expect(out.map(r => r.ts)).toEqual([
      ms('2023-01-01T00:00:00Z'),
      ms('2024-01-01T00:00:00Z'),
    ])
    expect(out).toEqual(bruteForce(dayRows(), ms(from), ms(to), '1y'))
  })
})
// §2 of `specs/calendar-composition-and-query-limits.md`: calendar tiers
// compose with each other as het-tiling sources. Before this, a `4mo`
// target tiled from day atoms (~9/bin); now it takes `3mo`/`1mo` bins.
// Equality with brute force alone wouldn't catch a regression to the slow
// path, so each test also pins which tiers the plan actually reads.
const calPyramid: Pyramid = {
  ...days,
  tiers: [
    { name: 'd1', bin: '1d', shards: ['1y'] },
    { name: 'd7', bin: '7d', shards: ['1y'] },
    { name: 'mo1', bin: '1mo', shards: ['1y'] },
    { name: 'mo3', bin: '3mo', shards: ['1y'] },
  ],
}

// Aggregate day rows onto a calendar tier's grid (`floorToSpan`) — what a
// materialized `1mo`/`3mo` tier's shards would contain.
function calendarTierRows(daily: Row[], dur: Duration): Row[] {
  const span = parseDuration(dur)
  const byBin = new Map<number, Row>()
  for (const r of daily) {
    const b = floorToSpan(new Date(r.ts as number), span).getTime()
    const acc = byBin.get(b)
    if (acc === undefined) {
      byBin.set(b, { ...r, ts: b })
    } else {
      combineInto(acc, r)
    }
  }
  return [...byBin.values()].sort((a, b) => (a.ts as number) - (b.ts as number))
}

interface CalServed {
  rows: Row[]
  segments: { from: string; to: string; tier: string }[]
  atomCount: number
}

function serveCal(from: string, to: string, targetBin: Duration): CalServed {
  const daily = dayRows()
  const rowsByTier: Record<string, Row[]> = {
    d1: daily,
    d7: tierRows(daily, 7 * DAY_MS),
    mo1: calendarTierRows(daily, '1mo'),
    mo3: calendarTierRows(daily, '3mo'),
  }
  const plan = planQuery(calPyramid, {
    range: { from: d(from), to: d(to) },
    binBudget: 1000,
    targetBin,
  })
  const shardRows = plan.segments.map(s => rowsByTier[s.shardTier.name]!)
  return {
    rows: stitch({ pyramid: calPyramid, plan, shardRows }),
    segments: plan.segments.map(s => ({
      from: s.from.toISOString(), to: s.to.toISOString(), tier: s.shardTier.name,
    })),
    atomCount: plan.atomCount,
  }
}

describe('calendar tiers compose as het-tiling sources', () => {
  test('4mo tiles from 3mo + 1mo (not day tiers)', () => {
    // 4 % 3 != 0, so no whole-bin 3mo cover exists: Jan–Apr takes one 3mo
    // + one 1mo, and the greedy walk coalesces the leftovers into runs.
    const from = '2023-01-01T00:00:00Z'
    const to = '2024-01-01T00:00:00Z'
    const got = serveCal(from, to, '4mo')
    expect(got.segments).toEqual([
      { from: '2023-01-01T00:00:00.000Z', to: '2023-04-01T00:00:00.000Z', tier: 'mo3' },
      { from: '2023-04-01T00:00:00.000Z', to: '2023-10-01T00:00:00.000Z', tier: 'mo1' },
      { from: '2023-10-01T00:00:00.000Z', to: '2024-01-01T00:00:00.000Z', tier: 'mo3' },
    ])
    expect(got.atomCount).toBe(5)
    expect(got.rows.map(r => r.ts)).toEqual([
      ms('2023-01-01T00:00:00Z'),
      ms('2023-05-01T00:00:00Z'),
      ms('2023-09-01T00:00:00Z'),
    ])
    expect(got.rows).toEqual(bruteForce(dayRows(), ms(from), ms(to), '4mo'))
  })

  test('6mo tiles from 2x3mo per bin', () => {
    const from = '2023-01-01T00:00:00Z'
    const to = '2024-01-01T00:00:00Z'
    const got = serveCal(from, to, '6mo')
    // Both bins pack purely from mo3; the runs coalesce into one segment.
    expect(got.segments).toEqual([
      { from: '2023-01-01T00:00:00.000Z', to: '2024-01-01T00:00:00.000Z', tier: 'mo3' },
    ])
    expect(got.atomCount).toBe(2)
    expect(got.rows.map(r => r.ts)).toEqual([
      ms('2023-01-01T00:00:00Z'),
      ms('2023-07-01T00:00:00Z'),
    ])
    expect(got.rows).toEqual(bruteForce(dayRows(), ms(from), ms(to), '6mo'))
  })

  test('5mo (year-0 grid, no whole-year alignment) mixes 3mo and 1mo', () => {
    // 5mo boundaries are months divisible by 5 since year 0: 2022-12,
    // 2023-05, 2023-10, 2024-03. Range picked grid-aligned so plan bins
    // and brute-force bins share bounds.
    const from = '2023-05-01T00:00:00Z'
    const to = '2024-03-01T00:00:00Z'
    const got = serveCal(from, to, '5mo')
    expect(got.segments).toEqual([
      { from: '2023-05-01T00:00:00.000Z', to: '2023-07-01T00:00:00.000Z', tier: 'mo1' },
      { from: '2023-07-01T00:00:00.000Z', to: '2024-01-01T00:00:00.000Z', tier: 'mo3' },
      { from: '2024-01-01T00:00:00.000Z', to: '2024-03-01T00:00:00.000Z', tier: 'mo1' },
    ])
    // bin 1: mo1 run [May, Jul) + mo3 [Jul, Oct); bin 2: mo3 [Oct, Jan) +
    // mo1 run [Jan, Mar).
    expect(got.atomCount).toBe(4)
    expect(got.rows.map(r => r.ts)).toEqual([
      ms('2023-05-01T00:00:00Z'),
      ms('2023-10-01T00:00:00Z'),
    ])
    expect(got.rows).toEqual(bruteForce(dayRows(), ms(from), ms(to), '5mo'))
  })

  test('2y tiles from 3mo runs (8 per bin), never day tiers', () => {
    const from = '2022-01-01T00:00:00Z'
    const to = '2026-01-01T00:00:00Z'
    const got = serveCal(from, to, '2y')
    expect(got.segments).toEqual([
      { from: '2022-01-01T00:00:00.000Z', to: '2026-01-01T00:00:00.000Z', tier: 'mo3' },
    ])
    expect(got.atomCount).toBe(2)
    expect(got.rows.map(r => r.ts)).toEqual([
      ms('2022-01-01T00:00:00Z'),
      ms('2024-01-01T00:00:00Z'),
    ])
    expect(got.rows).toEqual(bruteForce(dayRows(), ms(from), ms(to), '2y'))
  })

  test('watermarks demote coarse calendar sources to finer ones, then to day tiers', () => {
    // Each source seals less of the range than the one above it, so a
    // single 3mo bin walks the whole ladder: mo3 through Apr, mo1 through
    // Jun, d7 through Jun 15, then a clipped d1 atom to Jun 20 (only
    // day-divisor tiers may emit a partial trailing atom).
    const plan = planQuery(calPyramid, {
      range: { from: d('2023-01-01T00:00:00Z'), to: d('2023-07-01T00:00:00Z') },
      binBudget: 1000,
      targetBin: '3mo',
      watermarks: {
        'mo3@1y': d('2023-04-01T00:00:00Z'),
        'mo1@1y': d('2023-06-01T00:00:00Z'),
        'd7@1y': d('2023-06-15T00:00:00Z'),
        'd1@1y': d('2023-06-20T00:00:00Z'),
      },
    })
    expect(plan.segments.map(s => ({
      from: s.from.toISOString(), to: s.to.toISOString(), tier: s.shardTier.name,
    }))).toEqual([
      { from: '2023-01-01T00:00:00.000Z', to: '2023-04-01T00:00:00.000Z', tier: 'mo3' },
      { from: '2023-04-01T00:00:00.000Z', to: '2023-06-01T00:00:00.000Z', tier: 'mo1' },
      { from: '2023-06-01T00:00:00.000Z', to: '2023-06-15T00:00:00.000Z', tier: 'd7' },
      { from: '2023-06-15T00:00:00.000Z', to: '2023-06-20T00:00:00.000Z', tier: 'd1' },
    ])
    expect(plan.atomCount).toBe(4)
  })
})

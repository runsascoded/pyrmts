import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { describe, expect, test } from 'vitest'

import { addSpan, floorToSpan, parseDuration } from './axis.js'
import type { ExpectedShard } from './gap-discovery.js'
import { shardKey } from './keys.js'
import { tileFromExisting } from './tile-from-existing.js'
import type { Pyramid, Shard, Tier } from './types.js'

function d(s: string): Date {
  return new Date(s)
}

function makePyramid(shards: Shard[], keyTemplate = 'a/{tier}/{shard}/{period}.parquet'): Pyramid {
  return {
    storage: { fetch: async () => { throw new Error('not used') } } as Pyramid['storage'],
    keyTemplate,
    axis: 'time',
    binCol: 'dt',
    dims: [],
    metrics: [{ name: 'n', monoid: 'count' }],
    tiers: [
      { name: 'q', bin: '1d', shards },
    ],
  }
}

function makeGap(
  pyramid: Pyramid,
  shardDur: Shard,
  start: Date,
  end: Date,
  filter: Record<string, string | number> = {},
): ExpectedShard {
  return {
    tier: 'q',
    shardDur,
    periodStart: start,
    periodEnd: end,
    effectiveStart: start,
    effectiveEnd: end,
    key: shardKey(pyramid, 'q', shardDur, start, filter),
  }
}

// Daily keys for days [from, to) of a month, as `{rung, key}` picks.
function dailyPicks(pyramid: Pyramid, yyyymm: string, from: number, to: number): Array<{ rung: Shard; key: string }> {
  const out: Array<{ rung: Shard; key: string }> = []
  for (let day = from; day < to; day++) {
    const t = d(`${yyyymm}-${day.toString().padStart(2, '0')}T00:00:00Z`)
    out.push({ rung: '1d', key: shardKey(pyramid, 'q', '1d', t) })
  }
  return out
}

const GENESIS_2020 = d('2020-01-01T00:00:00Z')

describe('tileFromExisting', () => {
  test('fixed-width rungs: greedy 4d-first descent with per-day holes', () => {
    const pyramid = makePyramid(['1d', '4d', '32d'])
    // 32d slot on the epoch grid containing mid-2026.
    const span32 = parseDuration('32d')
    const start = floorToSpan(d('2026-06-01T00:00:00Z'), span32)
    const end = addSpan(start, span32)
    const gap = makeGap(pyramid, '32d', start, end)
    const span4 = parseDuration('4d')
    const span1 = parseDuration('1d')
    const at = (days: number) => new Date(start.getTime() + days * 24 * 60 * 60_000)
    // 4d shards cover days 0-15; dailies cover days 16-23; days 24-31 missing.
    const keySet = new Set([
      ...[0, 4, 8, 12].map(day => shardKey(pyramid, 'q', '4d', at(day))),
      ...[16, 17, 18, 19, 20, 21, 22, 23].map(day => shardKey(pyramid, 'q', '1d', at(day))),
    ])
    expect(floorToSpan(start, span4)).toEqual(start)  // 32d slot is 4d-aligned
    expect(floorToSpan(start, span1)).toEqual(start)
    const { picks, holes } = tileFromExisting(pyramid, pyramid.tiers[0]!, gap, keySet, { genesis: GENESIS_2020 })
    expect(picks).toEqual([
      ...[0, 4, 8, 12].map(day => ({ rung: '4d', key: shardKey(pyramid, 'q', '4d', at(day)) })),
      ...[16, 17, 18, 19, 20, 21, 22, 23].map(day => ({ rung: '1d', key: shardKey(pyramid, 'q', '1d', at(day)) })),
    ])
    expect(holes).toEqual(
      [24, 25, 26, 27, 28, 29, 30, 31].map(day => ({ start: at(day), end: at(day + 1) })),
    )
  })

  test('calendar rung: August 2026 consolidates 31 dailies, no holes', () => {
    const pyramid = makePyramid(['1d', '1mo'])
    const gap = makeGap(pyramid, '1mo', d('2026-08-01T00:00:00Z'), d('2026-09-01T00:00:00Z'))
    const expected = dailyPicks(pyramid, '2026-08', 1, 32)
    const keySet = new Set(expected.map(p => p.key))
    const { picks, holes } = tileFromExisting(pyramid, pyramid.tiers[0]!, gap, keySet, { genesis: GENESIS_2020 })
    expect(picks).toEqual(expected)
    expect(holes).toEqual([])
  })

  test('calendar rung: February 2026 consolidates 28 dailies', () => {
    const pyramid = makePyramid(['1d', '1mo'])
    const gap = makeGap(pyramid, '1mo', d('2026-02-01T00:00:00Z'), d('2026-03-01T00:00:00Z'))
    const expected = dailyPicks(pyramid, '2026-02', 1, 29)
    const keySet = new Set(expected.map(p => p.key))
    const { picks, holes } = tileFromExisting(pyramid, pyramid.tiers[0]!, gap, keySet, { genesis: GENESIS_2020 })
    expect(picks).toEqual(expected)
    expect(holes).toEqual([])
  })

  test('calendar rung: leap-year February 2028 consolidates 29 dailies', () => {
    const pyramid = makePyramid(['1d', '1mo'])
    const gap = makeGap(pyramid, '1mo', d('2028-02-01T00:00:00Z'), d('2028-03-01T00:00:00Z'))
    const expected = dailyPicks(pyramid, '2028-02', 1, 30)
    const keySet = new Set(expected.map(p => p.key))
    const { picks, holes } = tileFromExisting(pyramid, pyramid.tiers[0]!, gap, keySet, { genesis: GENESIS_2020 })
    expect(picks).toEqual(expected)
    expect(holes).toEqual([])
  })

  test('missing sub-rung yields a hole, not a wrong-sized pick', () => {
    const pyramid = makePyramid(['1d', '1mo'])
    const gap = makeGap(pyramid, '1mo', d('2026-02-01T00:00:00Z'), d('2026-03-01T00:00:00Z'))
    const all = dailyPicks(pyramid, '2026-02', 1, 29)
    const missing = shardKey(pyramid, 'q', '1d', d('2026-02-14T00:00:00Z'))
    const keySet = new Set(all.map(p => p.key).filter(k => k !== missing))
    const { picks, holes } = tileFromExisting(pyramid, pyramid.tiers[0]!, gap, keySet, { genesis: GENESIS_2020 })
    expect(picks).toEqual(all.filter(p => p.key !== missing))
    expect(holes).toEqual([
      { start: d('2026-02-14T00:00:00Z'), end: d('2026-02-15T00:00:00Z') },
    ])
  })

  test('pre-genesis segments are silently dropped', () => {
    const pyramid = makePyramid(['1d', '1mo'])
    const gap = makeGap(pyramid, '1mo', d('2026-01-01T00:00:00Z'), d('2026-02-01T00:00:00Z'))
    const expected = dailyPicks(pyramid, '2026-01', 20, 32)
    const keySet = new Set(expected.map(p => p.key))
    const genesis = d('2026-01-20T00:00:00Z')
    const { picks, holes } = tileFromExisting(pyramid, pyramid.tiers[0]!, gap, keySet, { genesis })
    expect(picks).toEqual(expected)
    expect(holes).toEqual([])
  })

  test('filter fills extra keyTemplate placeholders', () => {
    const pyramid = makePyramid(['1d', '1mo'], 'awair-{device_id}/{tier}/{shard}/{period}.parquet')
    const filter = { device_id: 17617 }
    const gap = makeGap(pyramid, '1mo', d('2026-08-01T00:00:00Z'), d('2026-09-01T00:00:00Z'), filter)
    const expected = []
    for (let day = 1; day < 32; day++) {
      const t = d(`2026-08-${day.toString().padStart(2, '0')}T00:00:00Z`)
      expected.push({ rung: '1d' as Shard, key: shardKey(pyramid, 'q', '1d', t, filter) })
    }
    expect(expected[0]!.key).toBe('awair-17617/q/1d/2026-08-01.parquet')
    const keySet = new Set(expected.map(p => p.key))
    const { picks, holes } = tileFromExisting(pyramid, pyramid.tiers[0]!, gap, keySet, { genesis: GENESIS_2020, filter })
    expect(picks).toEqual(expected)
    expect(holes).toEqual([])
  })
})

// Cross-impl parity (`specs/js-calendar-same-tier-tiling.md`): Python
// `tile_from_existing` is the deployed normative reference —
// `fixtures/tiling-parity.json` was generated from it and is asserted
// verbatim by BOTH suites (Python twin:
// `python/pyrmts_engine/tests/test_consolidate.py::test_tiling_parity_fixture`),
// so either implementation drifting from the contract fails its suite.
describe('tileFromExisting cross-impl parity', () => {
  test('reproduces fixtures/tiling-parity.json exactly', () => {
    const fixture = JSON.parse(
      readFileSync(join(__dirname, '../../../../fixtures/tiling-parity.json'), 'utf8'),
    ) as {
      keyTemplate: string
      tier: Tier
      gap: { shardDur: Shard; periodStart: string; periodEnd: string }
      genesis: string
      keySet: string[]
      picks: Array<{ rung: Shard; key: string }>
      holes: Array<{ start: string; end: string }>
    }
    const pyramid = makePyramid(fixture.tier.shards, fixture.keyTemplate)
    const gap = makeGap(pyramid, fixture.gap.shardDur, d(fixture.gap.periodStart), d(fixture.gap.periodEnd))
    const { picks, holes } = tileFromExisting(
      pyramid, fixture.tier, gap, new Set(fixture.keySet), { genesis: d(fixture.genesis) },
    )
    expect(picks).toEqual(fixture.picks)
    expect(holes.map(h => ({ start: h.start.toISOString(), end: h.end.toISOString() })))
      .toEqual(fixture.holes)
  })
})

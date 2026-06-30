import { describe, expect, test } from 'vitest'

import { listExpectedShards, listMissingShards, type ExpectedShard } from './gap-discovery.js'
import { ManifestShardIndex } from './manifest-shard-index.js'
import { memStorage } from './storage.js'
import type { Pyramid } from './types.js'

function d(s: string): Date {
  return new Date(s)
}

function makePyramid(): Pyramid {
  return {
    storage: { fetch: async () => { throw new Error('not used') } } as Pyramid['storage'],
    keyTemplate: 'avail/{tier}/{shard}/{period}.parquet',
    axis: 'time',
    binCol: 'ts',
    dims: [],
    metrics: [{ name: 'n', monoid: 'count' }],
    tiers: [
      { name: '1m', bin: '1min', shards: ['5min', '1h'] },
    ],
  }
}

function makeCalendarPyramid(): Pyramid {
  return {
    storage: { fetch: async () => { throw new Error('not used') } } as Pyramid['storage'],
    keyTemplate: 'a/{tier}/{shard}/{period}.parquet',
    axis: 'time',
    binCol: 'ts',
    dims: [],
    metrics: [{ name: 'n', monoid: 'count' }],
    tiers: [
      { name: '1d', bin: '1d', shards: ['1d', '1mo'] },
    ],
  }
}

function makeFilterPyramid(): Pyramid {
  return {
    storage: { fetch: async () => { throw new Error('not used') } } as Pyramid['storage'],
    keyTemplate: 'awair-{device_id}/{tier}/{shard}/{period}.parquet',
    axis: 'time',
    binCol: 'ts',
    dims: [],
    metrics: [{ name: 'n', monoid: 'count' }],
    tiers: [
      { name: '1m', bin: '1min', shards: ['1h'] },
    ],
  }
}

function summarize(
  shards: ExpectedShard[],
): Array<{ tier: string; shardDur: string; period: string; key: string }> {
  return shards.map(s => ({
    tier: s.tier,
    shardDur: String(s.shardDur),
    period: s.periodStart.toISOString(),
    key: s.key,
  }))
}

describe('listExpectedShards', () => {
  test('one tier × 2 ladder rungs over 1h: 12 × 5min + 1 × 1h = 13 entries', () => {
    const p = makePyramid()
    const got = listExpectedShards(p, {
      from: d('2026-06-01T00:00:00Z'),
      to: d('2026-06-01T01:00:00Z'),
    })
    expect(got).toHaveLength(13)
    const byRung = got.reduce<Record<string, number>>((acc, s) => {
      acc[s.shardDur] = (acc[s.shardDur] ?? 0) + 1
      return acc
    }, {})
    expect(byRung).toEqual({ '5min': 12, '1h': 1 })
  })

  test('substitutes {tier}, {shard}, {period} into keyTemplate', () => {
    const p = makePyramid()
    const got = listExpectedShards(p, {
      from: d('2026-06-01T00:00:00Z'),
      to: d('2026-06-01T00:15:00Z'),
    })
    const summary = summarize(got).sort((a, b) => a.key.localeCompare(b.key))
    expect(summary).toEqual([
      {
        tier: '1m', shardDur: '1h', period: '2026-06-01T00:00:00.000Z',
        key: 'avail/1m/1h/2026-06-01T00.parquet',
      },
      {
        tier: '1m', shardDur: '5min', period: '2026-06-01T00:00:00.000Z',
        key: 'avail/1m/5min/2026-06-01T00-00.parquet',
      },
      {
        tier: '1m', shardDur: '5min', period: '2026-06-01T00:05:00.000Z',
        key: 'avail/1m/5min/2026-06-01T00-05.parquet',
      },
      {
        tier: '1m', shardDur: '5min', period: '2026-06-01T00:10:00.000Z',
        key: 'avail/1m/5min/2026-06-01T00-10.parquet',
      },
    ])
  })

  test('calendar ladder (1d + 1mo over a 2-month range)', () => {
    const p = makeCalendarPyramid()
    const got = listExpectedShards(p, {
      from: d('2026-06-01T00:00:00Z'),
      to: d('2026-08-01T00:00:00Z'),
    })
    const byRung = got.reduce<Record<string, number>>((acc, s) => {
      acc[s.shardDur] = (acc[s.shardDur] ?? 0) + 1
      return acc
    }, {})
    // June has 30 days, July has 31 → 61 × 1d + 2 × 1mo.
    expect(byRung).toEqual({ '1d': 61, '1mo': 2 })
  })

  test('filter values fill in custom keyTemplate placeholders', () => {
    const p = makeFilterPyramid()
    const got = listExpectedShards(
      p,
      { from: d('2026-06-01T00:00:00Z'), to: d('2026-06-01T01:00:00Z') },
      { device_id: 17617 },
    )
    expect(got).toHaveLength(1)
    expect(got[0]!.key).toBe('awair-17617/1m/1h/2026-06-01T00.parquet')
  })

  test('throws on missing filter placeholder', () => {
    const p = makeFilterPyramid()
    expect(() => listExpectedShards(p, {
      from: d('2026-06-01T00:00:00Z'),
      to: d('2026-06-01T01:00:00Z'),
    })).toThrow('substituteKey: missing value for {device_id}')
  })
})

describe('listMissingShards', () => {
  test('returns expected ∖ recorded after recording half', async () => {
    const p = makePyramid()
    const storage = memStorage()
    const idx = new ManifestShardIndex(storage, {
      now: () => 0,
      includeInventory: true,
    })

    const expected = listExpectedShards(p, {
      from: d('2026-06-01T00:00:00Z'),
      to: d('2026-06-01T01:00:00Z'),
    })
    // Record every other expected entry.
    for (let i = 0; i < expected.length; i += 2) {
      const e = expected[i]!
      await idx.recordShard({
        pyramidName: 'avail',
        tier: e.tier,
        shardDur: e.shardDur,
        periodStart: e.periodStart,
        periodEnd: e.periodEnd,
        key: e.key,
      })
    }
    const missing = await listMissingShards(p, 'avail', idx, {
      from: d('2026-06-01T00:00:00Z'),
      to: d('2026-06-01T01:00:00Z'),
    })
    // 13 expected → 7 recorded (indices 0,2,4,6,8,10,12) → 6 missing.
    expect(missing).toHaveLength(6)
    // Verify identity: every missing entry's (tier, shardDur, start) is
    // not in the recorded set.
    const recordedKeys = new Set<string>()
    for (let i = 0; i < expected.length; i += 2) {
      const e = expected[i]!
      recordedKeys.add(`${e.tier}\x00${e.shardDur}\x00${e.periodStart.getTime()}`)
    }
    for (const m of missing) {
      const key = `${m.tier}\x00${m.shardDur}\x00${m.periodStart.getTime()}`
      expect(recordedKeys.has(key)).toBe(false)
    }
  })

  test('returns full expected set when index is empty', async () => {
    const p = makePyramid()
    const storage = memStorage()
    const idx = new ManifestShardIndex(storage, {
      now: () => 0,
      includeInventory: true,
    })
    const missing = await listMissingShards(p, 'avail', idx, {
      from: d('2026-06-01T00:00:00Z'),
      to: d('2026-06-01T01:00:00Z'),
    })
    expect(missing).toHaveLength(13)
  })

  test('returns empty when every expected shard is recorded', async () => {
    const p = makePyramid()
    const storage = memStorage()
    const idx = new ManifestShardIndex(storage, {
      now: () => 0,
      includeInventory: true,
    })
    const expected = listExpectedShards(p, {
      from: d('2026-06-01T00:00:00Z'),
      to: d('2026-06-01T01:00:00Z'),
    })
    for (const e of expected) {
      await idx.recordShard({
        pyramidName: 'avail',
        tier: e.tier,
        shardDur: e.shardDur,
        periodStart: e.periodStart,
        periodEnd: e.periodEnd,
        key: e.key,
      })
    }
    expect(await listMissingShards(p, 'avail', idx, {
      from: d('2026-06-01T00:00:00Z'),
      to: d('2026-06-01T01:00:00Z'),
    })).toEqual([])
  })

  test('throws when the index has inventory disabled', async () => {
    const p = makePyramid()
    const storage = memStorage()
    // Default ManifestShardIndex omits inventory.
    const idx = new ManifestShardIndex(storage, { now: () => 0 })
    await expect(listMissingShards(p, 'avail', idx, {
      from: d('2026-06-01T00:00:00Z'),
      to: d('2026-06-01T01:00:00Z'),
    })).rejects.toThrow(/inventory/)
  })
})

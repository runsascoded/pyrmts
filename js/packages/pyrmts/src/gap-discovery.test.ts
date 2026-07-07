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

function makeAvailV3Pyramid(): Pyramid {
  return {
    storage: { fetch: async () => { throw new Error('not used') } } as Pyramid['storage'],
    keyTemplate: 'avail/{tier}/{shard}/{period}.parquet',
    axis: 'time',
    binCol: 'ts',
    dims: [],
    metrics: [{ name: 'n', monoid: 'count' }],
    tiers: [
      { name: '1m', bin: '1min', shards: ['5min', '10min', '30min', '1h', '3h', '12h', '1d'] },
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

function makeStraddleFromPyramid(): Pyramid {
  return {
    storage: { fetch: async () => { throw new Error('not used') } } as Pyramid['storage'],
    keyTemplate: 'a/{tier}/{shard}/{period}.parquet',
    axis: 'time',
    binCol: 'ts',
    dims: [],
    metrics: [{ name: 'n', monoid: 'count' }],
    tiers: [{ name: '1d', bin: '1d', shards: ['1d', '1mo'] }],
  }
}

// Ctbk avail-v3 `/3d` tier: `4320d` max-shard, `1440d` non-max. For any
// short range where `floor(from, 4320d) === floor(to, 4320d)`, no max-
// shard tiles emit and the trailing greedy walk starts pre-genesis
// (`cur = floor(to, 4320d) = 2017-04-24` for `to` in 2026), grabbing
// `1440d` tiles that end at 2021-04-03 and 2025-03-13 — both fully
// pre-genesis and pruned by Fix A.
function makeCtbkStyleTier(): Pyramid {
  return {
    storage: { fetch: async () => { throw new Error('not used') } } as Pyramid['storage'],
    keyTemplate: 'a/{tier}/{shard}/{period}.parquet',
    axis: 'time',
    binCol: 'ts',
    dims: [],
    metrics: [{ name: 'n', monoid: 'count' }],
    tiers: [{ name: '3d', bin: '3d', shards: ['3d', '1440d', '4320d'] }],
  }
}

describe('listExpectedShards', () => {
  test('to aligned to max-shard emits one max-shard, no trailing', () => {
    const p = makePyramid()
    const got = listExpectedShards(p, {
      from: d('2026-06-01T00:00:00Z'),
      to: d('2026-06-01T01:00:00Z'),
    })
    expect(got).toHaveLength(1)
    expect(got[0]!).toMatchObject({
      tier: '1m', shardDur: '1h', key: 'avail/1m/1h/2026-06-01T00.parquet',
    })
  })

  test('trailing window tiles via largest fitting rungs', () => {
    const p = makePyramid()
    const got = listExpectedShards(p, {
      from: d('2026-06-01T00:00:00Z'),
      to: d('2026-06-01T00:15:00Z'),
    })
    const summary = summarize(got).sort((a, b) => a.key.localeCompare(b.key))
    expect(summary).toEqual([
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

  test('avail-v3 ladder over full day yields a single /1m@1d', () => {
    const p = makeAvailV3Pyramid()
    const got = listExpectedShards(p, {
      from: d('2026-06-01T00:00:00Z'),
      to: d('2026-06-02T00:00:00Z'),
    })
    expect(got).toHaveLength(1)
    expect(got[0]!.shardDur).toBe('1d')
    expect(got[0]!.periodStart.toISOString()).toBe('2026-06-01T00:00:00.000Z')
  })

  test('avail-v3 ladder partial-day trailing decomposes 18h35m → [12h, 3h, 3h, 30min, 5min]', () => {
    const p = makeAvailV3Pyramid()
    const got = listExpectedShards(p, {
      from: d('2026-06-01T00:00:00Z'),
      to: d('2026-06-01T18:35:00Z'),
    })
    expect(got.map(s => [String(s.shardDur), s.periodStart.toISOString()])).toEqual([
      ['12h',   '2026-06-01T00:00:00.000Z'],
      ['3h',    '2026-06-01T12:00:00.000Z'],
      ['3h',    '2026-06-01T15:00:00.000Z'],
      ['30min', '2026-06-01T18:00:00.000Z'],
      ['5min',  '2026-06-01T18:30:00.000Z'],
    ])
  })

  test('residual below smallest rung is left for next-finer tier', () => {
    const p = makePyramid()  // shards: 5min, 1h
    const got = listExpectedShards(p, {
      from: d('2026-06-01T00:00:00Z'),
      to: d('2026-06-01T00:07:00Z'),
    })
    // 7min = 1 × 5min (leading) + 2min residual → unmet.
    expect(got).toEqual([
      expect.objectContaining({ shardDur: '5min', key: 'avail/1m/5min/2026-06-01T00-00.parquet' }),
    ])
  })

  test('calendar ladder yields max-shards only', () => {
    const p = makeCalendarPyramid()
    const got = listExpectedShards(p, {
      from: d('2026-06-01T00:00:00Z'),
      to: d('2026-08-01T00:00:00Z'),
    })
    // [Jun 1, Aug 1) tiles as 2 × 1mo. The 1d rung is non-max and the
    // trailing window is empty (to is 1mo-aligned).
    expect(got.map(s => String(s.shardDur))).toEqual(['1mo', '1mo'])
  })

  test('filter values fill custom keyTemplate placeholders', () => {
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

  describe('effective{Start,End}', () => {
    test('shards fully inside range: effective === period', () => {
      const p = makePyramid()
      const got = listExpectedShards(p, {
        from: d('2026-06-01T00:00:00Z'),
        to: d('2026-06-01T01:00:00Z'),
      })
      // Every emitted tile fits inside `[00:00, 01:00)`; every entry has
      // `effective === period`.
      expect(got.length).toBeGreaterThan(0)
      for (const s of got) {
        expect(s.effectiveStart.getTime()).toBe(s.periodStart.getTime())
        expect(s.effectiveEnd.getTime()).toBe(s.periodEnd.getTime())
      }
    })

    test('max-shard tile straddling `from` clips effectiveStart to from', () => {
      const p = makeStraddleFromPyramid()
      const from = d('2026-06-15T00:00:00Z')
      const to = d('2026-07-01T00:00:00Z')
      const got = listExpectedShards(p, { from, to })
      // `firstMax = 2026-06-01`, `lastMax = 2026-07-01`. Max walk emits
      // one `1mo` tile `[2026-06-01, 2026-07-01)` straddling `from`. No
      // trailing (to is month-aligned).
      expect(got).toHaveLength(1)
      const s = got[0]!
      expect(s.shardDur).toBe('1mo')
      expect(s.periodStart.toISOString()).toBe('2026-06-01T00:00:00.000Z')
      expect(s.periodEnd.toISOString()).toBe('2026-07-01T00:00:00.000Z')
      expect(s.effectiveStart.toISOString()).toBe(from.toISOString())
      expect(s.effectiveEnd.toISOString()).toBe('2026-07-01T00:00:00.000Z')
    })

    test('trailing walk prunes fully-pre-genesis tiles (avail-v3 /3d shape)', () => {
      const p = makeCtbkStyleTier()
      const from = d('2026-04-08T00:00:00Z')
      const to = d('2026-06-30T00:00:00Z')
      const got = listExpectedShards(p, { from, to })
      // Contract: no emitted tile has periodEnd ≤ from. Without Fix A, the
      // walk would emit `1440d` tiles `[2017-04-24, 2021-04-03)` and
      // `[2021-04-03, 2025-03-13)` (both fully pre-genesis) before reaching
      // any in-range tile.
      expect(got.length).toBeGreaterThan(0)
      const fromMs = from.getTime()
      for (const s of got) {
        expect(s.periodEnd.getTime()).toBeGreaterThan(fromMs)
      }
      // The first emitted tile must straddle `from` (its periodStart ≤
      // from, periodEnd > from), and its effectiveStart clips to from.
      const first = got[0]!
      expect(first.periodStart.getTime()).toBeLessThanOrEqual(fromMs)
      expect(first.effectiveStart.toISOString()).toBe(from.toISOString())
    })

    test('no emitted tile has periodEnd > `to` (minimal cover never overshoots)', () => {
      // The greedy trailing walk uses `rEnd <= to`, and the max walk stops
      // at `lastMax = floor(to, maxSpan) ≤ to`. So `periodEnd ≤ to`
      // invariantly, and `effectiveEnd === periodEnd` always.
      const p = makeAvailV3Pyramid()
      const to = d('2026-06-01T18:35:00Z')
      const got = listExpectedShards(p, { from: d('2026-06-01T00:00:00Z'), to })
      const toMs = to.getTime()
      for (const s of got) {
        expect(s.periodEnd.getTime()).toBeLessThanOrEqual(toMs)
        expect(s.effectiveEnd.getTime()).toBe(s.periodEnd.getTime())
      }
    })
  })
})

describe('listMissingShards', () => {
  test('returns expected ∖ recorded after recording half', async () => {
    const p = makeAvailV3Pyramid()
    const storage = memStorage()
    const idx = new ManifestShardIndex(storage, {
      now: () => 0,
      includeInventory: true,
    })

    const expected = listExpectedShards(p, {
      // Partial-day trailing decomp = 5 shards (12h, 3h, 3h, 30min, 5min).
      from: d('2026-06-01T00:00:00Z'),
      to: d('2026-06-01T18:35:00Z'),
    })
    expect(expected).toHaveLength(5)
    // Record every other expected entry (indices 0, 2, 4 → 3 recorded).
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
      to: d('2026-06-01T18:35:00Z'),
    })
    // 5 expected → 3 recorded (0, 2, 4) → 2 missing (1, 3).
    expect(missing).toHaveLength(2)
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
    const p = makeAvailV3Pyramid()
    const storage = memStorage()
    const idx = new ManifestShardIndex(storage, {
      now: () => 0,
      includeInventory: true,
    })
    const missing = await listMissingShards(p, 'avail', idx, {
      from: d('2026-06-01T00:00:00Z'),
      to: d('2026-06-01T18:35:00Z'),
    })
    expect(missing).toHaveLength(5)
  })

  test('returns empty when every expected shard is recorded', async () => {
    const p = makeAvailV3Pyramid()
    const storage = memStorage()
    const idx = new ManifestShardIndex(storage, {
      now: () => 0,
      includeInventory: true,
    })
    const expected = listExpectedShards(p, {
      from: d('2026-06-01T00:00:00Z'),
      to: d('2026-06-01T18:35:00Z'),
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
      to: d('2026-06-01T18:35:00Z'),
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

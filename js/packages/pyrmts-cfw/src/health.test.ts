// Tests for the pyramid health SDK (`specs/pyrmts-ops-adoption.md`
// phase 4): min-cover status math, the build-progress doc contract
// (writer: `pyrmts_ops.rebuild.BuildProgress`), and the snapshot-cache
// pattern.

import { memStorage, type Pyramid, type Tier } from 'pyrmts'
import { describe, expect, test } from 'vitest'
import type { D1Like } from './d1.js'
import {
  computeAndStoreSnapshot,
  getBuildsHealth,
  pyramidCover,
  readCachedSnapshot,
  type BuildProgress,
} from './health.js'

const GENESIS = new Date('2026-01-02T00:00:00Z')
const NOW = new Date('2026-01-08T00:00:00Z')

const TIERS: Tier[] = [
  { name: 'q', bin: '15min', shards: ['6h', '1d'] },
  { name: 'h', bin: '1h', shards: ['1d', '4d'] },
]

const PYRAMID = {
  tiers: TIERS,
  keyTemplate: 'pyr/{tier}/{shard}/{period}.parquet',
} as Pick<Pyramid, 'tiers' | 'keyTemplate'>

const ms = (iso: string): number => Date.parse(iso)

function fakeDb(rows: unknown[], calls?: Array<{ sql: string; binds: unknown[] }>): D1Like {
  return {
    prepare(sql: string) {
      return {
        bind(...binds: unknown[]) {
          calls?.push({ sql, binds })
          return this
        },
        async all<T>() {
          return { results: rows as T[] }
        },
      }
    },
  } as unknown as D1Like
}

describe('pyramidCover', () => {
  test('present / pending / missing / stale / legacy-sentinel accounting', async () => {
    const rows = [
      // Legacy canonical sentinel: '' normalizes to q's largest rung (1d).
      { tier: 'q', sd: '', period_start: ms('2026-01-02T00:00:00Z') },
      { tier: 'q', sd: '1d', period_start: ms('2026-01-03T00:00:00Z') },
      { tier: 'q', sd: '1d', period_start: ms('2026-01-04T00:00:00Z') },
      { tier: 'q', sd: '1d', period_start: ms('2026-01-05T00:00:00Z') },
      { tier: 'q', sd: '1d', period_start: ms('2026-01-06T00:00:00Z') },
      { tier: 'h', sd: '4d', period_start: ms('2025-12-30T00:00:00Z') },
      // Not in the current min-cover → stale (GC candidate).
      { tier: 'h', sd: '1d', period_start: ms('2026-01-02T00:00:00Z') },
    ]
    const calls: Array<{ sql: string; binds: unknown[] }> = []
    const cover = await pyramidCover(fakeDb(rows, calls), PYRAMID, {
      name: 'avail', genesis: GENESIS, now: NOW,
    })
    expect(calls).toEqual([{
      sql: 'SELECT tier, shard_dur AS sd, period_start FROM pyramid_shards WHERE pyramid = ?',
      binds: ['avail'],
    }])
    expect(cover).toEqual({
      name: 'avail',
      genesis: '2026-01-02T00:00:00.000Z',
      now: '2026-01-08T00:00:00.000Z',
      tiers: [
        {
          tier: 'q',
          bin: '15min',
          maxRung: '1d',
          rungs: [
            { shardDur: '1d', role: 'max', expected: 6, present: 5, pending: 1 },
          ],
          segments: [
            { start: '2026-01-02T00:00:00.000Z', end: '2026-01-03T00:00:00.000Z', shardDur: '1d', status: 'present', key: 'pyr/q/1d/2026-01-02.parquet' },
            { start: '2026-01-03T00:00:00.000Z', end: '2026-01-04T00:00:00.000Z', shardDur: '1d', status: 'present', key: 'pyr/q/1d/2026-01-03.parquet' },
            { start: '2026-01-04T00:00:00.000Z', end: '2026-01-05T00:00:00.000Z', shardDur: '1d', status: 'present', key: 'pyr/q/1d/2026-01-04.parquet' },
            { start: '2026-01-05T00:00:00.000Z', end: '2026-01-06T00:00:00.000Z', shardDur: '1d', status: 'present', key: 'pyr/q/1d/2026-01-05.parquet' },
            { start: '2026-01-06T00:00:00.000Z', end: '2026-01-07T00:00:00.000Z', shardDur: '1d', status: 'present', key: 'pyr/q/1d/2026-01-06.parquet' },
            // Missing, but its period just closed (within grace) → pending.
            { start: '2026-01-07T00:00:00.000Z', end: '2026-01-08T00:00:00.000Z', shardDur: '1d', status: 'pending' },
          ],
          totalExpected: 6,
          totalPresent: 5,
          totalPending: 1,
          complete: true,
          firstMissingPeriod: null,
          lastMaxBoundary: '2026-01-08T00:00:00.000Z',
          dustAgeSec: 0,
          staleShardCount: 0,
        },
        {
          tier: 'h',
          bin: '1h',
          maxRung: '4d',
          rungs: [
            { shardDur: '4d', role: 'max', expected: 2, present: 1, pending: 0 },
            { shardDur: '1d', role: 'dust', expected: 1, present: 0, pending: 1 },
          ],
          segments: [
            // Head tile clipped to genesis (its notional period starts Dec 30).
            { start: '2026-01-02T00:00:00.000Z', end: '2026-01-03T00:00:00.000Z', shardDur: '4d', status: 'present', key: 'pyr/h/4d/2025-12-30.parquet' },
            { start: '2026-01-03T00:00:00.000Z', end: '2026-01-07T00:00:00.000Z', shardDur: '4d', status: 'missing' },
            { start: '2026-01-07T00:00:00.000Z', end: '2026-01-08T00:00:00.000Z', shardDur: '1d', status: 'pending' },
          ],
          totalExpected: 3,
          totalPresent: 1,
          totalPending: 1,
          complete: false,
          firstMissingPeriod: '2026-01-03T00:00:00.000Z',
          lastMaxBoundary: '2026-01-07T00:00:00.000Z',
          dustAgeSec: 86400,
          staleShardCount: 1,
        },
      ],
      totalMissing: 1,
      totalPending: 2,
      totalStale: 1,
      allComplete: false,
    })
  })

  test('source-lagged slot: pending (with buildableAt) until buildable + grace, then missing', async () => {
    // The `/1h@3h [18,21)` incident shape: source /30m's smallest rung is
    // 2h, so the 21:00-ending shard is buildable only at 22:00.
    const lagPyramid = {
      tiers: [
        { name: '30m', bin: '30min', shards: ['2h'] },
        { name: '1h', bin: '1h', shards: ['3h'] },
      ] as Tier[],
      keyTemplate: 'pyr/{tier}/{shard}/{period}.parquet',
    } as Pick<Pyramid, 'tiers' | 'keyTemplate'>
    const genesis = new Date('2026-01-02T12:00:00Z')
    const rows = (upTo20h: boolean) => [
      { tier: '30m', sd: '2h', period_start: ms('2026-01-02T12:00:00Z') },
      { tier: '30m', sd: '2h', period_start: ms('2026-01-02T14:00:00Z') },
      { tier: '30m', sd: '2h', period_start: ms('2026-01-02T16:00:00Z') },
      { tier: '30m', sd: '2h', period_start: ms('2026-01-02T18:00:00Z') },
      ...(upTo20h ? [{ tier: '30m', sd: '2h', period_start: ms('2026-01-02T20:00:00Z') }] : []),
      { tier: '1h', sd: '3h', period_start: ms('2026-01-02T12:00:00Z') },
      { tier: '1h', sd: '3h', period_start: ms('2026-01-02T15:00:00Z') },
      // /1h@3h [18,21) absent.
    ]

    // 21:30 — [18,21) closed 30 min ago (past periodEnd + grace) but its
    // source cover is open until 22:00 → pending, tier complete.
    const at2130 = await pyramidCover(fakeDb(rows(false)), lagPyramid, {
      name: 'avail', genesis, now: new Date('2026-01-02T21:30:00Z'),
    })
    expect(at2130).toEqual({
      name: 'avail',
      genesis: '2026-01-02T12:00:00.000Z',
      now: '2026-01-02T21:30:00.000Z',
      tiers: [
        {
          tier: '30m',
          bin: '30min',
          maxRung: '2h',
          rungs: [
            { shardDur: '2h', role: 'max', expected: 4, present: 4, pending: 0 },
          ],
          segments: [
            { start: '2026-01-02T12:00:00.000Z', end: '2026-01-02T14:00:00.000Z', shardDur: '2h', status: 'present', key: 'pyr/30m/2h/2026-01-02T12.parquet' },
            { start: '2026-01-02T14:00:00.000Z', end: '2026-01-02T16:00:00.000Z', shardDur: '2h', status: 'present', key: 'pyr/30m/2h/2026-01-02T14.parquet' },
            { start: '2026-01-02T16:00:00.000Z', end: '2026-01-02T18:00:00.000Z', shardDur: '2h', status: 'present', key: 'pyr/30m/2h/2026-01-02T16.parquet' },
            { start: '2026-01-02T18:00:00.000Z', end: '2026-01-02T20:00:00.000Z', shardDur: '2h', status: 'present', key: 'pyr/30m/2h/2026-01-02T18.parquet' },
          ],
          totalExpected: 4,
          totalPresent: 4,
          totalPending: 0,
          complete: true,
          firstMissingPeriod: null,
          lastMaxBoundary: '2026-01-02T20:00:00.000Z',
          dustAgeSec: 5400,
          staleShardCount: 0,
        },
        {
          tier: '1h',
          bin: '1h',
          maxRung: '3h',
          rungs: [
            { shardDur: '3h', role: 'max', expected: 3, present: 2, pending: 1 },
          ],
          segments: [
            { start: '2026-01-02T12:00:00.000Z', end: '2026-01-02T15:00:00.000Z', shardDur: '3h', status: 'present', key: 'pyr/1h/3h/2026-01-02T12.parquet' },
            { start: '2026-01-02T15:00:00.000Z', end: '2026-01-02T18:00:00.000Z', shardDur: '3h', status: 'present', key: 'pyr/1h/3h/2026-01-02T15.parquet' },
            { start: '2026-01-02T18:00:00.000Z', end: '2026-01-02T21:00:00.000Z', shardDur: '3h', status: 'pending', buildableAt: '2026-01-02T22:00:00.000Z' },
          ],
          totalExpected: 3,
          totalPresent: 2,
          totalPending: 1,
          complete: true,
          firstMissingPeriod: null,
          lastMaxBoundary: '2026-01-02T21:00:00.000Z',
          dustAgeSec: 1800,
          staleShardCount: 0,
        },
      ],
      totalMissing: 0,
      totalPending: 1,
      totalStale: 0,
      allComplete: true,
    })

    // 22:15 — past buildableAt (22:00) + 10-min grace → missing for real.
    const at2215 = await pyramidCover(fakeDb(rows(true)), lagPyramid, {
      name: 'avail', genesis, now: new Date('2026-01-02T22:15:00Z'),
    })
    expect(at2215?.totalMissing).toBe(1)
    expect(at2215?.tiers[1]).toEqual({
      tier: '1h',
      bin: '1h',
      maxRung: '3h',
      rungs: [
        { shardDur: '3h', role: 'max', expected: 3, present: 2, pending: 0 },
      ],
      segments: [
        { start: '2026-01-02T12:00:00.000Z', end: '2026-01-02T15:00:00.000Z', shardDur: '3h', status: 'present', key: 'pyr/1h/3h/2026-01-02T12.parquet' },
        { start: '2026-01-02T15:00:00.000Z', end: '2026-01-02T18:00:00.000Z', shardDur: '3h', status: 'present', key: 'pyr/1h/3h/2026-01-02T15.parquet' },
        { start: '2026-01-02T18:00:00.000Z', end: '2026-01-02T21:00:00.000Z', shardDur: '3h', status: 'missing', buildableAt: '2026-01-02T22:00:00.000Z' },
      ],
      totalExpected: 3,
      totalPresent: 2,
      totalPending: 0,
      complete: false,
      firstMissingPeriod: '2026-01-02T18:00:00.000Z',
      lastMaxBoundary: '2026-01-02T21:00:00.000Z',
      dustAgeSec: 4500,
      staleShardCount: 0,
    })
  })

  test('null on empty registry or registry error', async () => {
    expect(await pyramidCover(fakeDb([]), PYRAMID, { name: 'x', genesis: GENESIS, now: NOW })).toBeNull()
    const broken = { prepare() { throw new Error('D1 down') } } as unknown as D1Like
    expect(await pyramidCover(broken, PYRAMID, { name: 'x', genesis: GENESIS, now: NOW })).toBeNull()
  })

  test('tableName / shardCol options reach the SQL', async () => {
    const calls: Array<{ sql: string; binds: unknown[] }> = []
    await pyramidCover(fakeDb([], calls), PYRAMID, {
      name: 'x', genesis: GENESIS, now: NOW,
      tableName: 'shards_v2', shardCol: 'cadence',
    })
    expect(calls).toEqual([{
      sql: 'SELECT tier, cadence AS sd, period_start FROM shards_v2 WHERE pyramid = ?',
      binds: ['x'],
    }])
  })
})

describe('getBuildsHealth', () => {
  const NOW_MS = ms('2026-01-08T00:00:00Z')
  const encoder = new TextEncoder()

  function doc(pyramid: string, status: BuildProgress['status'], updatedAt: string): BuildProgress {
    return {
      pyramid, driver: 'lambda-fanout',
      startedAt: '2026-01-01T00:00:00Z', updatedAt, status,
      plan: { layers: 1, invocations: 1, scaffolds: 0 },
      byStatus: {}, layers: [], currentLayer: null,
    }
  }

  test('filters idle docs, skips malformed, running-first ordering', async () => {
    const storage = memStorage()
    const running = doc('b', 'running', '2026-01-07T23:00:00Z')
    const done = doc('a', 'done', '2026-01-07T23:30:00Z')
    const idle = doc('c', 'done', '2025-12-25T00:00:00Z')  // > 7 days idle
    await storage.put('build-progress/a.json', encoder.encode(JSON.stringify(done)))
    await storage.put('build-progress/b.json', encoder.encode(JSON.stringify(running)))
    await storage.put('build-progress/c.json', encoder.encode(JSON.stringify(idle)))
    await storage.put('build-progress/bad.json', encoder.encode('{not json'))

    const out = await getBuildsHealth(storage, { prefix: 'build-progress/', now: NOW_MS })
    // Running build first despite the older updatedAt; idle + malformed dropped.
    expect(out).toEqual([running, done])
  })
})

describe('snapshot cache', () => {
  const NOW_MS = ms('2026-01-08T00:00:00Z')

  test('round-trip, freshness, and miss', async () => {
    const storage = memStorage()
    expect(await readCachedSnapshot(storage, 'health/snapshot.json', 300, NOW_MS)).toBeNull()

    const snapshot = await computeAndStoreSnapshot(storage, 'health/snapshot.json', async () => ({
      generatedAt: Math.floor(NOW_MS / 1000) - 60,
      pyramids: ['avail'],
    }))
    expect(snapshot.pyramids).toEqual(['avail'])
    expect(await readCachedSnapshot(storage, 'health/snapshot.json', 300, NOW_MS)).toEqual(snapshot)
    // Stale past max age → null (caller recomputes).
    expect(await readCachedSnapshot(storage, 'health/snapshot.json', 30, NOW_MS)).toBeNull()
  })
})

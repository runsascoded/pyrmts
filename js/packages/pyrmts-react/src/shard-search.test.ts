import { describe, expect, test } from 'vitest'

import type { PyramidCoverStatus, PyramidTierCoverStatus } from 'pyrmts'
import { filterShardEntries, shardSearchEntries } from './shard-search.js'

function makeTier(tier: string, segments: PyramidTierCoverStatus['segments']): PyramidTierCoverStatus {
  return {
    tier,
    bin: '1min',
    maxRung: '1mo',
    rungs: [],
    segments,
    totalExpected: segments.length,
    totalPresent: segments.filter(s => s.status === 'present').length,
    totalPending: 0,
    complete: true,
    firstMissingPeriod: null,
    lastMaxBoundary: '2026-08-01T00:00:00.000Z',
    dustAgeSec: 0,
    staleShardCount: 0,
  }
}

function makeCover(name: string, tiers: PyramidTierCoverStatus[]): PyramidCoverStatus {
  return {
    name,
    genesis: '2026-01-01T00:00:00.000Z',
    now: '2026-08-16T00:00:00.000Z',
    tiers,
    totalMissing: 0,
    totalPending: 0,
    totalStale: 0,
    allComplete: true,
  }
}

const COVERS: PyramidCoverStatus[] = [
  makeCover('awair-17617', [
    makeTier('raw', [
      { start: '2026-07-01T00:00:00.000Z', end: '2026-08-01T00:00:00.000Z', shardDur: '1mo', status: 'present', key: 'awair-17617/raw/1mo/2026-07.parquet' },
      { start: '2026-08-01T00:00:00.000Z', end: '2026-08-02T00:00:00.000Z', shardDur: '1d', status: 'present', key: 'awair-17617/raw/1d/2026-08-01.parquet' },
      { start: '2026-08-02T00:00:00.000Z', end: '2026-08-03T00:00:00.000Z', shardDur: '1d', status: 'missing' },
    ]),
    makeTier('m3', [
      { start: '2026-07-01T00:00:00.000Z', end: '2026-08-01T00:00:00.000Z', shardDur: '1mo', status: 'present', key: 'awair-17617/m3/1mo/2026-07.parquet' },
    ]),
  ]),
  makeCover('awair-137496', [
    makeTier('raw', [
      { start: '2026-07-01T00:00:00.000Z', end: '2026-08-01T00:00:00.000Z', shardDur: '1mo', status: 'present', key: 'awair-137496/raw/1mo/2026-07.parquet' },
    ]),
  ]),
]

const hrefFor = (key: string) => `/files/${key}`

describe('shardSearchEntries', () => {
  test('one entry per keyed segment; missing segments skipped', () => {
    expect(shardSearchEntries(COVERS, { hrefFor })).toEqual([
      {
        id: 'shard:awair-17617/raw/1mo/2026-07.parquet',
        label: 'awair-17617 · raw/1mo · 2026-07-01',
        description: 'awair-17617/raw/1mo/2026-07.parquet',
        group: 'Shards',
        href: '/files/awair-17617/raw/1mo/2026-07.parquet',
        search: 'awair-17617 · raw/1mo · 2026-07-01 awair-17617/raw/1mo/2026-07.parquet',
      },
      {
        id: 'shard:awair-17617/raw/1d/2026-08-01.parquet',
        label: 'awair-17617 · raw/1d · 2026-08-01',
        description: 'awair-17617/raw/1d/2026-08-01.parquet',
        group: 'Shards',
        href: '/files/awair-17617/raw/1d/2026-08-01.parquet',
        search: 'awair-17617 · raw/1d · 2026-08-01 awair-17617/raw/1d/2026-08-01.parquet',
      },
      {
        id: 'shard:awair-17617/m3/1mo/2026-07.parquet',
        label: 'awair-17617 · m3/1mo · 2026-07-01',
        description: 'awair-17617/m3/1mo/2026-07.parquet',
        group: 'Shards',
        href: '/files/awair-17617/m3/1mo/2026-07.parquet',
        search: 'awair-17617 · m3/1mo · 2026-07-01 awair-17617/m3/1mo/2026-07.parquet',
      },
      {
        id: 'shard:awair-137496/raw/1mo/2026-07.parquet',
        label: 'awair-137496 · raw/1mo · 2026-07-01',
        description: 'awair-137496/raw/1mo/2026-07.parquet',
        group: 'Shards',
        href: '/files/awair-137496/raw/1mo/2026-07.parquet',
        search: 'awair-137496 · raw/1mo · 2026-07-01 awair-137496/raw/1mo/2026-07.parquet',
      },
    ])
  })

  test('pyramidLabel maps pyramid names; custom group carries through', () => {
    const entries = shardSearchEntries(
      [COVERS[1]!],
      { hrefFor, pyramidLabel: name => (name === 'awair-137496' ? 'Gym' : name), group: 'Pyramids' },
    )
    expect(entries).toEqual([
      {
        id: 'shard:awair-137496/raw/1mo/2026-07.parquet',
        label: 'Gym · raw/1mo · 2026-07-01',
        description: 'awair-137496/raw/1mo/2026-07.parquet',
        group: 'Pyramids',
        href: '/files/awair-137496/raw/1mo/2026-07.parquet',
        search: 'gym · raw/1mo · 2026-07-01 awair-137496/raw/1mo/2026-07.parquet',
      },
    ])
  })
})

describe('filterShardEntries', () => {
  const ENTRIES = shardSearchEntries(COVERS, { hrefFor })

  test('multi-term AND filter narrows progressively', () => {
    const page = { offset: 0, limit: 10 }
    expect(filterShardEntries(ENTRIES, 'raw', page).entries.map(e => e.id)).toEqual([
      'shard:awair-17617/raw/1mo/2026-07.parquet',
      'shard:awair-17617/raw/1d/2026-08-01.parquet',
      'shard:awair-137496/raw/1mo/2026-07.parquet',
    ])
    expect(filterShardEntries(ENTRIES, 'raw 17617 1d', page).entries.map(e => e.id)).toEqual([
      'shard:awair-17617/raw/1d/2026-08-01.parquet',
    ])
    expect(filterShardEntries(ENTRIES, 'nomatch', page)).toEqual({
      entries: [],
      total: 0,
      hasMore: false,
    })
  })

  test('empty query matches everything; search key stripped from results', () => {
    const { entries, total, hasMore } = filterShardEntries(ENTRIES, '', { offset: 0, limit: 10 })
    expect(total).toBe(4)
    expect(hasMore).toBe(false)
    expect(entries[0]).toEqual({
      id: 'shard:awair-17617/raw/1mo/2026-07.parquet',
      label: 'awair-17617 · raw/1mo · 2026-07-01',
      description: 'awair-17617/raw/1mo/2026-07.parquet',
      group: 'Shards',
      href: '/files/awair-17617/raw/1mo/2026-07.parquet',
    })
  })

  test('pagination: offset/limit windows with hasMore', () => {
    const first = filterShardEntries(ENTRIES, '', { offset: 0, limit: 3 })
    expect(first.entries.map(e => e.id)).toEqual([
      'shard:awair-17617/raw/1mo/2026-07.parquet',
      'shard:awair-17617/raw/1d/2026-08-01.parquet',
      'shard:awair-17617/m3/1mo/2026-07.parquet',
    ])
    expect(first.total).toBe(4)
    expect(first.hasMore).toBe(true)
    const second = filterShardEntries(ENTRIES, '', { offset: 3, limit: 3 })
    expect(second.entries.map(e => e.id)).toEqual([
      'shard:awair-137496/raw/1mo/2026-07.parquet',
    ])
    expect(second.hasMore).toBe(false)
  })
})

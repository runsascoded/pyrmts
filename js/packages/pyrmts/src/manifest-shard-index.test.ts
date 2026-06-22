import type { RecordShardInput } from './shard-index.js'
import type { Storage } from './types.js'
import { describe, expect, test } from 'vitest'
import { ManifestShardIndex } from './manifest-shard-index.js'
import { memStorage } from './storage.js'

const decoder = new TextDecoder()

function readManifest(storage: Storage, key: string): Promise<Record<string, unknown> | null> {
  return storage.get(key).then(bytes => {
    if (bytes === null) return null
    return JSON.parse(decoder.decode(bytes)) as Record<string, unknown>
  })
}

function makeRecordInput(overrides: Partial<RecordShardInput> = {}): RecordShardInput {
  return {
    pyramidName: 'avail',
    tier: '15m',
    cadence: '1h',
    periodStart: new Date('2026-06-21T14:00:00Z'),
    periodEnd: new Date('2026-06-21T15:00:00Z'),
    key: 'avail/15m/p1h/2026-06-21T14.parquet',
    ...overrides,
  }
}

describe('ManifestShardIndex.getWatermarks', () => {
  test('returns empty Map when manifest blob does not exist', async () => {
    const storage = memStorage()
    const idx = new ManifestShardIndex(storage, { now: () => 0 })
    expect(await idx.getWatermarks('avail')).toEqual(new Map())
  })

  test('round-trips canonical + partial watermarks', async () => {
    const storage = memStorage()
    const idx = new ManifestShardIndex(storage, { now: () => 0 })
    await idx.recordShard(makeRecordInput({ cadence: null }))
    await idx.recordShard(makeRecordInput({ cadence: '1h' }))
    await idx.recordShard(makeRecordInput({ cadence: '1d' }))
    expect(await idx.getWatermarks('avail')).toEqual(new Map<string, Date>([
      ['15m', new Date('2026-06-21T15:00:00Z')],
      ['15m@1h', new Date('2026-06-21T15:00:00Z')],
      ['15m@1d', new Date('2026-06-21T15:00:00Z')],
    ]))
  })

  test('returns empty Map on malformed JSON (defensive)', async () => {
    const storage = memStorage()
    await storage.put('pyrmts/avail/_manifest.json', new TextEncoder().encode('not json'))
    const idx = new ManifestShardIndex(storage, { now: () => 0 })
    expect(await idx.getWatermarks('avail')).toEqual(new Map())
  })

  test('returns empty Map on wrong version', async () => {
    const storage = memStorage()
    await storage.put(
      'pyrmts/avail/_manifest.json',
      new TextEncoder().encode(JSON.stringify({ version: 99, watermarks: { '15m': 1000 } })),
    )
    const idx = new ManifestShardIndex(storage, { now: () => 0 })
    expect(await idx.getWatermarks('avail')).toEqual(new Map())
  })

  test('returns empty Map on legacy ctbk format (no version, only tiers)', async () => {
    // Legacy: `{ tiers: { 15m: { latest_period: "2026-06" } } }`.
    // `ManifestShardIndex` is defensive: unrecognized → empty (no crash).
    // Consumers migrating from legacy use a separate one-shot script.
    const storage = memStorage()
    await storage.put(
      'pyrmts/avail/_manifest.json',
      new TextEncoder().encode(JSON.stringify({
        tiers: { '15m': { latest_period: '2026-06' } },
      })),
    )
    const idx = new ManifestShardIndex(storage, { now: () => 0 })
    expect(await idx.getWatermarks('avail')).toEqual(new Map())
  })
})

describe('ManifestShardIndex.recordShard', () => {
  test('creates manifest on first write', async () => {
    const storage = memStorage()
    const idx = new ManifestShardIndex(storage, { now: () => 999 })
    await idx.recordShard(makeRecordInput({ cadence: null }))
    const m = await readManifest(storage, 'pyrmts/avail/_manifest.json')
    expect(m).toEqual({
      version: 1,
      watermarks: { '15m': new Date('2026-06-21T15:00:00Z').getTime() },
      updatedAt: 999,
    })
  })

  test('watermark monotonic — older periodEnd does not regress', async () => {
    const storage = memStorage()
    const idx = new ManifestShardIndex(storage, { now: () => 0 })
    await idx.recordShard(makeRecordInput({
      cadence: null,
      periodStart: new Date('2026-06-21T14:00:00Z'),
      periodEnd: new Date('2026-06-21T15:00:00Z'),
    }))
    await idx.recordShard(makeRecordInput({
      cadence: null,
      periodStart: new Date('2026-06-21T13:00:00Z'),
      periodEnd: new Date('2026-06-21T14:00:00Z'),  // older
    }))
    expect(await idx.getWatermarks('avail')).toEqual(new Map<string, Date>([
      ['15m', new Date('2026-06-21T15:00:00Z')],
    ]))
  })

  test('cadence=null encodes as bare tier in watermarks key', async () => {
    const storage = memStorage()
    const idx = new ManifestShardIndex(storage, { now: () => 0 })
    await idx.recordShard(makeRecordInput({ cadence: null }))
    const m = await readManifest(storage, 'pyrmts/avail/_manifest.json')
    expect(Object.keys((m as { watermarks: Record<string, number> }).watermarks)).toEqual(['15m'])
  })

  test('cadence non-null encodes as tier@cadence in watermarks key', async () => {
    const storage = memStorage()
    const idx = new ManifestShardIndex(storage, { now: () => 0 })
    await idx.recordShard(makeRecordInput({ cadence: '1h' }))
    const m = await readManifest(storage, 'pyrmts/avail/_manifest.json')
    expect(Object.keys((m as { watermarks: Record<string, number> }).watermarks)).toEqual(['15m@1h'])
  })

  test('includeInventory: false (default) does not write shards array', async () => {
    const storage = memStorage()
    const idx = new ManifestShardIndex(storage, { now: () => 0 })
    await idx.recordShard(makeRecordInput())
    const m = await readManifest(storage, 'pyrmts/avail/_manifest.json')
    expect((m as Record<string, unknown>).shards).toBeUndefined()
  })

  test('includeInventory: true appends shard records', async () => {
    const storage = memStorage()
    const idx = new ManifestShardIndex(storage, { includeInventory: true, now: () => 0 })
    await idx.recordShard(makeRecordInput({
      cadence: '1h',
      periodStart: new Date('2026-06-21T14:00:00Z'),
      periodEnd: new Date('2026-06-21T15:00:00Z'),
    }))
    await idx.recordShard(makeRecordInput({
      cadence: '1h',
      periodStart: new Date('2026-06-21T15:00:00Z'),
      periodEnd: new Date('2026-06-21T16:00:00Z'),
      key: 'avail/15m/p1h/2026-06-21T15.parquet',
    }))
    const m = await readManifest(storage, 'pyrmts/avail/_manifest.json')
    expect((m as { shards: unknown[] }).shards).toEqual([
      {
        tier: '15m',
        cadence: '1h',
        periodStart: new Date('2026-06-21T14:00:00Z').getTime(),
        periodEnd: new Date('2026-06-21T15:00:00Z').getTime(),
        key: 'avail/15m/p1h/2026-06-21T14.parquet',
      },
      {
        tier: '15m',
        cadence: '1h',
        periodStart: new Date('2026-06-21T15:00:00Z').getTime(),
        periodEnd: new Date('2026-06-21T16:00:00Z').getTime(),
        key: 'avail/15m/p1h/2026-06-21T15.parquet',
      },
    ])
  })

  test('includeInventory: true overwrites the same (tier, cadence, periodStart) row', async () => {
    const storage = memStorage()
    const idx = new ManifestShardIndex(storage, { includeInventory: true, now: () => 0 })
    await idx.recordShard(makeRecordInput({
      cadence: '1h',
      periodStart: new Date('2026-06-21T14:00:00Z'),
      periodEnd: new Date('2026-06-21T15:00:00Z'),
      key: 'k1',
    }))
    await idx.recordShard(makeRecordInput({
      cadence: '1h',
      periodStart: new Date('2026-06-21T14:00:00Z'),
      periodEnd: new Date('2026-06-21T15:00:00Z'),
      key: 'k2',  // rewrite of same period
    }))
    const m = await readManifest(storage, 'pyrmts/avail/_manifest.json')
    expect((m as { shards: unknown[] }).shards).toHaveLength(1)
    expect(((m as { shards: { key: string }[] }).shards[0]).key).toBe('k2')
  })

  test('updatedAt comes from `now`', async () => {
    const storage = memStorage()
    let t = 1000
    const idx = new ManifestShardIndex(storage, { now: () => t })
    await idx.recordShard(makeRecordInput())
    expect((await readManifest(storage, 'pyrmts/avail/_manifest.json') as { updatedAt: number }).updatedAt).toBe(1000)
    t = 2000
    await idx.recordShard(makeRecordInput({ cadence: '1d' }))
    expect((await readManifest(storage, 'pyrmts/avail/_manifest.json') as { updatedAt: number }).updatedAt).toBe(2000)
  })
})

describe('ManifestShardIndex: manifestKey override', () => {
  test('string manifestKey is used directly (multiple pyramidNames share the same blob)', async () => {
    const storage = memStorage()
    const idx = new ManifestShardIndex(storage, { manifestKey: 'shared/manifest.json', now: () => 0 })
    await idx.recordShard(makeRecordInput({ pyramidName: 'avail' }))
    await idx.recordShard(makeRecordInput({ pyramidName: 'rides', tier: '1d', cadence: null }))
    expect(await idx.getWatermarks('avail')).toEqual(new Map<string, Date>([
      ['15m@1h', new Date('2026-06-21T15:00:00Z')],
      ['1d', new Date('2026-06-21T15:00:00Z')],
    ]))
    expect(await idx.getWatermarks('rides')).toEqual(new Map<string, Date>([
      ['15m@1h', new Date('2026-06-21T15:00:00Z')],
      ['1d', new Date('2026-06-21T15:00:00Z')],
    ]))
  })

  test('function manifestKey resolves per pyramidName', async () => {
    const storage = memStorage()
    const idx = new ManifestShardIndex(storage, {
      manifestKey: name => `manifests/${name}.json`,
      now: () => 0,
    })
    await idx.recordShard(makeRecordInput({ pyramidName: 'avail' }))
    await idx.recordShard(makeRecordInput({ pyramidName: 'rides', tier: '1d', cadence: null }))
    expect(await idx.getWatermarks('avail')).toEqual(new Map<string, Date>([
      ['15m@1h', new Date('2026-06-21T15:00:00Z')],
    ]))
    expect(await idx.getWatermarks('rides')).toEqual(new Map<string, Date>([
      ['1d', new Date('2026-06-21T15:00:00Z')],
    ]))
  })

  test('default key is `pyrmts/{pyramidName}/_manifest.json`', async () => {
    const storage = memStorage()
    const idx = new ManifestShardIndex(storage, { now: () => 0 })
    await idx.recordShard(makeRecordInput({ pyramidName: 'avail' }))
    const bytes = await storage.get('pyrmts/avail/_manifest.json')
    expect(bytes).not.toBeNull()
  })
})

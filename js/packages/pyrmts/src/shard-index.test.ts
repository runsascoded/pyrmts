import { describe, expect, test, vi } from 'vitest'
import {
  CachedShardIndex,
  decodeWatermarkKey,
  encodeWatermarkKey,
  type RecordShardInput,
  type ShardIndex,
} from './shard-index.js'

describe('watermark key codec', () => {
  test('encodes as tier@shardDur', () => {
    expect(encodeWatermarkKey('15m', '1h')).toBe('15m@1h')
  })

  test('decode round-trips', () => {
    expect(decodeWatermarkKey('15m@1h')).toEqual({ tier: '15m', shardDur: '1h' })
  })

  test('encode → decode round-trips a spread of tier/shardDur combos', () => {
    const cases: Array<{ tier: string; shardDur: string }> = [
      { tier: 'raw', shardDur: '1mo' },
      { tier: '2m', shardDur: '10min' },
      { tier: '15m', shardDur: '1h' },
      { tier: '1h', shardDur: '3d' },
      { tier: 'mo1', shardDur: '1y' },
    ]
    for (const c of cases) {
      const encoded = encodeWatermarkKey(c.tier, c.shardDur as never)
      expect(decodeWatermarkKey(encoded)).toEqual(c)
    }
  })

  test('decodeWatermarkKey throws on bare-tier (no separator)', () => {
    expect(() => decodeWatermarkKey('15m')).toThrow(/missing '@'/)
  })
})

// Stub `ShardIndex` whose `getWatermarks` returns a counter-stamped map so
// the cache test can tell which call produced the cached value.
function counterIndex(): { index: ShardIndex; calls: { get: number; record: number }; records: RecordShardInput[] } {
  const calls = { get: 0, record: 0 }
  const records: RecordShardInput[] = []
  const index: ShardIndex = {
    async getWatermarks(_pyramidName) {
      calls.get++
      // Stamp the result with the call number so distinct calls return
      // distinguishable values.
      const m = new Map<string, Date>()
      m.set('15m@1h', new Date(calls.get * 1000))
      return m
    },
    async recordShard(input) {
      calls.record++
      records.push(input)
    },
  }
  return { index, calls, records }
}

describe('CachedShardIndex', () => {
  test('caches getWatermarks within TTL — single underlying call across N reads', async () => {
    const { index, calls } = counterIndex()
    const cached = new CachedShardIndex(index, { ttlMs: 60_000, now: () => 0 })
    const m1 = await cached.getWatermarks('avail')
    const m2 = await cached.getWatermarks('avail')
    const m3 = await cached.getWatermarks('avail')
    expect(calls.get).toBe(1)
    expect(m1.get('15m@1h')).toEqual(new Date(1000))
    expect(m2.get('15m@1h')).toEqual(new Date(1000))
    expect(m3.get('15m@1h')).toEqual(new Date(1000))
  })

  test('refetches after TTL expires', async () => {
    const { index, calls } = counterIndex()
    let t = 0
    const cached = new CachedShardIndex(index, { ttlMs: 60_000, now: () => t })
    const m1 = await cached.getWatermarks('avail')
    expect(calls.get).toBe(1)
    expect(m1.get('15m@1h')).toEqual(new Date(1000))

    t = 59_999
    const m2 = await cached.getWatermarks('avail')
    expect(calls.get).toBe(1)
    expect(m2.get('15m@1h')).toEqual(new Date(1000))

    t = 60_000
    const m3 = await cached.getWatermarks('avail')
    expect(calls.get).toBe(2)
    expect(m3.get('15m@1h')).toEqual(new Date(2000))
  })

  test('caches independently per pyramidName', async () => {
    const { index, calls } = counterIndex()
    const cached = new CachedShardIndex(index, { ttlMs: 60_000, now: () => 0 })
    await cached.getWatermarks('avail')
    await cached.getWatermarks('rides')
    await cached.getWatermarks('avail')
    expect(calls.get).toBe(2)
  })

  test('recordShard invalidates the pyramid\'s cache', async () => {
    const { index, calls } = counterIndex()
    const cached = new CachedShardIndex(index, { ttlMs: 60_000, now: () => 0 })
    const m1 = await cached.getWatermarks('avail')
    expect(calls.get).toBe(1)
    expect(m1.get('15m@1h')).toEqual(new Date(1000))

    await cached.recordShard({
      pyramidName: 'avail',
      tier: '15m',
      shardDur: '1h',
      periodStart: new Date(0),
      periodEnd: new Date(3_600_000),
      key: 'avail/15m/1h/1970-01-01T00.parquet',
    })

    const m2 = await cached.getWatermarks('avail')
    expect(calls.get).toBe(2)
    expect(m2.get('15m@1h')).toEqual(new Date(2000))
  })

  test('recordShard invalidates only the affected pyramid', async () => {
    const { index, calls } = counterIndex()
    const cached = new CachedShardIndex(index, { ttlMs: 60_000, now: () => 0 })
    await cached.getWatermarks('avail')
    await cached.getWatermarks('rides')
    expect(calls.get).toBe(2)

    await cached.recordShard({
      pyramidName: 'avail',
      tier: '15m',
      shardDur: '1h',
      periodStart: new Date(0),
      periodEnd: new Date(3_600_000),
      key: 'k',
    })

    await cached.getWatermarks('avail')  // miss → +1
    await cached.getWatermarks('rides')  // still cached
    expect(calls.get).toBe(3)
  })

  test('dedupes concurrent in-flight calls — N parallel reads = 1 underlying call', async () => {
    let resolveUnderlying: ((value: Map<string, Date>) => void) | undefined
    let callsGet = 0
    const index: ShardIndex = {
      async getWatermarks(_) {
        callsGet++
        return new Promise(resolve => { resolveUnderlying = resolve })
      },
      async recordShard() {},
    }
    const cached = new CachedShardIndex(index, { ttlMs: 60_000, now: () => 0 })
    const p1 = cached.getWatermarks('avail')
    const p2 = cached.getWatermarks('avail')
    const p3 = cached.getWatermarks('avail')
    // All 3 parallel reads should be hooked to the same in-flight fetch.
    expect(callsGet).toBe(1)
    resolveUnderlying!(new Map([['15m@1h', new Date(42)]]))
    const [r1, r2, r3] = await Promise.all([p1, p2, p3])
    expect(r1).toBe(r2)
    expect(r2).toBe(r3)
    expect(r1.get('15m@1h')).toEqual(new Date(42))
  })

  test('failed underlying fetch evicts the in-flight entry so the next call retries', async () => {
    let callsGet = 0
    const index: ShardIndex = {
      async getWatermarks() {
        callsGet++
        if (callsGet === 1) throw new Error('transient')
        return new Map([['15m@1h', new Date(callsGet * 1000)]])
      },
      async recordShard() {},
    }
    const cached = new CachedShardIndex(index, { ttlMs: 60_000, now: () => 0 })
    await expect(cached.getWatermarks('avail')).rejects.toThrow('transient')
    const m = await cached.getWatermarks('avail')
    expect(callsGet).toBe(2)
    expect(m.get('15m@1h')).toEqual(new Date(2000))
  })

  test('default ttl is 60_000 (matches ctbk\'s manifest TTL)', async () => {
    const { index, calls } = counterIndex()
    // Override only `now`, not `ttlMs`.
    let t = 0
    const cached = new CachedShardIndex(index, { now: () => t })
    await cached.getWatermarks('avail')
    expect(calls.get).toBe(1)
    t = 59_999
    await cached.getWatermarks('avail')
    expect(calls.get).toBe(1)
    t = 60_000
    await cached.getWatermarks('avail')
    expect(calls.get).toBe(2)
  })

  test('default `now` is Date.now (smoke-check it doesn\'t throw)', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date(1_700_000_000_000))
    try {
      const { index, calls } = counterIndex()
      const cached = new CachedShardIndex(index, { ttlMs: 1000 })
      await cached.getWatermarks('avail')
      vi.advanceTimersByTime(500)
      await cached.getWatermarks('avail')
      expect(calls.get).toBe(1)
      vi.advanceTimersByTime(501)
      await cached.getWatermarks('avail')
      expect(calls.get).toBe(2)
    } finally {
      vi.useRealTimers()
    }
  })

  test('clear() drops every entry', async () => {
    const { index, calls } = counterIndex()
    const cached = new CachedShardIndex(index, { ttlMs: 60_000, now: () => 0 })
    await cached.getWatermarks('avail')
    await cached.getWatermarks('rides')
    expect(calls.get).toBe(2)
    cached.clear()
    await cached.getWatermarks('avail')
    await cached.getWatermarks('rides')
    expect(calls.get).toBe(4)
  })

  test('recordShard delegates to underlying with the same input', async () => {
    const { index, records } = counterIndex()
    const cached = new CachedShardIndex(index, { ttlMs: 60_000, now: () => 0 })
    const input: RecordShardInput = {
      pyramidName: 'avail',
      tier: '15m',
      shardDur: '1h',
      periodStart: new Date(3_600_000),
      periodEnd: new Date(7_200_000),
      key: 'avail/15m/1h/1970-01-01T01.parquet',
    }
    await cached.recordShard(input)
    expect(records).toEqual([input])
  })
})

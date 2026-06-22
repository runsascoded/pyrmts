import { describe, expect, test } from 'vitest'
import { validatePartials } from './partials.js'
import type { Pyramid, Storage } from './types.js'

const mockStorage: Storage = {
  head: async () => null,
  getRange: async () => new Uint8Array(),
  get: async () => null,
  put: async () => {},
  list: async function* () {},
}

function pyramid(overrides: Partial<Pyramid>): Pyramid {
  return {
    storage: mockStorage,
    keyTemplate: 'avail/{tier}/{period}.parquet',
    axis: 'time',
    binCol: 'ts',
    dims: [],
    metrics: [{ name: 'n', monoid: 'sum' }],
    tiers: [
      { name: '2m', bin: '2min', shard: '2d' },
      { name: '15m', bin: '15min', shard: '15d' },
      { name: '1h', bin: '1h', shard: '1mo' },
      { name: '7d', bin: '7d', shard: 'all' },
    ],
    ...overrides,
  }
}

describe('validatePartials: no-op when unset/empty', () => {
  test('returns null when partials is undefined', () => {
    expect(validatePartials(pyramid({}))).toBeNull()
  })

  test('returns null when partials is empty', () => {
    expect(validatePartials(pyramid({ partials: [], partialKey: 'k/{shard}/{period}' }))).toBeNull()
  })
})

describe('validatePartials: rejects misconfiguration', () => {
  test('rejects calendar cadence (mo)', () => {
    expect(() => validatePartials(pyramid({
      partials: ['1h', '1mo'],
      partialKey: 'avail/{tier}/p{shard}/{period}.parquet',
    }))).toThrow(/cadence '1mo' is calendar-variable/)
  })

  test('rejects calendar cadence (y)', () => {
    expect(() => validatePartials(pyramid({
      partials: ['1h', '1y'],
      partialKey: 'avail/{tier}/p{shard}/{period}.parquet',
    }))).toThrow(/cadence '1y' is calendar-variable/)
  })

  test('rejects non-divisibility-chained ladder', () => {
    // 1h and 5h: 5h % 1h == 0 ✓; 3h % 1h == 0 ✓; but 5h % 3h != 0.
    expect(() => validatePartials(pyramid({
      partials: ['1h', '3h', '5h'],
      partialKey: 'avail/{tier}/p{shard}/{period}.parquet',
    }))).toThrow(/cadence ladder not divisibility-chained/)
  })

  test('rejects missing partialKey when partials is non-empty', () => {
    expect(() => validatePartials(pyramid({
      partials: ['1h'],
    }))).toThrow(/pyramid.partialKey is unset/)
  })

  test('rejects partialKey that is empty string', () => {
    expect(() => validatePartials(pyramid({
      partials: ['1h'],
      partialKey: '',
    }))).toThrow(/pyramid.partialKey is unset/)
  })

  test('rejects ladder with no alignment-valid (tier, cadence) pair', () => {
    // Tiers with bins {2min, 15min}; cadences {7min} — none align (7%2≠0, 7%15≠0)
    // and we need to use a finer-only tier set so no pair works.
    expect(() => validatePartials(pyramid({
      tiers: [
        { name: '2m', bin: '2min', shard: '2d' },
        { name: '15m', bin: '15min', shard: '15d' },
      ],
      partials: ['7min'],
      partialKey: 'avail/{tier}/p{shard}/{period}.parquet',
    }))).toThrow(/no \(tier, cadence\) pair is alignment-valid/)
  })
})

describe('validatePartials: per-tier alignment filtering', () => {
  test('spec example: avail-v3 cadence ladder, per-tier filtering', () => {
    const result = validatePartials(pyramid({
      partials: ['5min', '10min', '30min', '1h', '3h', '12h', '1d', '3d'],
      partialKey: 'avail/{tier}/p{shard}/{period}.parquet',
    }))
    expect(result).not.toBeNull()
    expect(result!.cadences).toEqual([
      '5min', '10min', '30min', '1h', '3h', '12h', '1d', '3d',
    ])
    // 2m tier: bin=2min, shard=2d. Cadences with c%2==0 AND c<2d.
    //   5min (5%2=1) skipped; 10min ✓; 30min ✓; 1h ✓; 3h ✓; 12h ✓; 1d ✓; 3d (≥2d) skipped.
    expect(result!.perTier['2m']).toEqual(['10min', '30min', '1h', '3h', '12h', '1d'])
    // 15m tier: bin=15min, shard=15d. Cadences with c%15==0 AND c<15d.
    //   5min (5%15=5) skipped; 10min (10%15=10) skipped; 30min ✓; 1h ✓; 3h ✓; 12h ✓; 1d ✓; 3d ✓.
    expect(result!.perTier['15m']).toEqual(['30min', '1h', '3h', '12h', '1d', '3d'])
    // 1h tier: bin=1h, shard=1mo (calendar → all fixed cadences ≥1h pass shard check).
    //   5min, 10min, 30min (all <1h) skipped; 1h ✓; 3h ✓; 12h ✓; 1d ✓; 3d ✓.
    expect(result!.perTier['1h']).toEqual(['1h', '3h', '12h', '1d', '3d'])
    // 7d tier: bin=7d, shard=all → all fixed cadences ≥7d.
    //   only 7d would pass, but 7d isn't in the ladder; everything <7d skipped.
    expect(result!.perTier['7d']).toEqual([])
  })

  test('cadence equal to tier bin → 1-bin sub-shard (spec: degenerate but valid)', () => {
    const result = validatePartials(pyramid({
      partials: ['1h'],
      partialKey: 'avail/{tier}/p{shard}/{period}.parquet',
    }))
    expect(result).not.toBeNull()
    expect(result!.perTier['1h']).toEqual(['1h'])  // 1h@1h = 1 bin/sub-shard, allowed
  })

  test('skips calendar tier bins (mo/y) — sub-shards need fixed-width bins', () => {
    const result = validatePartials(pyramid({
      tiers: [
        { name: '15m', bin: '15min', shard: '15d' },
        { name: 'mo1', bin: '1mo', shard: '1y' },
      ],
      partials: ['1h', '1d'],
      partialKey: 'avail/{tier}/p{shard}/{period}.parquet',
    }))
    expect(result).not.toBeNull()
    expect(result!.perTier).toEqual({
      '15m': ['1h', '1d'],
      'mo1': [],
    })
  })

  test('shard=all → all fixed cadences ≥ tier.bin pass the shard check', () => {
    const result = validatePartials(pyramid({
      tiers: [{ name: 'allshard', bin: '15min', shard: 'all' }],
      partials: ['30min', '1h', '7d'],
      partialKey: 'k/{tier}/p{shard}/{period}',
    }))
    expect(result).not.toBeNull()
    expect(result!.perTier['allshard']).toEqual(['30min', '1h', '7d'])
  })

  test('sorts cadences ascending in result regardless of input order', () => {
    const result = validatePartials(pyramid({
      partials: ['1h', '30min', '12h', '10min'],
      partialKey: 'avail/{tier}/p{shard}/{period}.parquet',
    }))
    expect(result).not.toBeNull()
    expect(result!.cadences).toEqual(['10min', '30min', '1h', '12h'])
  })
})

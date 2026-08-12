import { describe, expect, test } from 'vitest'
import { memStorage } from './storage.js'
import { parsePyramidYaml, pyramidFromConfig } from './yaml.js'

const awairYaml = `
storage:
  type: r2
  bucket: 380nwk
  key: 'awair-{device_id}/{tier}/{shard}/{period}.parquet'

dims:
  - { name: device_id, type: int }

metrics:
  - { name: temp,  monoid: sum }
  - { name: co2,   monoid: sum }
  - { name: pm10,  monoid: sum }

tiers:
  - { name: raw, bin: 1min, shards: [1mo] }
  - { name: h1,  bin: 1h,   shards: [1mo] }
  - { name: d1,  bin: 1d,   shards: [1y]  }
  - { name: mo1, bin: 1mo,  shards: [1y]  }
`

describe('parsePyramidYaml', () => {
  test('parses the SPEC awair example', () => {
    expect(parsePyramidYaml(awairYaml)).toEqual({
      storage: { type: 'r2', bucket: '380nwk' },
      keyTemplate: 'awair-{device_id}/{tier}/{shard}/{period}.parquet',
      axis: 'time',
      binCol: 'ts',
      dims: [{ name: 'device_id', type: 'int' }],
      metrics: [
        { name: 'temp', monoid: 'sum' },
        { name: 'co2', monoid: 'sum' },
        { name: 'pm10', monoid: 'sum' },
      ],
      tiers: [
        { name: 'raw', bin: '1min', shards: ['1mo'] },
        { name: 'h1', bin: '1h', shards: ['1mo'] },
        { name: 'd1', bin: '1d', shards: ['1y'] },
        { name: 'mo1', bin: '1mo', shards: ['1y'] },
      ],
    })
  })

  test('respects explicit axis: step', () => {
    const cfg = parsePyramidYaml(`
storage: { type: fs, key: 'runs/{run_id}/{tier}/{shard}.parquet' }
axis: step
dims: [{ name: run_id, type: string }]
metrics: [{ name: loss, monoid: sum }]
tiers: [{ name: raw, bin: 1step, shards: [1run] }]
`)
    expect(cfg.axis).toBe('step')
    expect(cfg.tiers[0]).toEqual({ name: 'raw', bin: '1step', shards: ['1run'] })
  })

  test('respects explicit binCol override', () => {
    const cfg = parsePyramidYaml(`
storage: { type: r2, key: 'x/{tier}/{shard}/{period}.parquet' }
binCol: dt
dims: [{ name: x, type: string }]
metrics: [{ name: y, monoid: count }]
tiers: [{ name: raw, bin: 1d, shards: [1y] }]
`)
    expect(cfg.binCol).toBe('dt')
  })

  test('preserves extra storage fields (bucket, binding, region, etc.)', () => {
    const cfg = parsePyramidYaml(`
storage:
  type: r2
  binding: PYRAMID_BUCKET
  jurisdiction: eu
  key: 'k/{tier}/{shard}/{period}.parquet'
dims: [{ name: x, type: string }]
metrics: [{ name: y, monoid: count }]
tiers: [{ name: raw, bin: 1d, shards: [1y] }]
`)
    expect(cfg.storage).toEqual({ type: 'r2', binding: 'PYRAMID_BUCKET', jurisdiction: 'eu' })
  })

  test('parses multi-element shards ladder per tier', () => {
    const cfg = parsePyramidYaml(`
storage: { type: r2, key: 'k/{tier}/{shard}/{period}.parquet' }
dims: []
metrics: [{ name: y, monoid: count }]
tiers:
  - { name: 1m, bin: 1min, shards: [5min, 10min, 30min, 1h, 3h, 12h, 1d] }
  - { name: 15m, bin: 15min, shards: [30min, 1h, 3h, 12h, 1d, 15d] }
`)
    expect(cfg.tiers).toEqual([
      { name: '1m', bin: '1min', shards: ['5min', '10min', '30min', '1h', '3h', '12h', '1d'] },
      { name: '15m', bin: '15min', shards: ['30min', '1h', '3h', '12h', '1d', '15d'] },
    ])
  })
})

describe('parsePyramidYaml: validation', () => {
  test('throws on non-mapping top level', () => {
    expect(() => parsePyramidYaml('- foo\n- bar')).toThrow('parsePyramidYaml: top-level must be a mapping')
  })

  test('throws on missing storage.key', () => {
    expect(() => parsePyramidYaml(`
storage: { type: r2, bucket: x }
dims: []
metrics: []
tiers: [{ name: raw, bin: 1d, shards: [1y] }]
`)).toThrow('parsePyramidYaml: `storage.key` (key template) must be a string')
  })

  test('throws on missing storage.type', () => {
    expect(() => parsePyramidYaml(`
storage: { key: 'x/{tier}/{shard}/{period}.parquet' }
dims: []
metrics: []
tiers: [{ name: raw, bin: 1d, shards: [1y] }]
`)).toThrow('parsePyramidYaml: `storage.type` must be a string')
  })

  test('throws on invalid axis', () => {
    expect(() => parsePyramidYaml(`
storage: { type: r2, key: 'x' }
axis: epoch
dims: []
metrics: []
tiers: [{ name: raw, bin: 1d, shards: [1y] }]
`)).toThrow("parsePyramidYaml: invalid axis 'epoch' (want 'time' or 'step')")
  })

  test('throws on unknown monoid', () => {
    expect(() => parsePyramidYaml(`
storage: { type: r2, key: 'x' }
dims: []
metrics: [{ name: temp, monoid: average }]
tiers: [{ name: raw, bin: 1d, shards: [1y] }]
`)).toThrow("parsePyramidYaml: metrics[0].monoid 'average' invalid")
  })

  test('throws on unknown dim type', () => {
    expect(() => parsePyramidYaml(`
storage: { type: r2, key: 'x' }
dims: [{ name: foo, type: float }]
metrics: []
tiers: [{ name: raw, bin: 1d, shards: [1y] }]
`)).toThrow("parsePyramidYaml: dims[0].type 'float' invalid (want one of int/string/h3/geohash/s2)")
  })

  test('throws on empty tiers', () => {
    expect(() => parsePyramidYaml(`
storage: { type: r2, key: 'x' }
dims: []
metrics: []
tiers: []
`)).toThrow('parsePyramidYaml: `tiers` must be a non-empty array')
  })

  test('throws on empty per-tier shards', () => {
    expect(() => parsePyramidYaml(`
storage: { type: r2, key: 'x' }
dims: []
metrics: []
tiers: [{ name: raw, bin: 1d, shards: [] }]
`)).toThrow('parsePyramidYaml: tiers[0].shards must be a non-empty array of Shard strings')
  })
})

describe('parsePyramidYaml: {shard} placeholder guard', () => {
  // The awair Aug-2026 outage: `key: '.../{tier}/{period}.parquet'` (no
  // `{shard}`) paired with a multi-rung tier (`[1d, 4d, 32d]`) — two
  // rungs starting on the same day collided on one R2 key and silently
  // corrupted downstream reads for ~1 month. `parsePyramidYaml` must
  // reject the config at parse time so this never lands in production.

  test('multi-rung tier + template without {shard} throws (names offending tier + missing placeholder)', () => {
    const bad = `
storage: { type: r2, key: 'pyramid/awair-{device_id}/{tier}/{period}.parquet' }
dims: [{ name: device_id, type: int }]
metrics: [{ name: temp, monoid: sum }]
tiers:
  - { name: raw, bin: 1min, shards: [1h] }
  - { name: m3,  bin: 3min, shards: [1d, 4d, 32d] }
`
    expect(() => parsePyramidYaml(bad)).toThrow(
      /tier 'm3' has a multi-rung ladder \(\["1d","4d","32d"\]\) but keyTemplate 'pyramid\/awair-\{device_id\}\/\{tier\}\/\{period\}\.parquet' is missing the '\{shard\}' placeholder/,
    )
  })

  test('multi-rung tier + template with {shard} parses fine', () => {
    const cfg = parsePyramidYaml(`
storage: { type: r2, key: 'pyramid/awair-{device_id}/{tier}/{shard}/{period}.parquet' }
dims: [{ name: device_id, type: int }]
metrics: [{ name: temp, monoid: sum }]
tiers:
  - { name: raw, bin: 1min, shards: [1h] }
  - { name: m3,  bin: 3min, shards: [1d, 4d, 32d] }
`)
    expect(cfg.tiers[1]).toEqual({ name: 'm3', bin: '3min', shards: ['1d', '4d', '32d'] })
  })

  test('single-rung tiers + template without {shard} parses fine (no false positive)', () => {
    // Every tier is single-rung → each period gets a unique key regardless
    // of `{shard}`. This is the pre-multi-rung awair shape.
    const cfg = parsePyramidYaml(`
storage: { type: r2, key: 'pyramid/awair-{device_id}/{tier}/{period}.parquet' }
dims: [{ name: device_id, type: int }]
metrics: [{ name: temp, monoid: sum }]
tiers:
  - { name: raw, bin: 1min, shards: [1h] }
  - { name: h1,  bin: 1h,   shards: [1d] }
`)
    expect(cfg.tiers).toHaveLength(2)
  })

  test('single-rung tier + template with {shard} parses fine (placeholder is still valid)', () => {
    // awair's canonical `raw` tier + `.../{tier}/{shard}/{period}` template
    // — a placeholder is not required for single-rung tiers, but it's not
    // rejected either (substitutes to the single shard's label).
    const cfg = parsePyramidYaml(`
storage: { type: r2, key: 'pyramid/awair-{device_id}/{tier}/{shard}/{period}.parquet' }
dims: [{ name: device_id, type: int }]
metrics: [{ name: temp, monoid: sum }]
tiers:
  - { name: raw, bin: 1min, shards: [1h] }
`)
    expect(cfg.tiers).toEqual([{ name: 'raw', bin: '1min', shards: ['1h'] }])
  })
})

describe('pyramidFromConfig', () => {
  test('wires Storage into a full Pyramid', () => {
    const cfg = parsePyramidYaml(awairYaml)
    const storage = memStorage()
    const pyramid = pyramidFromConfig(cfg, storage)
    expect(pyramid.storage).toBe(storage)
    expect(pyramid.keyTemplate).toBe('awair-{device_id}/{tier}/{shard}/{period}.parquet')
    expect(pyramid.tiers).toHaveLength(4)
  })
})

describe('parsePyramidYaml: geo block', () => {
  const ctbkGeoYaml = `
storage:
  type: r2
  bucket: ctbk
  key: 'trips/{tier}/{shard}/{period}.parquet'

dims:
  - { name: station_id, type: string }
  - { name: gender,     type: string }
  - { name: rideable,   type: string }
  - { name: user_type,  type: string }

metrics:
  - { name: count, monoid: count }
  - { name: duration_s, monoid: sum }

tiers:
  - { name: h1, bin: 1h, shards: [1mo] }
  - { name: d1, bin: 1d, shards: [1y] }
  - { name: mo1, bin: 1mo, shards: [1y] }

geo:
  cellCol: h3
  resolutions: [12, 9, 7, 4]
`

  test('parses geo block with cellCol + resolutions', () => {
    const cfg = parsePyramidYaml(ctbkGeoYaml)
    expect(cfg.geo).toEqual({ cellCol: 'h3', resolutions: [12, 9, 7, 4] })
  })

  test('defaults cellCol to h3_cell when omitted', () => {
    const cfg = parsePyramidYaml(`
storage: { type: r2, key: 'k/{tier}/{shard}/{period}.parquet' }
dims: []
metrics: []
tiers: [{ name: raw, bin: 1d, shards: [1y] }]
geo: { resolutions: [10, 7] }
`)
    expect(cfg.geo).toEqual({ cellCol: 'h3_cell', resolutions: [10, 7] })
  })

  test('omits geo field when not in YAML', () => {
    const cfg = parsePyramidYaml(`
storage: { type: r2, key: 'k/{tier}/{shard}/{period}.parquet' }
dims: []
metrics: []
tiers: [{ name: raw, bin: 1d, shards: [1y] }]
`)
    expect(cfg.geo).toBeUndefined()
  })

  test('throws when resolutions are not finest-first (descending)', () => {
    expect(() => parsePyramidYaml(`
storage: { type: r2, key: 'k/{tier}/{shard}/{period}.parquet' }
dims: []
metrics: []
tiers: [{ name: raw, bin: 1d, shards: [1y] }]
geo: { resolutions: [4, 7, 9] }
`)).toThrow('parsePyramidYaml: geo.resolutions must be finest-first (descending); got 4, 7, 9')
  })

  test('throws on out-of-range resolution', () => {
    expect(() => parsePyramidYaml(`
storage: { type: r2, key: 'k/{tier}/{shard}/{period}.parquet' }
dims: []
metrics: []
tiers: [{ name: raw, bin: 1d, shards: [1y] }]
geo: { resolutions: [16] }
`)).toThrow('parsePyramidYaml: geo.resolutions[0] must be an integer 0-15 (got 16)')
  })

  test('pyramidFromConfig propagates geo to the Pyramid', () => {
    const cfg = parsePyramidYaml(ctbkGeoYaml)
    const pyramid = pyramidFromConfig(cfg, memStorage())
    expect(pyramid.geo).toEqual({ cellCol: 'h3', resolutions: [12, 9, 7, 4] })
  })
})

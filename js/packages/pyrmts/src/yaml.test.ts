import { describe, expect, test } from 'vitest'
import { memStorage } from './storage.js'
import { parsePyramidYaml, pyramidFromConfig } from './yaml.js'

const awairYaml = `
storage:
  type: r2
  bucket: 380nwk
  key: 'awair-{device_id}/{tier}/{period}.parquet'

dims:
  - { name: device_id, type: int }

metrics:
  - { name: temp,  monoid: sum }
  - { name: co2,   monoid: sum }
  - { name: pm10,  monoid: sum }

tiers:
  - { name: raw, bin: 1min, shard: 1mo }
  - { name: h1,  bin: 1h,   shard: 1mo }
  - { name: d1,  bin: 1d,   shard: 1y  }
  - { name: mo1, bin: 1mo,  shard: 1y  }
`

describe('parsePyramidYaml', () => {
  test('parses the SPEC awair example', () => {
    expect(parsePyramidYaml(awairYaml)).toEqual({
      storage: { type: 'r2', bucket: '380nwk' },
      keyTemplate: 'awair-{device_id}/{tier}/{period}.parquet',
      axis: 'time',
      binCol: 'ts',
      dims: [{ name: 'device_id', type: 'int' }],
      metrics: [
        { name: 'temp', monoid: 'sum' },
        { name: 'co2', monoid: 'sum' },
        { name: 'pm10', monoid: 'sum' },
      ],
      tiers: [
        { name: 'raw', bin: '1min', shard: '1mo' },
        { name: 'h1', bin: '1h', shard: '1mo' },
        { name: 'd1', bin: '1d', shard: '1y' },
        { name: 'mo1', bin: '1mo', shard: '1y' },
      ],
    })
  })

  test('respects explicit axis: step', () => {
    const cfg = parsePyramidYaml(`
storage: { type: fs, key: 'runs/{run_id}/{tier}.parquet' }
axis: step
dims: [{ name: run_id, type: string }]
metrics: [{ name: loss, monoid: sum }]
tiers: [{ name: raw, bin: 1step, shard: 1run }]
`)
    expect(cfg.axis).toBe('step')
    expect(cfg.tiers[0]).toEqual({ name: 'raw', bin: '1step', shard: '1run' })
  })

  test('respects explicit binCol override', () => {
    const cfg = parsePyramidYaml(`
storage: { type: r2, key: 'x/{tier}/{period}.parquet' }
binCol: dt
dims: [{ name: x, type: string }]
metrics: [{ name: y, monoid: count }]
tiers: [{ name: raw, bin: 1d, shard: 1y }]
`)
    expect(cfg.binCol).toBe('dt')
  })

  test('preserves extra storage fields (bucket, binding, region, etc.)', () => {
    const cfg = parsePyramidYaml(`
storage:
  type: r2
  binding: PYRAMID_BUCKET
  jurisdiction: eu
  key: 'k/{tier}/{period}.parquet'
dims: [{ name: x, type: string }]
metrics: [{ name: y, monoid: count }]
tiers: [{ name: raw, bin: 1d, shard: 1y }]
`)
    expect(cfg.storage).toEqual({ type: 'r2', binding: 'PYRAMID_BUCKET', jurisdiction: 'eu' })
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
tiers: [{ name: raw, bin: 1d, shard: 1y }]
`)).toThrow('parsePyramidYaml: `storage.key` (key template) must be a string')
  })

  test('throws on missing storage.type', () => {
    expect(() => parsePyramidYaml(`
storage: { key: 'x/{tier}/{period}.parquet' }
dims: []
metrics: []
tiers: [{ name: raw, bin: 1d, shard: 1y }]
`)).toThrow('parsePyramidYaml: `storage.type` must be a string')
  })

  test('throws on invalid axis', () => {
    expect(() => parsePyramidYaml(`
storage: { type: r2, key: 'x' }
axis: epoch
dims: []
metrics: []
tiers: [{ name: raw, bin: 1d, shard: 1y }]
`)).toThrow("parsePyramidYaml: invalid axis 'epoch' (want 'time' or 'step')")
  })

  test('throws on unknown monoid', () => {
    expect(() => parsePyramidYaml(`
storage: { type: r2, key: 'x' }
dims: []
metrics: [{ name: temp, monoid: average }]
tiers: [{ name: raw, bin: 1d, shard: 1y }]
`)).toThrow("parsePyramidYaml: metrics[0].monoid 'average' invalid")
  })

  test('throws on unknown dim type', () => {
    expect(() => parsePyramidYaml(`
storage: { type: r2, key: 'x' }
dims: [{ name: foo, type: float }]
metrics: []
tiers: [{ name: raw, bin: 1d, shard: 1y }]
`)).toThrow("parsePyramidYaml: dims[0].type 'float' invalid (want one of int/string/h3/geohash)")
  })

  test('throws on empty tiers', () => {
    expect(() => parsePyramidYaml(`
storage: { type: r2, key: 'x' }
dims: []
metrics: []
tiers: []
`)).toThrow('parsePyramidYaml: `tiers` must be a non-empty array')
  })
})

describe('pyramidFromConfig', () => {
  test('wires Storage into a full Pyramid', () => {
    const cfg = parsePyramidYaml(awairYaml)
    const storage = memStorage()
    const pyramid = pyramidFromConfig(cfg, storage)
    expect(pyramid.storage).toBe(storage)
    expect(pyramid.keyTemplate).toBe('awair-{device_id}/{tier}/{period}.parquet')
    expect(pyramid.tiers).toHaveLength(4)
  })
})

describe('parsePyramidYaml: geo block', () => {
  const ctbkGeoYaml = `
storage:
  type: r2
  bucket: ctbk
  key: 'trips/{tier}/{period}.parquet'

dims:
  - { name: station_id, type: string }
  - { name: gender,     type: string }
  - { name: rideable,   type: string }
  - { name: user_type,  type: string }

metrics:
  - { name: count, monoid: count }
  - { name: duration_s, monoid: sum }

tiers:
  - { name: h1, bin: 1h, shard: 1mo }
  - { name: d1, bin: 1d, shard: 1y }
  - { name: mo1, bin: 1mo, shard: 1y }

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
storage: { type: r2, key: 'k/{tier}/{period}.parquet' }
dims: []
metrics: []
tiers: [{ name: raw, bin: 1d, shard: 1y }]
geo: { resolutions: [10, 7] }
`)
    expect(cfg.geo).toEqual({ cellCol: 'h3_cell', resolutions: [10, 7] })
  })

  test('omits geo field when not in YAML', () => {
    const cfg = parsePyramidYaml(`
storage: { type: r2, key: 'k/{tier}/{period}.parquet' }
dims: []
metrics: []
tiers: [{ name: raw, bin: 1d, shard: 1y }]
`)
    expect(cfg.geo).toBeUndefined()
  })

  test('throws when resolutions are not finest-first (descending)', () => {
    expect(() => parsePyramidYaml(`
storage: { type: r2, key: 'k/{tier}/{period}.parquet' }
dims: []
metrics: []
tiers: [{ name: raw, bin: 1d, shard: 1y }]
geo: { resolutions: [4, 7, 9] }
`)).toThrow('parsePyramidYaml: geo.resolutions must be finest-first (descending); got 4, 7, 9')
  })

  test('throws on out-of-range resolution', () => {
    expect(() => parsePyramidYaml(`
storage: { type: r2, key: 'k/{tier}/{period}.parquet' }
dims: []
metrics: []
tiers: [{ name: raw, bin: 1d, shard: 1y }]
geo: { resolutions: [16] }
`)).toThrow('parsePyramidYaml: geo.resolutions[0] must be an integer 0-15 (got 16)')
  })

  test('pyramidFromConfig propagates geo to the Pyramid', () => {
    const cfg = parsePyramidYaml(ctbkGeoYaml)
    const pyramid = pyramidFromConfig(cfg, memStorage())
    expect(pyramid.geo).toEqual({ cellCol: 'h3', resolutions: [12, 9, 7, 4] })
  })
})

describe('parsePyramidYaml: partials + partialKey', () => {
  const availYaml = `
storage:
  type: r2
  bucket: ctbk
  key: 'avail-v3/{tier}/{period}.parquet'
  partialKey: 'avail-v3/{tier}/p{shard}/{period}.parquet'

partials:
  - 10min
  - 30min
  - 1h
  - 3h
  - 12h
  - 1d

dims:
  - { name: station_id, type: int }

metrics:
  - { name: n_bikes, monoid: sum }

tiers:
  - { name: 15m, bin: 15min, shard: 15d }
  - { name: 1h,  bin: 1h,    shard: 1mo }
`

  test('parses partials list + storage.partialKey', () => {
    const cfg = parsePyramidYaml(availYaml)
    expect(cfg.partials).toEqual(['10min', '30min', '1h', '3h', '12h', '1d'])
    expect(cfg.partialKey).toBe('avail-v3/{tier}/p{shard}/{period}.parquet')
  })

  test('storage block omits partialKey when missing', () => {
    const cfg = parsePyramidYaml(`
storage: { type: r2, key: 'k/{tier}/{period}.parquet' }
dims: []
metrics: [{ name: x, monoid: count }]
tiers: [{ name: raw, bin: 1d, shard: 1y }]
`)
    expect(cfg.partials).toBeUndefined()
    expect(cfg.partialKey).toBeUndefined()
  })

  test('pyramidFromConfig propagates partials + partialKey to the Pyramid', () => {
    const cfg = parsePyramidYaml(availYaml)
    const pyramid = pyramidFromConfig(cfg, memStorage())
    expect(pyramid.partials).toEqual(['10min', '30min', '1h', '3h', '12h', '1d'])
    expect(pyramid.partialKey).toBe('avail-v3/{tier}/p{shard}/{period}.parquet')
  })

  test('throws when partials is not an array', () => {
    expect(() => parsePyramidYaml(`
storage: { type: r2, key: 'k/{tier}/{period}.parquet' }
partials: '1h'
dims: []
metrics: [{ name: x, monoid: count }]
tiers: [{ name: raw, bin: 1d, shard: 1y }]
`)).toThrow('parsePyramidYaml: `partials` must be an array of Duration strings')
  })

  test('throws when storage.partialKey is not a string', () => {
    expect(() => parsePyramidYaml(`
storage:
  type: r2
  key: 'k/{tier}/{period}.parquet'
  partialKey: 42
dims: []
metrics: [{ name: x, monoid: count }]
tiers: [{ name: raw, bin: 1d, shard: 1y }]
`)).toThrow('parsePyramidYaml: `storage.partialKey` (sub-shard key template) must be a string')
  })

  test('storage block does not leak partialKey into storageMeta', () => {
    const cfg = parsePyramidYaml(availYaml)
    expect(cfg.storage).toEqual({ type: 'r2', bucket: 'ctbk' })
  })
})

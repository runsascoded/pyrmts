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

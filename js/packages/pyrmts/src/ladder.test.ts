// Calendar ladder validation (`specs/calendar-units.md`): calendar-calendar
// rung pairs divide in months (`y` ≡ `12mo`), `Nmo` entries must tile a
// year, mixed fixed/calendar pairs assert nominal-width (30d/365d)
// ascension and divisibility (`specs/calendar-rung-consolidation.md`).
// Python twin: `python/pyrmts/tests/test_yaml.py` calendar cases.

import { describe, expect, test } from 'vitest'

import { validateLadders } from './ladder.js'
import type { Pyramid, Shard, Tier } from './types.js'

function pyramidWith(tier: Partial<Tier> & { bin: Tier['bin']; shards: Shard[] }): Pyramid {
  return {
    storage: { fetch: async () => { throw new Error('not used') } } as Pyramid['storage'],
    keyTemplate: 'a/{tier}/{shard}/{period}.parquet',
    axis: 'time',
    binCol: 'ts',
    dims: [],
    metrics: [{ name: 'n', monoid: 'count' }],
    tiers: [{ name: 'mo', ...tier } as Tier],
  }
}

describe('validateLadders calendar rungs', () => {
  test('accepts multi-unit calendar chains', () => {
    expect(() => validateLadders(pyramidWith({ bin: '1mo', shards: ['1mo', '3mo', '1y'] }))).not.toThrow()
    expect(() => validateLadders(pyramidWith({ bin: '1y', shards: ['1y', '4y'] }))).not.toThrow()
  })

  test('rejects non-dividing calendar rung pairs', () => {
    expect(() => validateLadders(pyramidWith({ bin: '1mo', shards: ['2mo', '3mo'] })))
      .toThrow("validateLadders: tier 'mo' shards[0]='2mo' does not divide shards[1]='3mo' (in months)")
  })

  test('rejects month spans that do not tile a year', () => {
    expect(() => validateLadders(pyramidWith({ bin: '5mo', shards: ['1y'] })))
      .toThrow("validateLadders: tier 'mo' month-span '5mo' doesn't tile a year evenly (12 % 5 !== 0)")
  })

  test('rejects calendar shard smaller than calendar bin', () => {
    expect(() => validateLadders(pyramidWith({ bin: '6mo', shards: ['4mo'] })))
      .toThrow("validateLadders: tier 'mo' shards[0]='4mo' is smaller than bin '6mo' (in months)")
  })

  test('rejects descending mixed pair by nominal width', () => {
    expect(() => validateLadders(pyramidWith({ bin: '1d', shards: ['1mo', '14d'] })))
      .toThrow("validateLadders: tier 'mo' shards not ascending (shards[1]='14d' <= shards[0]='1mo' by nominal width)")
  })

  test('accepts mixed fixed/calendar chains (awair [1d, 1mo] shape)', () => {
    expect(() => validateLadders(pyramidWith({ bin: '1min', shards: ['1d', '1mo'] }))).not.toThrow()
    expect(() => validateLadders(pyramidWith({ bin: '1min', shards: ['3d', '1mo'] }))).not.toThrow()
    expect(() => validateLadders(pyramidWith({ bin: '1min', shards: ['1d', '3mo'] }))).not.toThrow()
  })

  test('rejects non-dividing mixed pair by nominal width', () => {
    expect(() => validateLadders(pyramidWith({ bin: '1d', shards: ['7d', '1mo'] })))
      .toThrow("validateLadders: tier 'mo' shards[0]='7d' does not divide shards[1]='1mo' (by nominal width)")
  })
})

describe('validateLadders {shard} placeholder guard', () => {
  // Same guard `parsePyramidYaml` enforces, but downstream — a Pyramid
  // constructed by hand (no yaml round-trip) still can't reach the
  // planner with a collision-prone template.

  test('rejects multi-rung tier when keyTemplate lacks {shard}', () => {
    const p: Pyramid = {
      storage: { fetch: async () => { throw new Error('not used') } } as Pyramid['storage'],
      keyTemplate: 'awair-{device_id}/{tier}/{period}.parquet',
      axis: 'time',
      binCol: 'ts',
      dims: [{ name: 'device_id', type: 'int' }],
      metrics: [{ name: 'n', monoid: 'count' }],
      tiers: [{ name: 'm3', bin: '3min', shards: ['1d', '4d', '32d'] }],
    }
    expect(() => validateLadders(p)).toThrow(
      /tier 'm3' has a multi-rung ladder \(\["1d","4d","32d"\]\) but keyTemplate 'awair-\{device_id\}\/\{tier\}\/\{period\}\.parquet' is missing the '\{shard\}' placeholder/,
    )
  })

  test('accepts single-rung tier when keyTemplate lacks {shard}', () => {
    const p: Pyramid = {
      storage: { fetch: async () => { throw new Error('not used') } } as Pyramid['storage'],
      keyTemplate: 'awair-{device_id}/{tier}/{period}.parquet',
      axis: 'time',
      binCol: 'ts',
      dims: [{ name: 'device_id', type: 'int' }],
      metrics: [{ name: 'n', monoid: 'count' }],
      tiers: [{ name: 'raw', bin: '1min', shards: ['1h'] }],
    }
    expect(() => validateLadders(p)).not.toThrow()
  })
})

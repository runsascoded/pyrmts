// Calendar ladder validation (`specs/calendar-units.md`): calendar-calendar
// rung pairs divide in months (`y` ≡ `12mo`), `Nmo` entries must tile a
// year, mixed fixed/calendar pairs assert nominal-width (30d/365d)
// ascension. Python twin: `python/pyrmts/tests/test_yaml.py` calendar cases.

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
})

// Key templates with the content-hash token — twin of `test_keys.py`.

import { describe, expect, test } from 'vitest'
import {
  hashWidth,
  keyPattern,
  keysEtag,
  parseKey,
  shardKey,
  slotKey,
  slotOf,
  substituteKey,
  templateHasHash,
  validateKeyTemplate,
} from './keys.js'
import { parsePyramidYaml } from './yaml.js'
import type { Pyramid } from './types.js'

const MD5 = '4f2a1c9d8e7b6a5f4e3d2c1b0a9f8e7d'
const T = 'rides/{tier}/{shard}/{period}.{hash:12}.parquet'
const V = { tier: 'base', shard: '1mo', period: '2026-01', hash: MD5 }

describe('substituteKey with {hash}', () => {
  test('full and truncated', () => {
    expect(substituteKey('blobs/{hash}.parquet', V)).toBe(`blobs/${MD5}.parquet`)
    expect(substituteKey(T, V)).toBe(`rides/base/1mo/2026-01.${MD5.slice(0, 12)}.parquet`)
    expect(substituteKey('{hash:1}', V)).toBe(MD5[0])
    expect(substituteKey('{hash:32}', V)).toBe(MD5)
    expect(hashWidth(T)).toBe(12)
    expect(hashWidth('blobs/{hash}.parquet')).toBe(32)
    expect(hashWidth('a/{tier}.parquet')).toBeNull()
  })

  test('validation', () => {
    expect(() => validateKeyTemplate('x/{hash:0}')).toThrow('{hash:0} must be 1..32')
    expect(() => validateKeyTemplate('x/{hash:33}')).toThrow('{hash:33} must be 1..32')
    expect(() => validateKeyTemplate('x/{period:8}')).toThrow('`:8` is only defined for {hash}')
    expect(() => substituteKey('{hash:8}', { hash: 'nope' })).toThrow('wants a 32-char md5 hex')
    expect(() => substituteKey(T, { tier: 'base', shard: '1mo', period: '2026-01' })).toThrow('missing value for {hash}')
    expect(templateHasHash(T)).toBe(true)
    expect(templateHasHash('rides/{tier}/{period}.parquet')).toBe(false)
    expect(() => parsePyramidYaml(`
storage:
  type: s3
  bucket: b
  key: "p/{tier}/{shard}/{period:8}.parquet"
binCol: ts
dims: []
metrics: [{ name: v, monoid: sum }]
tiers: [{ name: base, bin: 1h, shards: [1mo] }]
`)).toThrow('`:8` is only defined for {hash}')
  })

  test('slotKey keeps the hash placeholder', () => {
    const v = { tier: 'base', shard: '1mo', period: '2026-01' }
    expect(slotKey(T, v)).toBe('rides/base/1mo/2026-01.{hash:12}.parquet')
    expect(slotKey('rides/{tier}/{period}.parquet', v)).toBe('rides/base/2026-01.parquet')
  })

  test('keyPattern, parseKey, slotOf', () => {
    const key = `rides/base/1mo/2026-01.${MD5.slice(0, 12)}.parquet`
    expect(keyPattern(T).source).toBe('^rides\\/(?<tier>[^/]+)\\/(?<shard>[^/]+)\\/(?<period>[^/]+)\\.(?<hash>[0-9a-f]{12})\\.parquet$')
    expect(parseKey(T, key)).toEqual({ tier: 'base', shard: '1mo', period: '2026-01', hash: MD5.slice(0, 12) })
    expect(parseKey(T, 'rides/base/1mo/2026-01.parquet')).toBeNull()
    expect(parseKey(T, `rides/base/1mo/2026-01.${MD5.slice(0, 11)}.parquet`)).toBeNull()
    expect(slotOf(T, key)).toBe('rides/base/1mo/2026-01.{hash:12}.parquet')
    expect(slotOf('blobs/{hash}.parquet', `blobs/${MD5}.parquet`)).toBe('blobs/{hash}.parquet')
    expect(parseKey('a/{x}/{x}.parquet', 'a/1/1.parquet')).toEqual({ x: '1' })
    expect(parseKey('a/{x}/{x}.parquet', 'a/1/2.parquet')).toBeNull()
  })

  test('shardKey yields the slot key for a hashed template', () => {
    const pyramid = { keyTemplate: T } as Pyramid
    expect(shardKey(pyramid, 'base', '1mo', new Date('2026-01-01T00:00:00Z'))).toBe('rides/base/1mo/2026-01.{hash:12}.parquet')
  })
})

describe('keysEtag', () => {
  test('is order-independent, key-sensitive, and versioned', () => {
    const a = keysEtag(['p/1.abc.parquet', 'p/2.def.parquet'])
    expect(a).toBe(keysEtag(['p/2.def.parquet', 'p/1.abc.parquet']))
    expect(a).toMatch(/^"v1-2-[0-9a-f]{8}"$/)
    expect(keysEtag(['p/1.abc.parquet', 'p/2.fff.parquet'])).not.toBe(a)   // a registry swap changes the tag
    expect(keysEtag(['p/1.abc.parquet', 'p/2.def.parquet'], 2)).not.toBe(a)
    expect(keysEtag([])).toBe('"v1-0-' + keysEtag([]).slice(6))
  })
})

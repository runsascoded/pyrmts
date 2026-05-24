import { describe, expect, test } from 'vitest'
import { getMonoid, stateColumns, type Row } from './monoids.js'

describe('sum monoid', () => {
  test('combines (n, sum, sumsq) element-wise', () => {
    const m = getMonoid('sum')
    const target: Row = { temp_n: 10, temp_sum: 200, temp_sumsq: 4000 }
    const source: Row = { temp_n: 5, temp_sum: 100, temp_sumsq: 2000 }
    m.combine(target, source, 'temp')
    expect(target).toEqual({ temp_n: 15, temp_sum: 300, temp_sumsq: 6000 })
  })

  test('treats missing columns as zero', () => {
    const m = getMonoid('sum')
    const target: Row = {}
    const source: Row = { temp_n: 5, temp_sum: 100, temp_sumsq: 2000 }
    m.combine(target, source, 'temp')
    expect(target).toEqual({ temp_n: 5, temp_sum: 100, temp_sumsq: 2000 })
  })

  test('is associative: combine(combine(a, b), c) === combine(a, combine(b, c))', () => {
    const m = getMonoid('sum')
    const make = (n: number, s: number, sq: number): Row => ({ x_n: n, x_sum: s, x_sumsq: sq })
    const a = make(1, 10, 100)
    const b = make(2, 20, 400)
    const c = make(3, 30, 900)

    const ab = { ...a }
    m.combine(ab, b, 'x')
    const abc1 = { ...ab }
    m.combine(abc1, c, 'x')

    const bc = { ...b }
    m.combine(bc, c, 'x')
    const abc2 = { ...a }
    m.combine(abc2, bc, 'x')

    expect(abc1).toEqual(abc2)
    expect(abc1).toEqual({ x_n: 6, x_sum: 60, x_sumsq: 1400 })
  })

  test('only touches the named metric, not sibling columns', () => {
    const m = getMonoid('sum')
    const target: Row = { temp_n: 10, temp_sum: 200, temp_sumsq: 4000, co2_n: 7, co2_sum: 5000 }
    const source: Row = { temp_n: 5, temp_sum: 100, temp_sumsq: 2000, co2_n: 99, co2_sum: 99999 }
    m.combine(target, source, 'temp')
    expect(target).toEqual({
      temp_n: 15, temp_sum: 300, temp_sumsq: 6000,
      co2_n: 7, co2_sum: 5000,
    })
  })
})

describe('count monoid', () => {
  test('sums single-column state', () => {
    const m = getMonoid('count')
    const target: Row = { trips: 100 }
    const source: Row = { trips: 25 }
    m.combine(target, source, 'trips')
    expect(target).toEqual({ trips: 125 })
  })

  test('treats missing column as zero', () => {
    const m = getMonoid('count')
    const target: Row = {}
    const source: Row = { trips: 7 }
    m.combine(target, source, 'trips')
    expect(target).toEqual({ trips: 7 })
  })
})

describe('stateColumns', () => {
  test('sum returns 3 suffixed columns', () => {
    expect(stateColumns('sum', 'temp')).toEqual(['temp_n', 'temp_sum', 'temp_sumsq'])
  })

  test('count returns the bare metric name', () => {
    expect(stateColumns('count', 'trips')).toEqual(['trips'])
  })
})

describe('unimplemented monoids', () => {
  test('throws on histogram (not yet implemented)', () => {
    expect(() => getMonoid('histogram')).toThrow("Monoid 'histogram' not yet implemented")
  })

  test('throws on tdigest', () => {
    expect(() => getMonoid('tdigest')).toThrow("Monoid 'tdigest' not yet implemented")
  })
})

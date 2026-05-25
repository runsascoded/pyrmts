import { describe, expect, test } from 'vitest'
import { pivotTallToHistogram } from './pivot.js'
import type { Row } from './monoids.js'

describe('pivotTallToHistogram', () => {
  test('collapses tall rows into one histogram per group', () => {
    // ctbk avail-style: one row per (station, dt, state) with minutes count.
    const tall: Row[] = [
      { ts: 1000, station_id: 'A', state: '0', minutes: 5 },
      { ts: 1000, station_id: 'A', state: '1', minutes: 10 },
      { ts: 1000, station_id: 'A', state: '5', minutes: 45 },
      { ts: 1000, station_id: 'B', state: '0', minutes: 60 },
    ]
    const wide = pivotTallToHistogram(tall, {
      histogramCol: 'states',
      categoryCol: 'state',
      countCol: 'minutes',
      groupBy: ['ts', 'station_id'],
    })
    expect(wide).toEqual([
      { ts: 1000, station_id: 'A', states: { '0': 5, '1': 10, '5': 45 } },
      { ts: 1000, station_id: 'B', states: { '0': 60 } },
    ])
  })

  test('sums duplicate categories within a group', () => {
    const tall: Row[] = [
      { ts: 1, k: 'a', n: 3 },
      { ts: 1, k: 'a', n: 4 },
      { ts: 1, k: 'b', n: 1 },
    ]
    expect(pivotTallToHistogram(tall, {
      histogramCol: 'hist',
      categoryCol: 'k',
      countCol: 'n',
      groupBy: ['ts'],
    })).toEqual([
      { ts: 1, hist: { a: 7, b: 1 } },
    ])
  })

  test('treats missing count as zero, missing category as the literal string "undefined"', () => {
    // Defensive: prevents `NaN` poisoning the histogram. Categories should
    // be strings in well-formed input, but stringify defensively.
    const tall: Row[] = [
      { ts: 1, k: 'a' },                // no n → 0
      { ts: 1, k: undefined, n: 5 },    // undefined category
    ]
    expect(pivotTallToHistogram(tall, {
      histogramCol: 'hist',
      categoryCol: 'k',
      countCol: 'n',
      groupBy: ['ts'],
    })).toEqual([
      { ts: 1, hist: { a: 0, undefined: 5 } },
    ])
  })

  test('returns empty array for empty input', () => {
    expect(pivotTallToHistogram([], {
      histogramCol: 'h',
      categoryCol: 'k',
      countCol: 'n',
      groupBy: ['ts'],
    })).toEqual([])
  })

  test('preserves group-key column values verbatim (not stringified) in output rows', () => {
    const tall: Row[] = [
      { ts: 1735689600000, station_id: 42, state: 'x', minutes: 1 },
    ]
    const [out] = pivotTallToHistogram(tall, {
      histogramCol: 'h',
      categoryCol: 'state',
      countCol: 'minutes',
      groupBy: ['ts', 'station_id'],
    })
    expect(out?.ts).toBe(1735689600000)
    expect(out?.station_id).toBe(42)
  })
})

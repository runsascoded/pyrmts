// Strict-cascade source selection + shard readiness
// (`specs/source-readiness-pending.md`). The avail-v5 enumeration is the
// parity twin of `pyrmts_engine/tests/test_materialize.py::
// test_buildable_at_avail_v5_enumeration`.

import { describe, expect, test } from 'vitest'
import { ceilToSpan, parseDuration, shardPeriodsCovering } from './axis.js'
import { shardBuildableAt, sourceTierFor } from './cascade-source.js'
import type { Pyramid, Shard, Tier } from './types.js'

const ladder = (...tiers: Tier[]): Pick<Pyramid, 'tiers'> => ({ tiers })

const utc = (iso: string): Date => new Date(`${iso}Z`)

describe('ceilToSpan', () => {
  test('aligned instants are fixed points; misaligned round up', () => {
    const h2 = parseDuration('2h')
    expect(ceilToSpan(utc('2026-07-31T22:00:00'), h2)).toEqual(utc('2026-07-31T22:00:00'))
    expect(ceilToSpan(utc('2026-07-31T21:00:00'), h2)).toEqual(utc('2026-07-31T22:00:00'))
    expect(ceilToSpan(utc('2026-07-31T21:00:00'), parseDuration('45min'))).toEqual(utc('2026-07-31T21:00:00'))
    expect(ceilToSpan(utc('2026-07-31T22:00:00'), parseDuration('45min'))).toEqual(utc('2026-07-31T22:30:00'))
    expect(ceilToSpan(utc('2026-02-01T12:00:00'), parseDuration('1mo'))).toEqual(utc('2026-03-01T00:00:00'))
  })
})

describe('sourceTierFor', () => {
  const p = ladder(
    { name: 'q', bin: '15min', shards: ['6h', '1d'] },
    { name: 'h', bin: '1h', shards: ['1d', '4d'] },
    { name: 'd', bin: '1d', shards: ['4d'] },
  )

  test('base tier → null; otherwise largest divisor wins', () => {
    expect(sourceTierFor(p, 'q')).toBeNull()
    expect(sourceTierFor(p, 'h')?.name).toBe('q')
    expect(sourceTierFor(p, 'd')?.name).toBe('h')
  })

  test('malformed ladder throws', () => {
    const bad = ladder(
      { name: '2m', bin: '2min', shards: ['1h'] },
      { name: '3m', bin: '3min', shards: ['1h'] },  // 3min % 2min != 0
    )
    expect(() => sourceTierFor(bad, '3m')).toThrow(
      'no source tier for /3m — pyramid ladder is malformed',
    )
  })
})

describe('shardBuildableAt', () => {
  test('aligned endings and the base tier are ready at periodEnd', () => {
    const p = ladder(
      { name: 'q', bin: '15min', shards: ['6h', '1d'] },
      { name: 'h', bin: '1h', shards: ['1d', '4d'] },
      { name: 'd', bin: '1d', shards: ['4d'] },
    )
    for (const [tier, end] of [
      ['q', '2026-01-03T00:00:00'],  // base tier: raw territory
      ['h', '2026-01-03T00:00:00'],  // 1d ends ≡ 0 mod q's 6h
      ['h', '2026-01-02T06:00:00'],  // sub-day ending, still 6h-aligned
      ['d', '2026-01-03T00:00:00'],  // 4d ends ≡ 0 mod h's 1d
    ] as const) {
      expect(shardBuildableAt(p, tier, utc(end))).toEqual(utc(end))
    }
  })

  test('the incident shape: /1h@3h odd-hour endings wait +1h', () => {
    const p = ladder(
      { name: '30m', bin: '30min', shards: ['2h', '6h'] },
      { name: '1h', bin: '1h', shards: ['3h', '6h'] },
    )
    expect(shardBuildableAt(p, '1h', utc('2026-07-31T21:00:00'))).toEqual(utc('2026-07-31T22:00:00'))
    expect(shardBuildableAt(p, '1h', utc('2026-08-01T00:00:00'))).toEqual(utc('2026-08-01T00:00:00'))
  })

  test('two levels of misalignment compound', () => {
    // Ceil to 2h (22:00), then the source tile's own source cover
    // ceils to 45min (22:30).
    const p = ladder(
      { name: '15m', bin: '15min', shards: ['45min'] },
      { name: '30m', bin: '30min', shards: ['2h'] },
      { name: '1h', bin: '1h', shards: ['3h'] },
    )
    expect(shardBuildableAt(p, '1h', utc('2026-07-31T21:00:00'))).toEqual(utc('2026-07-31T22:30:00'))
  })
})

// The avail-v5 ladder (ctbk `configs/pyramids/avail-v5.yaml`, extended
// view: `shards` + `lambda_shards`).
const AVAIL_V5_TIERS: Array<[string, string, Shard[]]> = [
  ['1m', '1min', ['5min', '10min', '30min', '1h', '3h', '6h', '12h', '1d', '2d']],
  ['2m', '2min', ['10min', '30min', '1h', '3h', '6h', '12h', '1d', '2d', '4d']],
  ['3m', '3min', ['15min', '30min', '1h', '3h', '6h', '12h', '1d', '2d', '4d', '8d']],
  ['5m', '5min', ['15min', '30min', '1h', '3h', '6h', '12h', '1d', '2d', '4d', '8d']],
  ['10m', '10min', ['30min', '1h', '3h', '6h', '12h', '1d', '2d', '4d', '8d', '16d']],
  ['15m', '15min', ['1h', '3h', '6h', '12h', '1d', '2d', '4d', '8d', '16d', '32d']],
  ['30m', '30min', ['2h', '6h', '12h', '1d', '2d', '4d', '8d', '16d', '32d', '64d']],
  ['1h', '1h', ['3h', '6h', '12h', '1d', '2d', '4d', '8d', '16d', '32d', '64d', '128d']],
  ['2h', '2h', ['6h', '12h', '1d', '2d', '4d', '8d', '16d', '32d', '64d', '128d', '256d']],
  ['3h', '3h', ['12h', '1d', '2d', '4d', '8d', '16d', '32d', '64d', '128d', '256d', '512d']],
  ['6h', '6h', ['1d', '2d', '4d', '8d', '16d', '32d', '64d', '128d', '256d', '512d', '1024d']],
  ['12h', '12h', ['2d', '4d', '8d', '16d', '32d', '64d', '128d', '256d', '512d', '1024d', '2048d']],
  ['1d', '1d', ['4d', '8d', '16d', '32d', '64d', '128d', '256d', '512d', '1024d', '2048d']],
  ['3d', '3d', ['12d', '24d', '48d', '96d', '192d', '384d', '768d', '1536d', '3072d']],
  ['7d', '7d', ['28d', '56d', '112d', '224d', '448d', '896d', '1792d', '3584d', '7168d']],
]

describe('avail-v5 enumeration', () => {
  test('the only structurally-lagged class is /1h@3h at odd-hour endings (+1h)', () => {
    const p = ladder(...AVAIL_V5_TIERS.map(
      ([name, bin, shards]) => ({ name, bin, shards }) as Tier,
    ))
    const start = utc('2026-07-28T00:00:00')
    const stop = new Date(start.getTime() + 4 * 86_400_000)
    const lags: Record<string, number> = {}
    for (const [name, , shards] of AVAIL_V5_TIERS) {
      for (const rung of shards) {
        for (const { end } of shardPeriodsCovering(start, stop, rung)) {
          const at = shardBuildableAt(p, name, end)
          if (at.getTime() !== end.getTime()) {
            lags[`/${name}@${rung} ${end.toISOString()}`] = at.getTime() - end.getTime()
          }
        }
      }
    }
    const expected: Record<string, number> = {}
    for (let d = 28; d < 32; d++) {
      for (const h of [3, 9, 15, 21]) {
        const end = utc(`2026-07-${d}T${String(h).padStart(2, '0')}:00:00`)
        expected[`/1h@3h ${end.toISOString()}`] = 3_600_000
      }
    }
    expect(lags).toEqual(expected)
  })
})

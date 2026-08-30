import { describe, expect, test } from 'vitest'

import { coverageWindow, monthGridlines, spotlightClass } from './cover-timeline.js'

const MS_PER_DAY = 86_400_000
const t = (s: string) => Date.parse(s)

describe('coverageWindow', () => {
  test('pads 2% of the span for long histories', () => {
    // 100-day span → pad = 2 days.
    const genesis = t('2026-01-01T00:00:00Z')
    const now = genesis + 100 * MS_PER_DAY
    expect(coverageWindow(genesis, now)).toEqual({
      genesis: genesis - 2 * MS_PER_DAY,
      now,
    })
  })

  test('pads at least 1 day for short histories', () => {
    // 10-day span → 2% = 0.2 days → clamped to 1 day.
    const genesis = t('2026-08-01T00:00:00Z')
    const now = genesis + 10 * MS_PER_DAY
    expect(coverageWindow(genesis, now)).toEqual({
      genesis: genesis - MS_PER_DAY,
      now,
    })
  })
})

describe('spotlightClass', () => {
  test('no highlight → empty suffix (default rendering, all callers)', () => {
    expect(spotlightClass(null, '32d', '1m')).toBe('')
    expect(spotlightClass(undefined, '32d', '1m')).toBe('')
  })

  test('matching rung → lit; every other rung → faded', () => {
    const hl = { tier: '32d', shardDur: '1m' }
    expect(spotlightClass(hl, '32d', '1m')).toBe(' tt-hl')
    expect(spotlightClass(hl, '32d', '2d')).toBe(' tt-faded') // same tier, other rung
    expect(spotlightClass(hl, '8d', '1m')).toBe(' tt-faded')  // other tier, same shardDur
  })
})

describe('monthGridlines', () => {
  test('first-of-month lines from at-or-before genesis; January is major + year-labeled', () => {
    expect(monthGridlines(t('2025-11-15T06:00:00Z'), t('2026-02-10T00:00:00Z'))).toEqual([
      { t: t('2025-11-01T00:00:00Z'), label: 'Nov', major: false },
      { t: t('2025-12-01T00:00:00Z'), label: 'Dec', major: false },
      { t: t('2026-01-01T00:00:00Z'), label: '2026', major: true },
      { t: t('2026-02-01T00:00:00Z'), label: 'Feb', major: false },
    ])
  })

  test('genesis on a month boundary starts exactly there', () => {
    expect(monthGridlines(t('2026-07-01T00:00:00Z'), t('2026-08-16T00:00:00Z'))).toEqual([
      { t: t('2026-07-01T00:00:00Z'), label: 'Jul', major: false },
      { t: t('2026-08-01T00:00:00Z'), label: 'Aug', major: false },
    ])
  })
})

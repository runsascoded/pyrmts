// Calendar-floor parity (`specs/calendar-units.md`): `floorToSpan` is the
// deployed normative reference — `fixtures/calendar-floors.json` was
// generated from it and is asserted verbatim by BOTH suites (Python twin:
// `python/pyrmts/tests/test_axis.py::test_calendar_floor_parity`), so
// either implementation drifting from the contract fails its suite.

import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { describe, expect, test } from 'vitest'

import { floorToSpan, parseDuration } from './axis.js'

interface FloorCase { span: string; t: string; floor: string }

describe('floorToSpan calendar parity', () => {
  test('reproduces fixtures/calendar-floors.json exactly', () => {
    const fixture = JSON.parse(
      readFileSync(join(__dirname, '../../../../fixtures/calendar-floors.json'), 'utf8'),
    ) as { cases: FloorCase[] }
    const got = fixture.cases.map(c => ({
      ...c,
      floor: floorToSpan(new Date(c.t), parseDuration(c.span as never)).toISOString(),
    }))
    expect(got).toEqual(fixture.cases)
  })

  test('Nmo spans must tile a year evenly', () => {
    expect(() => floorToSpan(new Date('2026-05-10T00:00:00Z'), { count: 5, unit: 'mo' }))
      .toThrow("Month-span 5mo doesn't tile a year evenly (12 % 5 !== 0)")
  })
})

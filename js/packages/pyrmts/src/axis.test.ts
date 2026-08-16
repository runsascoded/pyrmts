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

  test('Nmo spans that do not divide 12 drift on the year-0 month grid', () => {
    // From 2026-01 the 5mo grid is …, 2025-11, 2026-04, 2026-09, … —
    // year-crossing drift is inherent to widths that don't divide 12
    // (`specs/calendar-composition-and-query-limits.md` §1).
    const span5 = { count: 5, unit: 'mo' } as const
    expect(floorToSpan(new Date('2026-01-15T12:00:00Z'), span5).toISOString()).toBe('2025-11-01T00:00:00.000Z')
    expect(floorToSpan(new Date('2026-05-10T00:00:00Z'), span5).toISOString()).toBe('2026-04-01T00:00:00.000Z')
    expect(floorToSpan(new Date('2026-04-01T00:00:00Z'), span5).toISOString()).toBe('2026-04-01T00:00:00.000Z')
    expect(floorToSpan(new Date('2026-09-30T23:59:59Z'), span5).toISOString()).toBe('2026-09-01T00:00:00.000Z')
    expect(floorToSpan(new Date('2026-05-10T00:00:00Z'), { count: 7, unit: 'mo' }).toISOString()).toBe('2025-12-01T00:00:00.000Z')
  })

  test('Ny ≡ (12N)mo identity under year-0 anchoring', () => {
    const t = new Date('2026-05-10T00:00:00Z')
    expect(floorToSpan(t, { count: 48, unit: 'mo' }).toISOString()).toBe('2024-01-01T00:00:00.000Z')
    expect(floorToSpan(t, { count: 4, unit: 'y' }).toISOString()).toBe('2024-01-01T00:00:00.000Z')
  })
})

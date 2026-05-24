// Time-axis arithmetic. UTC throughout; calendar-correct for `mo`/`y` units
// (variable-width) and millisecond-multiplied for fixed-width units.

import type { Duration, Shard, TimeUnit } from './types.js'

const MS: Record<Exclude<TimeUnit, 'mo' | 'y'>, number> = {
  min: 60_000,
  h: 60 * 60_000,
  d: 24 * 60 * 60_000,
}

const SPAN_RE = /^(\d+)(min|h|d|mo|y)$/

export interface ParsedTimeSpan {
  count: number
  unit: TimeUnit
}

export function parseDuration(s: Duration | string): ParsedTimeSpan {
  const m = SPAN_RE.exec(s)
  if (!m) throw new Error(`Not a valid Duration: ${s}`)
  return { count: parseInt(m[1]!, 10), unit: m[2] as TimeUnit }
}

// Add `span` to a UTC instant. Calendar-aware for mo/y.
export function addSpan(t: Date, span: ParsedTimeSpan): Date {
  const { count, unit } = span
  if (unit === 'mo') {
    const r = new Date(t)
    r.setUTCMonth(r.getUTCMonth() + count)
    return r
  }
  if (unit === 'y') {
    const r = new Date(t)
    r.setUTCFullYear(r.getUTCFullYear() + count)
    return r
  }
  return new Date(t.getTime() + count * MS[unit])
}

// Floor a UTC instant to the start of its span. Supports count=1 for all
// units, and count>1 for fixed-width units (min/h/d) via ms division.
// Multi-unit calendar bins (e.g. `2mo`) aren't supported yet — alignment
// semantics aren't well-defined without an anchor.
export function floorToSpan(t: Date, span: ParsedTimeSpan): Date {
  const { count, unit } = span
  if (count !== 1) {
    if (unit === 'mo' || unit === 'y') {
      throw new Error(`Multi-unit calendar bins not supported: ${count}${unit}`)
    }
    const binMs = count * MS[unit]
    return new Date(Math.floor(t.getTime() / binMs) * binMs)
  }
  switch (unit) {
    case 'min':
      return new Date(Date.UTC(
        t.getUTCFullYear(), t.getUTCMonth(), t.getUTCDate(),
        t.getUTCHours(), t.getUTCMinutes(),
      ))
    case 'h':
      return new Date(Date.UTC(
        t.getUTCFullYear(), t.getUTCMonth(), t.getUTCDate(),
        t.getUTCHours(),
      ))
    case 'd':
      return new Date(Date.UTC(
        t.getUTCFullYear(), t.getUTCMonth(), t.getUTCDate(),
      ))
    case 'mo':
      return new Date(Date.UTC(t.getUTCFullYear(), t.getUTCMonth()))
    case 'y':
      return new Date(Date.UTC(t.getUTCFullYear(), 0))
  }
}

// Count bins of `bin` overlapping [from, to). Bins are span-aligned (floored
// at the start of each bin's boundary, not rolling from `from`).
export function binsInRange(from: Date, to: Date, bin: Duration): number {
  if (to <= from) return 0
  const span = parseDuration(bin)
  let cursor = floorToSpan(from, span)
  let n = 0
  while (cursor < to) {
    n++
    cursor = addSpan(cursor, span)
  }
  return n
}

// Enumerate shard periods covering [from, to]. Each period is the half-open
// interval [start, end) plus a `label` suitable for `{period}` substitution.
export function shardPeriodsCovering(
  from: Date,
  to: Date,
  shard: Shard,
): { start: Date; end: Date; label: string }[] {
  if (shard === 'all') {
    return [{ start: new Date(0), end: new Date(8.64e15), label: 'all' }]
  }
  if (shard === '1run') {
    throw new Error("'1run' shards are step-axis only; not yet supported")
  }
  const span = parseDuration(shard)
  const out: { start: Date; end: Date; label: string }[] = []
  let cursor = floorToSpan(from, span)
  while (cursor < to) {
    const next = addSpan(cursor, span)
    out.push({ start: cursor, end: next, label: formatPeriod(cursor, span) })
    cursor = next
  }
  return out
}

// Format a UTC instant as a `{period}` substitution string. The resolution
// of the label matches the shard's unit:
//   1y   → '2026'
//   1mo  → '2026-05'
//   1d   → '2026-05-24'
//   1h   → '2026-05-24T17'
//   1min → '2026-05-24T17-30'
export function formatPeriod(t: Date, span: ParsedTimeSpan): string {
  const yyyy = t.getUTCFullYear().toString().padStart(4, '0')
  const mm = (t.getUTCMonth() + 1).toString().padStart(2, '0')
  const dd = t.getUTCDate().toString().padStart(2, '0')
  const hh = t.getUTCHours().toString().padStart(2, '0')
  const mi = t.getUTCMinutes().toString().padStart(2, '0')
  switch (span.unit) {
    case 'y':   return yyyy
    case 'mo':  return `${yyyy}-${mm}`
    case 'd':   return `${yyyy}-${mm}-${dd}`
    case 'h':   return `${yyyy}-${mm}-${dd}T${hh}`
    case 'min': return `${yyyy}-${mm}-${dd}T${hh}-${mi}`
  }
}

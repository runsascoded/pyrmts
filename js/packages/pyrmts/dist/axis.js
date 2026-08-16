// Time-axis arithmetic. UTC throughout; calendar-correct for `mo`/`y` units
// (variable-width) and millisecond-multiplied for fixed-width units.
const MS = {
    min: 60_000,
    h: 60 * 60_000,
    d: 24 * 60 * 60_000,
};
const SPAN_RE = /^(\d+)(min|h|d|mo|y)$/;
export function parseDuration(s) {
    const m = SPAN_RE.exec(s);
    if (!m)
        throw new Error(`Not a valid Duration: ${s}`);
    return { count: parseInt(m[1], 10), unit: m[2] };
}
// Fixed-width Duration in ms. Throws for mo/y (calendar-variable).
// Use for arithmetic where calendar drift doesn't matter — e.g. snapping
// a smoothing window against an output bin's stride.
export function fixedDurationMs(d) {
    const { count, unit } = parseDuration(d);
    if (unit === 'mo' || unit === 'y') {
        throw new Error(`fixedDurationMs: '${d}' is calendar-variable; not representable as ms`);
    }
    return count * MS[unit];
}
// Add `span` to a UTC instant. Calendar-aware for mo/y.
export function addSpan(t, span) {
    const { count, unit } = span;
    if (unit === 'mo') {
        const r = new Date(t);
        r.setUTCMonth(r.getUTCMonth() + count);
        return r;
    }
    if (unit === 'y') {
        const r = new Date(t);
        r.setUTCFullYear(r.getUTCFullYear() + count);
        return r;
    }
    return new Date(t.getTime() + count * MS[unit]);
}
// Floor a UTC instant to the start of its span. Supports count=1 for all
// units, and count>1 for fixed-width units (min/h/d) via ms division.
// Multi-unit calendar bins (`Nmo`, `Ny`) anchor at year 0: `Nmo` floors
// months-since-year-0 (`M = 12*yyyy + mm`) to a multiple of N, so
//   3mo  → Jan, Apr, Jul, Oct       (Gregorian quarters)
//   6mo  → Jan, Jul                  (semesters)
//   2y   → year aligned to floor(yyyy / 2) * 2
//   5mo  → …, 2025-11, 2026-04, 2026-09, …  (drifts across years — inherent
//          to any width that doesn't divide 12, not a defect)
// Year-0 anchoring makes `Ny ≡ (12N)mo` an identity for all N, and reduces
// calendar-grid containment to count divisibility
// (`specs/calendar-composition-and-query-limits.md` §1).
export function floorToSpan(t, span) {
    const { count, unit } = span;
    if (count !== 1) {
        if (unit === 'mo') {
            const m = Math.floor((12 * t.getUTCFullYear() + t.getUTCMonth()) / count) * count;
            return new Date(Date.UTC(Math.floor(m / 12), ((m % 12) + 12) % 12));
        }
        if (unit === 'y') {
            return new Date(Date.UTC(Math.floor(t.getUTCFullYear() / count) * count, 0));
        }
        const binMs = count * MS[unit];
        return new Date(Math.floor(t.getTime() / binMs) * binMs);
    }
    switch (unit) {
        case 'min':
            return new Date(Date.UTC(t.getUTCFullYear(), t.getUTCMonth(), t.getUTCDate(), t.getUTCHours(), t.getUTCMinutes()));
        case 'h':
            return new Date(Date.UTC(t.getUTCFullYear(), t.getUTCMonth(), t.getUTCDate(), t.getUTCHours()));
        case 'd':
            return new Date(Date.UTC(t.getUTCFullYear(), t.getUTCMonth(), t.getUTCDate()));
        case 'mo':
            return new Date(Date.UTC(t.getUTCFullYear(), t.getUTCMonth()));
        case 'y':
            return new Date(Date.UTC(t.getUTCFullYear(), 0));
    }
}
// Nominal width in ms — for ordering/eligibility comparisons only (`mo` =
// 30d, `y` = 365d). NOT for axis arithmetic: calendar spans vary (Feb ≠
// Aug); use `addSpan`/`floorToSpan` for actual boundaries. Twin of Python
// `pyrmts.nominal_delta_ms`.
export function nominalMs(dur) {
    const { count, unit } = parseDuration(dur);
    if (unit === 'mo')
        return count * 30 * MS.d;
    if (unit === 'y')
        return count * 365 * MS.d;
    return count * MS[unit];
}
// `t` if span-aligned, else the next span boundary.
export function ceilToSpan(t, span) {
    const floored = floorToSpan(t, span);
    return floored.getTime() === t.getTime() ? floored : addSpan(floored, span);
}
// Count bins of `bin` overlapping [from, to). Bins are span-aligned (floored
// at the start of each bin's boundary, not rolling from `from`).
export function binsInRange(from, to, bin) {
    if (to <= from)
        return 0;
    const span = parseDuration(bin);
    let cursor = floorToSpan(from, span);
    let n = 0;
    while (cursor < to) {
        n++;
        cursor = addSpan(cursor, span);
    }
    return n;
}
// Enumerate shard periods covering [from, to]. Each period is the half-open
// interval [start, end) plus a `label` suitable for `{period}` substitution.
export function shardPeriodsCovering(from, to, shard) {
    if (shard === '1run') {
        throw new Error("'1run' shards are step-axis only; not yet supported");
    }
    const span = parseDuration(shard);
    const out = [];
    let cursor = floorToSpan(from, span);
    while (cursor < to) {
        const next = addSpan(cursor, span);
        out.push({ start: cursor, end: next, label: formatPeriod(cursor, span) });
        cursor = next;
    }
    return out;
}
// Format a UTC instant as a `{period}` substitution string. The resolution
// of the label matches the shard's unit:
//   1y   → '2026'
//   1mo  → '2026-05'
//   1d   → '2026-05-24'
//   1h   → '2026-05-24T17'
//   1min → '2026-05-24T17-30'
export function formatPeriod(t, span) {
    const yyyy = t.getUTCFullYear().toString().padStart(4, '0');
    const mm = (t.getUTCMonth() + 1).toString().padStart(2, '0');
    const dd = t.getUTCDate().toString().padStart(2, '0');
    const hh = t.getUTCHours().toString().padStart(2, '0');
    const mi = t.getUTCMinutes().toString().padStart(2, '0');
    switch (span.unit) {
        case 'y': return yyyy;
        case 'mo': return `${yyyy}-${mm}`;
        case 'd': return `${yyyy}-${mm}-${dd}`;
        case 'h': return `${yyyy}-${mm}-${dd}T${hh}`;
        case 'min': return `${yyyy}-${mm}-${dd}T${hh}-${mi}`;
    }
}
//# sourceMappingURL=axis.js.map
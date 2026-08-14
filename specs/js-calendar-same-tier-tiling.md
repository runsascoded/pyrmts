# JS port: calendar-aware same-tier tiling walk (`tileFromExisting`)

Source: awair session, 2026-08-14 (spec written by awair session at pyrmts session's suggestion; ~/c/awair). Companion: `specs/done/calendar-rung-consolidation.md` (the Python side, landed 2026-08-14; this spec is its JS twin).

## TL;DR

Awair's CFW cascade already does same-tier rung promotion for fixed-width rungs (verified live: `m30/128d/2026-01-01.parquet: wrote 1292 rows` in tonight's tick log). Its tiling walk assumes fixed-width epoch-modulo strides, so it can't stride **calendar rungs** (`1mo`, `1y`). That's the sole remaining blocker on awair's `raw: [1mo] → [1d, 1mo]` migration (task #32).

Ask: port the calendar-aware greedy rung-tiling walk from `pyrmts.consolidate.tile_from_existing` (Python, landed yesterday) to `pyrmts` JS. Small (~50 lines + tests), the primitive that closes the "same-tier consolidation works for every rung shape awair uses" gap.

Concat-sort-write half stays app-side (awair's TS cascade already has its own shard writer). Pyrmts JS exposes just the walk — the mechanical calendar-aware descent — matching how it exposes read-side primitives today (`listExpectedShards`, `listMissingShards`, etc.).

## Motivation

Awair's tip-write pattern (per `specs/done/streaming-tip-writer.md`): Lambda in-place-grows the current-month raw parquet each minute, ~40KB→~1.2MB round-trip. Multi-rung `raw: [1d, 1mo]` bounds the tip to ~40KB constant through the day, with cascade rolling 28–31 daily shards into the 1mo shard at month-close.

The Python side of same-tier consolidation now handles calendar rungs (`calendar-rung-consolidation.md`, `bba2a92`). The CFW cascade — the natural home for awair's month-close rollup, since it already runs every minute and already handles same-tier rung promotion for m3/m10/m30/etc. — needs the same primitive on the JS side to complete the loop.

The Python `tile_from_existing` API + tiling walk map directly to TS. The calendar-correct axis primitives (`floorToSpan`, `addSpan`, `ceilToSpan`) already exist in `pyrmts/js/packages/pyrmts/src/axis.ts` (per `specs/done/calendar-units.md`), so the port is mostly mechanical.

## Current state (JS)

`pyrmts/js/packages/pyrmts/src/axis.ts`:
- `parseDuration(s) → {count, unit}` ✓
- `addSpan(t, span)` — calendar-aware for `mo`/`y` ✓
- `floorToSpan(t, span)` — calendar-anchored for `mo`/`y` ✓
- `ceilToSpan(t, span)` ✓
- `fixedDurationMs(d)` — throws on `mo`/`y` (correctly) ✓

`pyrmts/js/packages/pyrmts/src/ladder.ts`:
- `nominalMs(dur)` — private helper, `mo`=30d, `y`=365d for ordering (mirror of Python's `_nominal_ms` before it was promoted). Needs to be promoted for `tileFromExisting`'s eligibility filter.

`pyrmts/js/packages/pyrmts/src/planner.ts`, `keys.ts`, etc.: `shardKey(pyramid, tierName, rung, period)` and friends already exist for cross-tier planning; reusable here.

**Missing**: `tileFromExisting` — the calendar-aware greedy rung descent that the Python `consolidate.py` uses. Every same-tier consumer (awair CFW cascade, any future TS consolidator) would want to call it.

## Design

### 1. Promote `nominalMs` to `pyrmts/axis`

Move from `ladder.ts` (private) to `axis.ts` (exported). Matches the Python-side promotion of `_nominal_ms` → `pyrmts.axis.nominal_delta_ms`. `ladder.ts` re-imports; no behavior change, no test churn.

```ts
// pyrmts/axis.ts
export function nominalMs(dur: Duration | string): number {
  const parsed = parseDuration(dur)
  const dayMs = 24 * 60 * 60_000
  if (parsed.unit === 'mo') return parsed.count * 30 * dayMs
  if (parsed.unit === 'y') return parsed.count * 365 * dayMs
  return fixedDurationMs(dur)
}
```

### 2. New module `pyrmts/tile-from-existing.ts`

Direct TS twin of Python `tile_from_existing` (`pyrmts_engine/consolidate.py:71-123`, ~50 lines).

```ts
import { addSpan, ceilToSpan, nominalMs, parseDuration } from './axis.js'
import { shardKey } from './keys.js'
import type { ExpectedShard, Pyramid, Tier } from './types.js'

export interface TilingResult {
  picks: Array<{ rung: string; key: string }>  // in period order
  holes: Array<{ start: Date; end: Date }>     // uncovered
}

/**
 * Greedy largest-first tiling of a gap period from EXISTING same-tier
 * shards (`keySet` — snapshot of the caller's listing).
 *
 * The prescriptive expected cover is wrong for this problem: it demands
 * largest-fitting sub-rungs that no min-cover ever materialized; what's
 * actually on storage is whatever mix of rungs history produced.
 *
 * Aligned slots of each rung within [seg_start, seg_end) — the epoch
 * grid for fixed rungs, calendar boundaries for `mo`/`y` (each slot's
 * width varies with the cursor: Feb ≠ Aug). Divisibility chaining ⇒
 * seg boundaries align to some rung ≤ the current one; misaligned
 * leading/trailing parts descend to finer rungs.
 *
 * Pre-genesis segments are dropped.
 */
export function tileFromExisting(
  pyramid: Pyramid,
  tier: Tier,
  gap: ExpectedShard,
  keySet: Set<string>,
  opts: { genesis: Date },
): TilingResult {
  const rungs = tier.shards.filter(r => nominalMs(r) < nominalMs(gap.shardDur))
  const picks: Array<{ rung: string; key: string }> = []
  const holes: Array<{ start: Date; end: Date }> = []

  const tile = (segStart: Date, segEnd: Date, idx: number): void => {
    if (segEnd <= opts.genesis) return
    if (idx < 0) { holes.push({ start: segStart, end: segEnd }); return }
    const rung = rungs[idx]!
    const span = parseDuration(rung)
    let cur = segStart
    let slot = ceilToSpan(segStart, span)
    while (slot < segEnd) {
      const nxt = addSpan(slot, span)
      if (nxt > segEnd) break
      if (cur < slot) tile(cur, slot, idx - 1)
      const key = shardKey(pyramid, tier.name, rung, slot)
      if (keySet.has(key)) picks.push({ rung, key })
      else tile(slot, nxt, idx - 1)
      cur = nxt
      slot = nxt
    }
    if (cur < segEnd) tile(cur, segEnd, idx - 1)
  }

  tile(gap.periodStart, gap.periodEnd, rungs.length - 1)
  return { picks, holes }
}
```

Export from `pyrmts/index.ts` alongside the other planner primitives.

### 3. What consumers do with it

The walk is deliberately narrow — it doesn't read or write; it returns the tile picks + uncovered holes. Consumers own:
- Fetching each pick's parquet bytes from storage
- Concatenating rows (bins match by definition — same-tier, no rebin)
- Sorting by primary key
- Writing the output shard

Awair's CFW cascade already has all four halves for the fixed-width case; the change is swapping its bespoke tiling walk for `tileFromExisting`. Any uncovered `holes` at the raw tier surface as "fetch from Lambda's tip layout" or "raise if we're past month-close" — app policy, not pyrmts's concern.

## Not in scope

- **Cross-tier cascade** (`overlap_cover` in Python). Awair's cascade already handles this in TS via `previous_tier` chains + `enumerateSourceKeys`. Only same-tier tiling is missing.
- **The concat-sort-write half** of consolidation. Stays app-side. Adding it to `pyrmts` JS would drag a parquet writer dep tree in, and awair already has its own via `hyparquet-writer`. If a second consumer shows up wanting the whole consolidator, factor then — YAGNI now.
- **Python-side lazy-import refactor of `consolidate.py`.** Discussed as option #3 in the "who runs consolidation" thread (see pyrmts session transcript 2026-08-14T19:16); dropped in favor of the JS port per user's "cascade owns all promotion, tip Lambda only appends" preference. If some future project wants pyrmts-engine consolidation without polars, that spec can be written then.

## Acceptance

1. `nominalMs` exported from `pyrmts/axis.ts`; `ladder.ts` re-imports (no behavior change; existing 469 tests stay green).
2. New `pyrmts/tile-from-existing.ts` module with `tileFromExisting` + `TilingResult`, exported from `pyrmts/index.ts`.
3. Unit tests for `tileFromExisting`:
   - Fixed-width rungs only (`[1d, 4d, 32d]`): parity with existing hand-rolled path (a small synthetic pyramid + `keySet` — assert `picks` matches by-hand construction).
   - Calendar rung (`[1d, 1mo]`): August (31 daily → 1 monthly) and February 2026 (28 daily; check leap-year 2028 = 29 daily). Assert `picks.length` = 31/28/29, keys correct, no holes.
   - Missing sub-rungs descend: given `[1d, 1mo]` with only 30 dailies present (28 for Feb - 1, say), assert the missing day yields a `holes` entry, not a wrong-sized pick.
   - Pre-genesis clipping: gap starting before `genesis` — leading pre-genesis segment silently dropped.
4. Cross-impl parity fixture: pick a mixed-rung `[1d, 1mo]` scenario, run both Python `tile_from_existing` and JS `tileFromExisting` against equivalent inputs, assert identical `picks` (same rung, same key ordering) + identical `holes`. Same shape as the calendar-units cross-impl parity test.
5. Awair-side follow-up (separate; awair repo): rewire `cfw/cascade/src/write.ts`'s tiling to use `tileFromExisting` for same-tier consolidation, flip `raw: [1mo] → [1d, 1mo]` in `pyramid.yml`, and update Lambda `updater.py` to write to the tip 1d shard instead of the monolithic 1mo. Live-verify: month-close consolidation runs, resulting `raw/1mo/2026-08.parquet` is byte-identical to the pre-migration file.

## Rollout

Small, additive:
1. Promote `nominalMs` to `pyrmts/axis.ts`. (~5 lines moved.)
2. Add `pyrmts/tile-from-existing.ts` (~60 lines including exports/comments). Direct port of `tile_from_existing`.
3. Add the four unit tests + the cross-impl parity fixture.
4. Push `main`, run `build-dist` GHA, record new `dist` SHA. Awair repins + rewires cascade + flips `raw` config. Live migration deletes the existing `raw/1mo/2026-08.parquet` after the first month-close consolidation confirms byte-identical output (or leaves it — the consolidation overwrites).

Total pyrmts-side effort estimate: ~1 hour including tests + parity fixture + push.

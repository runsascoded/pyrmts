# Same-tier consolidation: support calendar rungs (`mo`/`y`)

> **Status (2026-08-14, pyrmts session): implemented.** Deviations/notes vs the ask:
> - `_fixed_delta` deleted; `tile_from_existing` walks slots via `ceil_to_span`/`add_span`, eligibility via the new `pyrmts.axis.nominal_delta_ms` (promoted per §4; `yaml._nominal_ms` deleted, `plan._approx_ms` now a thin delegate kept for its many engine/ops importers).
> - **`overlap_cover` was NOT calendar-safe** (§2's claim was off — it called `_fixed_delta` on every source rung, so a `[1d, 1mo]` source tier would raise on any cross-tier hole fill). Refactored to `floor_to_span`/`add_span` too; covered by a new clip test against a `1mo` source tile.
> - **§3 was also off**: mixed fixed/calendar pairs only checked nominal *ascension*, not divisibility — acceptance #4's `[7d, 1mo]` reject required adding the nominal-divisibility check. Added to `pyrmts/yaml.py` `_validate_shard_ladder` **and** the JS mirror `ladder.ts` `validateLadders` (kept in lockstep, matching messages), with tests on both sides (`[1d, 1mo]`/`[3d, 1mo]`/`[1d, 3mo]` accept, `[7d, 1mo]` rejects "by nominal width").
> - Acceptance #2 test (`test_calendar_consolidation_byte_identical_to_engine_build`): Feb 2026 (28×1d) + Aug 2026 (31×1d) consolidate byte-identical to the engine building each `1mo` shard directly from the 1d source (stronger than concat-equality — independent wide→long→rebin path), plus 'exists' short-circuit and forced-rebuild RGIP (same md5).
> - Python 212 passed, JS 441 passed, `tsc -b` clean. Awaiting awair adoption (repin, `raw: [1d, 1mo]` flip, Lambda 1d tip writes) before moving to `done/`.
>
> **Pushed (2026-08-14) — re-pin SHAs** (after rebasing onto `r/main`'s `9aa7c45` JS-invalidation-port + `fb1e727` `{shard}`-guard, one trivial test-file conflict; full suites re-green post-rebase: 202 py + 17 ops + 7 geo + 469 js): `main` = `f8bfe0c01290b02cda636fd5d759b8cb2c2fb6d4` (this spec's implementation now at `82171cc`); `dist` = `bf6aa2e` ("dist: pyrmts + pyrmts-cfw + pyrmts-geo @ f8bfe0c"). Build-dist CI green (run 31830037513).

Source: awair session, 2026-08-14 (spec written by awair session at pyrmts's request; ~/c/awair). Companion consumer note: awair `#32 Multi-rung raw: [1d, 1mo]` (blocked on this).

## TL;DR

Same-tier consolidation already exists in pyrmts (`pyrmts_engine.consolidate`, per `specs/done/pyrmts-ops-adoption.md` phase 2 — extension shards tile from finer same-tier rungs, concat, sort, write). It **explicitly raises** for calendar-variable rungs (`mo`, `y`) at `consolidate.py:60-67`:

```python
def _fixed_delta(dur: str) -> timedelta:
    span = parse_duration(dur)
    if span.unit not in UNIT_MS:
        raise ValueError(
            f"consolidate: calendar-variable rung {dur!r} — same-tier tiling "
            f"needs fixed-width (epoch-aligned) rungs"
        )
    return timedelta(milliseconds=span.count * UNIT_MS[span.unit])
```

Ask: swap `_fixed_delta` for calendar-correct axis primitives (`add_span`, `floor_to_span`, `ceil_to_span` — all already in `pyrmts.axis` and calendar-aware for `mo`/`y`). The tiling walk becomes `cursor = add_span(cursor, rung_span)` instead of `cursor += _fixed_delta(rung)`. Duration comparisons (`_fixed_delta(r) < _fixed_delta(gap.shard_dur)`) become nominal-width comparisons already used by `pyrmts.yaml` for the divisibility-chain check (`specs/calendar-units.md`).

## Motivation: awair's Lambda write pattern

Awair's raw tier is currently `shards: [1mo]` — the Lambda in-place-grows the current month's parquet each minute (read whole month → append new rows → write whole month back → invalidate downstream). Read/write amplification climbs monotonically through the month: early August it's a 100 KB round-trip, late August a 1.2 MB round-trip, every minute per device.

Multi-rung `shards: [1d, 1mo]` fixes this: Lambda writes to a tip 1d shard (~40 KB, constant through the day), cascade consolidates 28–31 1d shards → one 1mo shard at month-close. The tip's read/write cost is bounded regardless of how far into the month we are.

The 1mo rung specifically (not `28d` / `30d` / `32d`) matters because:
- The R2 layout already keys history by calendar months (`awair-17617/2026-08.parquet` — the S3 monolithic file, and now the `raw/1mo/2026-08.parquet` pyramid mirror). Fixed-width 28d/32d rungs would double-write history in a second layout, or force a one-way migration.
- Downstream tiers still key by `1mo` in URL/UI ("last month", "August 2026") — a mismatched raw layout (fixed-width) vs. the calendar-month rest complicates every "same period across tiers" query.
- Ctbk's `avail-v6.yaml` already uses `1mo` periods for its coarser rungs (`d1/{1mo}/*`); consolidation into a `1mo` rung is the natural next step there too if they add multi-rung to `d1`.

## Current state

Same-tier tiling logic (`tile_from_existing`, `consolidate.py:71-158`):
1. Pick eligible sub-rungs — rungs shorter than the gap's shard duration (`_fixed_delta(r) < _fixed_delta(gap.shard_dur)`).
2. Greedy largest-first tile of the gap period from existing keys in `key_set`; returns `([(rung, key)…], [uncovered_holes])`.
3. Consolidator (`materialize_extension_shard`) reads picked keys, concats + sorts, writes the output shard.

The **duration arithmetic** is what breaks for calendar rungs — everything else (concat/sort/write, `key_set` skip, injected `raw_fill`/`cross_tier_fill` for holes) is duration-agnostic. `parse_duration('1mo')` returns `ParsedTimeSpan(count=1, unit='mo')`; `UNIT_MS` doesn't have `mo` (correctly — a month has no fixed ms width); `_fixed_delta` raises.

Pyrmts axis already has the calendar-correct primitives (`pyrmts/axis.py`):
- `add_span(t, span)`: `1mo` steps `t.replace(year=..., month=...)`, handling year rollover.
- `floor_to_span(t, span)`: aligns `t` to the span boundary (calendar-anchored for `mo`/`y`).
- `ceil_to_span(t, span)`: next boundary or `t` itself if aligned.
- `bins_in_range(from_, to, bin)`: counts calendar-correct bins.

The divisibility-chain check in `pyrmts/yaml.py:214-223` already uses nominal-width comparisons for mixed fixed/calendar rung pairs. Same trick applies to `tile_from_existing`'s "shorter than the output rung" filter.

## Design

### 1. Replace `_fixed_delta` with calendar-aware equivalents

Two use sites in `consolidate.py`:

**a. Eligibility filter** (`tile_from_existing:88`):
```python
rungs = [r for r in tier.shards if _fixed_delta(r) < _fixed_delta(gap.shard_dur)]
```
Replace with nominal-width comparison (as `yaml.py` does — `mo`=30d, `y`=365d for ordering only):
```python
from pyrmts.axis import nominal_delta_ms  # exists via yaml.py's helper
rungs = [r for r in tier.shards if nominal_delta_ms(r) < nominal_delta_ms(gap.shard_dur)]
```

**b. Tiling walk** (`tile_from_existing`'s `tile()` inner):
Replace the fixed-delta step with `add_span`:
```python
from pyrmts.axis import add_span, parse_duration
rung_span = parse_duration(rung)
cursor = seg_start
while cursor < seg_end:
    next_bound = add_span(cursor, rung_span)
    key = shard_key(pyramid, tier, rung, cursor)
    if key in key_set:
        picks.append((rung, key))
        cursor = next_bound
    else:
        # fall through to finer rung
        tile(cursor, min(next_bound, seg_end), idx + 1)
        cursor = next_bound
```

This handles calendar-variable stride natively: `next_bound` for `cursor=2026-08-01` at `1mo` is `2026-09-01` (31d span); for `cursor=2026-02-01` it's `2026-03-01` (28/29d span). No changes needed at the shard-content layer — `1d` sub-shards for Feb 2026 are 28 files, for Aug 2026 are 31 files; `concat + sort` handles both identically.

### 2. Update tiling arithmetic in `materialize_extension_shard`

The consolidator itself (`materialize_extension_shard`, `consolidate.py:220-` roughly) uses the tiles returned by `tile_from_existing`. Once (1) is fixed, it doesn't need calendar-awareness — it operates on the returned `(rung, key)` pairs. Any hole-fill fall-through via `cross_tier_fill` already uses `overlap_cover`, which walks the source tier's shard grid via axis primitives (calendar-safe already).

### 3. Guardrails (yaml validation)

`pyrmts/yaml.py:214-223` already enforces the divisibility-chain contract for mixed fixed/calendar rung pairs (nominal-width divisibility). Once consolidation supports calendar rungs, the existing check is sufficient — no new validation needed. Add a test case for `[1d, 1mo]` to `test_yaml.py` covering the chain (`1mo` % `1d` nominal — 30 % 1 = 0, allowed) and the reject case (`[3d, 1mo]` — 30 % 3 = 0, allowed; `[7d, 1mo]` — 30 % 7 ≠ 0, rejected).

### 4. `axis_delta_ms` helper (if not already exported)

`pyrmts/yaml.py:319` mentions "Width for ordering only — calendar entries use nominal 30d/365d." — that helper is currently private. Promote it (or a wrapper) to `pyrmts.axis` as `nominal_delta_ms` so `consolidate.py` doesn't reach into `yaml.py`. Cheap; keeps the module boundary clean.

## Alternatives considered

- **Fixed-width `[1d, 32d]` (awair-side workaround, no pyrmts change).** Would work today. Rejected because it forks the storage layout: raw tier keyed by 32d epoch-aligned windows, all other tiers keyed by 1mo calendar months. Every "current month raw + current month m3" join has to translate between the two.
- **`[1d, 30d]` with month-anchored offsets.** Same problem — 30d slots don't align with month boundaries (drift by ~5 days/year). Rejected.
- **Special-case `mo`/`y` inside `_fixed_delta` (add a synthetic "effective delta" per-invocation).** Loses the "one bound per rung" contract; different calls in the same period walk different bounds. Rejected — the loop needs the real per-cursor `add_span`.

## Acceptance

1. `consolidate.py` no longer raises for `mo`/`y` rungs; `_fixed_delta` either handles them (via `add_span`) or is replaced by axis primitives at all call sites.
2. New unit test: build a `[1d, 1mo]` pyramid with a fixture that populates 31 daily shards for 2026-08 + 28 for 2026-02; consolidate both months; assert each `1mo` shard equals `concat(sorted daily shards)` byte-for-byte. Assert re-consolidation is idempotent (same bytes, same md5).
3. Existing byte-identity tests for fixed-width consolidation stay green (calendar path is additive).
4. `yaml.py` validation test: `shards: [1d, 1mo]` accepts, `[7d, 1mo]` rejects (nominal-divisibility 30 % 7 ≠ 0), `[1d, 3mo]` accepts (12 % 3 = 0 for year-tiling, 90 % 1 = 0 for daily inputs).
5. Consumer smoke: awair sets `raw: [1d, 1mo]`, Lambda writes tip 1d shards, cascade consolidates at month-close, downstream tier reads see byte-identical output vs. the current `raw: [1mo]` monolith on the same data range. (Awair-side; not a pyrmts acceptance criterion but the acceptance criterion for the whole effort.)

## Not in scope

- The **write-side producer contract** for streaming into the finest rung (awair's Lambda pattern). Currently hand-rolled in awair (`src/awair/lmbda/updater.py`); factoring it into a pyrmts SDK helper is a separate spec — `streaming-tip-writer.md`. This spec only unblocks awair's ability to declare `[1d, 1mo]` in config; the write path stays app-owned.
- Ctbk's `cons` chain retirement: ctbk's `agg=1m` rungs are all fixed-width (`5m, 15m, 1h, ...`), so consolidation works there today — this spec doesn't change anything for ctbk. If ctbk later adds `1mo` rungs to a tier, this spec unblocks that too.

## Rollout

Small, low-risk. Path:
1. Add `nominal_delta_ms` to `pyrmts.axis` (or promote the existing helper).
2. Refactor `consolidate._fixed_delta` and its two call sites.
3. Add the two acceptance tests (`[1d, 1mo]` consolidation, yaml validation).
4. Push to `main`; awair repins, flips `raw: [1d, 1mo]` in `pyramid.yml`, and updates Lambda to write 1d shards. Awair-side migration deletes the existing `raw/1mo/*.parquet` files after cascade re-derives them from the daily rung (or leaves them in place — cascade overwrites on consolidation).

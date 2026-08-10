# Calendar units: multi-unit spans + calendar-target het-tiling (ctbk #122)

Status: **phases 1 + 2 implemented; ctbk downstream integration green (Python side) — clear to push `r/main`** (spec 2026-08-07, ctbk session; phase 1 impl 2026-08-10, phase 2 impl 2026-08-10, pyrmts session). Unblocks ctbk rides-v5 calendar tiers (`specs/rides-v5.md` over there) and, longer-term, the avail multi-unit calendar shard target ladder (`ctbk/specs/pyramid-cascade.md` §target-vs-shipped).

**ctbk downstream integration (2026-08-10, ctbk session, this checkout @ `bcb6e4f` local-linked)**: ctbk pytest suite 62/62 green; both `configs/pyramids/rides-v5-{start,end}.yaml` extended with the calendar family (`{1,2,3,6}mo + 1y`, pow-2-year shards per the rides-v5 RESOLVED DESIGN) and parse clean through `parse_pyramid_yaml`; ladder validation correctly rejects `5mo` ("doesn't tile a year evenly"); `list_expected_shards` over genesis→2026-08-10 enumerates sensible calendar covers (e.g. `1mo`: `16y/2000` + `4y/2016` + `2y/2024`; 18 calendar shards/anchor). Not yet exercised from ctbk: engine calendar *build* (needs the r/main push → new engine image → Batch calendar fill) and the TS het-tiling serve path (adopting after push, via `pds`). Push away; record the final SHA here for the ctbk re-pin.

**pyrmts status (2026-08-10): phase 1 landed.** Python 200 tests green (+14), JS 419 (+8), `tsc` clean; not yet pushed — per §Sequencing, ctbk runs downstream integration from this checkout before the `r/main` push.

- `floor_to_span`: `Nmo` (validates `12 % N == 0`, same message as JS) + `Ny` (year-0 anchored) per the contract; `ceil_to_span`/`bins_in_range`/`shard_periods_covering`/`format_period` delegate and needed no changes, as predicted.
- **Parity fixture**: `fixtures/calendar-floors.json` — 77 cases (7 spans × 11 instants: boundaries, leap day, pre-epoch, epoch, 2048), *generated from JS `floorToSpan`* (the deployed reference) and asserted verbatim by both suites (`test_axis.py::test_calendar_floor_parity`, `axis.test.ts`). Sentinel case: `4y` floors 1970-01-01 to **1968** — the exact instant the polars epoch-anchor bug would get wrong.
- `bin_floor_expr`: `Nmo` via `dt.truncate` (epoch ≡ year anchoring when `12 % N == 0`); `Ny` via year-arithmetic `pl.datetime(year // N * N, 1, 1)` — the divergence was confirmed empirically before fixing (`truncate('4y')`: 2026→2026, 1921→1918; contract: 2024, 1920). `test_plan.py::test_bin_floor_expr_calendar_parity` runs the polars path over all 77 fixture cases.
- `_divides` verified against the spec's truth table (no code change needed — already calendar-aware).
- Ladder validation (both languages): calendar-calendar pairs divide in months (`y` ≡ `12mo`), `Nmo` must tile a year, mixed fixed/calendar pairs assert nominal-width (30d/365d) ascension; both-fixed pairs keep the pre-existing exact-ms checks and messages. `[1mo, 3mo, 1y]` accepted, `[2mo, 3mo]` / `5mo` / descending-mixed rejected with pinned messages.
- Gap discovery: multi-unit rungs work unmodified in both languages; twin tests (`1mo`-bin tier, `[1y, 4y]` rungs, mid-year genesis clips `effective_start`, live tip descends `4y`→`1y` and leaves the sub-`1y` tail to finer tiers) assert the identical cover.
- Engine calendar build (`test_calendar.py`): `1mo/3mo/1y`-bin tiers cascade (`1mo ← 15min`, `3mo ← 1mo`, `1y ← 3mo`) over full leap-2024, window-split-invariant byte-for-byte (`4d` vs `32d` windows), matching an independent hand-derived anchor; keys come out `pyr/mo/1y/2024.parquet`-shaped. (Test uses bounded synthetic values — the shared fixture's `sumsq = i²` exceeds 2^53 float-exactness at year scale.)
- Out of scope, unchanged as specced: `_validate_window` (fixed-width ingest windows), `consolidate.py`'s fixed-width wall.

**pyrmts status (2026-08-10, later): phase 2 landed.** JS 432 tests green (+13), `tsc` clean; still not pushed pending ctbk integration.

- `planRagged`/`planRaggedFromInventory` delegate calendar `targetBin` to new `planRaggedCalendar`/`planRaggedCalendarFromInventory` (`planner.ts`): target bins enumerated via `floorToSpan`/`addSpan`; per bin, a materialized calendar tier of exactly the target's width (months-normalized, `1y` ≡ `12mo`) serves the bin whole when covered, else greedy coarsest-first fully-inside het-tiling from whole-day-multiple fixed tiers with edge-residue recursion (`packCalendarWatermark`/`packCalendarInventory`), day-divisor base tier required (throws without one unless an exact calendar tier exists).
- Watermark flavor: per-atom sealed checks (`effective` ≥ atom end, `earliest` ≤ atom start); day-divisor tiers may emit a trailing atom clipped to `effective` (rows can't straddle the calendar boundary — main-walk clip semantics); multi-day tiers stay strictly sealed-and-fully-inside (a mid-period `14d` row could pull cross-boundary data into the target bin). Uncovered residue drops — only at genesis/tip edges since watermark coverage is edge-monotone.
- Inventory flavor: per-atom registered-tile containment, interior gaps recurse finer, unregistered residue drops ("unlisted is intentional"). Exact-tier bins additionally require the tier's effective watermark to seal the bin — registration is shard-granular (a half-filled `1y`-of-months shard registers with full-period bounds), so the watermark is what makes the un-closed tip fall through to finer registered tiles.
- Cross-bin coalescing of adjacent same-tier atoms (reaggregation floors rows individually, so month-boundary-spanning `1d` segments are safe); `stitch` unchanged as predicted.
- Acceptance #2 (`calendar-ragged.test.ts`): `{1d,3d,7d,14d}` toy pyramid, LCG sum-monoid data over 2023–2024 (leap Feb), `1mo`/`3mo`/`1y` het-tiled plan+stitch exactly equals brute-force 1d→calendar groupby across year/month boundaries.
- Pinned plan tests both flavors (`planner.test.ts`, `planner-inventory.test.ts`): Feb-2026 pack (`1d/7d/14d/1d`, epoch-day-derived), cross-bin `1d` coalescing, materialized-tier-preferred with het-tiled tip, mid-day partial-seal `1d` clip, genesis `earliest` clip, registered-day-tile tip serving, watermark-gated calendar-tier tip, no-base-tier error, `5mo` rejection.
- Untouched as specced: `NICE_WIDTHS`, smoothing semantics (`resolveSmoothing` already handled calendar output bins; ragged calendar plans carry `smoothSourceTier` = exact tier name or `<ragged:1mo>` placeholder), fixed-target ragged path (`emitRaggedSegment`/inventory emit refactored to width-normalized `reaggregate` — behavior-identical for fixed).

## Motivation

Calendar bins (months, quarters, years) are the natural display unit for long-timescale views — ctbk's Home chart has always shown calendar-month bars. They are not fixed durations (28–31d months, leap years), so they sit outside the fixed-width `min/h/d` machinery. Two capabilities are wanted, sharing one core:

1. **Query-time het-tiling to calendar targets**: serve `bin=1mo` (or `2mo/3mo/6mo/1y`) by decomposing each target bin into aligned fixed-day bins fully contained in it (largest-first greedy containment, segment-tree style), reaggregating at stitch. With a `{1d, 3d, 7d, 14d}` day-bin ladder a month packs into ~5–9 pieces. This must exist **regardless** of materialization: a `/1mo` shard for the in-progress month structurally cannot exist until the month closes, so the live tip of any monthly view is het-tiled from finer tiers — it's the calendar flavor of mixed-tier tail coverage.
2. **Materialized calendar tiers** as an optional per-pyramid "index on top" of the fixed base: tiers like `{ name: 1mo, bin: 1mo, shards: [1y, 2y, …] }` in the ladder YAML, built by the engine cascade (exact: month boundaries are whole days, `1mo ← 1d`; then calendar-calendar edges `2mo ← 1mo`, `3mo ← 1mo`, `6mo ← 3mo`, `1y ← 6mo`). Whether a pyramid materializes calendar tiers or serves them purely query-time is decided by whether the tiers appear in its config — the planner uses a materialized+registered tier when present, and het-tiles otherwise (and always for the un-closed tip).

Downstream driver: ctbk rides-v5's full-history monthly Home view (13y × system-wide cover). Serving it from the `1d` tier OOM-kills the CFW worker; het-tiling alone is a ~4× row reduction (borderline); a materialized `/1mo` tier is ~30× (~160 bins/series) and makes it boring. ctbk will add a `{1,2,3,6}mo + 1y` calendar family (bin-SUFs 2.17, 2, 1.5, 2, 2 — all ≤2.2 above the `14d` tier).

## Anchoring contract (normative)

JS `floorToSpan` (`js/packages/pyrmts/src/axis.ts:61-97`) is already multi-unit and is the deployed reference. Its contract, which Python must mirror exactly:

- `Nmo`: requires `12 % N == 0`; floors month-of-year to a multiple of N (`3mo` → Jan/Apr/Jul/Oct). Year-anchored ≡ epoch-anchored (1970-01 is a January, pattern repeats yearly).
- `Ny`: `floor(yyyy / N) * N` — **year-0 anchored** (`128y` → 1920 for 1920–2047).

⚠️ **Polars divergence (latent bug)**: `dt.truncate('Ny')` anchors at the Unix epoch (1970), not year 0 — `4y` gives 1970+4k vs the contract's 1968+4k, `128y` gives 1970 vs 1920. `dt.truncate('Nmo')` is fine (epoch ≡ year-0 for `12 % N == 0`). `bin_floor_expr` must implement `Ny` via year arithmetic (e.g. `pl.datetime((dt.year() // N) * N, 1, 1)`), not `dt.truncate`. Add a regression test pinning `4y`/`128y` floors on both sides.

## Python changes (the #122 unblock proper)

Current multi-unit guard sites: `axis.py:57-59` (`floor_to_span`), `plan.py:72-74` (`bin_floor_expr`), `consolidate.py:62-66`, `engine.py:901-903`.

1. **`axis.py` `floor_to_span`**: implement `Nmo` (validate `12 % N == 0`, raise otherwise — same message as JS) and `Ny` per the contract above. `ceil_to_span`, `bins_in_range`, `shard_periods_covering`, and `format_period` all delegate and should then Just Work — `format_period` keys off unit only (label = period-start instant; `{shard}` disambiguates the path), no change needed.
2. **`plan.py` `bin_floor_expr`**: drop the `count != 1` guard; `Nmo` via `dt.truncate(f'{N}mo')`, `Ny` via year-arithmetic expr (see divergence note). `_divides` (`plan.py:48-66`) is already calendar-aware including multi-unit counts — verify with tests: `1d | 1mo` ✓, `3d ∤ 1mo` (only divisors of one day divide months), `1mo | 3mo`, `3mo | 6mo`, `6mo | 1y`, `2mo ∤ 3mo` ✗.
3. **`yaml.py` validation gap**: `_validate_shard_ladder` currently skips *all* checks when either side is calendar (`_fixed_ms_or_none` → `None` → `continue`), so `[2mo, 3mo]` is silently accepted today, contrary to `specs/done/python-unified-ladder.md:207-211`. Enforce: calendar-calendar rung pairs must divide in months (`y` = `12mo`); calendar `Nmo` bins/rungs require `12 % N == 0`; ascending check should use nominal widths (30d/365d) for calendar entries. Mirror the same checks in JS `ladder.ts` (`shardMsOrNull`/`validateLadder` have the identical leniency).
4. **`gap_discovery.py`**: should work once `floor_to_span` lands (`_largest_fitting_rung`'s alignment test is `floor_to_span(cur, span) != cur`). Add coverage: `list_expected_shards` for a `1mo`-bin tier with `[1y, 4y]` rungs across a genesis mid-year (effective_start clipping) and across the live tip (trailing-rung descent).
5. **`engine.py`**: cascade edges for calendar tiers come free via `compile_plan` + `bin_floor_expr` + `shard_periods_covering` (shard routing is pre-computed epoch-ms intervals, unit-agnostic). `_validate_window` stays fixed-width — ingest windows are unrelated to tier bins. **Verify**: cross-window partial-bin merge already handles bins ≫ window (14d bins with `-w 2d` windows built byte-equal for rides-v5), calendar bins are the same shape; add a toy build test with a `1mo` tier and a small window asserting exact equality with a whole-frame groupby.
6. **`consolidate.py` (Lambda same-tier tiling)**: **out of scope** — the fixed-width wall stays (calendar tiers in ctbk are Batch/fill-owned monthly, no Lambda cadence). Keep the existing raise; `specs/pyrmts-ops-adoption.md:32` already documents this.

## TS changes: calendar-target ragged decomposition

`planRagged` / `planRaggedFromInventory` (`planner.ts:289-298`, `:959-969`) throw on calendar `targetBin` and exclude calendar-binned tiers; the packing arithmetic is epoch-ms gcd/lcm, i.e. assumes a periodic decomposition. Calendar months break periodicity — but non-periodic packing is not much harder, it just can't be pattern-per-lcm:

- Enumerate target bins `[a_i, b_i)` by walking `floorToSpan`/`addSpan` (calendar-correct already).
- Eligible source tiers: fixed tiers whose bin is a whole-day multiple (calendar boundaries are day-aligned, so sub-day tiers are never needed). Exactness requires a `1d` (or finer day-divisor) tier as the base case; error if absent.
- Per target bin, greedy containment: for each eligible tier coarsest-first, take all bins aligned to that tier's own epoch grid that lie **fully inside** `[a_i, b_i)`; recurse the uncovered edge intervals with finer tiers; `1d` tiles any day-aligned residue exactly. (~5–9 pieces/month from `{14d, 7d, 3d, 1d}`; no pow-2 requirement.)
- Emit per-tier segments with `reaggregate: true`. `stitch` already floors rows to the output bin via `floorToSpan` (`stitch.ts:58`) — calendar-correct, no change.
- Inventory flavor: same decomposition restricted to registered shards; unregistered coverage falls to finer tiers — this is what serves the un-closed tip of materialized calendar tiers.
- When a materialized calendar tier matching the target exists and is registered, `pickTier`-style selection should prefer it and only het-tile the uncovered residue (tip) — i.e. the materialize-vs-query-time choice is resolved per-range at plan time, not globally.
- Untouched: `NICE_WIDTHS`, smoothing (`resolveSmoothing` keeps rejecting fixed smoothing over calendar outputs), fixed-target ragged path.

TS `gap-discovery`/`listExpectedShards` should already handle calendar rungs (JS `floorToSpan` is ahead) — ctbk's reconcile loop will lean on this for calendar-tier registration; please add tests mirroring the Python ones.

## Acceptance

1. **Cross-impl parity fixture**: shared table of (instant × span) → floored instant covering `1mo/2mo/3mo/6mo/1y/4y/128y`, month/year boundaries, leap years; both test suites assert exact equality (this pins the polars `Ny` anchor fix).
2. **Ragged calendar exactness**: for a toy pyramid with `{1d, 3d, 7d, 14d}` tiers and random sum-monoid data, `bin=1mo|3mo|1y` het-tiled results byte-equal a brute-force 1d→calendar groupby, across ranges straddling month/year boundaries and February.
3. **Engine calendar build**: toy config with calendar tiers cascades (`1mo ← 1d`, `3mo ← 1mo`, `1y ← 6mo`), small-window build equals whole-frame groupby; `shard_periods_covering`/`format_period` produce the expected keys (`…/1mo/1y/2026.parquet` shapes).
4. **Ladder validation**: `[1mo, 3mo, 1y]` accepted; `[2mo, 3mo]` rejected; `5mo` bin rejected (`12 % 5 ≠ 0`) — both languages.

## Sequencing / handoff

1. Python multi-unit spans + validation gap (+ parity fixture) — unblocks ctbk's engine build of rides-v5 calendar tiers (Batch fill per anchor; ctbk will re-pin `main`).
2. TS calendar-target ragged packing — unblocks live monthly tips + serving calendar bins over pyramids without materialized calendar tiers (ctbk re-pins `dist` in `gbfs/api`).
3. Record re-pin SHAs in this spec per the usual convention; ctbk session runs downstream integration (build + API-level v3-parity compare) before `r/main` push.

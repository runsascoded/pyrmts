# Calendar composition, unrestricted `Nmo`, and query limits

Status: **spec** (2026-08-16, ctbk session). Successor to `specs/calendar-units.md` (phases 1+2, landed 2026-08-10). Four independent changes, sequenced so each is separately shippable.

Downstream driver: ctbk wants to expose pyrmts' full het-cover flexibility through `/api/rides-v5`, which today hard-codes `bin=1mo|2mo|3mo|6mo|1y` and 400s everything else (`gbfs/api/src/rides_v1.ts:674`). Three things block simply relaxing that validation, and one is a latent inefficiency worth fixing alongside.

## Motivation

The planner already documents the general contract — `targetBin` accepts any width, non-materialized widths get ragged DP decomposition. In practice three gaps keep it from being usable as advertised:

1. **Calendar tiers don't compose with each other.** `calendarEligibleTiers` (`planner.ts:518`) collects an `exactTier` (a calendar tier whose month-count equals the target's) and otherwise populates `eligible` with **fixed** tiers only — calendar tiers hit `continue`. So a `4mo` target het-tiles from 14d/7d/3d/1d atoms (~9 per bin) instead of 2×`2mo`, and `2y` from ~52 14d atoms instead of 2×`1y`. Purely missing capability: greedy containment is already the right algorithm, it just isn't offered calendar sources.

2. **`Nmo` is restricted to `12 % N == 0`.** `floorToSpan` (`axis.ts:65`) throws otherwise, so `5mo`, `7mo`, `8mo`, `18mo` are unrepresentable. The restriction exists to keep the month grid year-anchored, but the fixed-width path has never required its grids to align to any calendar unit (`5d` bins drift freely), and §Anchoring below shows the restriction is unnecessary: a strictly more general rule reproduces every current behavior exactly.

3. **No cost ceiling.** `binBudget` is explicitly ignored when `targetBin` is set — *"restoring budget enforcement is left to the caller"* (`planner.ts:46`). No caller does. Opening `bin=` up without this means `bin=1h&from=2013-06` plans ~115k output bins, and nothing anywhere bounds the number of shard keys a plan may touch.

Plus one adjacent gap: **`pyrmts-geo`'s `planGeoQuery*` has no `targetBin` parameter** at all, so geo callers can't request an explicit width. ctbk works around this by planning calendar queries through the time-only `planQueryFromInventory` and filtering cells post-hoc, which works but means geo plan metadata (`outputRes`, per-segment cells) is unavailable on the calendar path.

---

## 1. Anchoring: year-0 month anchoring (normative)

**Contract change.** Define the `Nmo` grid by flooring *months since year 0*:

```
floorToSpan(t, {count: N, unit: 'mo'}):
    M = 12 * t.getUTCFullYear() + t.getUTCMonth()
    M = Math.floor(M / N) * N
    → Date.UTC(Math.floor(M / 12), M % 12)
```

No divisibility validation. `Ny` stays `floor(yyyy / N) * N`.

**This is a strict generalization with zero behavior change for every currently-legal span.** Verified exhaustively over 1900–2100 × all 12 months:

- For every currently-valid `Nmo` (N ∈ {1,2,3,4,6,12}), year-0 month anchoring is identical to the current floor-month-of-year rule. They agree precisely when `N | 12`, which is exactly the set the current rule admits.
- For every `Ny`, year-0 month anchoring of `12N mo` is identical to the current `floor(yyyy/N)*N`. So **`Ny ≡ (12N)mo` becomes an identity for all N**, where today it holds only for N=1 (`1y ≡ 12mo`, relied on by `calendarEligibleTiers`' months-normalized comparison).

0 divergences across 2,400 (span, instant) pairs. No parity-fixture regeneration required; `fixtures/calendar-floors.json` should keep its 77 cases verbatim and gain rows for newly-legal spans.

**Why year-0 and not epoch-month anchoring.** Epoch-month anchoring (`floor((M - 12*1970) / N) * N + 12*1970`) also gives a coherent drifting grid, and coincides with year-0 for many N — including `5mo`, since `12 × 1970 = 23640` is divisible by 5. But it diverges wherever `N ∤ 23640` (e.g. `7mo`), and more importantly it would break the `Ny ≡ 12N mo` identity, since `Ny` is year-0 anchored: `48mo` epoch-anchored floors 1970 → 1970, while `4y` → 1968. Year-0 keeps the two unit families consistent, which is what makes "years are clean compositions of months" true rather than approximately true.

**UX consequence to accept explicitly**: `5mo` bins do not align to years. From 2026-01 the grid is `2025-11, 2026-04, 2026-09, …`. That is inherent to any width that doesn't divide 12, not a defect.

### Sites

| Site | Change |
|---|---|
| `js/packages/pyrmts/src/axis.ts:61-97` | `floorToSpan` `Nmo` branch → year-0 month arithmetic; drop the `12 % count` throw |
| `js/packages/pyrmts/src/ladder.ts:179` | drop `Nmo` tile-a-year validation; keep calendar-calendar divisibility (months) and nominal-width ascension |
| `python/pyrmts/src/pyrmts/axis.py:74-78` | mirror `floor_to_span` |
| `python/pyrmts/src/pyrmts/yaml.py:262` | mirror the ladder-validation relaxation |
| `python/pyrmts_engine/src/pyrmts_engine/plan.py:69-74` | `bin_floor_expr`: `dt.truncate('Nmo')` is **epoch**-anchored and is only correct when `12 % N == 0` — the same class of bug already documented for `Ny` in `calendar-units.md`. Implement via month arithmetic (`(12*year + month) // N * N`) for the general case |

⚠️ `plan.py` is the one place where the relaxation introduces a *new* correctness hazard rather than just widening an accepted set — `dt.truncate` silently returns epoch-anchored results for non-year-dividing `N`. Pin `5mo`/`7mo` floors against the JS reference in the parity fixture.

### Tests that pin the old behavior

`axis.test.ts`, `ladder.test.ts`, `planner.test.ts` each pin a `5mo` rejection (per `calendar-units.md` phase-2 notes), as does ctbk's `parse_pyramid_yaml` ladder validation. These flip from "throws with message X" to "produces grid Y". Replace, don't delete: assert the actual `5mo` boundaries so the drift behavior is itself pinned.

---

## 2. Calendar tiers as het-tiling sources

**Change.** `calendarEligibleTiers` should return calendar tiers *finer than* the target as packing sources, ranked coarsest-first alongside (ahead of) the fixed day tiers, and `packCalendarWatermark` / `packCalendarInventory` should be able to emit atoms on a calendar grid.

Eligibility rule for a calendar source tier `S` against calendar target `T` (both months-normalized): `S.months < T.months` and `T.months % S.months == 0`. Under §1's year-0 anchoring, count divisibility is *sufficient* for grid alignment — both grids anchor at year 0, so every `T` bin boundary is also an `S` bin boundary. (This is a second reason to prefer year-0 over epoch-month anchoring: it makes the containment test a divisibility check rather than a phase computation.)

Where `T.months % S.months != 0` — e.g. `T=5mo`, `S=2mo` — the source is still usable for *part* of a bin by the existing greedy fully-inside containment: a 5mo bin starting at a 2mo boundary takes 2×`2mo` + 1×`1mo`. So the packer should not filter on divisibility at all; it should offer every finer calendar tier and let containment decide, exactly as it does for fixed tiers. Divisibility only matters as the guarantee that an *exact* whole-bin cover exists.

**Implementation shape.** Both packers currently recurse in raw ms (`Math.ceil(startMs / ms) * ms`, `DAY_MS % ms === 0`). Generalize the atom recursion over a grid abstraction:

```ts
interface PackGrid {
  tier: Tier
  floor(t: Date): Date      // floorToSpan(t, tier.bin)
  next(t: Date): Date       // addSpan(t, tier.bin)
  nominalMs: number         // ordering only (mo = 30d, y = 365d)
  isBase: boolean           // divides 1d — can serve a partial calendar bin exactly
}
```

Fixed tiers keep today's behavior (`floor`/`next` are ms arithmetic). Calendar tiers get `floorToSpan`/`addSpan`. Ordering stays coarsest-first by `nominalMs`. The `hasBase` requirement is unchanged: without a day-divisor tier, a residue that isn't a whole number of source bins can't be served, so het-tiling stays disabled (uncovered bins omitted) rather than silently under-reporting.

The watermark flavor's "strictly sealed and fully inside" rule for multi-day tiers applies unchanged to calendar tiers: a mid-period `1mo` row could pull cross-boundary data into the target bin, so only sealed whole source bins may be emitted; only day-divisor tiers may emit a clipped trailing atom.

**Acceptance.** Extend `calendar-ragged.test.ts`: a pyramid with `{1d, 7d, 1mo, 3mo}`, LCG sum-monoid data over 2023–2024 (leap Feb), and `bin=4mo` / `bin=6mo` / `bin=5mo` / `bin=2y` plans that stitch **exactly equal** to brute-force `1d` → calendar groupby. Assert the plans use the calendar sources (`4mo` = 2×`3mo`? no — `4 % 3 != 0`, so `1mo`×4 or `3mo`+`1mo`; `6mo` = 2×`3mo`; `2y` = 2×`1y` where materialized) rather than day tiers, since equality alone wouldn't catch a regression to the slow path.

---

## 3. Query limits

**Change.** Add an optional `limits` to `PlanQueryInput` (and a per-pyramid default on `Pyramid`), honored under **both** `targetBin` and `binBudget`:

```ts
interface PlanLimits {
  maxOutputBins?: number   // bins in the planned range
  maxAtoms?: number        // total ragged packing atoms across all bins
  maxKeys?: number         // distinct shard keys the plan touches
}
```

Violations throw a typed `PlanLimitError { limit: 'bins'|'atoms'|'keys', requested, allowed }` so callers can map to 400/413 instead of discovering the problem as an OOM. Defaults: unset (current behavior), so this is non-breaking; ctbk sets its own.

The three axes are genuinely independent and capping only the first is insufficient:

- **bins** — response size and client render cost. `1h` over 13y = ~115k bins but only ~150 keys.
- **atoms** — source rows fetched and stitched. A poorly-packed calendar target is few bins but many atoms per bin; this is the axis §2 improves.
- **keys** — R2 GETs and manifest lookups: the axis that costs money and drives tail latency, and the one uncorrelated with the other two.

`binsInRange` (`axis.ts:118`) already computes the first cheaply and pre-plan; atoms and keys are known only after packing, so the check belongs at plan assembly, before returning.

Note `binBudget` and `targetBin` remain distinct inputs with distinct meanings — budget says "pick a width that fits N bins", target says "use exactly this width". They are not modes to be unified; the fix is that limits apply to both. When both are supplied, `targetBin` wins for width selection and `binBudget` is treated as `maxOutputBins` if `limits.maxOutputBins` is unset.

---

## 4. `targetBin` in `pyrmts-geo`

**Change.** Thread `targetBin` through `PlanGeoQueryInput` → `planGeoQuery` / `planGeoQueryFromInventory` (`pyrmts-geo/src/planner.ts:38,92,165`), delegating to the core ragged/calendar planners the same way the fixed-tier path delegates to `planQuery*` today, and preserving `outputRes` / `outputCells` resolution in the returned `GeoQueryPlan`.

Lowest priority of the four — ctbk's post-hoc cell filter is correct without it — but it removes a real asymmetry (calendar queries currently return `outputRes: -1` by construction) and lets the geo planner do per-segment cell selection on explicit-width queries.

---

## Sequencing

1. **§1 anchoring** — self-contained, both languages, zero behavior change for existing spans. Land + push first; ctbk needs nothing.
2. **§3 limits** — independent of §1/§2, non-breaking (defaults unset). Land second so ctbk can wire guards *before* opening `bin=` up.
3. **§2 calendar composition** — the largest change; benefits from §1 landing first (divisibility ⇒ alignment).
4. **§4 geo `targetBin`** — optional cleanup.

ctbk integration (separate session, `ctbk/specs/`): after §1+§3, relax `serveRidesV5`'s `bin=` validation to any parseable `Duration`, wire `limits` from a config, and keep routing explicit-width queries through `planQueryFromInventory` until §4 lands. Re-pin JS SHAs via `pds gh`, Python via the `pyproject.toml` uv source rev, per `calendar-units.md`'s push protocol.

## Out of scope

- `consolidate.py`'s fixed-width wall (unchanged; calendar tiers are Batch/fill-owned).
- `NICE_WIDTHS` / smoothing semantics — `resolveSmoothing` already handles calendar output bins.
- `_validate_window` (ingest windows are fixed-width and unrelated to tier bins).

---

## Status (2026-08-16, pyrmts session)

All four sections implemented in spec order, one commit each. JS 507 vitest + `tsc -b --force` clean; Python 203 core+engine / 17 ops / 7 geo.

### §1 anchoring (`ee6fa32`)

Landed as specified. `floorToSpan`/`floor_to_span` floor months-since-year-0; the tile-a-year validation is gone from `ladder.ts`, `yaml.py`, and both floor implementations. `plan.py`'s `bin_floor_expr` no longer calls `dt.truncate('Nmo')` (epoch-anchored, wrong for non-year-dividing N) — it computes the year-0 month index directly, the same treatment `Ny` already had.

`fixtures/calendar-floors.json` keeps its 77 cases verbatim and gains 55 for `5mo`/`7mo`/`8mo`/`18mo`/`48mo`, generated from the JS reference. Both parity suites sweep every span, so the polars anchoring hazard the spec flagged is pinned in Python (`bin_floor_expr`) and in JS. The `Ny ≡ (12N)mo` identity is verified directly: `4y` and `48mo` agree on all 11 shared fixture instants.

The `5mo`-rejection pins in `axis`/`ladder`/`planner`/`yaml`/`plan` tests were replaced (not deleted) with assertions on the grids those spans now produce — e.g. from 2026-01 the 5mo grid is 2025-11, 2026-04, 2026-09.

### §3 limits (`9eca867`)

`PlanLimits` + `PlanLimitError` live in `types.ts` alongside `EtagConflict`; `PlanQueryInput.limits` overrides `Pyramid.limits` wholesale (not merged field-by-field). All six planner return sites route through one `finalize` choke point; `maxOutputBins` is additionally checked pre-plan so oversized requests fail before packing.

**Deviation**: the spec didn't say where the atom count lives, and it can't be recovered from a finished plan (coalescing merges adjacent same-tier atoms, including across bin boundaries). Every plan now carries `atomCount` — the pre-coalesce count. It's what `maxAtoms` bounds and is useful cost metadata in its own right.

**Back-compat hazard, please read before re-pinning**: per §3, `binBudget` now stands in for `maxOutputBins` when that's unset. Callers passing a small placeholder `binBudget` alongside `targetBin` — relying on the old "ignored" contract — will now get `PlanLimitError` instead. This is not hypothetical: two tests in this repo used exactly that idiom (`binBudget: 1` + `targetBin: '1mo'`) and had to be updated. Audit ctbk's `targetBin` call sites for placeholder budgets.

### §2 calendar composition (`f6be464`)

`calendarEligibleTiers` returns `PackGrid[]` (the spec's abstraction) instead of `{tier, ms}[]`; both packers walk grids rather than doing ms division, so calendar sources stride correctly. No divisibility filter, per the spec — containment decides.

Measured on the spec's `{1d, 7d, 1mo, 3mo}` pyramid: `4mo` → 5 atoms (`3mo` + 3×`1mo` + `3mo`), `6mo` → 2 (2×`3mo` per bin), `5mo` → 4 (mixed `3mo`/`1mo`), `2y` → 2 (8×`3mo` per bin). All four stitch exactly equal to the brute-force day→calendar groupby, and each test pins the tiers the plan reads so a regression to day-tiling fails rather than silently costing more.

One acceptance-list correction: the spec's fifth case ("day tiers still serve the residue") doesn't exist as described — calendar target bins are always whole, so a mid-month query start still packs the full containing bin from calendar sources. Replaced with a watermark-demotion test that does exercise the fallback: one `3mo` bin walking `mo3` → `mo1` → `d7` → clipped `d1` as each watermark runs out.

### §4 geo `targetBin` (`ff17960`)

`targetBin` and `limits` thread through both geo entry points. Explicit-width geo queries now resolve `outputRes` and per-segment cells, so ctbk can retire the post-hoc cell filter once it re-pins.

This commit also **fixes a build break introduced by §3**: `GeoQueryPlan extends Omit<QueryPlan, 'segments'>`, so `atomCount` became required in three places in `pyrmts-geo`. Incremental `tsc -b` was skipping the package on stale buildinfo and reporting success; only `tsc -b --force` surfaced it. Worth knowing for anyone verifying this repo — plain `tsc -b` can lie after a cross-package type change.

### ctbk integration

Ready for all four. `bin=` can accept any parseable `Duration`; wire `limits` from config (and audit placeholder `binBudget`s per §3 above); explicit-width queries can go straight through `planGeoQuery*` now rather than waiting on §4.

### Re-pin SHAs (for ctbk)

- `main`: `6378cdd679e5f01da16628faca4552a8cf1f40aa` (Python pin — uv source rev; §1 touches `pyrmts.axis`, `pyrmts.yaml`, `pyrmts_engine.plan`)
- `dist`: `69de58b` (JS pin via `pds gh`)

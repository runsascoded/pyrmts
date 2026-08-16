# Vocab-restricted ± covers: region queries over a finite stored vocabulary

Status: **implemented** (pyrmts session, 2026-07-28 — see Status section at bottom). Written by the ctbk session (2026-07-28). Last serving gap for ctbk's avail-v5 (drop-LUC) cutover: **region/bbox queries**. v5 stores rows only at a *frozen vocabulary* — a ragged set of S2 cells (L10–L16, descend-while->T ancestry) plus one `s:<short_name>` identity row per station — so a cover produced by today's `s2Index.minimalCover` (arbitrary cells at the pyramid's `resolutions`) silently undercounts: cover cells not in the vocabulary match no rows. Station-point queries are already fine (identity keys pass through the planner opaquely — verified in prod 2026-07-28, exact value parity + latency parity vs v3); only *set/region* selection needs this.

## Contract

New pyrmts-geo capability (name suggestion: `vocabCover`):

1. **Inputs**: a `VocabGraph` (consumer-built, see below) + a target station *set* (a bbox/region is just one selector: consumer maps geometry → station set via the registry's lat/lngs).
2. **Output**: a minimal ± term list `{include: string[], exclude: string[]}` over vocabulary members only — directly usable as the existing `cells` / `cells.exclude` query params (the worker's sum-monoid exclude subtraction already exists; note ctbk's avail path deliberately avoids ± for histograms — see "monoid caveat" below).
3. **`VocabGraph`**: built by the consumer from its vocab + station registry — nodes = vocab cells, edges by S2 containment, leaves = stations (each station a child of its finest containing vocab cell; identity key `s:<name>` is the leaf's term). lat/lngs enter *only* at graph build + leaf classification; the cover computation itself is geometry-free.
4. **Optimal cover is the linear two-function DP** (from ctbk `specs/drop-luc-station-keys.md`, benched in `scripts/cover-bench.py`):

   ```
   pos(node) = min(Σ pos(children), 1 + neg(node))   # node − complement
   neg(node) = min(Σ neg(children), 1 + pos(node))   # symmetric
   leaf(station): pos = (1 if in set else 0); neg symmetric
   ```

   Per-node greedy is NOT optimal (an L12 with four partial children: `L12 − Σ outsiders` beats four per-child forms). Reconstruct terms by walking the argmin choices.

## Monoid caveat (important for ctbk avail)

ctbk's avail worker currently refuses sign-flip arithmetic for histogram metrics (subtraction of histograms is exact as counts but the serving path does a lineage-walk filter instead — `avail_geo.ts` header note). Two options, consumer's choice per metric monoid:
- **Sum-like monoids (rides counts)**: use full ± output — maximal term compression.
- **Histogram/lineage-filter paths (avail)**: request `positive-only` mode — DP restricted to `pos = Σ pos(children)` (no complement branch), i.e. exact union of vocab cells + identity keys, no excludes. Still vocab-restricted (the correctness fix), just less compressed. This mode is the immediate ctbk-avail unblock; ± mode benefits rides-v4 later.

## Placement

`pyrmts-geo` (it owns covers/indexes; the DP is index-agnostic — works for any containment forest, so H3 vocabularies get it free). The `VocabGraph` builder can ship alongside (`buildVocabGraph(cells: string[], leaves: {key, cell}[])` using the existing s2 containment helpers); consumers keep their registry/vocab as data.

## ctbk-side follow-through (recorded here for coordination)

- Regenerate `region-cells` selections over the vocab (today they're raw `minimalCover` outputs baked as JSON) — becomes: region → station set (registry point-in-region) → `vocabCover(..., positiveOnly)` → cells param.
- Home/region + `/api/totals` parity runs via the existing `ctbk gbfs parity` harness (extend with region cases) → then default `?pyramid=` flip and #161 close-out.

## Tests

- DP optimality on the drop-LUC spec's counter-example (4-partial-children L12) and on a random-forest fuzz vs brute-force term enumeration (small N).
- Positive-only mode = exact union (row-set equality vs a per-leaf query union on a fixture pyramid).
- End-to-end: fixture vocab pyramid; bbox → station set → cover → query rows ≡ union of the stations' identity-row queries.

## Status (pyrmts session, 2026-07-28)

Implemented in `pyrmts-geo` (`js/packages/pyrmts-geo/src/vocab-cover.ts`): `buildVocabGraph(index, cells, leaves)` + `vocabCover(graph, wanted, {positiveOnly})`, both exported from the package index. Notes against the contract:

1. **API shape**: `vocabCover` takes the wanted *station-key* set (`Iterable<string>` of leaf identity keys) and returns the existing `SpatialSet` (`{include, exclude}`) so it drops into `cells`/`cells.exclude` unchanged. It throws on wanted keys absent from the graph — a silently-dropped station is exactly the undercount this spec exists to kill.
2. **`VocabGraph`**: `buildVocabGraph` takes each leaf as `{key, cell}` where `cell` is *any* cell containing the station (typically its finest-level cell) and classifies it under the nearest containing vocab member via the backend's parent walk — so the ragged vocab (skipped levels) needs no consumer-side classification. Cells/stations contained by no vocab member become forest roots (a root station stays selectable by its identity key). `VocabNode` is a plain exported interface, so consumers can also build graphs directly.
3. **DP**: the two-function ± recurrence, memoized per node (each node's pos/neg can be demanded from both parent branches), reconstruction by argmin walk, ties → explicit (matching `minimalCover`'s convention). Geometry- and index-free as specced — `SpatialIndex` is used only by `buildVocabGraph`.
4. **positive-only**: the complement branch is dropped from `pos` (`neg` is then unreachable); output is the exact union of fully-wanted vocab cells + wanted identity keys, `exclude` always `[]`.
5. **Tests** (13, `vocab-cover.test.ts`): the counter-example asserts the exact 5-term `P − 4 outsiders` form (vs 8 per-child); seeded random-forest fuzz proves the DP hits the brute-force minimum over all ± (and, in positive-only mode, all +-only) term assignments under nearest-signed-ancestor semantics, 30 trials each; value-arithmetic test (station values = distinct powers of two, cell rows = Σ under) asserts `Σ include − Σ exclude ≡ Σ wanted` in both modes — the monoid form of "cover rows ≡ union of identity-row queries". The full-pyramid e2e (bbox → registry station set → cover → served rows) lives naturally in ctbk's parity harness, where the real registry/vocab/worker are; the value-arithmetic test covers the pyrmts-side exactness claim.

Not pushed to GitHub yet (standing hold): ctbk consumes this via `pds l pyrmts-geo` against the local checkout for the region-cells regeneration + parity runs; the TS-pin bump / dist-branch push happens when ctbk is ready to take it to CI/prod.

## Adoption confirmed (2026-08-16, pyrmts session) — moving to `done/`

The standing hold above was discharged on 2026-07-30 and the status line was never updated. Pushed as part of the ops-adoption batch (`main` = `3073c60`, `dist` = `37c363f`; see `specs/pyrmts-ops-adoption.md`), which records ctbk running "P1–P4 + fill-mode + min-cover + **vocabCover**" in prod that day. Verified against ctbk's checkout today:

- `gbfs/api/src/avail_geo.ts:409` — `buildVocabGraph(s2Index, stationVocab.cells, leaves)` on the frozen `station-vocab.json`, feeding the region-cover path.
- `www/src/pages/CellsDebug.tsx` — `vocabCover` is a first-class Cover mode alongside raw-S2 `minimalCover`, with exact-selection + ⌘-drag rectangle select built on it (ctbk `84609674`).
- ctbk pins `pyrmts-geo` at `dist 69de58b` in `gbfs/api` and `www`, well past the `37c363f` that first carried this.

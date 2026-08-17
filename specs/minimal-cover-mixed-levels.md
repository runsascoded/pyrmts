# `minimalCover`: support mixed-level systems, remove premature stall-exit

> **Status (2026-08-14, pyrmts session): implemented.** `buildTree` rewritten as the level-stratified deepest-first walk (fixes defects 1+2); disjointness contract enforced with a throw naming ancestor + original descendant token (the error-path descends `children[0]` to a leaf, so deep nesting names the real system cell, not an intermediate chain cell); dead `MinimalCoverOpts.maxLevel`/`resolutions` deleted (no in-repo callers passed them). Tests 1–7 all land in `spatial-index-cover.test.ts` (`describe('minimalCover: mixed-level systems (LUC cells)')`, using the spec's pinned ctbk tokens where given), plus a deep-nested-ancestor throw variant; suite 439/439, `tsc -b` clean. Awaiting ctbk verification (CellsDebug LUC switch + `maxLevel`-caller audit) before moving to `done/`.

Source: ctbk `/cells-debug` session, 2026-08-13. The page tried passing station **LUC cells** (levels 10–20, unique per station) as the `system` of `pyrmts-geo`'s `minimalCover`, instead of uniform-L15 point cells. Result: **empty covers** for non-empty `include`.

The uniform-L15 workaround it reverted to is itself lossy: ~1100 of 2340 ctbk stations have LUC level ≥16, i.e. share their L15 cell with a neighbor — so an L15-system cover for station A silently also covers neighbor B (observed: selecting JC081 "Brunswick & 6th" yields an L14 cell that also contains unselected JC075 "Monmouth & 6th"; both live in L15 `89c2574b4`). Mixed-level (LUC) systems are the correct input; `minimalCover` needs to handle them.

## Defects (all in `spatial-index-cover.ts` `buildTree`)

### 1. Count propagation assumes a lockstep (uniform-level) walk

`buildTree` steps the whole frontier up one parent per iteration, and propagates `includeCount`/`excludeCount` into a parent **only at link time** (`if (node.parent !== parentNode) { ... parentNode.includeCount += node.includeCount }`). With mixed-level system cells, a node can link into its parent (propagating its then-current counts) **before** a deeper chain later links into *it* — the late-arriving counts are stranded below the already-propagated ancestor and never reach the roots. Roots then see `includeCount === 0` and `encodeInclude` returns `[]`.

Repro (ctbk assets, `www/public/assets/station-luc.json` + `stations-regional.json`):

```js
// system = 2337 LUC cells (L10..L20) + 3 L15 fallbacks; include = JC081's LUC cell
minimalCover(s2Index, ['89c2574b3'], system, { allowSubtraction: true })
// → { include: [], exclude: [] }   // WRONG: non-empty include must yield a non-empty cover
```

### 2. Stall-exit fires on any zero-merge iteration

The walk terminates when `frontier.size !== prevSize` fails — i.e. the **first** iteration in which no two chains merge. For sparse/deep systems that's almost immediately, long before real convergence, leaving a forest of near-leaf roots and no coarsening:

```js
// 3-cell mixed-level system: include diverges from the exclude at L13
minimalCover(s2Index, ['89c2574b3'], ['89c2574b3', '89c2574c4'], { allowSubtraction: true })
// → { include: ['89c2574b4'] }    // L15; optimal is ['89c2574b'] (L14)

// uniform-L20 system of all 2340 ctbk stations, include = JC region (77 stations)
// → +77 −0 (77 singleton cells, zero rollup); same selection at uniform L15 → +2 −4
```

### 3. Dead `MinimalCoverOpts` fields

`maxLevel` and `resolutions` are declared in `MinimalCoverOpts` but never read by the DP (only `allowSubtraction` and `coarsestLevel` are). ctbk was passing `maxLevel: 10` believing it capped output coarseness; it type-checked and did nothing. Propose deleting both fields (BC break fine per usual) and auditing callers — anything passing `maxLevel` should pass `coarsestLevel`.

## Fix: level-stratified bottom-up walk

Replace the lockstep loop with a by-level walk:

- Bucket frontier nodes by level. Repeatedly take the current **deepest** level L present and step *all* nodes at L to their parents (create/link/propagate as today). Since every child is at exactly `parent.level + 1` (`cellToParent` is single-step), deepest-first guarantees all of a node's children have linked into it before it propagates its own counts upward. This fixes defect 1 for arbitrary level mixes.
- A node stops stepping at level 0 (backend root) or `level <= coarsestLevel`. The walk ends when every frontier node is stopped; optional early-exit when the frontier collapses to a single root. No `prevSize` stall heuristic — this fixes defect 2. (The stall check's only real job was terminating multi-face forests, which "all nodes stopped" covers.)
- Complexity unchanged: O(Σ chain lengths) = O(|system| × levels).

### Contract: system cells must be mutually disjoint

Ancestor–descendant pairs inside `system` are geometrically ill-defined for point-set covers (an "exclude" ancestor cell spatially contains an "include" descendant). Detect and `throw`: while stepping a node, if the parent cell it links into is itself a member of `system`, raise with both tokens. Cheap (a `Set` lookup on an existing code path). ctbk hit this via `_`-suffixed alias stations whose L15 fallback cells nested real L16 LUC cells (`89c259b23 ⊂ 89c259b24`); ctbk will dedupe/drop aliases before calling.

## Tests (exact-equality assertions)

1. Mixed-level siblings: `minimalCover(s2Index, ['89c2574b3'], ['89c2574b3','89c2574b5'])` → `{ include: ['89c2574b3'], exclude: [] }`.
2. Mixed-level coarsening: `minimalCover(s2Index, ['89c2574b3'], ['89c2574b3','89c2574c4'])` → `{ include: ['89c2574b'], exclude: [] }` (rolls to L14, the divergence child — not the L15 the stall-exit used to stop at).
3. Sparse-deep coarsening: two L20 tokens diverging at L12, include one → exactly its L13 ancestor (divergence child), no stall.
4. Mixed-level subtraction: an L15 parent whose 4 L16 children are 3 include + 1 exclude → `{ include: [parent], exclude: [excluded child] }` (2 ops beat 3 explicit includes).
5. `coarsestLevel` respected with mixed levels: same as (2) with `coarsestLevel: 15` → stays `{ include: ['89c2574b4'] }`.
6. Ancestor-in-system throws: `system` containing both `89c259b24` and `89c259b23` → error naming both tokens.
7. Existing uniform-level tests stay green (lockstep is a special case of stratified).

## ctbk follow-ups (not this repo)

- `www/src/pages/CellsDebug.tsx`: switch the cover system from `latLngToCell(lat, lng, 15)` to per-station LUC cells (asset `station-luc.json`), keep `coarsestLevel: 10`; dedupe `_`-alias stations (`5308.04_` etc.) whose fallback cells nest real LUC cells.
- Audit other `minimalCover` callers for the dead `maxLevel` opt (region-cells generation, gbfs/api).

### ⚠️ Reopened 2026-08-17 — the ctbk acceptance evidence below was withdrawn

Closed on 2026-08-16 (`6f21c01`) on the strength of ctbk's LUC switch; they reverted that change about an hour later and asked us to reopen (`specs/mixed-level-cover-evidence-withdrawn.md`, since moved to `done/`). The note below was accurate when written — recording it verbatim because the *measurements* in it are real and were taken against this implementation — but it is no longer live evidence. See "Where the acceptance now stands" at the bottom.

<details><summary>Withdrawn adoption note (2026-08-16)</summary>

#### Follow-up status: **both adopted (2026-08-16, ctbk)** — moving to `done/`

Audited mid-day as half-done (the `maxLevel` caller sweep had landed in ctbk `db95f1e5`, the LUC switch hadn't, and `CellsDebug.tsx:55` still carried a stale "requires a uniform-level system" comment that had been false since 2026-08-14). Flagged to ctbk via `~/c/hccs/ctbk/specs/pyrmts-geo-h3-removal.md`; they landed the switch the same day.

**Acceptance on real data** (their `specs/done/pyrmts-geo-h3-removal.md`), which is what no pyrmts-side fixture could supply:

- The pinned counter-example reproduces and is now resolved: `JC081` Brunswick & 6th (`luc=89c2574b3`, L16) and `JC075` Monmouth & 6th (`luc=89c2574b5`, L16) are sibling L16 LUC cells sharing L15 `89c2574b4` — indistinguishable under the old uniform-L15 system, distinct now.
- At scale: the NYC preset (2,232 stations) yields a **12-term cover (+9 −3)** where the pre-fix `buildTree` produced an empty one. That's the defect-1 count-propagation bug and the defect-2 stall-exit both cleared on a 2,340-cell mixed-level (L10–L20) system.

**One correction to this spec's prescription.** The follow-up above said to "dedupe the `_`-alias stations". ctbk found that's the wrong operation — the aliases must be *mapped to the base station's cell*, not dropped, and the reason traces back to acceptance #6 (ancestor-in-system throws):

> a lat/lng fallback would insert an L15 cell into a system containing L16-20 cells nested inside it, and an ancestor in the system is exactly the lineage conflict the DP can't represent.

Three of the four aliases (`6569.09_`, `5308.04_`, `6517.08_`) have no LUC entry because LUC resolved them onto their base station; the fourth (`5303.06_`) does. Mapping all of them to the base station's cell is both semantically right (same physical dock) and *required* by the disjointness contract. Verified over all 2,340 stations: every one resolves, 2,337 distinct cells, zero ancestor/descendant pairs.

Worth keeping in view for future consumers: the disjointness throw is doing real work as a design constraint, not just a guardrail — it rules out the obvious "fall back to a point cell" handling of any station missing from a mixed-level vocabulary.

</details>

### Where the acceptance now stands (2026-08-17)

**ctbk exercises this code path nowhere, and can't be the acceptance evidence.** Verified their claim directly: `buildTree` is referenced only by `minimalCover` inside `spatial-index-cover.ts`, and `vocab-cover.ts` imports nothing from that module (`import type { SpatialIndex, SpatialSet } from './spatial-index.js'` is its only local import). `vocabCover` walks its own `VocabGraph` — a genuinely separate DP. So the mixed-level `buildTree` rewrite is not on ctbk's serving path even though their vocabulary spans levels 10–16, and their revert note's hope that `vocabCover` still exercised it was mistaken.

Their three reasons for reverting are all sound and none of them is about this implementation:

1. **LUC is relational** — a station's LUC cell is defined against every other station, so adding one churns existing anchors (166 moved in their 2026-07 re-key without physically moving). Their `drop-luc-station-keys.md` deliberately replaced LUC anchoring with fixed coarse cells + `s:<short_name>` identity keys and says "No `station-luc.json` denorm anywhere" — the switch walked that back.
2. **An exact LUC cover is unservable** — LUC reaches L20 while their pyramids materialize `[15..10]`; `coarsestLevel` caps the rollup but nothing caps *depth*, so an exact cover names cells no tier can answer. Worth noting as a general contract point: `minimalCover` guarantees minimality over the system it's given, not servability against a materialized ladder. A consumer whose vocabulary is finer than its finest tier has to coarsen the *system*, not the cover.
3. **The CellsDebug row has to mirror the FE, not improve on it** — it runs the same uniform-L15 `minimalCover` as `useRegionCoversV3`, whose output the worker uses only as a point-in-set test before re-deriving served terms via `vocabCover`. Its value is showing what the FE actually emits.

The L15 lossiness this spec's counter-example describes is real and now documented in place rather than fixed; ctbk absorbs it downstream (a fat L15 cell's extra stations are co-located neighbours, in the same region anyway). The latent edge case is a region boundary splitting an L15 cell — and they're clear LUC isn't the fix for it.

**Correction to this spec's own text**, per their note: the follow-up said to "dedupe the `_`-alias stations", and dedup is precisely the operation that doesn't work. The withdrawn note above already records the right handling (map aliases onto the entry they alias) and the reason (a lat/lng fallback inserts an ancestor into the system, which the disjointness check rejects). That lesson survives the revert — it's a property of ragged vocabularies generally, not of ctbk's code.

**Closure path.** No consumer exercises mixed-level systems today, so waiting on one is waiting indefinitely; ctbk's note sanctions closing "on pyrmts-side fixtures or a different consumer". Taking the first: a scale fixture lands alongside this note in `spatial-index-cover.test.ts`, `describe('minimalCover: ragged vocabulary at scale')`. It models the shape the seven unit cases don't — 2,340 deterministically-generated cells, each at whatever level makes it unique, ragged across **L11–L19** in one call, which is exactly where both defects bit.

Four cases, and two of them surfaced things worth keeping:

- **Fixture sanity** — level span, distinctness, and lineage-disjointness pinned, so a fixture-generation change can't silently weaken the test.
- **A 2,200-member subset covers in 139 terms** — `1` include + `138` exclude. The DP names the root and subtracts rather than enumerating, which is the ± reasoning the cover exists for and a far sharper demonstration than the hand cases. Membership is then verified exhaustively: `isCellInCover` agrees with the include set for all 2,340 members.
- **`coarsestLevel` bounds one direction only.** Setting it to 15 caps what the walk *rolls up to*, but system members already coarser than 15 pass through untouched — the walk stops at them and can't invent finer cells absent from the vocabulary. This is the general form of ctbk's reason #2 above: a vocabulary misaligned with the materialized ladder has to be fixed in the *system*, not with this knob. Now asserted in both directions.
- **The alias constraint, as a test.** A vocabulary entry with no member of its own can't be back-filled with a fixed-level cell from lat/lng: that cell lands as an ancestor of the finer members nested inside it, and the disjointness check rejects it. Pinned with the exact throw message, so the constraint ctbk discovered the hard way is now a regression test rather than a paragraph.

JS 513 passed, `tsc -b --force` clean.

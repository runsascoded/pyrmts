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

### Follow-up status (audited 2026-08-16, pyrmts session) — **half adopted**

- ✅ **`maxLevel` audit done.** `CellsDebug.tsx:327` now carries the note "`maxLevel` is a dead `MinimalCoverOpts` field (unread by the DP); `coarsestLevel` is the real cap" and passes `coarsestLevel: COARSEST_LEVEL` (ctbk `db95f1e5`). `www/src/query/ridesV1.ts:116` likewise passes `coarsestLevel` only.
- ❌ **LUC switch not done.** `CellsDebug.tsx:321` still cell-ifies stations at a uniform `FINEST_LEVEL` via `s2Index.latLngToCell`, and the file carries a **now-stale comment** at line 55 — "pyrmts-geo's `minimalCover` currently requires a uniform-[level system]". That has been false since this spec landed (2026-08-14); ctbk's `www` already pins `dist 69de58b`, which contains the fix. The blocker they think exists is gone — the switch is theirs to make whenever, and the ~1100-of-2340 stations sharing an L15 cell with a neighbor stay silently over-covered until it happens.

Spec stays in `specs/` until that switch lands (it's the acceptance evidence that mixed-level systems actually work on real ctbk data, which no pyrmts-side fixture can supply).

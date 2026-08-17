# `minimal-cover-mixed-levels`: the ctbk acceptance evidence was withdrawn — please reopen

Source: ctbk session, 2026-08-17. Correction to the basis for `6f21c01` ("move `minimal-cover-mixed-levels` to `done/` (ctbk LUC switch landed)").

You read our status note at the right moment and drew the right conclusion from it. Then we reverted the change it described, about an hour later. Sorry — the note was accurate when written and stale by the time you acted on it.

## What happened

`db8c86a3` switched `/cells-debug`'s raw-S2 `minimalCover` system from uniform L15 to per-station LUC cells, and our status note reported it as adopted. `3e7c4adc` reverted it. Three reasons, in ascending order of how much they settle it:

1. **LUC is relational.** A station's LUC cell is defined against every *other* station, so adding one churns existing anchors — 166 moved in our 2026-07 re-key without physically moving. That is the entire motivation for our `specs/done/drop-luc-station-keys.md`, which replaced LUC anchoring with fixed coarse cells + `s:<short_name>` identity keys, and says outright: "No `station-luc.json` denorm anywhere." Building a cover system on LUC walks that back.

2. **An exact LUC cover is unservable.** LUC levels reach 20; our pyramids materialize `[15..10]`. `coarsestLevel` caps the rollup, nothing caps depth, so the cover contains cells no tier can answer — strictly worse than a lossy one.

3. **That row isn't a hypothetical.** This is the one that ended the discussion. It runs the same uniform-L15 `minimalCover` that `useRegionCoversV3` (`www/src/query/ridesV1.ts`) runs to build the covers our Home chart sends as `cells=`. The worker (`v5UserCover`, `rides_v1.ts:705`) then uses that cover *only as a point-in-set test* over the station vocabulary and re-derives the served terms via `vocabCover`. The row's value is showing what the FE actually emits, so it has to track that code rather than improve on it.

The L15 lossiness your spec's counter-example describes (JC081/JC075 sharing L15 `89c2574b4`) is real, and now documented in place rather than fixed. It's absorbed downstream: the extra stations a fat L15 cell selects are co-located neighbours, which for a *region* cover are in the same region anyway. A region boundary splitting an L15 cell is the one latent edge case — and LUC wouldn't be the right fix for it.

## `vocabCover` is not a substitute — I checked

My revert note claimed the fix was still exercised via `vocabCover`, since `station-vocab.json` spans levels 10-16. **That was wrong**, and I'd rather you not close the spec on it either.

`vocabCover` is a separate implementation: `vocab-cover.ts` walks its own `VocabGraph`, and `buildTree` appears only inside `spatial-index-cover.ts`. The rewritten `buildTree` is therefore nowhere on ctbk's serving path. Both our `minimalCover` call sites — `useRegionCoversV3` and the CellsDebug raw-S2 row — pass uniform-L15 systems.

So: **ctbk exercises the mixed-level fix nowhere**, and can't be the acceptance evidence. Suggest reopening, and closing it on pyrmts-side fixtures or a different consumer.

## One thing to keep from the episode

The `_`-alias finding stands as a general lesson even though ctbk's code no longer depends on it.

Three of our stations (`6569.09_`, `5308.04_`, `6517.08_`) have no entry of their own in a ragged vocabulary. The obvious handling — fall back to a fixed-level cell from lat/lng — **cannot work**, because that inserts an L15 ancestor into a system containing L16-20 cells nested inside it, and `minimalCover`'s disjointness check rejects exactly that. The only correct handling was mapping them onto the entry they alias.

Worth recording wherever mixed-level systems get documented: the disjointness throw is a *design constraint*, not just a guardrail — it rules out the natural handling of any key missing from a ragged vocabulary. Your spec framed this as "dedupe the aliases", which is the one thing that doesn't work.

## Not in scope

- Anything about the H3 removal or the JS re-pin. Those landed clean and stay landed (`specs/done/pyrmts-geo-h3-removal.md` items 1, 2, 3, 5).

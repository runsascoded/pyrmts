# Lower `s2Index.bboxToCells` `COVERER_MAX_CELLS` to a CFW-safe cap

## Problem

`pyrmts-geo/src/s2-index.ts:33` hard-codes
`COVERER_MAX_CELLS = 1_000_000`. The comment correctly notes that S2's
`RegionCoverer` uses `maxCells` as both an output cap **and** a
work-bound for the covering search, but the chosen value (1M) is
unsafe in any caller subject to the V8 / CF Worker 128 MB heap cap.

Empirical evidence (from a ctbk session, 2026-06-17):

Synthetic curl to `/api/rides-v3?from=2024-12-15T00:00:00Z&to=…
&bbox=40.749,-73.991,40.751,-73.989&bin_budget=2&cell_budget=2`
(narrow bbox, tight budgets, no `cells=`) consistently 503s. Wrangler
tail shows `"outcome": "exceededMemory"` after ~3.5s CPU / ~4.8s wall:

```
"exceptions":[{"name":"Error","message":"Worker exceeded memory limit."}]
```

Root cause is `pickResolution` calling `bboxToCells(bbox, level)` for
each finest-first candidate level. At S2 L15, even with a small bbox
the `RegionCoverer` allocates aggressively up to `COVERER_MAX_CELLS`
during the level-uniform covering search — exceeds the 128 MB heap.

## Why this hasn't bitten production users

Once the `outputCells` short-circuit shipped
(`specs/done/plan-geo-query-precomputed-cover.md`, commit `347f822a`'s
underlying `6dc28bd`), the ctbk FE stopped calling the bbox path
entirely — it now precomputes a `s2Index.minimalCover(...)` and passes
the result via `cells=`. So no user-facing impact today. But the bbox
path is documented as a fallback, and any ad-hoc API user (curl,
scripts, future callers) trips the OOM.

## Fix

Lower the cap to a value safe for the worst-case CFW caller heap.

Each `RegionCoverer` candidate cell carries an `S2CellID` (8 bytes) +
priority-queue metadata + parent/score references — rough back-of-
envelope: 100-200 bytes per candidate. At 1M cells that's 100-200 MB;
at 10k it's 1-2 MB. 100k cells would be ~10-20 MB — comfortable but
still well past anything any realistic caller needs.

**Recommend `10_000`**. Rationales:

- The actual returned covers are always <1000 cells for any sane bbox
  (NYC at S2 L15 ≈ 25k cells but a covering of NYC at the FE's coarser
  level picks ≈ a few-hundred cells). 10k upper-bounds the work to ~1-2
  MB peak, well within the 128 MB cap.
- Callers already pass a `cellBudget` (typically 1024) for the actual
  cover size. The internal `RegionCoverer` cap should be a small
  multiple of any realistic `cellBudget` to give the covering search
  room to refine, but not orders of magnitude more.
- If a future caller has a legitimate need for >10k cells, expose
  `coverer.maxCells` as an optional `bboxToCells` argument so the
  caller can lift it explicitly per-call. (Recommend deferring until
  there's a real use case.)

## Diff

```diff
-// 1M is well past anything we'd realistically query (NYC bbox at S2 lvl
-// 14 ≈ 25k cells; per-tile global cover at S2 lvl 10 ≈ 1.5M, so we cap
-// just under that to prevent runaway).
-const COVERER_MAX_CELLS = 1_000_000
+// 10k bounds the covering search to ~1-2 MB peak, safe under the V8 /
+// CF Worker 128 MB heap cap. Actual returned covers are <1000 cells
+// for any realistic bbox; the budget exists only to give the
+// `RegionCoverer` candidate-priority-queue room to refine. Callers
+// pass an explicit `cellBudget` (typically 1024) for the actual cover
+// size; the internal cap is a small multiple of that.
+const COVERER_MAX_CELLS = 10_000
```

## Verification

After dist rebuild + ctbk pyrmts pin bump:

1. Reproduce the OOM against the pre-fix prod worker (we already have
   this — see the ctbk session's wrangler-tail log).
2. Deploy ctbk-gbfs-api-dev with the new pyrmts pin.
3. Curl the same OOM-triggering URL against the dev worker:
   `https://ctbk-gbfs-api-dev.ryan-0dc.workers.dev/api/rides-v3?from=2024-12-15T00:00:00Z&to=2024-12-16T00:00:00Z&bbox=40.74,-73.99,40.76,-73.97&bin_budget=24&cell_budget=10`
   Expect: 200 with valid data (rather than 503 / exceededMemory).
4. Also try a wider bbox / smaller cell_budget to confirm we haven't
   underbounded:
   `…&bbox=40.5,-74.2,41.0,-73.7&cell_budget=1024`
   Expect: 200, sensible cell-count in `plan.outputCells` (~few hundred).

## Done criteria

- [ ] Patch lands in pyrmts main + npm-dist mirrors at new SHA
- [ ] ctbk's pyrmts pin bumped to the new dist
- [ ] Dev worker no longer OOMs on bbox-only queries
- [ ] Returns sensible covers across small bbox + wide bbox + various
      `cell_budget` values

## Closing context

Tracks ctbk issue #107. The ctbk session originally proposed fixing
this in the ctbk worker (reject bbox-only requests with a 400), but
that approach (a) doesn't actually fix the upstream bug for any other
pyrmts-geo caller, (b) leaves the bbox API surface dead-but-documented.
The upstream cap-lowering is cleaner and a one-line change.

## Resolution

Shipped the proposed change verbatim: `COVERER_MAX_CELLS` lowered from
`1_000_000` to `10_000` in `pyrmts-geo/src/s2-index.ts:33`, with an
updated comment explaining the 1-2 MB peak heap vs. the 128 MB CFW
heap limit. All 250 pre-existing tests still pass (no real query ever
needed >10k candidates).

Added one regression test in `s2-index.test.ts` covering the spec's
OOM-triggering bbox: `bboxToCells({minLat: 40.749, maxLat: 40.751,
minLng: -73.991, maxLng: -73.989}, 15)` (200m × 220m bbox over Times
Square at S2 L15) → asserts cover is non-empty, all cells at L15,
contains the bbox-center cell, count < 100.

Not exposing `maxCells` as a per-call argument (spec's follow-up
suggestion); deferred until a real use case appears for >10k candidates.

ctbk's pyrmts pin will need a bump to pick this up; the FE doesn't
trigger the bbox path anymore (it uses the `outputCells` short-circuit)
but the cap-lowering protects ad-hoc / curl / future callers.

# planner: multi-tier bin packing (ragged decomposition)

## Goal

Generalize `planQuery` so that each user-requested output bin can be
composed of disjoint patches from **multiple** finer tiers, picking the
largest tier that fits at each offset. Unlocks closely-spaced-rung
pyramids where the planner serves arbitrary-phase, arbitrary-width
queries efficiently — without over-densifying the tier list.

## Motivating example

Pyramid has tiers `{1m, 2m, 3m, 5m, 10m, 15m, 30m, 1h, ...}`. User
queries for `5m` bins starting at `t0` (arbitrary phase, doesn't align
to epoch-floored 5m). Today's planner can't compose this — there's no
"requested bin width" input, and `pickTier` only returns ONE tier whose
bins fit the budget. If the user is forced to receive 5m bins by
aggregating from 1m, they pay 5× the read cost they'd pay if the planner
were smart enough to pack `{1 × 3m + 2 × 1m}` per output bin.

The capability is generic: any output bin can decompose into a maximum
packing of finer-tier bins.

## Current state

`planQuery` (`planner.ts:79`):
- Input: `range`, `binBudget`, optional `watermarks`, optional `smoothing`.
- Picks finest tier whose `binsInRange ≤ binBudget` (`pickTier`,
  `planner.ts:159-162`).
- Emits one segment per tier covering time-range gaps between watermarks
  (`planner.ts:117-133`). Each segment fetches whole bins from a single
  tier.

Limitations:
- No "user wants bin width B" input — can't express the divisor question.
- Single-tier-per-segment — even with finer tiers available, can't pack
  multiple tiers into one output bin.
- No phase-awareness — picked tier's bins are epoch-floored regardless
  of the user's range start phase.

## API change

### Input

Add `outputBin: Duration` (optional). When set:

- Planner verifies at least one tier's bin evenly divides `outputBin`
  (else throws — no valid decomposition).
- Output tier = `outputBin` itself; segments compose its bins from finer
  tiers via packing.

When `outputBin` is unset, planner falls back to the existing
binBudget-only behavior (picks a single tier; emits whole bins).

```typescript
export interface PlanQueryInput {
  range: { from: Date; to: Date }
  binBudget: number
  outputBin?: Duration         // NEW — if set, planner does ragged decomposition
  watermarks?: Record<string, Date>
  earliestWatermarks?: Record<string, Date>
  filter?: Record<string, string | number>
  smoothing?: SmoothingSpec
  smoothMode?: SmoothMode
}
```

### Output

Two changes to `QueryPlan`:

1. `outputTier` becomes optional (null when `outputBin` was set and the
   user's bin isn't a stored tier).
2. New segment kind: tier-interval segments. Each segment now carries
   `{ shardTier, from, to, reaggregate }`, but `from`/`to` are
   tier-bin-aligned offsets within the output range — not full output
   bins.

```typescript
export interface PlanSegment {
  from: Date            // tier-bin-aligned start
  to: Date              // tier-bin-aligned end
  shardTier: Tier
  keys: string[]
  reaggregate: boolean  // true if this tier's bin != outputBin
  // NEW: which output bin(s) this segment contributes to.
  // Stitcher sums tier rows in this range and assigns to the output
  // bin(s) covering [from, to). Usually 1 output bin; >1 if tier
  // straddles multiple output bins (which the planner avoids by
  // construction).
  outputBinStart?: Date
}
```

## Algorithm

### Per-output-bin decomposition

For each output bin `B = [b_start, b_start + outputBin)`:

1. Initialize `cursor = b_start`.
2. While `cursor < b_start + outputBin`:
   a. Among tiers whose bin evenly divides `(b_start + outputBin - cursor)`
      AND whose bin-aligned start ≤ `cursor`, pick the COARSEST.
   b. Emit `{shardTier: T, from: cursor, to: cursor + T.bin}`, charge
      to output bin `B`.
   c. `cursor += T.bin`.
3. If no tier fits at some `cursor`, no valid decomposition — throw
   (caller picked an `outputBin` no tier divides at this phase).

The "evenly divides remaining" rule ensures the chosen tier doesn't
overshoot `B`. The "bin-aligned start ≤ cursor" rule ensures we don't
fetch a tier bin that starts before our needed offset (we'd waste data).

### Segment coalescing

Adjacent segments at the same tier coalesce into one read range. After
the per-bin decomposition emits its segments, run a sweep:

```
for each tier T:
  contiguous_segments = group(segments where shardTier == T, by adjacency)
  merge each contiguous group into one segment with {from: first.from, to: last.to}
```

This is just an optimization — fewer R2 GETs at the cost of slightly
more decoded rows.

### Watermark interaction

The existing watermark-walk in `planQuery` produces segments that say
"for this time range, use tier T." With ragged decomposition, this
becomes a constraint: a segment from the watermark-walk gates which
tiers are *available* in that time range. Output-bin decomposition then
runs within those gated tier sets.

In practice:
- Current period (past finest watermark): only finest tiers available.
- Past complete periods: all tiers available; coarsest-first packing
  picks coarsest.

### Bin-phase-misalignment edge case

If the user's `outputBin` start phase doesn't align with ANY tier at
all (e.g., user wants `7d` bins starting at a Tuesday, but all tiers
are epoch-aligned), the cursor might not advance at some step → algo
throws. Caller should fall back: either retry with finer-grained
`outputBin` or pick a tier-aligned start.

For ctbk's use, the FE doesn't pick weird phases — it queries `[now -
7d, now]` and accepts whatever bin width the planner serves. So this
edge case is unlikely to surface for our queries.

## Stitcher requirements

Existing stitcher (not in `planner.ts`; check the consumer package)
needs:

1. **Group rows by output bin**, not by tier-bin. Each tier-interval
   segment's rows are summed (histogram-merge) into the output bin
   `B` it serves.
2. **Re-aggregation**: when segment.bin < outputBin, the segment's rows
   are bin-aligned to its tier, not to outputBin. Stitcher must monoid-
   coarsen them to outputBin before assigning to B. (Existing
   `reaggregate: true` already signals this; the only change is that
   re-aggregation no longer always means "this tier's whole range needs
   coarsening" — it can be a sub-tier-interval that contributes to
   exactly one output bin.)

## Storage / planner state

No on-storage changes. Pyramids stay the same. Only `planQuery`'s
algorithm changes; the per-tier shard format is unchanged.

## Migration

1. Add `outputBin: Duration` to `PlanQueryInput`. When unset, current
   behavior preserved.
2. Implement decomposition algorithm in `planner.ts`. Existing tests
   pass (they don't pass `outputBin`).
3. Add new tests covering:
   - `outputBin` set, tiers cleanly divide (`5m`, pyramid `{1m, 5m}`)
   - `outputBin` set, only finer tiers (`5m`, pyramid `{1m, 2m}` — must
     pack `2m, 2m, 1m`)
   - `outputBin` set, mixed packing (`5m`, pyramid `{1m, 3m}` — pack
     `3m, 1m, 1m`)
   - `outputBin` set, watermark interaction (current period in finer
     tier, past in coarser)
   - `outputBin` set, no valid decomposition (must throw)
4. Update stitcher in consumer packages to handle the new segment shape.
   (Existing `reaggregate: true` flag continues to work; the new
   `outputBinStart` field is an addition.)
5. Mark deprecated: callers using only `binBudget` get a no-op behavior
   change (still works). Callers that want phase-aligned arbitrary bins
   should pass `outputBin`.

## Open questions

1. **Phase tolerance**: should the planner allow output bins that
   "almost-align" by snapping to the nearest tier-aligned start? My
   take: no, that's caller responsibility. Throw on misalignment.
2. **Segment ordering in output**: time-ordered? Tier-ordered? Both
   work; pick time-ordered for natural row-stream consumption.
3. **Filter pushdown**: existing `filter` field is per-segment via
   `shardKeys`. With multi-tier segments emitting more keys, filter
   pushdown is unchanged but emits keys for each tier's shard contribute.
4. **Output `outputTier` field**: keep nullable, or always set to the
   coarsest tier in the packing? Nullable is cleaner; callers can derive
   if they need it.

## Effort

Real PR: maybe 1–2 days of focused work on the planner, plus another
1–2 days on the stitcher in pyrmts-geo and any test updates. Total ~1
week. Worth doing now since the downstream cascade work (ctbk
pyramid-cascade) is built on top.

## Resolution

Shipped in a single planner-only commit; stitcher needed no changes.
~3 hours of focused work, not the 1 week the spec estimated, because
(a) the stitcher already handles ragged-decomp output without
modification, and (b) the DP is small.

### Deviations from the spec

1. **Input rename: `outputBin` → `targetBin`.** `QueryPlan.outputBin`
   already exists (the resolved output width); reusing the same name
   for the input field would have collided. The output field's
   semantics are unchanged.

2. **DP, not greedy.** Spec proposed coarsest-first greedy ("among
   tiers that evenly divide the remaining, pick the COARSEST"). Greedy
   is suboptimal in cases like tiers={1m, 4m, 6m}, target=9m at
   cursor=0: greedy picks 6m → 4 atoms total; DP picks 4m+4m+1m → 3
   atoms. Implemented as a memoized DP (shortest-path on tier-bin-
   aligned cursor positions). Memo key is absolute-ms cursor; per
   bin: O(B/g × n) where B = bin width, g = gcd(tier widths). For
   typical 5min targets at minute granularity this is ~25 ops.
   Per-(eligible-set, phase mod LCM) DP results cached across output
   bins.

3. **Strict-equality alignment, not the spec's "≤ cursor".** Spec
   line 104 says pick a tier whose "bin-aligned start ≤ cursor", but
   that lets the algo pick a tier bin that starts before the cursor —
   which fetches the wrong absolute range. Corrected to strict equality:
   `cursor % tier.ms === 0`. Naturally handles the spec's `/4m`
   straddling case (`/4m` is skipped at non-aligned cursors and the DP
   falls back to finer tiers).

4. **No `outputBinStart` field on `PlanSegment`.** Spec proposed
   threading "which output bin does this segment serve" through each
   `PlanSegment`. Unnecessary — the existing stitcher derives the
   output bin from each row's `binCol` value via `floorToSpan(ts,
   outputBin)`. Sub-tier-bin segments (and coalesced segments spanning
   multiple output bins) work without changes because per-row binning
   is the authority. Verified: 240 pre-existing tests pass; 10 new
   tests assert ragged-decomp behavior including a case where one
   coalesced `/3min` segment spans two `/5min` output bins.

5. **`outputTier` made optional (not nullable).** Under the project's
   `exactOptionalPropertyTypes: true`, optional + omitted distinguishes
   "no stored tier matches `targetBin`" from "field exists with value
   undefined". Same change applied to `PlanMeta.outputTier` in the wire
   format. Consumers (`pyrmts-cfw/serve.ts`, `pyrmts-geo/{serve,planner,query}.ts`,
   `pyrmts-geo/GeoQueryPlan`) updated to omit the field via the spread
   idiom when the planner doesn't set it.

6. **`smoothSourceTier` carries `<ragged:<targetBin>>` when no
   outputTier.** Field is purely informational (stitcher doesn't read
   it; it's exposed on the wire for client-side display); the marker
   distinguishes ragged mode from a real tier name without forcing the
   field to be optional too.

7. **Calendar-variable `targetBin` (`mo`/`y`) throws.** Spec didn't
   explicitly handle this. Divisibility against fixed-width tiers is
   undefined for variable-width months/years, and ctbk's use case
   doesn't need it. Throw at planQuery entry with a clear error.

8. **`binBudget` ignored when `targetBin` is set.** Caller asserts the
   bin width; planner doesn't second-guess. Caller can verify
   `binsInRange(range, targetBin) ≤ binBudget` upfront if needed.

### Algorithm (as implemented)

`planRagged(pyramid, input, targetBin)`:

1. **Validate**: targetBin is fixed-width; eligible tiers (fixed-width,
   bin ≤ targetBin) is non-empty; `gcd(eligible tier widths)` divides
   targetBinMs (necessary condition for decomposability — by Bezout).
2. **Resolve**: outputTier = eligible tier whose bin matches targetBin
   exactly (if any).
3. **Smoothing**: snap against targetBin, extend planning window.
4. **Watermarks**: compute effective + earliest per tier.
5. **Per output bin** in `[floor(plannedFrom, targetBin),
   ceil(plannedTo, targetBin))`:
   a. Filter eligible tiers to those whose effective watermark covers
      `bin.end` AND whose earliest watermark ≤ `bin.start`.
   b. Phase = `binStart % LCM(eligible tier widths × targetBin)`.
      Cache DP results keyed on `(eligible-set-name, phase)`.
   c. DP: shortest path on aligned cursor positions from `binStart` to
      `binStart + targetBin`. Throws if no path (alignment dead-end).
6. **Coalesce** adjacent same-tier atoms (linear sweep).
7. **Emit `PlanSegment`s** with `reaggregate = (tier.bin !== targetBin)`.

### Test coverage

10 new tests in `planner.test.ts` under
`describe('planQuery: targetBin (ragged decomposition)')`:

1. `targetBin` matches a stored tier exactly → outputTier set; single
   coalesced segment.
2. `targetBin` not in tiers → outputTier omitted; per-bin DP across 6
   `/5min` output bins (3 distinct phases) using `{1m, 3m}`.
3. Coalescing across output-bin boundaries (`/3min` segment spans two
   `/5min` bins).
4. DP beats greedy (`{1m, 4m, 6m}` target `/9min`: 4+4+1 = 3 atoms,
   not greedy's 6+1+1+1 = 4).
5. Watermark restricts coarser tiers to older bins; younger bins fall
   back to finer tiers.
6. Throws when no eligible tier (all bins > targetBin).
7. Throws when `gcd(tier widths)` doesn't divide targetBin (`{2m,4m}`
   target `/5min`).
8. Throws for calendar-variable targetBin (`/1mo`).
9. Throws on per-bin alignment dead-end (`{2m,3m}` target `/5min`:
   gcd=1 divides 5, but strict-equality DP can't reach 5 from 0).
10. Smoothing snaps against targetBin in ragged mode; smoothSourceTier
    carries the `<ragged:5min>` placeholder.

### Not landed in this PR

- **Stitcher updates**: not needed. The existing stitcher (`stitch.ts`)
  groups by output bin via `floorToSpan(row.ts, outputBin)`, which is
  the correct grouping regardless of which tier each row comes from.
  Sub-tier-bin segment ranges and post-coalescing segments spanning
  multiple output bins both work.
- **`pyrmts-geo`'s `planGeoQuery` plumbing for `targetBin`**: not
  exposed in pyrmts-geo. ctbk's geo query path can pass `targetBin`
  through `planQuery` directly when needed; routing it through
  `planGeoQuery` is a one-line addition in a follow-up if desired.
- **CFW perf measurement**: spec's motivating motivation (avoiding
  redundant 5× read cost) isn't quantified here. Behavioral correctness
  is the bar this PR clears.

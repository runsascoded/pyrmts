# Mixed-tier tail coverage (fall through to finer partials past output tier's watermark)

> **Superseded by [`unified-shard-ladder.md`](./unified-shard-ladder.md).**
> The finer-tier fall-through behavior this spec described is now the
> default behavior of the cursor-aware-largest-first planner walk; no
> separate "tail-coverage" concept needed.

## Resolution: already implemented — spec misdiagnosed

After authoring this spec, traced `planner.ts` and found the feature
already landed by `partial-shards.md` (the `for (let i = outputIdx;
i >= 0; i--)` walk in `planQuery`, with `reaggregate: i !== outputIdx`
on emitted segments). `per-cadence-earliest.md` then made the walk
robust to forward-only partial coverage.

Existing planner tests that cover the exact "tail-from-finer-partials"
shape:

- `planner.test.ts:1060` — `"fall-through to finer tier picks up after coarser tier's max effective"`
- `planner.test.ts:1097` — `"reaggregate=true for partial segments emitted by a finer tier"`
- `planner.test.ts:1147` — `"fully gates one entry; sibling entries in same tier emit normally; finer fall-through covers the gated tail"` (combined with per-cadence-earliest gating, matching avail-v3's `/1m` shape)

The observed single-segment plan from ctbk's avail_geo.ts is consistent
with ctbk not yet wiring up partial-cadence watermarks (no
`'1m@5min' / '1m@1h' / '1m@12h'` keys in the `watermarks` arg). Without
those keys, `effectiveShardWatermarks` skips them from the grid (per
its missing-partial-vs-canonical default semantics), so `/1m` has only
its canonical entry effective at the day before cron deploy, and the
walk terminates with no tail segments emitted.

Debug note + checklist of ctbk-side wiring to verify:
`~/c/hccs/ctbk/specs/avail-v3-tail-coverage-debug.md`.

No pyrmts changes landed from this spec; kept in `done/` as the paper
trail. The Design / Implementation sketch sections below are preserved
for reference but should be read as "what the planner already does",
not "what's to be implemented."

## Goal

When a query's `to` extends past the picked output tier's authoritative
watermark, let the planner emit additional segments sourced from
**finer** tiers' partial sub-shards (re-aggregated to the output bin)
to cover the tail — instead of returning a truncated result. The
output tier still determines the bin width of every returned row;
finer tiers contribute via the existing `reaggregate: true` segment
mechanism.

## Motivation

ctbk avail-v3 has a two-writer model:

- `e`-side `pyramid-cascade` (periodic) writes canonical shards for
  `/1m..7d`. Latest watermark advances per run.
- CFW `gbfs/cascade` (continuous, every minute) writes `/1m` **partial**
  sub-shards (5min, 10min, 30min, 1h cadences) for fresh data, then
  promotes them to `/1m` canonical at midnight UTC.

For a "Latest · 7d" StationDetail query at, e.g., 02:35 UTC today, the
range is `[now - 7d, now]`. The planner picks `/15m` (fits the bin
budget); `/15m`'s canonical watermark is 2026-06-28T00:00Z (end of
yesterday's `/15m@15d` shard window). The segment for [from, 06-28T00:00]
returns fine. But the **tail** `[06-28T00:00, now]` is past `/15m`'s
watermark, and the planner returns 0 segments for it — even though
`/1m@p5min` etc. partials have continuous data for that window via
`earliestPerCadence` (shipped in `done/per-cadence-earliest.md`).

The api worker observes:

```json
{
  "outputTier": "15m",
  "authoritativeEnd": "2026-06-28T00:00:00.000Z",
  "segments": [
    {"tier": "15m", "from": "...", "to": "2026-06-28T00:00Z",
     "reaggregate": false, "keys": ["avail-v3/15m/2026-06-21.parquet"]}
  ]
}
```

`segments` stops at `authoritativeEnd`; FE chart's right edge is empty
for ~12 hours into today. ctbk task #111 (flip `StationDetail`
`availSrc` to `v3`) is blocked on this — flipping today regresses
prod (the legacy `/api/totals` path doesn't have this issue because
it's a single-tier, freshness-first endpoint that doesn't try to
optimize bin counts).

The api-side workaround would be: detect `to > maxOutputTierWatermark`
in ctbk, issue a SECOND `planGeoQuery` for the tail with binBudget
forcing /1m, concat segments + records. ~30 LOC, lives in ctbk only.
Punted in favor of fixing it here, because the same bug will surface
for every pyrmts consumer that combines a periodic canonical writer
with realtime partial writers (which is the whole point of the
partials feature).

## Current planner behavior

Per `ctbk/gbfs/api/src/avail_geo.ts` and the comment around line 413,
the segment loop "walks `<picked tier>` only ... no fall-through to
coarser indices". I.e., once `pickTier` returns tier `T`, segments
are sourced exclusively from `T`'s (tier, cadence) entries.

(I haven't traced `planner.ts` for this turn — implementer should
confirm the current walk shape before designing the change.)

When `T`'s coverage exhausts before query.to, the planner emits no
further segments. `authoritativeEnd` reflects the picked tier's
right edge; the FE has no way to know that finer tiers could have
filled the tail.

## Design

### Concept

After the picked tier `T`'s segment loop terminates at its
authoritative right edge `W_T`, if `cursor < to` remaining, walk
**finer tiers** in the ladder (`T_finer` with `bin < T.bin`) and
emit additional segments sourced from their `(tier, cadence)`
entries with `earliestPerCadence` coverage spanning `[cursor, to]`.

Each such segment carries `reaggregate: true` because input bin
(`T_finer.bin`) ≠ output bin (`T.bin`). The existing reaggregate
mechanism (used elsewhere in pyrmts for finer→coarser bin packing)
handles the actual row re-binning at execution time.

The walk stops when:
- `cursor >= to` (full coverage), OR
- No finer tier has a `(tier, cadence)` entry whose effective
  coverage extends past `cursor` (genuine gap; leave it).

### Direction: finer only

Coarser tiers are NOT a fall-through target for the tail because
their watermarks are bounded by the same canonical-write cadence
(if the canonical writer hasn't reached today, neither has any
coarser tier). Only the partial-sub-shard mechanism (finer tiers
with continuous /1m@p<C> writes) extends past the canonical
watermark.

This is asymmetric with the historical case (where coarser tiers
DO have data the picked tier lacks, because of finer tiers'
limited backfill). The historical case is already handled by
existing per-tier `earliestWatermarks` propagation +
`pickTier`-budget-cap workarounds. This spec is purely about the
**forward** tail.

### Picking finer tier(s) for the tail

For each finer tier `T_finer` in ladder order (finest first or
coarsest-of-finer first — see "Open: order" below), pick the
cadence `C` whose entry has the latest `earliestPerCadence[T_finer@C]`
gate satisfying `earliestEntry <= cursor`. Prefer **longer-cadence**
partials (fewer files, larger per-file row counts) when multiple
cadences cover the same `[cursor, segEnd]` range.

E.g. tail `[06-28T00:00, 06-29T02:35]` ≈ 26.5 hours:
- `/1m@p1h` partials: 27 files covering each hour boundary
- `/1m@p12h` partials: 3 files (two 12h boundaries + partial-of-canonical)
- `/1m@p5min` partials: 318 files

Prefer `/1m@p12h` (3 files) when its watermark covers the range.

### Segment emit

For each chosen `(T_finer, C, [segStart, segEnd])`:

```ts
segments.push({
  tier: T_finer.name,
  cadence: C,                   // e.g. '12h', or '' for canonical
  from: segStart,
  to: segEnd,
  reaggregate: true,            // T_finer.bin != T.bin (output)
  keys: [/* partial-shard R2 keys */],
})
```

Set `cursor = segEnd`. Loop until `cursor >= to` or no candidates.

### Output schema

No change to row schema — every row still output at `T.bin`. The
reaggregate execution path collapses fine bins into output bins as
today.

`authoritativeEnd` should advance to reflect the final emitted
segment's `to`. A new flag could indicate the tail was synthesized
from partials (`tailFromPartials: true`) for consumers who care to
visually mark "fresh-but-partial" data — out of scope for v1.

### Interaction with `pickTier`

`pickTier` currently picks `T` based on `binBudget` over the full
`[from, to]` range. With the tail-coverage extension, `T` may be a
coarser tier than `T_finer` (the tail source). That's fine: `T`
still drives the output bin width; the tail's finer-tier source
gets re-aggregated to `T.bin` at execution.

`pickTier` itself doesn't change. The segment loop's POST-T behavior
changes.

### What if no finer tier covers the tail?

Leave the gap. `authoritativeEnd = W_T` as today. FE renders empty
on the tail — same as current behavior. The fall-through is purely
additive: it can only ADD coverage, never reduce.

## Out of scope

- **Coarser fall-through for the tail** — coarser tiers can't have
  data that finer tiers' partials don't, by construction.
- **Multi-tier output bin** (mixed bin widths in one response) —
  output bin stays `T.bin`. All tail rows are reaggregated to it.
- **Per-segment authoritativeness flag** — `tailFromPartials` left as
  a follow-up. The single `authoritativeEnd` field doesn't
  distinguish "fully sealed canonical" from "fresh partial" today.
- **Backfill resilience** — if `T_finer`'s partials have GAPS within
  `[cursor, to]` (some files missing), the segment emits with the
  partial gap still empty. Out of scope; same behavior as canonical
  segment with shard-gap.

## Use cases

### ctbk avail-v3 StationDetail Latest 7d

Today (2026-06-29 ~02:35 UTC):
- `pickTier = /15m`, `binBudget = 744` (7d × 24h × 4 = 672 bins),
  output bin 15min
- `/15m` canonical watermark = 2026-06-28T00:00Z
- Picked tier emits segment for [06-22T02:35, 06-28T00:00]
- **NEW**: tail walk:
  - `/1m@p12h` earliestPerCadence covers from ~06-28T00:00 (per D1
    `MIN(period_start)`)
  - Emit segment `{tier: '1m', cadence: '12h', from: 06-28T00:00,
    to: 06-28T12:00, reaggregate: true, keys: ['avail-v3/1m/p12h/2026-06-28T00.parquet']}`
  - Then `/1m@p12h` for [06-28T12:00, 06-29T00:00] (next 12h
    partial)
  - Then `/1m@p1h` (or @p30min, @p5min) for [06-29T00:00, 06-29T02:35]
    — the freshest 2.5h covered by smaller-cadence partials
- Records: re-aggregated to 15-min bins; FE chart fills to the right
  edge

Unblocks ctbk #111 (StationDetail v3 flip).

### Other consumers

Any pyrmts consumer with a periodic canonical writer + realtime
partials gets fresh-tail coverage "for free" — no consumer-side
change beyond passing `earliestPerCadence` (which it already does
post-#135).

## Implementation sketch

1. After the existing segment loop for picked tier `T` exits with
   `cursor < to`:
   ```ts
   while (cursor.getTime() < to.getTime()) {
     const tierEntry = findFinerTailSource(T, cursor, to, earliestPerCadence, shardWatermarks)
     if (!tierEntry) break
     const { tier: T_finer, cadence, segEnd, keys } = tierEntry
     segments.push({ tier: T_finer.name, cadence, from: cursor, to: segEnd,
                     reaggregate: true, keys })
     cursor = segEnd
   }
   ```
2. `findFinerTailSource` iterates `pyramid.tiers` finer-than-T,
   prefers longest cadence with `earliestPerCadence` ≤ cursor, returns
   the entry + computed segEnd (limited by partial's right edge or
   `to`).
3. Tests: golden plans covering (a) full canonical coverage (no tail
   work), (b) partial tail from one cadence, (c) tail spanning
   multiple cadences/partials, (d) genuine gap (finer tier also has
   no coverage).

## Open

- **Walk order** — finest-first (more partials, smallest read each)
  vs coarsest-of-finer-first (fewer partials, bigger read each).
  Recommend **coarsest-finer-first** for read efficiency, accepting
  marginally staler data within each partial's bin window. Revisit
  if profiling shows it matters.
- **Multiple finer tiers in a pyramid** — the avail-v3 ladder has
  only `/1m` finer than `/2m..7d`, but other pyramids may have
  many finer levels. The walk should still work, but the algorithm
  benefits from heuristic narrowing (e.g. "pick the finest tier
  with canonical writer running, otherwise the next finer with
  partial coverage"). Premature; spec the simple walk first.

## Schema additions

None. Reuses existing `PlanQueryInput.earliestPerCadence`
(per-cadence-earliest.md) and existing `Segment.reaggregate` flag.

## Risks

- **Re-aggregation cost** at execution: re-binning /1m → /15m for a
  ~26h tail = ~1560 input rows × N cells, in-memory groupby. Bounded.
- **Cache invalidation**: tail segments change every ~5min as new
  partials land. Acceptable: the FE was already polling /api/avail-v3
  every N seconds; the planner's `authoritativeEnd` advancing means
  the cached response is naturally short-TTL'd.

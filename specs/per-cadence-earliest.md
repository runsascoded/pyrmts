# Per-(tier, cadence) earliest watermarks (no cross-tier propagation)

## Goal

Let `planQuery` correctly model the case where a `(tier, cadence)`
**partial sub-shard** has data only from some `earliest_period_start`
forward — without that constraint propagating up the tier ladder to
poison coarser tiers that have full back-of-time coverage via their
canonical shards.

## Motivation

`partial-shards.md` (shipped) added forward-rolling sub-shards: each
tick of the consumer's cron writes the latest partial period and
advances a per-(tier, cadence) watermark in the `ShardIndex`. The
planner's existing `(tier, cadence)` watermark grid treats each
watermark as "I've sealed every period up to here" — which assumes
back-to-epoch coverage. That's true for canonical shards (always
backfilled) and for partials whose consumer **also** backfilled
from epoch.

It breaks for consumers whose partials roll forward from a deploy
date and don't backfill. ctbk avail-v3's `/1m` tier is the immediate
driver:

- /1m has no canonical shards (pyramid-cascade's reduce step on `e`
  doesn't write the /1m level — it consumes a /1m ingester input but
  the finest output is /2m).
- /1m partials are written by ctbk's cascading CFW (`gbfs/cascade`,
  task #130). The CFW started writing on 2026-06-27; periods before
  that don't exist on R2.
- pyrmts trusts the /1m@5min watermark as "covers everything up to
  16:00 UTC 6/27". A query for 6/15 hits the planner's segment loop,
  /1m@5min's effective covers the segment, planner emits
  `avail-v3/1m/p5min/2026-06-15T10-00.parquet` → 404.

`earliestWatermarks: Record<tier, Date>` exists (per-tier) and helps
when an entire tier has limited coverage, but its propagation rule
("coarser tiers can't start before their finer source") clamps every
coarser tier in the ladder. For avail-v3, coarser tiers have MORE
history (via `e`'s canonical builds) than /1m. Setting
`earliestWatermarks: { '1m': cascadeStartDate }` poisons /2m..7d into
also-can't-start-before-cascadeStart, breaking historical queries
entirely. Symmetric-but-opposite of what's needed.

ctbk task: #130 (cascading sub-shards); see
`ctbk/specs/avail-v3-cascade-cfw.md` and the rollback at ctbk commit
`287bed61` for the production failure mode.

## Current state

`planner.ts#effectiveEarliestWatermarks` walks tiers finest → coarsest:

```ts
function effectiveEarliestWatermarks(
  tiers: Tier[],
  declared: Record<string, Date>,
): Record<string, Date | undefined> {
  const out: Record<string, Date | undefined> = {}
  let finerBound: Date | undefined = undefined
  for (const tier of tiers) {
    const decl = declared[tier.name]
    let eff: Date | undefined
    if (decl && finerBound) {
      // Take the LATER of (declared, finerBound) — coarser can't start
      // before its finer source did.
      eff = decl.getTime() > finerBound.getTime() ? decl : finerBound
    } else {
      eff = decl ?? finerBound
    }
    out[tier.name] = eff
    finerBound = eff
  }
  return out
}
```

`PlanQueryInput.earliestWatermarks?: Record<string, Date>` is the input
shape — keyed by tier name only.

The segment-walk loop uses `earlyT = earliest[tier.name]` as a per-tier
floor:

```ts
const tierFloor = earlyT && earlyT.getTime() > cursor.getTime() ? earlyT : cursor
const segStart = tierFloor
```

So a tier with `earlyT = 2026-06-27` can't emit segments whose end is
before 2026-06-27.

## Design

### Concept

Extend `earliestWatermarks` to accept **per-(tier, cadence)** entries
in addition to per-tier entries. Per-(tier, cadence) entries gate
individual entries of the `effectiveShardWatermarks` grid; per-tier
entries continue to behave as today (gate the whole tier + propagate
up-ladder).

The propagation rule does NOT apply to per-cadence entries — they
represent "this specific partial has only-from-X coverage", which is
information about THAT partial, not about the underlying data shape
the tier could synthesize.

### Input shape

```ts
export interface PlanQueryInput {
  // ... existing fields
  // tier_name → earliest available bin instant. Same semantics as today:
  // gates the whole tier AND propagates up-ladder (coarser can't start
  // before finer's source).
  earliestWatermarks?: Record<string, Date>
  // (tier, cadence) → earliest available bin instant. Per-entry gating
  // (not per-tier). Does NOT propagate up-ladder. Use for partial
  // sub-shards that only have forward coverage from a deploy / backfill
  // start; tier-level shape (its other cadences, its canonical) is
  // unaffected.
  //
  // Key encoding matches `ShardIndex.getWatermarks`:
  //   `${tier}@${cadence}` — partial sub-shard
  //   `${tier}` — canonical (NOTE: if both `earliestWatermarks[tier]` and
  //   `earliestPerCadence[tier]` exist, the per-cadence one wins for the
  //   canonical entry; the per-tier one continues to apply to OTHER
  //   tiers via propagation).
  earliestPerCadence?: Record<string, Date>
}
```

Why a separate `earliestPerCadence` rather than expanding
`earliestWatermarks` to be polymorphic over key shape? Two reasons:
1. Backwards compatibility — every existing consumer passes
   tier-keyed `earliestWatermarks` with propagation semantics. A
   single field with mixed key shape would silently change
   propagation behavior at the call site.
2. Type narrowness — the encoder/decoder for the `@cadence` key
   form lives in `shard-index.ts` (`encodeWatermarkKey`).
   A separate field documents the calling convention without
   pretending it's the same shape.

### Effective grid construction

`effectiveShardWatermarks` (existing) builds the 2D grid by:
1. Walking tiers finest → coarsest.
2. For each tier, walking cadences finest → coarsest within the tier
   (the canonical entry being a "cadence" too, treated as coarsest).
3. Within-tier `min` propagation: `effective[t, s] = min(declared[t, s],
   effective[t, next-finer-s])`.
4. Cross-tier `min` bound: `effective[coarser, *] = min(its current
   eff, max-eff-of-finer-tier)`.

The new earliest gate applies BEFORE the within-tier and cross-tier
propagation, as a per-entry filter:

```ts
const earliestPerCadence = input.earliestPerCadence ?? {}
for (const tier of pyramid.tiers) {
  for (const cadence of [null, ...cadencesForTier]) {  // null = canonical
    const key = encodeWatermarkKey(tier.name, cadence)
    const earliestEntry = earliestPerCadence[key]
    // If this entry has an earliest gate, store it on the entry so the
    // segment loop can use it as a per-entry floor (analogous to the
    // existing per-tier `earlyT`, but scoped to this single entry).
    entries.push({ cadence, effective: ..., earliestEntry })
  }
}
```

The within-tier and cross-tier `min` propagation rules apply to the
`effective` field only — `earliestEntry` rides alongside but doesn't
propagate. A coarser cadence's `effective` may still be clamped by a
finer cadence's `effective`, but its `earliestEntry` is whatever the
caller declared for that coarser cadence (or undefined).

### Segment loop change

The loop currently computes `tierFloor = max(cursor, earlyT)` where
`earlyT = earliest[tier.name]`. Extend to also consider the entry's
own `earliestEntry`:

```ts
const earlyT = earliest[tier.name]            // existing per-tier
const earlyE = entry.earliestEntry            // new per-(tier, cadence)
const floors = [cursor, earlyT, earlyE].filter(Boolean)
const tierFloor = floors.reduce((a, b) => a.getTime() > b.getTime() ? a : b)
const segStart = tierFloor
if (segEnd.getTime() > segStart.getTime()) {
  segments.push({ ... })
}
```

The segment for this `(tier, cadence)` entry can only cover periods
≥ the latest of cursor / per-tier / per-cadence floors.

### Effective grid: no propagation for per-cadence

`effectiveEarliestWatermarks` (per-tier) stays unchanged — same
propagation rules, same back-compat.

A new helper `effectiveEarliestPerCadence` is a pure projection of the
input map keyed by `${tier}@${cadence}` (or `${tier}` for canonical) —
NO propagation, NO computation. Caller passes whatever they want
gated, planner trusts it per-entry.

```ts
function effectiveEarliestPerCadence(
  declared: Record<string, Date>,
): Record<string, Date> {
  return { ...declared }  // pass-through for clarity + future hook
}
```

## Use cases

### ctbk avail-v3 (the immediate driver)

The cascading CFW records each /1m partial write in D1's `pyramid_shards`
table (via pyrmts-cfw's `D1ShardIndex.recordShard`). The api worker
queries:

```sql
SELECT tier, cadence, MIN(period_start) AS earliest
FROM pyramid_shards
WHERE pyramid = 'avail' AND tier = '1m'
GROUP BY tier, cadence
```

Returns one row per `(tier, cadence)` cell with data. Builds:

```ts
const earliestPerCadence = {
  '1m@5min':  new Date(/* ... */),  // when cascade started writing 5min
  '1m@10min': new Date(/* ... */),  // when cascade started writing 10min
  '1m@30min': new Date(/* ... */),  // etc.
  '1m@1h':    new Date(/* ... */),
  '1m@3h':    new Date(/* ... */),
  '1m@12h':   new Date(/* ... */),
  // '1m' (canonical) not yet — promotion runs at midnight UTC
}
```

Pass to `planGeoQuery → planQuery`. Query for 6/15 with /1m walked:
- /1m@5min: earliestEntry = 6/27 16:00. segStart = max(cursor=6/15 10:00,
  earliestEntry=6/27 16:00) = 6/27 16:00. segEnd = ... < segStart →
  no emit. Correct.
- Coarser tiers (/2m, /3m, ...) have NO earliestPerCadence entry, so
  they're not gated. Their normal segment emission proceeds. Query
  for 6/15 lands on /2m canonical. Correct.

### Other consumers

A pyramid with full canonical backfill + recently-deployed partials
gets the same fix. A pyramid with limited-history canonicals (the
existing `earliestWatermarks` use case) is unchanged.

## Schema additions

None on the ShardIndex side. `pyramid_shards` already has
`period_start`; consumers do their own `MIN(period_start)` query.

A future ergonomic addition could be `ShardIndex.getEarliestPerCadence(pyramid)`
that returns the map directly — but it's a thin wrapper. Defer until
multiple consumers need it.

## Out of scope

- `earliestPerCadence` for CANONICAL shards (key `${tier}` no `@`).
  Workable but redundant with the existing per-tier `earliestWatermarks`
  in most use cases. If a future consumer needs canonical-with-no-propagation,
  add then.
- D1ShardIndex helper for the `MIN(period_start)` lookup. Consumer-side
  one-liner; not worth a pyrmts-cfw API change yet.
- Cross-cadence propagation within a tier. The within-tier `min`
  cascade for `effective` already handles "if my finer partial has
  X data, my coarser partial does too" — but the `earliestEntry`
  semantics are about file availability, not data shape. No
  cascade.

## Test plan

1. **Unit (planner.ts):**
   - Per-cadence earliest gates the targeted entry only; sibling
     entries in the same tier emit normally.
   - Per-cadence earliest does NOT propagate to coarser tiers. A query
     for an old window with /1m partials gated and /2m+ ungated picks
     /2m+ canonical.
   - Per-tier + per-cadence both set: per-tier behaves as today (with
     propagation); per-cadence further restricts the targeted entry
     within /1m without changing coarser tiers.
2. **Backwards compat:** all existing planner tests pass without
   changes. `earliestWatermarks` semantics unchanged.
3. **Integration (ctbk avail-v3 dev):** /1m partials trusted only
   for windows after cascade-start; old queries route to /2m+
   canonical without 404. (Lives in ctbk; this spec doesn't gate on
   it.)

## Migration

- Add `PlanQueryInput.earliestPerCadence?: Record<string, Date>` — additive,
  no breaking changes.
- Bump pyrmts minor version. Publish dist branch SHA.
- ctbk bumps its pyrmts pin, restores `/1m` to `TIERS` + `partials`/
  `partialKey` (the previously-rolled-back state in commit `287bed61`),
  threads `earliestPerCadence` from D1.

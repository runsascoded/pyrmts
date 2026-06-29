# Unified shard-duration ladder per tier (drop canonical/partial dichotomy)

## Goal

Refactor pyrmts's tier model from a singleton-`shard`-plus-optional-
`partials` shape to a **list of shard durations per tier** — a per-tier
ladder. Drop the conceptual and code-level distinction between
"canonical" and "partial" shards. They are all just shards at different
durations on the same tier, tiling the timeline at varying granularity.

Today's API treats one duration per tier as privileged ("the
canonical") and the rest as second-class ("partials") with a separate
`partialKey` template, separate planner path, separate
`earliestPerCadence` gating. That dichotomy is incidental — pyrmts
naturally generalizes to N shard sizes per tier with uniform handling.

The cleaner model:

```
Tier T has shard-duration ladder [D₁ < D₂ < … < D_max]   where D₁ ≥ T.bin
At each D_i, T is tiled by shards of duration D_i.
Compactor writes at D₁; promotes consecutive D_i shards into a single
D_{i+1} shard at D_{i+1} boundaries.
```

The planner picks "for each contiguous chunk of the query range, the
largest-D shard at T that contains it" — preferring fewer/bigger files,
falling smaller (or to a finer tier) for the freshness tail.

## Motivation

The current model has three failures, in priority order:

1. **No way to express "every shard size has a tiling across
   `[genesis, present)`."** `tier.shard` is singular: each tier
   commits to ONE shard duration (the canonical / largest). `pyramid.partials`
   adds a freshness tail of smaller-duration shards, but ONLY for the
   window since the last canonical seal — forward-only, bounded by
   the canonical writer's cadence. There's no way to say "/1m/3h
   shards exist for every 3h block back to genesis," which is what
   you'd want for a planner that picks the largest-shard-duration
   covering each cursor position (and falls smaller only where the
   larger isn't available). The unified ladder model says shards
   live at every declared duration; smaller-near-present and
   larger-further-back falls out of streaming promotion + retention.

2. **Planner walk-order pathology** (downstream of (1)). With sparse
   `(tier, cadence)` coverage from incomplete compaction, the
   ascending-effective walk in
   [`done/per-cadence-earliest.md`](done/per-cadence-earliest.md)
   skips prefix windows that smaller-cadence entries could have
   served. Concrete repro observed in ctbk: `/1m@p3h` started writing
   2026-06-29 03:00 (post-`mergeRows`-fix); `/1m@p1h` has continuous
   data from 2026-06-28 00:00. A 1-day query
   `[06-28 13:48, 06-29 13:48]` plans /p3h to `[03:00, 12:00]` →
   cursor jumps to 12:00 → /p1h only fills `[12:00, 13:00]`. The 13h
   `[13:48 06-28, 03:00 06-29]` where /p1h had data gets dropped.
   With full tilings ((1)) this pathology mostly disappears (no
   sparse coverage); the cursor-aware walk handles the residual.

3. **Storage layout duplication and asymmetric writer code.**
   `keyTemplate` (canonical) + `partialKey` (partials, with a
   `{shard}` placeholder) — two templates the consumer has to keep
   in sync. The `p`-prefix convention exists only to disambiguate
   cadence labels from tier names because of the structural decision
   to nest partials under `<tier>/p<shard>/…`. With a uniform
   `<tier>/<shard_dur>/<period>` layout the prefix is meaningless.
   On the writer side, ctbk's CFW has one code path for "write /1m
   partials" and another for "promote /1m partials → canonical at
   midnight UTC"; the unified ladder collapses both into a single
   "write at `shards[0]`, promote up at boundaries" loop. ctbk
   `avail-v3-steady-state.md` Phase 3/4 already plans this
   generalization for all tiers and is blocked on the pyrmts-side
   model.

## Design

### Type changes

```ts
// Was:
export interface Tier {
  name: string
  bin: Bin
  shard: Shard       // singular largest duration
}

// To:
export interface Tier {
  name: string
  bin: Bin
  // Sorted ascending. `shards[0]` is the smallest (≥ `bin`),
  // `shards.at(-1)` is the largest. Each entry divides the next
  // (so promotion is clean concat). Single-element ladders are
  // legal (collapse to today's single-shard-size behavior).
  shards: Shard[]
}
```

`Shard` drops the `'all'` sentinel — use an explicit "comically
large" duration like `'120y'` instead. Two reasons: (a) `'all'`
was special-cased in `floorToSpan` / `addSpan`; eliminating it
removes a branch, and (b) the unified-ladder planner walks shards
by duration order, which needs an Ms-comparable value at every
position. A large explicit Duration is uniformly comparable; `'all'`
isn't.

```ts
// Was: type Shard = Bin | '1run' | 'all'
type Shard = Bin | '1run'
```

`Pyramid.partialKey` is removed. Storage key template is a single
field with a `{shard}` slot:

```ts
export interface Pyramid {
  // Required slots: {tier}, {shard}, {period}.
  // Example: `avail-v3/{tier}/{shard}/{period}.parquet`
  keyTemplate: string
  // (existing fields...)
}
```

No back-compat shim. See "No backwards compatibility" below.

### Watermark grid

Watermark key encoding is uniform: `${tier}@${shard_dur}` for every
entry. No canonical/partial distinction; no bare-`${tier}` form.

`effectiveShardWatermarks` builds the grid by walking
`(tier, shard_dur)` pairs. Within-tier `min` propagation walks
**ascending shard duration** (smaller-duration shards "see" the smaller
horizon; promoting upward bounds the larger). Cross-tier `min`
propagation stays as today (finer tier's max-effective bounds coarser
tier's per-shard-duration effective).

`earliestPerCadence` is renamed to `earliestPerShard`, keyed
`${tier}@${shard_dur}` for every entry. Per-(tier, shard_dur) gate
that doesn't propagate (same semantics as today's `earliestPerCadence`,
just under the unified vocabulary).

### Planner walk

Replace the current "walk entries by ascending effective" with a
cursor-aware per-position probe:

```
cursor = plannedFrom
while cursor < plannedTo:
  for T in [outputTier, outputTier-1, ..., tiers[0]]:
    for shard_dur in T.shards, LARGEST first:
      period = containingPeriod(cursor, shard_dur)
      if shardExists(T, shard_dur, period):              // ShardIndex check
        // emit a one-period segment, clipped to plannedTo
        emit { tier: T, shard_dur, from: max(cursor, period.start),
               to: min(plannedTo, period.end),
               reaggregate: T !== outputTier }
        cursor = min(plannedTo, period.end)
        continue outer  // try largest-shard-at-T again from new cursor
    // no tier had a shard covering this cursor position → gap
    advance cursor by output bin and continue (or break, per gap policy)

// Post-walk: coalesce adjacent segments with the same (tier, shard_dur)
// where seg[i].to === seg[i+1].from. Re-derive `keys` with
// shardPeriodsCovering(from, to, shard_dur) on the coalesced range.
```

`shardExists(T, shard_dur, period)` checks the watermark grid:
period sealed (`effective ≥ period.end`) AND not before
`earliestPerShard` (`earliestPerShard.get(T@shard_dur) ≤ period.start`).
The walk respects this per-period rather than relying on watermark-
grid ordering.

#### Worked examples

**(a) Full tiling at all sizes — largest wins everywhere.** Tier `/1m`
with `shards: [5min, 1h, 1d]`. Every position has shards at every
size. Cursor at `2026-06-15T13:30`: try `/1m/1d` for period
`[06-15T00:00, 06-16T00:00]`; sealed; emit. cursor → 06-16T00:00.
Next iteration picks `/1m/1d` again, etc. Result: one coalesced
`/1m/1d` segment for the whole range.

**(b) Sparse mid-ladder — fall to next smaller within tier.** Same
tier, `/1m/1d` exists for `[06-13, 06-15)` but the in-progress
06-15 day hasn't been sealed yet; `/1m/1h` exists for every hour up
to `06-15T13:00`. Cursor at `06-14T18:00`: `/1m/1d` for
`[06-14T00:00, 06-15T00:00]` sealed → emit, cursor → 06-15T00:00.
Next iteration: `/1m/1d` for `[06-15T00:00, 06-16T00:00]` NOT sealed.
Try `/1m/1h` for `[06-15T00:00, 06-15T01:00]` sealed → emit,
cursor → 06-15T01:00. Continue with `/1m/1h` until cursor reaches
`06-15T13:00`. Coalesce: `/1m/1d` for `[06-14T00:00, 06-15T00:00]`
+ `/1m/1h` for `[06-15T00:00, 06-15T13:00]`.

**(c) Sparse + finer-tier fall-through.** Output tier `/15m`. Query
extends past `/15m`'s latest effective. Cursor past `/15m`'s last
sealed period: no `/15m/<dur>` shard covers. Fall to `/5m`; try
largest-first; if none, fall to `/1m`. Emit with `reaggregate: true`.
This subsumes the mixed-tier-tail-coverage behavior — finer tier
fills the freshness tail naturally.

### Compactor / promotion

Pyrmts specifies the contract; the consumer wires the runtime. The
contract is per-tier:

```
On each tick t (for tier T):
  Write shard at T.shards[0] for the just-closed smallest-duration window.
  For each i ∈ 1..len(T.shards):
    If t aligns with a T.shards[i] boundary, concat the
    N = T.shards[i] / T.shards[i-1] just-closed T.shards[i-1] shards →
    one T.shards[i] shard.
```

Promotion is `mergeRows` over already-deduplicated bins — cheap per
step, but the cascade can get long at higher rungs (e.g. a 1mo
boundary that triggers `1d → 1mo` is 30 shards merged into one,
~30× the row count of a single-step promotion).

**Runtime flexibility.** Pyrmts exposes the promotion API as a
library call (`promote(tier, fromDur, toDur, boundary) → key[]`); the
consumer's cron triggers it. The library doesn't know or care about
CFW vs GHA vs whatever — but the consumer can SPLIT promotion
responsibility along the ladder to fit per-backend CPU / wall-clock
limits. E.g. ctbk could run `[shards[0] → shards[1]]` through
`shards[3] → shards[4]` from CFW cron-per-minute (fast, no
parallelism issue), and `shards[4] → shards[5]` (e.g. `1d → 1mo`)
from a GHA monthly workflow (no CFW timeout). Pyrmts's job: define
the boundaries cleanly so two compactor instances split without
conflict. A shared `pyramid_shards` D1 table (the existing
`D1ShardIndex`) coordinates: each compactor reads its input shards,
writes its output shard, atomically upserts a row.

**Retention contract.** Retention is per-(tier, shard_dur), declared
in the pyramid config (or by the writer). The planner reads ONLY
what `ShardIndex` reports declared — it has no opinion on whether
post-promotion smaller shards stick around or get expired. Two
common patterns:

- *LSM-style* — delete smaller shards after promotion (storage-
  optimal; planner uses larger shards for old periods, smaller only
  for freshness tail).
- *Keep-all* — retain every size forever (read-perf-optimal at small
  granularities for cold queries; storage cost scales linearly with
  ladder depth).

The planner is correct under both — it picks the largest covering
shard at each cursor position, and that's whichever is still
declared in the index.

### Storage layout migration

Before: mixed
- canonical: `avail-v3/15m/2026-06-21.parquet`
- partial:   `avail-v3/1m/p1h/2026-06-28T13.parquet`

After: uniform
- `avail-v3/15m/15d/2026-06-21.parquet`
- `avail-v3/1m/1h/2026-06-28T13.parquet`

One-shot rename per pyramid: list existing R2 keys, rename per the
new schema, atomically upsert each row of the D1 `pyramid_shards`
inventory to match (or rewrite the manifest blob for
`ManifestShardIndex` consumers). Cheap at ctbk scale (~500 shards).
No dual-read fallback — single breaking change.

### Backfill of intermediate shard sizes

**Required for live-readiness.** The unified ladder's premise is
"every declared shard size has a tiling across `[genesis, present)`";
the planner expects to find shards at intermediate sizes for
historical periods, not just at the largest size. Going live without
backfill leaves the planner with sparse historical coverage at
non-largest sizes, and queries fall to the largest shard (correct,
but defeats the perf benefit of the ladder).

Two approaches, per pyramid:

**(a) Regen from raw.** Re-run the compactor from raw historical
data. Cleanest output (byte-identical to live-write path). Requires
raw data to still be available (for ctbk avail-v3, GBFS snapshots
are retained in R2; this is feasible).

**(b) Split existing largest-size shards into intermediate sizes.**
Read each existing largest-size shard, split its row-group(s) by
`${period}` boundary at each intermediate shard duration, write the
splits. Cheaper than (a) because no re-fetching / re-binning;
output is row-identical to what the live promoter would have
produced (since promotion is just concat).

Recommend (b) for ctbk: ~500 largest-size shards → ~3000 intermediate
shards (rough EM). Single one-shot script.

### No backwards compatibility

Pre-prod-use, no real consumers — clean rip-and-replace, single
breaking change, single version bump. No deprecation aliases:

- `Tier.shard` is removed (use `Tier.shards`).
- `Pyramid.partialKey` is removed (use `Pyramid.keyTemplate` with
  `{shard}` slot).
- `Shard = 'all'` is removed (use an explicit large Duration like
  `'120y'`).
- Watermark keys are uniformly `${tier}@${shard_dur}` (no bare
  `${tier}`).
- `earliestPerCadence` is renamed to `earliestPerShard`.

`done/partial-shards.md` and `done/per-cadence-earliest.md` get a
"superseded by unified-shard-ladder.md" header — the unified model
subsumes them.

## Out of scope

- **Non-monotonic shard ladders.** Each tier's ladder must be sorted
  ascending and each entry must divide the next (so promotion is a
  clean concat). Same divisibility-chain constraint as today's
  `partials` ladder, just generalized.
- **Different ladders per pyramid instance.** Pyramids using the
  same schema but different storage backends still share a single
  Tier list.
- **Time-axis ↔ step-axis cross-cutting.** The refactor is mechanical
  for both; the bin/shard distinction is orthogonal.
- **Non-epoch-anchored / rolling-window bins.** Tier bins remain
  calendar/epoch-aligned (`/1d` = midnight UTC, `/1h` = top of hour,
  `/1mo` = 1st of month). Phase-shifted coarse bins (e.g. /1d
  centered on local-time-midnight, computed by re-binning finer
  shards) are doable but not in this refactor. The "partial most-
  recent bin" case is already handled by the stitcher's
  in-progress-coarsening (re-aggregate from next finer tier on the
  fly).

## Use cases

### ctbk avail-v3 (immediate driver)

```yaml
tiers:
  - name: 1m
    bin: 1min
    shards: [5min, 10min, 30min, 1h, 3h, 12h, 1d]
  - name: 2m
    bin: 2min
    shards: [10min, 30min, 1h, 3h, 12h, 1d, 2d]
  - name: 15m
    bin: 15min
    shards: [30min, 1h, 3h, 12h, 1d, 15d]
  - name: 1h
    bin: 1h
    shards: [3h, 12h, 1d, 1mo]
  # ...
```

Steady-state CFW: write at `shards[0]`, promote up the ladder on
boundary ticks. Backfill = same code, run over historical input.

### Other pyramids

A pyramid with a single shard size today (e.g. awair) is exactly
representable as `shards: [Shard]` (one element) — zero behavior
change.

## Open questions

- **Shard ladder validation.** Today `validatePartials` checks the
  divisibility chain. The same check applies to the new `shards`
  field. Move that helper out of the partials-specific module into
  a generic ladder-validator.
- **`prefer: smaller` knob.** Default is largest-shard-first for
  read efficiency (file size sub-linear in duration → fewer files
  for fewer requests). A future pyramid might want
  smallest-shard-first (best locality / RG-skipping for sparse
  queries). Not needed for v1; flag if a consumer asks.
- **Per-compactor responsibility declaration.** When splitting the
  ladder across multiple compactor backends (e.g. CFW for
  `shards[0..i]`, GHA for `shards[i..n]`), where does the split
  point live? Probably a per-compactor config the consumer maintains
  outside pyrmts; pyrmts just provides the `promote(tier, fromDur,
  toDur, boundary)` primitive. Worth checking after ctbk wires it
  up whether a shared declaration would prevent split-brain bugs.
- **Stale-promotion cleanup.** Under LSM-style retention (delete
  smaller shards post-promotion), if the larger-shard write succeeds
  and the smaller-shard delete fails (or vice versa), the shared D1
  inventory should be the source of truth; an orphan R2 file is
  storage waste, not a correctness issue. Document the recovery
  procedure.

## Migration plan

Single breaking version, no deprecation cycle. Order:

1. **Pyrmts refactor.** `Tier.shards: Shard[]` (drop `Tier.shard` +
   `Shard='all'`), unified `keyTemplate` with `{shard}` slot
   (drop `Pyramid.partialKey`), watermark keys uniformly
   `${tier}@${shard_dur}`, rename `earliestPerCadence` →
   `earliestPerShard`. Cursor-aware-largest-first planner walk
   (same PR or immediate follow-up). New planner tests for the
   (a/b/c) "Worked examples" cases. Existing partial-shards and
   per-cadence-earliest tests collapse into the ladder tests —
   their semantics survive; the API is renamed.
2. **Consumer pyramid configs.** ctbk avail-v3 rewrites its YAML to
   declare per-tier `shards` ladders. Other consumers (awair etc.)
   with single-element ladders need a one-line YAML edit
   (`shard: X` → `shards: [X]`).
3. **Consumer storage migration.** R2 rename (`<tier>/p<dur>/...` →
   `<tier>/<dur>/...`), D1 inventory rewrite (drop any `is_partial`
   flag column — no longer meaningful).
4. **Backfill intermediate-size shards.** Required before queries
   benefit from the ladder; see "Backfill of intermediate shard
   sizes" above. Per pyramid; one-shot script.
5. **Compactor unification on ctbk.** Single per-tier loop: write at
   `shards[0]`, promote at boundaries. Unblocks
   `avail-v3-steady-state.md` Phase 3/4. Split promotion
   responsibility across CFW (fast / small-cascade) vs GHA (slow /
   large-cascade) per "Runtime flexibility" above.

Steps 1-3 land as two/three PRs (pyrmts core, then a ctbk PR
rewriting config + migration script). Step 4 is its own ctbk PR
that depends on 1-3. Step 5 unblocks subsequent ctbk work.

## Cross-reference

- `done/partial-shards.md` — phase-1 shipped the dichotomy this spec
  unifies.
- `done/per-cadence-earliest.md` — the per-cadence gate this spec
  generalizes per-(tier, shard_dur).
- `done/mixed-tier-tail-coverage.md` — finer-tier fall-through, which
  the unified planner subsumes.
- ctbk `specs/avail-v3-steady-state.md` — `f4a0989a` plans Phase 3/4
  cascade generalization; this spec is the pyrmts-side enabler.

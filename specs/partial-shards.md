# Partial sub-shards: multi-cadence sub-shards per tier

## Goal

Let each tier have **multiple shard granularities** — a canonical sealed
shard plus one or more "partial" sub-shards at finer cadences — so the
planner can serve fresh data without either rebuilding the canonical
shard frequently (expensive) or falling all the way through to the base
tier (huge fan-out).

The planner walks `(tier, shard-size)` pairs coarsest → finest within
each tier, then walks tiers coarsest → finest, using the watermark grid
to pick the lightest combination covering each segment.

## Motivation

Today's pyrmts: one shard size per tier. Watermarks gate per-tier trust.
For data fresher than the latest sealed canonical shard, the planner has
two options:

1. **Rebuild the canonical shard frequently.** Pyramids in production
   tend to have ≥15-day shards at fine tiers (ctbk's `/15m` shard is
   15 days; `/30m`/`/1h` shards are 1 month). Rebuilding a 15-day shard
   every hour rewrites 1440 bins per cell to refresh ~4 bins. Wasteful.

2. **Fall through to the finest tier.** ctbk's `/1m` base tier has hourly
   files. A `/15m` query over the post-watermark tail (1h tail = 60
   /1m bins per cell; 1d tail = 1440 /1m bins per cell; 7d tail = 10080
   /1m bins per cell). The fan-out is unbounded by the tier ladder.

Neither is acceptable for an interactive "Latest · N" UX. The missing
piece is sub-canonical-shard partial shards at intermediate cadences
that close the gap between "last canonical seal" and "now."

ctbk's avail-v3 pyramid is the immediate driver — see ctbk
`specs/pyramid-cascade.md` and ctbk task #127.

## Current state

`Tier` in `pyrmts/src/types.ts`:

```typescript
export interface Tier {
  name: string
  bin: Duration         // bin width
  shard: Duration | 'all'   // shard width (one shard size)
  rg_size?: number      // optional row-group target
}
```

`planQuery` (`pyrmts/src/planner.ts`):
- Picks one tier per segment based on `binBudget` and watermarks.
- For watermark fall-through, walks finer tiers (smaller bin) for the
  post-watermark tail of each segment.
- Each segment names one (tier, period) pair; storage resolves via
  `keyTemplate` + `substituteKey`.

`Manifest` (informal, read by `planQuery` via the `watermarks` input):

```json
{ "tiers": { "1h": { "latest_period": "2026-06" }, ... } }
```

There is no model of "the same tier has multiple shard sizes." Adding
this is the spec.

## Design

### Concept

A **shard granularity** is a `Duration` (e.g. `1d`, `1h`, `5m`) at which
a tier's data is materialized. Each tier has:
- One **canonical shard** (the existing `shard` field) — sealed
  end-to-end, the most efficient read when available.
- Zero or more **partial sub-shards** at finer cadences, each
  materialized incrementally for the in-progress canonical shard.

Sub-shard cadences are declared at the **pyramid** level (not per tier):
a single ladder of cadences that applies, with per-tier filtering, to
every tier whose `canonical_shard > cadence`.

For each `(tier, cadence)` pair where `tier.bin ≤ cadence < tier.canonical_shard`:
- Sub-shards are written at intervals of `cadence` (one shard per
  `cadence`-aligned period).
- A sub-shard is **valid** for query iff its watermark says so.
- The cadence/tier alignment constraint: `cadence % tier.bin == 0`
  (the cadence must be an integer multiple of the bin so that each
  sub-shard contains a whole number of bins).

Sub-shards write the same row schema as canonical shards — same
`(*dims, bin_col, *metric_cols)` — only the period covered differs.

### YAML schema

Pyramid YAML adds an optional `partials` list:

```yaml
tiers:
  - { name: 2m,  bin: 2min,  shard: 2d  }
  - { name: 15m, bin: 15min, shard: 15d }
  - { name: 1h,  bin: 1h,    shard: 1mo }
  - { name: 7d,  bin: 7d,    shard: all }

# Sub-shard cadence ladder. Each cadence applies to every tier whose
# canonical shard > cadence AND whose bin divides cadence.
partials:
  - 5min
  - 10min
  - 30min
  - 1h
  - 3h
  - 12h
  - 1d
  - 3d
  - 7d
```

Per-tier filtering is automatic. For the avail example above:

| tier | bin | canonical shard | applicable cadences |
|---|---|---|---|
| 2m | 2min | 2d | 10min, 30min, 1h, 3h, 12h, 1d (5min skipped: 5%2≠0) |
| 15m | 15min | 15d | 30min, 1h, 3h, 12h, 1d, 3d, 7d |
| 1h | 1h | 1mo | 1h-only filtered out (bin = cadence; 1h sub-shard would equal 1 bin) actually keep, see below. 3h, 12h, 1d, 3d, 7d |
| 7d | 7d | all | 7d (one bin per shard) |

Notes:
- A tier whose `bin == cadence` produces 1-bin sub-shards. That's
  degenerate but valid; allow it (cheap to store, useful as a fall-
  through bridge to the base tier). Per-tier opt-out via a deny list
  if needed:

  ```yaml
  partials:
    cadences: [5min, 10min, ...]
    skip:
      1h: [1h]          # don't materialize 1h@1h
  ```

  Optional refinement; default to materialize all alignment-valid pairs.

- Cadences MUST be fixed-duration (`min/h/d`). Calendar cadences (`mo`,
  `y`) are forbidden in the `partials` list — sub-shards exist to
  serve fresh-data queries on a regular cron, and calendar boundaries
  don't divide cleanly. (Canonical shards may still be calendar-aligned
  — that's unchanged.)

### Storage path schema

Sub-shard keys interpolate a new placeholder `{shard}`:

```yaml
storage:
  key: "avail-v3/{tier}/{shard}/{period}.parquet"
```

For canonical shards, `{shard}` is empty or the canonical shard label
(implementation choice — see below). For partials, `{shard}` is the
cadence label (e.g. `5m`, `1h`, `1d`).

**Recommendation: implicit canonical placeholder.** Canonical shard
keys omit `{shard}` entirely; only sub-shards inject it. Two
keyTemplates:

```yaml
storage:
  key: "avail-v3/{tier}/{period}.parquet"               # canonical
  partialKey: "avail-v3/{tier}/p{shard}/{period}.parquet"   # sub-shards
```

The `p` prefix disambiguates `p5m` (a cadence) from a tier name.
Concrete examples:

- `avail-v3/15m/2026-06-15.parquet` — canonical 15-day shard for the
  period starting 2026-06-15.
- `avail-v3/15m/p1h/2026-06-21T14.parquet` — 1h-cadence sub-shard
  for the hour starting 14:00 on 2026-06-21 (contains 4 bins for that
  hour).
- `avail-v3/15m/p1d/2026-06-21.parquet` — 1d-cadence sub-shard for
  the day 2026-06-21 (contains 96 bins for that day).

Alternative: single template with `{shard}` always present, canonical
gets `'canon'` or the canonical-shard label literal. Cleaner for
template substitution; uglier paths. Prefer dual template.

### Manifest format

Extend the manifest to record per-`(tier, shard)` watermarks:

```json
{
  "tiers": {
    "1h": {
      "latest_period": "2026-06",
      "partials": {
        "1h":  { "latest_period": "2026-06-21T14" },
        "1d":  { "latest_period": "2026-06-21" },
        "3d":  { "latest_period": "2026-06-19" }
      }
    },
    "2m": {
      "latest_period": "2026-06-19",
      "partials": {
        "10min": { "latest_period": "2026-06-21T14:40" },
        "1h":    { "latest_period": "2026-06-21T14" },
        "1d":    { "latest_period": "2026-06-20" }
      }
    }
  }
}
```

Backward-compatible: clients that ignore `partials` see the existing
canonical-shard manifest unchanged.

The cron job writing sub-shards is responsible for atomically updating
the manifest after each successful sub-shard write. R2 has strong
read-after-write consistency for `put` — sufficient for our needs.

Cache the manifest module-level in the worker with a short TTL (already
done — 60s in ctbk's `gbfs/api/src/avail_geo.ts`). With sub-shards on a
≤5-min cadence, manifest staleness up to 60s is acceptable: a query
might miss a just-written sub-shard for up to a minute and fall back to
a finer cadence (which has the same data) or to the base tier.

### Planner algorithm

Today: `planQuery` picks one tier per segment, falls through finer tiers
for post-watermark tail.

New: for each segment, the planner picks `(tier, shard)` pairs by:

1. **Build the effective watermark grid.** For each `(tier, shard)`
   pair declared in the pyramid (canonical + each cadence), compute the
   effective watermark — same propagation rules as today:
   `effective[t,s] = min(declared[t,s], next-finer-within-tier[t,s'])`,
   where "next-finer-within-tier" = the same tier with a finer shard
   cadence. Across tiers, the existing "min with next-finer-tier"
   propagation also applies (finer tier's effective watermark bounds
   coarser tier's effective).

2. **Pick the coarsest (tier, shard) per segment.**
   For each output bin or sub-segment, walk:
   - Tiers coarsest → finest (existing behavior).
   - Within each tier: canonical shard → finest partial cadence.
   - Pick the first `(tier, shard)` whose effective watermark covers
     the segment AND whose `bin` ≤ `outputBin` (or whatever the
     existing constraint is).

3. **Emit segments per (tier, shard) — the existing `PlanSegment`
   structure gains an optional `shardCadence: Duration | null`:**

   ```typescript
   export interface PlanSegment {
     from: Date
     to: Date
     shardTier: Tier
     shardCadence: Duration | null   // null = canonical; else partial cadence
     keys: string[]
     reaggregate: boolean
   }
   ```

4. **Storage backend** substitutes the right key template based on
   `shardCadence`. Canonical: `keyTemplate`. Partial: `partialKey`.

### Discovery mechanism

> > > planner needs to learn what sub-shards exist (manifest vs D1)

Three options:

1. **Manifest-only (recommended).** Single source of truth. Manifest
   declares which `(tier, shard)` pairs exist and their watermarks. The
   sub-shard writer is responsible for keeping it current. Planner
   trusts the manifest; missing entries = "no sub-shards at this cadence
   yet."

   Pros: O(1) read regardless of shard count; single round-trip; cleanly
   models declared sub-shard cadences (planner knows the keyspace at
   config time, only needs watermarks).

   Cons: a stale or corrupt manifest can mask available sub-shards.
   Mitigation: short TTL + manifest is rewritten on every sub-shard
   build (idempotent). On parse error, return empty (no watermark
   gating) — same defensive fallback as today.

2. **R2 LIST.** Planner LISTs the shard prefix at query time. Robust
   to manifest staleness; expensive on cold path (Workers + R2 LIST
   round-trips; not cacheable across queries without a manifest-like
   index anyway, which makes it a manifest by another name).

3. **D1 index.** Each sub-shard write appends to a D1 table; planner
   queries D1 for "latest covering shard for `(tier, [from, to])`."
   Indexable. Heavier than manifest.

**Pick #1.** The manifest is already in place for canonical watermarks;
extending it costs one nested object level. D1 (#3) buys nothing the
manifest doesn't, and adds operational surface area (the rides-v3 D1
bakeoff — ctbk #88-101 — already established D1's cold-path latency is
worse than parquet for cell-keyed reads). LIST (#2) is the
fallback-of-last-resort if the manifest gets out of sync; not the
primary path.

### Cascading sub-shard builder

Out of scope for pyrmts proper (this is a writer concern, lives in the
consumer project), but the spec records the pattern because it
constrains the design.

One job runs at the finest cadence (e.g. /5m via CFW cron `*/5 * * * *`)
and at each invocation:

1. Compute `now`.
2. For each cadence `c` in the partials ladder, finest → coarsest:
   - If `now` is on a `c`-aligned boundary, materialize all
     `(tier, c)` sub-shards for the just-completed `c`-window.
     For each tier, read inputs from the next-finer cadence's sub-shards
     (or the base tier if `c` is the finest), aggregate, write.
   - Else: skip remaining (coarser) cadences (they're all unaligned
     too, since `c | c'` for coarser `c'`).
3. Update the manifest with the new `latest_period` for each written
   `(tier, c)` pair.

Each level's work is bounded — `O(step-up factor × tiers × cells)`,
typically <10 MB per level for NYC-scale GBFS data. The whole cascade
at the worst-aligned invocation (e.g. midnight when all cadences fire)
finishes in seconds and fits CFW Unbound (128 MB RAM, 30 sec CPU).

### Backfill semantics

Sub-shards have no historical-backfill requirement — they exist only
for the post-canonical-watermark tail. When a new canonical shard seals
(via the larger batch job), the partial sub-shards covering that
shard's period become redundant and can be deleted (or left to age out
via lifecycle rules). The planner ignores partials whose period falls
within a canonical-watermark-covered range.

## Constraints

1. **Alignment**: For each `(tier, cadence)` pair to be valid,
   `cadence % tier.bin == 0`. The planner skips invalid pairs.
2. **Fixed-duration only**: Sub-shard cadences must be `min/h/d`
   durations. Calendar (`mo`, `y`) is canonical-only.
3. **Cadence ladder divisibility**: For the cascading writer's
   "break early" optimization to work, each cadence must divide all
   coarser cadences in the ladder. Validated at config-load time.
4. **Watermark coverage is closed**: The effective watermark for
   `(tier, cadence)` propagates from finer cadences within the same
   tier AND from the next-finer tier. Existing "finer-bounds-coarser"
   rule is preserved.

## Migration

Existing pyramids without `partials`: unchanged behavior. Manifest's
`partials` key is absent; planner walks only canonical shards (today's
behavior).

Existing pyramids that opt in to `partials`: bumping pyrmts is a
no-op until the consumer also (1) deploys a sub-shard builder, and
(2) updates the manifest writer to include `partials`. Until then, the
planner sees no `partials` watermarks and behaves as today.

Storage layout for partials is additive — canonical shards' key paths
are unchanged. No rewrite of existing data.

## Open questions

- Per-tier vs per-pyramid cadence ladder. Default: pyramid-wide, with
  per-tier filtering by alignment. The `skip:` config above is an
  escape hatch. A consumer that wants entirely-different cadence
  ladders per tier could declare them per-tier; not in v1.

- Should the planner prefer "coarser cadence within tier" or "coarser
  tier with finer cadence" for the same watermark coverage? Both reduce
  fan-out vs the base tier; coarser-tier-finer-cadence reduces sum
  cost; coarser-cadence-within-tier reduces aggregation cost (fewer
  rows per cell). Default: coarser tier wins (consistent with today's
  "coarsest tier first"); revisit if a workload prefers otherwise.

- Sub-shard row-group sizing. Inherit from canonical `rg_size` of the
  same tier by default; allow per-cadence override in `partials.skip`-
  style config if a profile emerges.

## Implementation phasing

1. **Pyrmts types**: extend `Tier` with optional `partialCadences:
   Duration[]` (or accept the pyramid-wide list and compute per-tier
   in `planQuery`). Extend `PlanSegment` with `shardCadence`.
2. **Manifest schema**: extend the watermark loader (consumer-side
   today) to accept `partials.{cadence}.latest_period` and produce a
   `(tier, cadence) → Date` map. Pass as `watermarks` input (or a new
   `partialWatermarks` input) to `planQuery`.
3. **Planner**: update `effectiveWatermarks` to operate on the
   `(tier, cadence)` grid. Update segment emission to thread
   `shardCadence` through. Update storage backend key resolution to
   pick `partialKey` when `shardCadence != null`.
4. **Tests**: planner unit tests for grid walk, watermark propagation
   across both axes, edge cases (single cadence, single tier, all-
   canonical-no-partials, all-partials-no-canonical).
5. **Docs**: add to `SPEC.md` and a short worked example.

Consumer side (ctbk) — out of scope here, tracked in ctbk repo:
- Cascading sub-shard CFW (cron + cascade logic).
- Direct /1m PQT write from the poller (separate, but unblocks
  freshness now).
- Manifest writer update on each sub-shard build.
- Worker config: declare `partials` in `configs/pyramids/avail.yaml`.

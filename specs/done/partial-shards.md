# Partial sub-shards: multi-cadence sub-shards per tier

> **Superseded by [`unified-shard-ladder.md`](./unified-shard-ladder.md).**
> The canonical/partial dichotomy this spec introduced is gone; every
> shard is just an entry on a per-tier `shards: Shard[]` ladder.
> `pyramid.partials` + `pyramid.partialKey` removed; watermark keys
> are uniformly `${tier}@${shardDur}` (no bare-`${tier}` form).
> Planner walk replaced with cursor-aware-largest-first per position.

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

### Watermark index

Watermarks need a per-`(tier, cadence)` lookup. Two viable backends:

**(a) D1 / SQLite (RECOMMENDED for Cloudflare Workers consumers).**

```sql
CREATE TABLE pyramid_watermarks (
  pyramid TEXT NOT NULL,
  tier TEXT NOT NULL,
  cadence TEXT,                       -- NULL = canonical shard
  latest_period_end INTEGER NOT NULL, -- unix ms (the end-of-shard for the
                                      -- latest written shard at this cadence)
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (pyramid, tier, cadence)
);

-- Optional shard inventory for verification/diagnostics
CREATE TABLE pyramid_shards (
  pyramid TEXT NOT NULL,
  tier TEXT NOT NULL,
  cadence TEXT,
  period_start INTEGER NOT NULL,
  period_end INTEGER NOT NULL,
  key TEXT NOT NULL,
  written_at INTEGER NOT NULL,
  PRIMARY KEY (pyramid, tier, cadence, period_start)
);
```

Planner read: one indexed query per request returns all watermarks for
the pyramid; client builds the `(tier, cadence) → Date` map in memory.

Sub-shard write: one `INSERT OR REPLACE INTO pyramid_watermarks` per
shard. Atomic per row. Concurrent writers don't conflict (separate
`(tier, cadence)` rows). No read-modify-write cycle.

This avoids the race window inherent to a single-blob manifest: with
the cascading writer at /5m × ~10 tiers × up to ~5 cadences firing per
invocation, the worst-case is ~50 watermark updates per invocation.
A blob manifest forces 50 read-modify-write cycles serially; D1
handles them as 50 independent upserts, and overlapping invocations
don't clobber each other's writes.

Cold-path latency caveat: D1 has a known ~5s cold-path penalty when the
worker isolate and D1 colo diverge (see consumer notes — addressed via
Smart Placement + a `SELECT 1` keep-warm cron + Read Replication). The
per-cell fan-out pattern that exposed this in ctbk task #100 does NOT
apply here — watermark read is ONE indexed query per request, the same
pattern as the existing `lookupStation` path that runs fine in prod.

**(b) JSON manifest (FALLBACK for consumers without D1).**

Single blob keyed under the storage backend (e.g. `<root>/_manifest.json`):

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

Workable when (1) the consumer doesn't have D1 (non-Cloudflare deploys,
or simpler stacks), AND (2) writers are serialized externally (single
job, no concurrent retries), AND (3) update cadence is low enough that
the read-modify-write cost is tolerable. Lower-write-pressure consumers
or canonical-shard-only pyramids (no `partials`) fit cleanly. The
current ctbk avail-v3 setup uses this and the spec preserves it for
back-compat.

### Pluggable `ShardIndex` interface

To support both backends, pyrmts exposes a `ShardIndex` interface:

```typescript
export interface ShardIndex {
  /** Read all watermarks for a pyramid. Returns a map from
   *  (tier, cadence | null) to end-of-shard Date. */
  getWatermarks(pyramidName: string): Promise<Map<string, Date>>
  // Key encoding: `${tier}` for canonical; `${tier}@${cadence}` for partials.

  /** Record a new shard write. Idempotent: re-recording a (tier,
   *  cadence, period) overwrites the entry. */
  recordShard(input: {
    pyramidName: string
    tier: string
    cadence: string | null
    periodStart: Date
    periodEnd: Date
    key: string
  }): Promise<void>
}
```

Consumers provide their own implementation: `D1ShardIndex` (one
implementation in pyrmts-cfw), `ManifestShardIndex` (one in pyrmts-core
for R2/S3/filesystem backends), or a hand-rolled one. The planner takes
a `ShardIndex` and is backend-agnostic.

Cache layer: a `CachedShardIndex` wrapper applies a TTL (e.g. 60s) to
any underlying impl. Workers can use this to keep watermark reads at
constant O(1) within a request and short-cycle stale.

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

The planner is `ShardIndex`-backed; "discovery" reduces to "which
implementation backs the index." See the previous section for the two
viable backends (D1 indexed table, JSON manifest blob) and why D1 is
preferred when available.

A third option — **R2 LIST at query time** — is mentioned only to be
ruled out. It works (LIST the shard prefix, parse keys to derive
periods), but it's per-query latency on the cold path with no shared
state, and the result has to be cached *somewhere* anyway, which makes
it just a manifest by another name. Useful only as a one-off recovery
tool when the primary index is broken.

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

1. **Pyrmts types**: extend `Tier` is unnecessary if cadences are
   pyramid-wide; just thread the cadence list through `planQuery`'s
   pyramid argument. Extend `PlanSegment` with `shardCadence: Duration | null`.
2. **`ShardIndex` interface**: define in pyrmts-core; provide
   `CachedShardIndex` (TTL wrapper) in same package.
3. **`D1ShardIndex`**: implement in pyrmts-cfw against the D1 schema
   above. Includes the SQL migrations and a `recordShard` helper for
   writers.
4. **`ManifestShardIndex`**: implement in pyrmts-core against a
   storage-backend-keyed JSON blob (backward-compat for current ctbk
   manifest format; consumes the existing `_manifest.json`).
5. **Planner**: update `effectiveWatermarks` to operate on the
   `(tier, cadence)` grid. Update segment emission to thread
   `shardCadence` through. Update storage backend key resolution to
   pick `partialKey` when `shardCadence != null`.
6. **Tests**: planner unit tests for grid walk, watermark propagation
   across both axes, edge cases (single cadence, single tier, all-
   canonical-no-partials, all-partials-no-canonical). Index tests for
   both `D1ShardIndex` and `ManifestShardIndex` against the same
   interface contract.
7. **Docs**: add to `SPEC.md` and a short worked example for both
   backends.

Consumer side (ctbk) — out of scope here, tracked in ctbk repo:
- Cascading sub-shard CFW (cron + cascade logic).
- Direct /1m PQT write from the poller (separate, but unblocks
  freshness now).
- Wire `D1ShardIndex` to the worker (or migrate from manifest to D1
  in a separate commit) — both reads (planner) and writes (sub-shard
  CFW).
- Worker config: declare `partials` in `configs/pyramids/avail.yaml`.

## Resolution

Shipped across phases 1-6 in commits `892f9b8`..HEAD — each phase a
single commit at the natural seam. Deviations from the proposed spec:

1. **No per-tier `skip:` deny-list.** The spec's optional refinement
   to opt a tier out of a given cadence wasn't needed for ctbk's avail
   workload — alignment filtering already prunes nonsensical pairs.
   Easy to add later if a real workload wants it.
2. **Missing partial watermark → SKIPPED, not FAR_FUTURE.** The spec
   didn't explicitly call this out. Treating a declared-but-never-
   sealed cadence as FAR_FUTURE would let within-tier propagation
   drag canonical's effective down to 0 (because canonical = min(
   declared, finer-within-tier.effective)). Skip-from-grid keeps
   canonical usable until the cron writes its first sub-shard.
3. **Manifest format is new-flat, not legacy-nested.** `ManifestShardIndex`
   serializes `{ version: 1, watermarks: Record<encodedKey, ms>, ... }` —
   the flat shape `getWatermarks` returns directly. The legacy ctbk
   `{ tiers: { name: { latest_period: "2026-06", partials: ... } } }`
   nested-label format is *not* parsed; ctbk migrates straight to D1
   anyway per the spec's recommendation, so the legacy adapter wasn't
   built. Defensive parser: unrecognized format → empty Map.
4. **Empty-string sentinel for D1 cadence column.** SQLite allows
   NULL in PRIMARY KEY without enforcing uniqueness (well-known
   footgun); `D1ShardIndex` stores canonical rows with `cadence = ''`
   instead of NULL. Translated at the impl boundary so the
   `Duration | null` contract is preserved for callers.
5. **`planRagged` (the `targetBin` ragged-decomposition path) is
   canonical-only.** The DP doesn't currently consider partial-shard
   atoms. Easy to add later (each `(tier, cadence)` becomes an
   eligible atom source with its own watermark) but no workload
   exercises it today; deferred.
6. **`assertShardIndexConformance` shared via `pyrmts/test-utils`
   subpath.** Both impls call the same suite to guarantee
   observational equivalence. Required a vitest-config alias plus an
   `exports` map entry on the `pyrmts` package.
7. **Authoritative-end semantics updated.** Now sourced from the raw
   tier's max effective (across canonical + partials), not just
   canonical's — partial sub-shards extend the authoritative range.
8. **Sort-tie rule.** When canonical and a partial share an effective
   watermark, canonical wins (coarser shard = fewer keys to fetch).
   Spec didn't specify; matches the "minimize fan-out" planner intent.
9. **Cursor advances unconditionally per-entry.** Even when a shard's
   segment is dropped (e.g. earliest > effective), cursor advances to
   the shard's segEnd. Preserves the canonical-only earliest-watermark
   semantics existing tests codify ("a tier owns its watermark range;
   finer tier picks up *after*").

346 tests pass across the workspace.

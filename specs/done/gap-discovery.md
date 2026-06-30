# Gap discovery — `list_missing_shards`

Pure-function primitive: given a pyramid config + a `ShardIndex` + a
time range, return the list of `(tier, shard_dur, period_start)`
tuples that *should* exist per the pyramid's declared ladders but
aren't in the index. Consumers (ctbk fsck, rides-v3 fsck) wrap this
with project-specific source readers and a dependency-ordered fill
loop.

## Motivation

Backfilling a partially-cascaded pyramid requires knowing what's
missing. Today consumers reimplement this every time:
- `pyramid-status.py` lists per-(tier, cadence) counts but doesn't
  enumerate expected vs present
- `ctbk pyramid-cascade` builds everything in a range; doesn't
  distinguish "missing" from "needs rebuild"

Centralizing the gap calculation in pyrmts keeps it correct against
the ladder semantics (period boundaries, calendar arithmetic) and
gives consumers a clean handoff: "here's what to fill — you decide
how (raw source vs cascade-up)."

## API

```ts
interface ExpectedShard {
  tier: string
  shardDur: Duration
  periodStart: Date
  periodEnd: Date         // exclusive
  key: string             // substituted keyTemplate path
}

/** List every shard the pyramid's ladders declare for `range`. Pure
 *  enumeration over the YAML — no storage access. Useful as a
 *  ground-truth set to diff against. */
function listExpectedShards(
  pyramid: Pyramid,
  range: { from: Date; to: Date },
  filter?: Partial<Record<string, string | number>>,  // for keyTemplate {param} subs
): ExpectedShard[]

/** Subtract `shard_index`-recorded shards from the expected set.
 *  Returns the gap list. Cheap: one `getShards(pyramid)` call +
 *  set diff. */
async function listMissingShards(
  pyramid: Pyramid,
  shardIndex: ShardIndex,
  range: { from: Date; to: Date },
  filter?: Partial<Record<string, string | number>>,
): Promise<ExpectedShard[]>
```

(Python: equivalent `list_expected_shards` / `list_missing_shards`
returning a list of dataclasses; mirrors the existing `cascade_tiers`
style.)

## Implementation notes

`listExpectedShards`:
- For each tier T in `pyramid.tiers`:
  - For each shard_dur S in T.shards:
    - Compute the period grid covering `range` aligned to `S`. Use
      the existing `shardPeriodsCovering(from, to, S)` (axis helper).
    - For each period, substitute `keyTemplate` with `(tier, shard,
      period)` to derive the key.
- Output: flat array of `ExpectedShard`s.

`listMissingShards`:
- Call `listExpectedShards`.
- Query `shardIndex` for the set of `(tier, shard_dur, period_start)`
  tuples already recorded (`ShardIndex.listShards` or equivalent).
- Set-diff. Return entries in `expected` not in `recorded`.

### Existence semantics

`listMissingShards` is **index-driven**, not storage-driven. A shard
that exists on R2 but isn't recorded in D1 is "missing" from this
function's POV. That's the right behavior — the planner uses
ShardIndex as ground truth, so a non-indexed R2 shard is invisible to
queries. Consumers wanting a "storage-only" check can layer a
secondary `r2.head(key)` pass on top.

### Calendar / variable-duration shards

`shardPeriodsCovering` already handles calendar units (`mo`, `y`) via
`floor_to_span`. Gap discovery inherits that — `pyramid_shards`
declared with `shards: [..., 1mo, 1y]` enumerate as month-aligned and
year-aligned periods, not minute-aligned.

### Dependency order

`listMissingShards` returns gaps in an arbitrary order (typically
insertion order from `listExpectedShards`). Consumers wanting to fill
in dependency order (base tier rungs first, then coarser tiers,
within each tier smallest rung first) sort the result themselves —
pyrmts doesn't impose a fill strategy.

A helper could ship later:

```ts
function sortByDependency(
  pyramid: Pyramid,
  shards: ExpectedShard[],
): ExpectedShard[]
```

Sorts by `(tier_index, shard_dur_min, period_start)` — base tier
first, smallest rung first within each tier, earliest period first.
Defer until a consumer actually needs it.

## Acceptance

- `listExpectedShards` on a pyramid with `/1m: [5min, 1h]` over
  `[2026-06-01T00:00, 2026-06-01T01:00]` returns 12 × /1m@5min + 1 ×
  /1m@1h = 13 entries.
- `listMissingShards` after `shardIndex.recordShard(...)` for half the
  expected entries returns the other half.
- Calendar-handling: a `/1d: [1d, 1mo]` pyramid over a 2-month range
  returns the expected mix of 1d-aligned and 1mo-aligned entries.

## Out-of-scope follow-ups

- **Fill orchestration**: the actual "fill this missing shard" logic
  is consumer-side. ctbk's pyramid-cascade wraps it with its raw GBFS
  source reader; another consumer might wrap with a different source.
- **Watermark-aware filtering**: "only return gaps after watermark X"
  — consumers can post-filter the returned list.
- **Bulk re-build**: distinct from gap-fill; not in scope here.

## Cross-reference

- `~/c/pyrmts/specs/unified-shard-ladder.md` — ladder model gaps are
  computed against.
- `~/c/hccs/ctbk/specs/avail-v3-fsck-backfill.md` — ctbk consumer
  spec that will use this primitive.

## Resolution

Implemented 2026-06-30 (commit pending). 360/360 JS tests pass +
59/59 Python tests pass.

Deviations from the spec as drafted:

- **`listMissingShards` signature: added `pyramidName: string` arg.**
  Spec drafted it as `(pyramid, shardIndex, range, filter?)`. But
  `Pyramid` has no `name` field, and `ShardIndex.{getWatermarks,
  listShards}` are keyed by pyramidName — passing the index is not
  enough to identify which pyramid's recorded shards to read. Final
  signature: `listMissingShards(pyramid, pyramidName, shardIndex,
  range, filter?)`. Mirrors how consumers already call
  `ShardIndex.recordShard({ pyramidName, ... })`.
- **Added `ShardIndex.listShards(pyramidName): Promise<RecordedShard[]>`.**
  Spec said "`ShardIndex.listShards` or equivalent" — the interface
  had only `getWatermarks` + `recordShard`. Added `listShards` with a
  new `RecordedShard` type (mirrors `RecordShardInput` minus
  `pyramidName`). Throws when the impl was constructed with inventory
  disabled (`D1ShardIndex({ skipInventory: true })` or
  `ManifestShardIndex({ includeInventory: false })`) — silently
  returning `[]` would make every expected shard look "missing".
- **Conformance suite split: inventory-on vs inventory-off**. Added
  `ShardIndexConformanceOptions.inventory` (default `true`); when
  `true` the suite asserts list/round-trip behavior, when `false` it
  asserts `listShards` throws. Both `ManifestShardIndex` and
  `D1ShardIndex` now run conformance under both modes.
- **Extracted `substituteKey` to its own module** (`js/.../keys.ts`)
  rather than re-inlining. Used by planner + gap-discovery. The
  associated error-message change (`planQuery: missing key template
  value for {x}` → `substituteKey: missing value for {x}`) required
  small updates in `planner.test.ts` and `pyrmts-cfw/serve.test.ts`.
- **Python parity scope: `list_expected_shards` only.** Python has no
  `ShardIndex` port (deferred per `specs/done/python-unified-ladder.md`
  §Out of scope), so `list_missing_shards` is JS-only. When a Python
  query consumer materializes, it gets its own `ShardIndex` port + a
  `list_missing_shards` follow-up.
- **`sortByDependency` helper not added.** Spec called it out as
  deferrable; no consumer needs it yet.

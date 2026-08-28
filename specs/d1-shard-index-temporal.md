# `D1ShardIndex`: make windowed `listShards` seekable

Status: **proposed**. Written by the ctbk session (2026-08-28).

`listShards(pyramid, { range })` already pushes the window down into SQL — `WHERE pyramid = ? AND period_end > ? AND period_start < ?` — but the table `pyrmts-cfw` creates has no index that can serve it. The predicate is evaluated by scanning the pyramid's entire partition, so a per-request serving query pays the full inventory on every call.

## Measured on ctbk prod (2026-08-28)

A 1-hour window against `avail-v6` (14,524 registered shards):

```
SELECT COUNT(*) FROM pyramid_shards
WHERE pyramid='avail-v6' AND period_end > <now-1h> AND period_start < <now>
→ matched rows:     17
→ rows_read:    14,561      (857× amplification)

EXPLAIN QUERY PLAN
→ SEARCH pyramid_shards USING PRIMARY KEY (pyramid=?)
```

`pyramid` is the leading PK column so the seek lands in the right partition and then reads all of it: `PRIMARY KEY (pyramid, tier, shard_dur, period_start)` can't seek on `period_start` without `tier` *and* `shard_dur` pinned, and the serving path pins neither — it wants every tier in a time window.

The account-level effect: ctbk's D1 reports **175.7M rows read / 24h across 18,973 queries** (~9,300 rows/query). At ~14.5K rows per windowed `listShards` that is ≈12,000 serving calls — i.e. essentially *all* of the read volume is this one amplification. ~5.3B rows/month against the 25B included allotment, for 17 rows of answer.

## Why this is pyrmts' problem, not the consumer's

`pyrmts-cfw`'s `shard-index.ts` **owns the DDL** — it emits `CREATE TABLE IF NOT EXISTS pyramid_shards (...) PRIMARY KEY (pyramid, tier, shard_dur, period_start) WITHOUT ROWID` and creates no secondary index. It also owns the query. A consumer can't fix the plan without either hand-writing DDL that pyrmts will not know about, or bypassing `D1ShardIndex` entirely.

And the amplification isn't ctbk-specific: it's `O(shards in pyramid)` per windowed call for **any** consumer that serves from `listShards`. It stays invisible while a pyramid is small and becomes the dominant read cost as history accumulates — the worst failure shape, since nothing changes at the point where it starts costing. ctbk only noticed at 14.5K shards/pyramid; the amplification was there at 500.

## Contract

Add to the `ddl()` output, alongside the existing `CREATE TABLE`:

```sql
CREATE INDEX IF NOT EXISTS <shards>_period ON <shards> (pyramid, period_end)
```

Rationale for `(pyramid, period_end)` specifically:

- **`period_end > ?` is the selective half.** A serving window asks for recent data; `period_end > from` prunes to the tail. `period_start < to` is nearly always true across history, so indexing it first would prune almost nothing.
- **Near-covering for free.** On a `WITHOUT ROWID` table, secondary-index entries carry the PK columns, so this index already holds `(pyramid, period_end, tier, shard_dur, period_start)`. Only `key` and `written_at` need a table lookup — 17 of them in the measurement above, not 14,561.
- **Degrades to today, never worse.** A whole-history query (`from` = epoch) matches everything and scans, which is what it does now. Queries that *do* pin `tier` keep the PK path; SQLite picks per-query.

Write cost: one extra index entry per `recordShard`. ctbk writes ~14.5K D1 rows/day *total* across every table, so this is noise in both time and dollars — but it should be stated in the docstring, since D1 bills writes per row and a consumer with a hot ingest path deserves the number rather than a surprise.

## Migration

`ddl()` is the setup path, so new deployments get it automatically. Existing ones need the statement applied once — it is `CREATE INDEX IF NOT EXISTS`, so re-running `ddl()` is the migration, and it is safe to run against a live table (SQLite builds the index in one pass; at ctbk's 60K rows this is sub-second). Worth a line in the release notes: consumers who provisioned before this change must re-run `ddl()` or apply the statement by hand, or they keep the scan silently.

## Tests

- `EXPLAIN QUERY PLAN` for a windowed `listShards` asserts `SEARCH ... USING INDEX <shards>_period`, not `USING PRIMARY KEY`. Assert on the exact plan string — a plan regression is the whole failure mode this spec exists to prevent.
- Fixture with shards spread across a wide history: a narrow recent window returns exactly the expected shard set, byte-identical to the pre-index result. The index must change only the plan.
- `tier`-pinned query still returns the same rows (planner may choose either index; correctness is what's asserted).
- Whole-history window returns everything (the degenerate case).
- `ddl()` run twice is a no-op (the `IF NOT EXISTS` path).

## Non-goals

- Caching `listShards`. `CachedShardIndex` deliberately passes it through — the comment says gap-discovery callers are infrequent (fsck/audit), which was true when written but isn't how ctbk's serving path uses it. Whether the passthrough should change is a **separate** question, and it should be decided after this index lands: a seekable query reading 17 rows may simply not be worth caching, and adding a TTL over stale inventory has its own freshness cost.
- Changing the `pyramid_shards` PK. The clustered order is right for the write path and for tier-pinned reads; this is an additive index.
- The `rg_manifest` access pattern, which already has `(pyramid, key, cell_min, cell_max)` and is not implicated in the measurement above.

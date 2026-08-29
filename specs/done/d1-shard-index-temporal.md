# `D1ShardIndex`: make windowed `listShards` seekable

Status: **done (2026-08-28)** — implemented `a7a33e0`, validated by ctbk on `pds l` links, pushed to `r/main`, dist `612f144e79fe5573baca9d3e785b42255e05d69a`. Written by the ctbk session (2026-08-28).

Implementation notes: the DDL entry point is `D1ShardIndex.schemaSql()` (this spec's `ddl()`). The index statement is emitted after the shards `CREATE TABLE` (and respects `shardsTable` overrides: `<shards>_period`); `skipInventory` omits it along with the table. Tests landed in two layers: the existing mock-based file asserts the emitted DDL strings, and a new `shard-index.sqlite.test.ts` runs `D1ShardIndex` against a real SQLite (`node:sqlite`, no new dep) — the windowed-`listShards` `EXPLAIN QUERY PLAN` asserts the exact plan `SEARCH pyramid_shards USING INDEX pyramid_shards_period (pyramid=? AND period_end>?)`, plus the fixture/tier-pinned/whole-history/re-run-`schemaSql()` cases below, with the narrow-window result additionally compared byte-identical against the same query after `DROP INDEX`. ctbk already applied the statement to prod by hand (14,561 → 22 rows read on the 1-hour-window measurement), so its re-run of `schemaSql()` will hit the `IF NOT EXISTS` no-op path.

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

## Outcome

- **pyrmts**: `a7a33e0` (2026-08-28). `main` = `72f2552` on `r/main`; `build-dist.yml` green (run 33222611136). Dist pin: `"pyrmts-cfw": "https://github.com/runsascoded/pyrmts#612f144&path:/js/packages/pyrmts-cfw"` (`pyrmts` at the same SHA — `pyrmts-cfw` carries `pyrmts@workspace:*`, so the two swap together; linking `pyrmts-cfw` alone fails `ERR_PNPM_WORKSPACE_PKG_NOT_FOUND`).
- **ctbk validation** (on `pds l` links, before the push): api 207 + cascade 113 + `pyrmts-cfw` 93 tests green, `tsc` clean on both workers, including the new `shard-index.sqlite.test.ts`.
- **ctbk prod** had already applied the statement by hand, which is where the confirming measurement comes from: `SEARCH pyramid_shards USING PRIMARY KEY (pyramid=?)` matched 17 / read 14,561 → `SEARCH … USING INDEX pyramid_shards_period` matched 21 / read 22 (**662×**). Index build: 60,591 rows, 63 ms, +4 MB. Their `gbfs/d1/schema.sql` records the hand-applied statement pointing back here; re-running `schemaSql()` is the `IF NOT EXISTS` no-op that reconciles it.
- **Follow-on the measurement surfaced** (not this spec's job): nothing *runs* `schemaSql()` as a migration against an existing deployment, so a DDL change upstream reaches prod only if a human remembers. ctbk offered to spec a migration runner for `pyrmts-ops`; see that spec if it lands.

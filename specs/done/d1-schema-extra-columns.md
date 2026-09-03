# D1 `verifySchema`: register consumer-added columns instead of flagging them as drift

Status: **done** (2026-09-02). Implemented pyrmts-side this session; requested by the awair session (its `/health` schema badge was showing a false-positive drift). Extends [`d1-schema-drift.md`](./d1-schema-drift.md).

## The gap

`verifySchema` (`D1ShardIndex.verifySchema`, and the Python twin `pyrmts.d1.verify_schema`) diffs a live D1 against `schemaObjects()`. For a **table** it compared columns as **exact set-equality** — `a.length !== e.length || …`. That is stricter than [`d1-schema-drift.md`](./d1-schema-drift.md) intended: that spec planned for a consumer adding its own **tables** ("consumer tables are the consumer's") and said a table should "compare as a **set** of columns" — but the implementation compared as *the same* set, not a *superset*.

awair extends pyrmts's own `pyramid_shards` table with five app-owned columns — `n_rows`, `n_rgs`, `rg_row_counts`, `size_bytes` (migration `0003_shard_stats.sql`) and `footer_bytes` (`0004_footer_cache.sql`) — cascade writes them, serve reads them (`cfw/serve/src/index.ts`). Those columns are entirely outside `D1ShardIndex`'s concern: its seven index columns are all present and correct. But exact set-equality on 7-vs-12 columns flagged the table as `mismatched`, so `/health` had shown `schema drift` continuously since awair first wired in `verifySchema` (`fcb6263`, Aug 29). A false positive — nothing was broken.

## The choice: register, don't blanket-tolerate

Three options were on the table:

1. **Subset check** — accept any extra column on a table. Simplest, but it discards the invariant the whole drift feature is about: an unregistered/typo'd column should be *noticed*, not silently tolerated.
2. **Side table** — move awair's five columns to `pyramid_shard_stats`. Keeps `pyramid_shards` byte-pure to pyrmts, but it's a real migration plus cascade-write / serve-read rewrites, for no functional gain.
3. **Register the extras** (chosen) — the consumer declares its app-owned columns; `verifySchema` unions them into the expected set, then keeps the exact check. A live column that is *neither* expected *nor* registered still reads as drift.

#3 keeps a strict gate (catches both a missing expected column and a stray/typo'd one) while letting a consumer's legitimate extras through — and it is truer to the drift spec's framing: extras aren't "ignore whatever's there," they're a **declared** part of the consumer's contract.

## Contract

A new option, twinned across both languages, consulted only by `verifySchema`:

- **TS** — `D1ShardIndexOptions.extraColumns?: Record<string, string[]>` (resolved table name → app-owned columns). Table branch: `e = [...o.columns, ...(opts.extraColumns?.[o.name] ?? [])].sort()`, unchanged exact compare against the sorted live columns.
- **Python** — `verify_schema(..., extra_columns: dict[str, tuple[str, ...]] | None = None)`. Table branch: `expected_cols = set(o.columns) | set(extra_columns.get(o.name, ()))`.
- **CLI** — `pyrmts-ops d1 verify -x/--extra-column TABLE:COL[,COL…]` (repeatable, merges repeats of a table), so a CI gate agrees with what serve's `/health` reports. Malformed spec → exit 1 with `d1 verify: expected TABLE:COL[,COL…], got …`.

Keys are the **resolved** table name (post-`shardsTable`/`watermarksTable` override), matching how `o.name` is resolved. `schemaObjects()` / `schema_sql()` are untouched — extras widen only what `verify` *accepts*, never what pyrmts *emits*.

## Tests

- **TS** (`shard-index.sqlite.test.ts`, real SQLite): `ALTER TABLE` in awair's four stats/footer columns → registered `extraColumns` verifies clean; the same DB with no registration still `mismatched`; a further stray column is caught even with the four registered.
- **Python** (`test_d1.py`): the five-column `pyramid_shards` extension → registered passes, unregistered flags, a stray beyond the registered set still flags.
- **CLI** (`test_d1_cli.py`): `-x` registered → exit 0 `schema up to date`; partial registration → exit 1 with the JSON diff; malformed `-x` spec → exit 1.

Full suites green: JS 547, Python 170.

## awair adoption (their repo)

Once a dist bundling this lands and awair pins it:

1. serve passes the list in `buildHealthSnapshot`:
   ```ts
   D1ShardIndex.verifySchema(env.DB, {
     extraColumns: { pyramid_shards: ['footer_bytes', 'n_rgs', 'n_rows', 'rg_row_counts', 'size_bytes'] },
   })
   ```
2. `pyrmts-ops d1 verify` (if wired into awair CI) gets the matching `-x pyramid_shards:footer_bytes,n_rgs,n_rows,rg_row_counts,size_bytes`.
3. Kill the duplication between that list and migrations `0003`/`0004` — source both from one shared const (e.g. `cfw/shared/`), or at minimum cross-reference the migrations so a future sixth column gets registered too.

The `/health` badge then reads `schema ✓` (awair's `pyramid_shards_period` index is already applied, so no other drift remains).

## Non-goals

- No change to index verification — order stays load-bearing.
- pyrmts does not learn about specific consumer columns; `extraColumns` is opaque to it.
- No auto-discovery of "columns that look app-owned" — registration is explicit on purpose.

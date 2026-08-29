# D1 schema drift: pyrmts owns the DDL, so pyrmts should emit, verify, and apply it

Status: **implemented pyrmts-side (2026-08-28)**; awaiting consumer validation (`pds l` for the TS half, editable install for the Python half) before push. Written by the pyrmts session while scoping the IaC question ctbk raised in `specs/done/d1-shard-index-temporal.md` — see `specs/iac-boundary.md` for the other half of that answer.

## The gap

`D1ShardIndex.schemaSql()` is the only DDL emitter in the repo. It defines two tables and (since `a7a33e0`) one index, and **nothing runs it against an existing deployment**. New deployments get the schema from setup; everything after that reaches prod only if a person remembers.

That is not hypothetical, and it isn't one consumer's sloppiness — both consumers independently built a hand-maintained copy, and both copies are wrong right now:

- **awair** — `cfw/cascade/migrations/0001_shard_index.sql` (2026-07-09) transcribes the two `CREATE TABLE`s and says so in its header: *"Schema copied verbatim from `pyrmts-cfw` `D1ShardIndex.schemaSql()` … Keeping the statements identical avoids drift with pyrmts library assumptions — **update this file if that method's output changes**."* The method's output changed today. The file has not, so awair's D1 is missing `pyramid_shards_period`.
- **ctbk** — `gbfs/d1/schema.sql:87` records the index they applied to prod by hand, with a comment that names the problem outright: *"`pyramid_shards` itself is created by pyrmts-cfw's `D1ShardIndex.ddl()`, **not here** … Applied by hand to prod 2026-08-28; kept here so a re-provision doesn't silently lose it … at which point this becomes a redundant no-op rather than **drift**."*

Two consumers, two mechanisms (a wrangler migration file; a `schema.sql` nothing reads), one shared property: **a comment asking a human to notice.** The instruction is the bug. A library that hands out DDL and no way to check it has outsourced its own invariant to memory.

There was also a plain asymmetry. Python **writes** these tables — `pyrmts.d1.register_shard`, `pyrmts_engine.materialize.emit_d1_insert_sql`, `pyrmts_ops.gc.D1GcRegistry` — while having no DDL at all, so the build side could populate a schema it could neither create nor inspect.

## Contract

Three verbs, twinned across both languages, over the objects the index needs rather than over opaque SQL text.

**Describe** — `schema_objects()` / `D1ShardIndex.schemaObjects()` return `{name, kind, sql, columns}` per object: two tables and the `(pyramid, period_end)` index, in dependency order, honoring `watermarksTable`/`shardsTable`/`skipInventory`. `schema_sql()` / `schemaSql()` project the `sql` field, byte-for-byte as before (the existing tests pin those strings, and the two languages are asserted against the same literals — edit one side's DDL without the other and that side's test fails).

**Verify** — `verify_schema()` / `D1ShardIndex.verifySchema(db)` diff a live database, read-only, returning `{ok, missing, mismatched}`. Existence comes from `sqlite_master`, columns from `PRAGMA table_info` / `PRAGMA index_info` — all three supported by D1 (`PRAGMA page_count` and `dbstat` are not, and aren't needed). A table compares as a **set** of columns; an index compares **in order**, because `(period_end, pyramid)` cannot serve the seek `(pyramid, period_end)` serves and must not read as equivalent.

**Apply** — `apply_schema()` runs every statement. Each is `IF NOT EXISTS`, so applying is the migration and re-applying is a no-op — including against the two consumers who already applied the index by hand.

**CLI** — `pyrmts-ops d1 {schema,verify,apply}` (the package's first CLI; shape follows `pyrmts-engine`'s). `schema` prints a runnable `;`-terminated script for redirecting into a migration file; `verify` exits **1** on drift so it works as a CI or deploy gate, with `-j/--json` for machines; `apply` takes `-n/--dry-run`. Credentials are the ones `pyrmts.d1` already reads.

## Non-goals

- **Owning migration numbering.** wrangler tracks applied files in `d1_migrations` and consumers interleave their own tables between pyrmts' (awair's `0002_devices.sql` … `0004_footer_cache.sql`). pyrmts emits statements; the consumer decides which file they land in. `pyrmts-ops d1 schema > migrations/00NN_pyrmts_schema.sql` is the whole integration.
- **Replacing `wrangler d1 migrations`.** Where a consumer already has that (awair), it stays the applier; `verify` is what closes their gap. `apply` exists for the consumers who don't (ctbk applies by hand today) and for headless runners with no wrangler.
- **Applying DDL at runtime.** A Worker reconciling its schema on cold start would put DDL-capable credentials in the serving path and re-run on every isolate. `verifySchema` is deliberately read-only so it can be surfaced from an existing health endpoint — reporting drift is the serving-path job; fixing it is a deploy-path job.
- **General-purpose migrations.** Only the objects pyrmts owns. Consumer tables are the consumer's.

## Tests

- **Python** (`pyrmts/tests/test_d1.py`, +9): the three DDL literals; `skip_inventory`; custom table names propagating into both the DDL and the derived index name; `quote_ident` escaping; verify against a clean database asserting the **exact** query sequence; verify reporting the absent index (the state both consumers are in); a table missing a column; an index on reversed columns; apply running every statement with no params.
- **TypeScript** (`shard-index.sqlite.test.ts`, +7): all verify cases against **real SQLite** via `node:sqlite`, since the value of `verifySchema` is agreeing with what SQLite actually reports through `sqlite_master`/PRAGMA rather than with a mock of it. Clean, empty, pre-index, wrong-column-order index, short table, `skipInventory`, custom names.
- **CLI** (`pyrmts_ops/tests/test_d1_cli.py`, 8): emitted script shape, `skip_inventory`, custom names, clean verify + exact wire calls, drift exit code, `--json` body, apply call sequence, dry-run touching nothing.
- Cross-language byte-identity of `schema_sql()` vs `schemaSql()` was checked directly while implementing (`node -e` against the built dist vs the Python function): equal, including after the `schemaObjects` refactor.

## Adoption

1. **awair** — apply the missing index: `pyrmts-ops d1 schema > cfw/cascade/migrations/0005_pyrmts_period_index.sql` (or copy the one statement), `wrangler d1 migrations apply awair-cascade --remote`. Then drop the hand-transcription warning from `0001_shard_index.sql`'s header and point it at `pyrmts-ops d1 verify` instead.
2. **ctbk** — `pyrmts-ops d1 verify` should already report clean (they applied the index by hand). `gbfs/d1/schema.sql`'s pyrmts block becomes deletable once verify runs somewhere; their own tables stay.
3. **Either** — wire `verify` into CI or the deploy script, and/or surface `D1ShardIndex.verifySchema` as a line in the existing `/health` payload. The point is that *something* notices; which something is the consumer's call.

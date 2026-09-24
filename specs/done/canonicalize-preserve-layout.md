# `canonicalize_shards`: preserve the shard's sort + row-group layout

Status: **done** (2026-09-24; proposed the same day from ctbk's rides re-key validation, where it blocked P4: canonicalized `rides/` shards blew the CF Worker CPU limit, error 1102, on full-range queries). Landed as option 1 (footer stamp) + option 3 (legacy inference) + explicit overrides; see "Landed" at the end.

## The bug

`canonicalize_shards` (`python/pyrmts/src/pyrmts/canonicalize.py`) rewrites each shard with a bare

```python
pq.write_table(out, buf, compression='snappy')
```

so the output has:

1. **One row group per shard.** `pq.write_table`'s default `row_group_size` (1 Mi rows) swallows any real shard: ctbk `rides/start/1mo/16y/2000.parquet` went from the engine's 2048-row RGs (39 RGs in the `rides-v5` equivalent) to **1 RG × 112,598 rows**. Every read then decodes the whole shard, and RG-manifest / min-max pruning can't skip anything. ctbk dev `/api/rides` p50 is 1.65 s vs 0.29 s for `rides-v5` over 2024, and full-range single-station queries hit 1102.
2. **Broken sort order.** `recanonicalize_table` returns `pa.concat_tables([raw, canon])`: the `c:` rows are appended after all raw rows (sorted among themselves only). The build wrote the shard sorted by its configured sort columns (ctbk: `-s cell,dt,gender,user_type,bike_type`), and read-side pruning relies on that order.
3. Other writer defaults may differ from the build's `write_tier_parquet` call as well, e.g. anything `write_tier_parquet` gains later.

## Fix

Canonicalize must write with the **same layout the build used**:

- **Route the write through `write_tier_parquet`**, as the engine's `_write_shard` does, so there's one writer and layout can't drift.
- **Layout source of truth:** the build knows `sort` and `row_group_size` (CLI `-s` / `-g`, or per-tier `rg_size_for`), but canonicalize doesn't. Options, in order of preference:
  1. **Stamp the layout into each shard's parquet key-value metadata at build time**, e.g. `pyrmts.sort=cell,dt,…` and `pyrmts.row_group_size=2048`. Canonicalize (and any future in-place rewriter) reads it back from the input footer. Self-describing, so no config plumbing is needed.
  2. Declare it in the pyramid config (`layout: { sort: [...], rowGroupSize: 2048 }`, per tier optionally) and have both build and canonicalize read it. This is also good: it removes the CLI-only `-s`/`-g`.
  3. Fallback when neither exists (legacy shards): infer. Take `row_group_size` from the input's first RG `num_rows`, and the sort from the pyramid's declared dims (`_default_sort_cols`), or require a `--sort` CLI arg.
- `recanonicalize_table` itself can stay "raw ∪ canon, unsorted". The writer sorts.

## Tests (TFFP)

- Build a small shard with an explicit `sort` + `row_group_size=4` (several RGs), canonicalize it, then assert:
  - the output's RG count and per-RG `num_rows` equal what `write_tier_parquet(…, row_group_size=4, sort=…)` produces for the same logical table
  - rows are globally sorted by the sort columns, with `c:` rows interleaved at their sorted position, not appended
- The same test must fail on the current code (1 RG, `c:` rows at the end).

## ctbk follow-up once released

Bump pyrmts in ctbk → re-run `ctbk gbfs engine canonicalize -C rides-{start,end}` on `e` → `ctbk gbfs lambda reconcile -C rides-<a> -f` → `ctbk gbfs manifest backfill`, which re-does today's backfill (~$1.40 of D1 writes). Re-running over shards that already carry `c:` rows is safe: `recanonicalize_table` drops existing `c:` rows before rebuilding them. Or, since the build layout is fine, fold that re-run into a rebuild if content-addressed keys (`content-addressed-shards.md`) land first.

## Landed

- `write_tier_parquet` stamps its effective layout into the parquet key-value metadata: `pyrmts.sort` (comma-joined sort columns, after dropping absent ones) and `pyrmts.row_group_size`. Every engine-written shard is now self-describing; `read_layout(metadata | schema)` → `ShardLayout(sort, row_group_size)`, or None for a legacy shard. Both exported from `pyrmts`.
- `canonicalize_shards` writes through `write_tier_parquet` with `shard_layout(pf, pyramid, sort=, row_group_size=)`: explicit overrides win, else the footer stamp, else (legacy) the first row group's size + `_default_sort_cols(pyramid)`. `recanonicalize_table` is unchanged (raw ∪ canon, unsorted; the writer sorts, so `c:` rows land at their sorted position).
- CLI `pyrmts-engine canonicalize` gains `-g/--rg-size` and `-s/--sort` (same spellings as the build command).
- Tests (TFFP, both failed on the old code: one 16-row group, bytes differ): `test_canonicalize_preserves_the_build_layout` (a `write_tier_parquet` shard with a non-default sort and 4-row groups canonicalizes to the byte-identical output of `write_tier_parquet(recanonicalize_table(...))` with the same layout: `[4, 4, 4, 4]` groups, globally sorted, `c:` interleaved), `test_canonicalize_infers_layout_for_legacy_shards_and_honours_overrides`, and the writer's stamp round-trip.
- Option 2 (a `layout:` config block) was not needed for this and stays open; the stamp makes any future rewriter (content-addressed re-keys, consolidate) layout-preserving for free.

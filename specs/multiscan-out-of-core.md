# Multi-scan intervals at fleet scale: streaming output, key-range parallelism, one-scan append

Status: **Phase 1 landed and accepted** (2026-10-08 / disky switchover 2026-10-09, see "Phase 1 — landed" below); **Phase 2 open**, waiting on disky's daily-delta format. From disky's static name search.

## Origin

disky (`runsascoded/disky`, branch `ch-store`) rebuilt a GCS fleet's full version history: 70 daily scans → 1,153,480,980 opened and 545,152,608 closed versions. That took 38 min on 32 spot Batch tasks, and the result is equal to an independent ClickHouse build on every scan. It uses pyrmts' gaps-and-islands kernel (`pyrmts_engine.multiscan_duckdb._intervals_sql`), but had to **restate** it in `cloud/src/dt_cloud/static_names.py` (`islands_sql`, pinned to pyrmts by `test_islands_equal_pyrmts`), because:

1. `consolidate_parquet_duckdb` returns an in-memory `MultiScan` (`to_arrow_table()`), and `scan_digest` reads each scan whole (`pq.read_table`). Neither fits 1.15B rows.
2. Parallelism needs the key space cut into ranges, with each range built independently (256 ranges → 32 tasks). pyrmts has no notion of this.
3. The daily job appends **one** scan to existing intervals. pyrmts only consolidates a full scan list.
4. The job image ships `pyrmts` but not `pyrmts-engine` (or its polars dependency), and the kernel lives in the engine package.

pyrmts already has the matching policy layer: `multiscan seal` with `scheme: exponential` (Bentley–Saxe / LSM leveling). What's missing is this mechanism underneath it, at scale.

## Phase 1 — the interval kernel as a streaming, partitionable, appendable library API

### 1.1 Public kernel, no heavy dependencies
- Export the gaps-and-islands SQL builder publicly, from a module importable with only `duckdb` (no polars/pyarrow-heavy engine imports). That means core `pyrmts` with an optional `duckdb` extra, or a slim `pyrmts-engine[duckdb]`. Pick one; disky's job image will depend on it, and disky then deletes `islands_sql`.
- Inputs are generic: a `long` relation `(__scan, *key_cols, *state_cols)`. The kernel must not depend on a `Pyramid`. The `Pyramid`-based `_cols` stays a thin adapter on top.
- **Change columns vs carried columns.** A run should be able to open only when the *change* columns differ, while *carried* columns ride along with a stated policy (`first` | `last` within the run).
  - disky needs this now: name answers depend only on `size`/`n_files`, but rows also carry `last_read` / mean-mtime, which churn daily and inflate versions.
  - The default (all state columns are change columns) is today's behavior.
  - If the carried policy is `last`, appending a scan rewrites the carried values of open runs. Document that.
- Optional sort: the final `ORDER BY` stays opt-in, because disky re-sorts downstream and the sort is the expensive part.

### 1.2 Streaming output
- `write_intervals(con, sources, key_cols, state_cols, out, *, row_group_size, sort=None, ...)` runs `COPY (...) TO 'out.parquet'`, never materializing in Python.
- Per-scan digests are computed in DuckDB (an order-insensitive aggregate hash over the canonical row encoding), not by `pq.read_table`.
- **Scan index → observation stamp.** Optionally map `__scan_lo/__scan_hi` to `(vf, vt)` stamps: `vf` is the run's first scan's stamp; `vt` is the stamp of the first scan *after* the run, or an `OPEN` sentinel if the run is still open at the last scan. disky stores `[vf, vt)` this way, and readers filter `vf ≤ D < vt`.

### 1.3 Key-range partitioning
- `plan_ranges(sources, key_cols, k)` returns `k` contiguous, disjoint key ranges of roughly equal row count, from a sample or histogram of the newest scan. It is deterministic (same inputs → same ranges) and serialized as JSON.
- `write_intervals(..., range=r)` builds one range: every scan's read is predicate-pushed to the range, so ranges run in independent processes or machines. The union of the range outputs is exactly the full build.
- disky's reference: `static_names.py` `ranges` + `intervals` (256 ranges over `(depth, path)`).

### 1.4 One-scan append
- `append_intervals(prev, new_scan, ...)` takes the previous interval files and one new scan, and writes the next interval files.
  - Closed runs are copied unchanged. Only open runs (`scan_hi == last`) can extend or close; keys new in the scan open runs.
  - Its output is **byte-identical** to a full rebuild over all scans. disky already tests this property (`append` vs rebuild, md5-equal on real ranges).
  - It also emits the scan's **delta**: rows opened and closed by that scan, which is what downstream daily deltas consume.
- Per range, like 1.3.

### Acceptance
- disky's `static-names` intervals stage switches to the pyrmts API with no restated kernel. It must produce byte-identical outputs on generation `2026-10-08` (per-range md5) and keep `append == rebuild`.
- pyrmts' own tests: the Python fold stays the oracle for the kernel; add change/carried coalescing, range-union-equals-whole, and append-equals-rebuild.

### Phase 1 — landed

`pyrmts.intervals` in **core** `pyrmts`, with a new `pyrmts[duckdb]` extra. It imports only pyarrow (and duckdb at call time): no polars, no engine, no `Pyramid`. disky's job image can depend on `pyrmts[duckdb]` alone.

- **1.1 Kernel:** `islands_sql(long_sql, key_cols, state_cols, *, carried=None, scan_col='__scan', order=False)`. Output `(*key_cols, *state_cols, __scan_lo, __scan_hi)`. `carried={col: 'first' | 'last'}` columns never open a run; values via `arg_min` / `arg_max` over the scan index. Change columns use `any_value` (equal within a run). `order=True` appends `ORDER BY key_cols, __scan_lo`; off by default. `long_sql(sources)` builds the `__scan` union. `pyrmts_engine.multiscan_duckdb._intervals_sql` is now a thin adapter (kernel + the `Pyramid`'s canonical sort), and its byte-identity tests against the Python fold pass unchanged.
- **1.2 Streaming output:** `write_query(con, sql, out, schema, *, row_group_size, sort, compression, dictionary)` streams DuckDB batches into `write_exact_row_groups` (disky's `write_sorted`, lifted): exact row groups, so bytes depend only on rows. Stamps: `stamped_sql(runs, key_cols, state_cols, stamps, open_stamp)` → `(*keys, vf, vt, *state)`. Digests in DuckDB: `relation_digest(con, sql, cols)` and `interval_digests(con, table, open_cols, close_cols, open_stamp)` (disky's per-stamp opened/closed format; `md5_number_upper` over `col::VARCHAR` joined by `|`). Not done: `consolidate_parquet_duckdb` still builds an in-memory `MultiScan` with pyarrow digests; it is the small-scale `Pyramid` path, and fleet scale now goes through `pyrmts.intervals`.
- **1.3 Ranges:** `plan_ranges(footers: [(FileMetaData, weight)], range_cols, k, *, floor=None)`, generalized from disky: a row group is a cut candidate when every range column but the last is constant in it. Footer fetching stays the caller's (disky reads GCS footers with two ranged reads). `key_range_pieces(range_cols, lo, hi)` → conjunctive predicates for any number of columns; read each piece separately and `UNION ALL`, so each prunes by row-group stats.
- **1.4 Append:** `append_intervals(con, prev, new, key_cols, state_cols, stamp, open_stamp, *, carried=None)` creates the next intervals table (prev's column order) and returns `(opened, closed)`. `delta_sql(table, stamp)` gives the scan's delta (`op` = 1 opened, −1 closed). Carried `last` rewrites the carried values of continuing open runs (documented). Refuses a stamp at or below the intervals' newest `vf`.

**Verified against disky's code** (scratch harness, not committed): disky's own fixture (`cloud/tests/test_static_names.py`, v1 + v2 scans) run through disky's `plan_ranges` / `build_range` / `digests` / `append_range` vs the same stages composed from `pyrmts.intervals`. Ranges JSON equal at k = 1, 4, 7; every per-range interval file **md5-equal**; digests equal; appended intervals and delta files md5-equal. disky keeps its per-scan source parsing (`V1_SELECT` / `V2_SELECT` / `MERGED`) and swaps its `Piece`s for `key_range_pieces` predicates.

**Tests:** `pyrmts/tests/test_intervals.py` (13): kernel vs a sequential-ingest oracle with no / `first` / `last` carried columns, carried columns never opening a run, argument checks, `key_range_pieces` vs brute force (union exact, pieces disjoint), `key_range_terms` structure + ClickHouse rendering, plan-ranges contiguity + determinism + range-union byte-identical to a whole build, append byte-identical to a rebuild (all three carried modes) with exact delta, exact row groups independent of batch chunking, digests order-insensitive and split-additive. Python 466 passed; the core suite also passes on pyarrow 21 without polars.

**Acceptance — met (disky `ch-store` 98ec30a4, on pyrmts 4365396):** `static-names` intervals / append run on `pyrmts.intervals`. Real generation `2026-10-08`: `ranges.json` md5-equal; full rebuilds of 12 ranges (incl. r0, r255, and the largest, r41 at 63.4M rows) md5-equal to the stored intervals, hist and digests; appending 10-08 byte-identical to the rebuild on all 12 (6 with real churn, e.g. r85 294,864 opened / 290,697 closed). disky's coalesced intervals (`2026-10-08c`, versions only on `size` / `n_files` change) are reproduced md5-equal by `islands_sql` over just those two state columns (no `carried` needed). disky deleted `islands_sql` and `test_islands_equal_pyrmts`.

**Follow-ups from the integration:**
- `key_range_terms` returns the pieces as structured `(column, op, value)` conjunctions; `key_range_pieces(..., ident=, lit=)` renders them, with `ident_clickhouse` / `lit_clickhouse` (backslash-escaping) alongside DuckDB's defaults. disky can drop its `Piece`.
- `islands_sql` projects its input to `(scan, *key_cols, *state_cols)` explicitly. On DuckDB 1.5.5 the optimizer already pruned unused columns through `SELECT *` and the `long_sql` union (identical `EXPLAIN`), so this is robustness, not a memory fix.
- Memory: DuckDB exceeded `memory_limit` on wide 70-scan unions (disky: 59 GB RSS at a 36 GB limit; OOM under a 30 GB cgroup at 16 GB). Documented on `islands_sql`: run under a hard cap and size ranges to fit it. The plan has two full `WINDOW` sorts plus a hash aggregate; which operator overshoots is not yet measured.
- `append_intervals` documents that `prev` is read three times (newest `vf`, open rows, full copy), so callers should materialize remote or computed `prev`.

## Phase 2 — sorted-run merge (after disky's daily deltas exist)

disky's daily updates add a small sorted delta per scan and merge deltas under a tiered (binary-counter) policy. That's the same shape as `scheme: exponential` and as `engine-incremental-consolidation.md` Direction 1 (k-way merge of already-sorted shards):

- **Mechanism:** a streaming k-way merge of parquet runs that share a sort key. Resident memory is about one row group per input plus one output row group. It takes the output row-group size and an optional per-group sidecar `(min, max, offset, length, rows)`.
- **Combine hook for equal keys:**
  - disky: delta *close records* (`vt` set) override the base's open rows.
  - pyrmts engine consolidation: disjoint inputs (no-op), possibly zero-decode row-group concatenation (the existing design card).
- **Policy:** reuse the Bentley–Saxe scheme from `MultiScanPolicy` to decide which runs merge when.

The API shape here depends on disky's delta file format (disky "step 3"). Spec it once that format is committed; this section records the intent so the two efforts share one implementation.

## Consumers
- **disky:** `dt_cloud/static_names.py` (fleet name search: intervals → suffix postings, daily append), `dt_cloud/overtime.py` (over-time index).
- **pyrmts:** `multiscan seal` / consolidation at fleet scale, and `engine-incremental-consolidation.md` (Phase 2's merge).

# Multi-scan intervals at fleet scale: streaming output, key-range parallelism, one-scan append

Status: **proposed** (2026-10-08, from disky's static name search). Phase 1 is ready to implement: its shape comes from code that already runs at full scale in disky. Phase 2 waits on disky's daily-delta format.

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

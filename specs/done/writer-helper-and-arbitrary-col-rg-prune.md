# Spec: `write_tier_parquet` helper + arbitrary-column RG pruning

> Status: **done** (2026-05-26). Shipped both halves; ctbk's avail v2
> and shadow paths can both migrate.
>
> ## Resolution
>
> - **§1 `write_tier_parquet`** (`python/pyrmts/src/pyrmts/writer.py`):
>   Sorts by `(bin_col, *dims, geo.cellCol)` by default; default
>   `row_group_size = max(4096, min(16384, total_rows // 100))`; default
>   `compression='snappy'` (per the hyparquet/ZSTD note). Accepts
>   `Iterable[Row]` or `pa.Table`; writes to file-like or path. Returns
>   bytes written. Tolerates sort cols missing from the actual table
>   schema (skips them) so dim-declaring pyramids don't break on rows
>   that don't populate every dim. **`pyramid` is optional** — callers
>   without a Python `Pyramid` declaration can pass `sort=[...]`
>   explicitly (added after ctbk feedback: declaring a Python Pyramid
>   just to mirror the TS pyramid was meaningful new wiring for a pure
>   refactor; `sort=` keeps the API usable without it). 15 tests covering
>   sort order, RG sizing clamps, per-RG bin-col stats tightness, snappy
>   compression, geo cell ordering, pa.Table input, overrides, path
>   output, empty rows, missing-dim schema tolerance, pyramid-less
>   invocations, and the error cases for missing `out` / `sort`.
> - **§2 Arbitrary-column RG pruning** (`js/packages/pyrmts/src/fetch.ts`):
>   `FetchOptions.filters: ColumnFilter[]` plumbed through. Refactored
>   `selectRowGroupRuns` around a generic `RgPredicate` interface;
>   `binCol+range` and each filter contribute one predicate; they AND.
>   Per spec, missing stats / missing columns default to "must read"
>   (no silent data omission). New `decodeArbitraryStatValue` handles
>   string + numeric stats for non-timestamp filter columns. 8 tests
>   covering value-set / numeric-range / multiple-value matches,
>   no-match → empty, filter AND binCol range, missing-column
>   fallback, multi-filter AND, byte savings observable.
> - **TS port of `write_tier_parquet` deferred** (per spec's §1 non-goals).
> - **Histogram-aware filters deferred**; current filters are
>   numeric/string only.
>
> ## Migration after this lands
>
> - ctbk avail v2 writer (`ctbk/avail_v2.py:296`): replace `pq.write_table(...)`
>   with `write_tier_parquet(table, out=buf, sort=['dt', 'cell'])`. No
>   Python `Pyramid` declaration needed; `sort=` is explicit. Then
>   `ctbk avail-v2-build --force` to rewrite R2 shards. Unblocks v2 OOM.
> - ctbk shadow (`gbfs/api/src/avail_pyrmts.ts:251`): pass
>   `filters: [{ col: 'station_id', values: p.filterStationId }]` to
>   `fetchShardData`. Existing `(station_id, dt)`-sorted shards become
>   RG-prunable without a data rewrite.
>
> Motivated by ctbk's avail v2 pyramid hitting CFW memory limits on
> sub-day queries (`~/c/hccs/ctbk/specs/avail-pyramid-v2.md` §7) and the
> shadow-mode pyrmts call OOMing on station-filtered availability queries.
>
> Closes SPEC.md Open questions §321 ("Row-group sizing") and §323 ("Sort
> order"), plus introduces an adjacent reader-side extension (arbitrary-
> column RG pruning) needed for non-`binCol`-sorted pyramids.

## Motivation

Two ctbk consumers blew the CFW free-tier memory limit (128 MB) trying
to read pyrmts shards that were correct in content but unfriendly in
layout:

### (1) ctbk avail v2 — `avail-v2/<tier>/<period>.parquet`

`ctbk/avail_v2.py` wrote shards with pyarrow defaults:
- `num_row_groups = 1` (default `row_group_size = 1M`; v2 1h shards
  have ~956K rows, so they fit in a single RG).
- Rows sorted `(cell, dt)`.

`pqm` output:
```
num_row_groups: 1, num_rows: 956114, total_byte_size: ~130 MB
```

CFW reads via pyrmts → hyparquet receives `binCol='dt', range=<24h>` →
walks the one RG → its `dt`-stats span the whole month → no skip → decode
entire shard → OOM.

### (2) ctbk avail shadow — `avail/agg/<tier>/<period>.parquet`

Cascade-compactor shards (Python writer pre-pyrmts) for the legacy
`/api/totals` path:
- `num_row_groups: 201` ✓ (healthy granularity, ~3266 rows each).
- BUT rows sorted `(station_id, dt)` — every RG spans the full day's
  `dt` range.

`pqm` output:
```
RG[ 0] dt min=1779404400 max=1779490800   station_id min=00284700... max=01839a78...
RG[ 1] dt min=1779404400 max=1779490800   station_id min=01839a78... max=04d557d6...
RG[200] dt min=1779404400 max=1779490800   station_id min=fe93b3ae... max=fffb18be...
```

`binCol='dt'` pruning is ineffective (every RG overlaps the query
window). What would prune effectively here is `station_id` — every RG
has a tight `station_id` range — but pyrmts doesn't expose
arbitrary-column RG pruning yet (per `gbfs/api/src/avail_pyrmts.ts:248`
comment).

## Resolution sketch

Two related changes, each independently useful:

1. **Writer helper** (§1 below): give consumers a one-call way to write
   shards laid out for read-side pruning. Default to `dt`-first sort +
   sensibly-sized RGs.
2. **Reader: arbitrary-column RG pruning** (§2): extend
   `FetchOptions` so consumers can prune by any column with stats, not
   just `binCol`. Unblocks ctbk's shadow path without re-writing the
   cascade-compactor shards.

Order doesn't matter; either lands first. ctbk needs (1) to unblock v2;
(2) is the cleaner long-term fix for station-filtered shadow + future
pyramids with per-row dimensional sharding.

## 1. `write_tier_parquet`

### API

```py
# pyrmts (python)
from pyrmts import write_tier_parquet, Pyramid

write_tier_parquet(
    rows: Iterable[Row] | pa.Table,
    pyramid: Pyramid,
    out: BinaryIO | str | Path,
    *,
    row_group_size: int | None = None,    # default: derived from row count + dims (see below)
    sort: Sequence[str] | None = None,    # default: [pyramid.bin_col, *pyramid.dims, *geo.cell_col]
    compression: str = 'snappy',          # hyparquet doesn't decode ZSTD; SNAPPY is the safe default
) -> int:
    """Write rows as a tier shard, laid out for read-side RG pruning.
    Returns bytes written."""
```

### Behavior

1. **Sort.** Rows sorted by `sort` cols (default: `bin_col` first, then
   dim cols, then geo cell col). This makes each RG's `bin_col` stats
   tight, which is what hyparquet prunes by.
2. **Row-group sizing.** If `row_group_size` is `None`, pick one that
   keeps each RG at ~5-20K rows (small enough that decoded RG fits in
   ~1 MB for typical schemas; big enough that overhead per RG is
   amortized). Concrete default: `max(4096, min(16384, total_rows // 100))`.
3. **Stats.** Ensure stats are written for `bin_col` and all dim cols
   (pyarrow's default already does this; document the requirement).
4. **Compression.** Default `snappy` — hyparquet can't decode ZSTD
   (ctbk hit this earlier; see `avail_geo.py` comment from `3f41ed62`).

### Non-goals

- Doesn't aggregate or transform rows; pure write layer.
- Doesn't choose `pyramid.tiers[i]` — caller specifies which tier this
  is being written for (via `pyramid`), but write_tier_parquet doesn't
  cascade.
- TS port deferred until a TS consumer needs offline builders.

### Migration for ctbk

`ctbk/avail_v2.py:296` becomes:
```py
from pyrmts import write_tier_parquet
write_tier_parquet(table, AVAIL_V2_PYRAMID, buf)  # replaces pq.write_table(...)
```
Rows already in `(cell, dt)` order get resorted to `(dt, cell)` (or
whatever the pyramid's dims order is) inside the helper. Then `ctbk
avail-v2-build --force` rewrites all R2 shards.

## 2. Reader: arbitrary-column RG pruning

### API

Extend `FetchOptions` in `pyrmts/fetch.{ts,py}`:

```ts
// pyrmts (ts)
interface FetchOptions {
  binCol?: string                    // existing
  range?: { from: Date; to: Date }   // existing
  // NEW: prune by arbitrary-column stats. Each filter checks the named
  // column's RG min/max stats against `values` (set-membership) or
  // `range` (closed interval). RGs not provably containing a match are
  // skipped; missing stats fall back to "must read".
  filters?: ColumnFilter[]
}

type ColumnFilter =
  | { col: string; values: string[] | number[] }                 // set membership (e.g. station_id IN (...))
  | { col: string; range: { min: number; max: number } }         // closed numeric interval
```

```py
# pyrmts (python) — same shape
```

### Behavior

`selectRowGroupRuns` (currently `binCol`-only in `fetch.ts:92`) gets
extended to AND-combine all filter predicates. An RG is selected iff:
- `binCol`+`range`: stats overlap (existing behavior), AND
- each `filters[i]`: stats overlap / contain a matching value.

If stats are missing for any required column, default to "read this RG"
(safer than throwing — caller can still filter post-fetch).

### Migration for ctbk

`gbfs/api/src/avail_pyrmts.ts:251` becomes:
```ts
plan.segments.map((seg) =>
  Promise.all(seg.keys.map((k) => fetchShardData(pyramid.storage, k, {
    binCol: pyramid.binCol,
    range: { from: seg.from, to: seg.to },
    filters: p.filterStationId?.length
      ? [{ col: 'station_id', values: p.filterStationId }]
      : undefined,
  }).catch(() => [] as Row[]))).then(arrs => arrs.flat()),
)
```

Shadow OOMs on `avail/agg/<tier>` shards resolve without rebuilding
the cascade-compactor data — the existing `(station_id, dt)`-sorted
layout becomes RG-prunable.

## Sequencing

Both changes are independent and parallel-safe:
- `write_tier_parquet` (§1) unblocks ctbk's v2 rebuild (concrete
  recipe in `~/c/hccs/ctbk/specs/avail-pyramid-v2.md` §7).
- Arbitrary-column RG pruning (§2) unblocks ctbk's shadow-mode parity
  signal without any data rewrite, and is the right primitive for any
  future pyramid that wants dim-sharded RG layout.

If both ship before ctbk's next EC2 session, the v2 rebuild can use
both (skip ctbk-side `pq.write_table` plumbing entirely).

## References

- `~/c/hccs/ctbk/specs/avail-pyramid-v2.md` §7 — ctbk consumer side
- `~/c/hccs/ctbk/ctbk/avail_v2.py` — v2 writer with the pyarrow defaults
  that triggered (1)
- `~/c/hccs/ctbk/gbfs/api/src/avail_pyrmts.ts:248` — comment flagging
  the rg-prune limitation (2) addresses
- `~/c/pyrmts/SPEC.md` §321/§323 — Open questions this spec closes
- `~/c/pyrmts/js/packages/pyrmts/src/fetch.ts:92` — `selectRowGroupRuns`,
  the function to extend in (2)

"""Sorted-run merge (`specs/multiscan-out-of-core.md`, Phase 2): a streaming
k-way merge of runs that share a sort key, with a per-identity reduce.

The mechanism under tiered (Bentley–Saxe / binary-counter) run merging:
disky's daily-append tiers (suffix rows and version deltas, combined on
identity with the smallest `vt`), and pyrmts' consolidation of already-sorted
shards (disjoint inputs, no reduce).

- `merge_sorted`: input runs (each an iterable of sorted record batches,
  oldest first) → one sorted batch stream. Rows whose `identity` (a prefix of
  `key`) is equal are reduced to one row: per-column `min` / `max` (every
  other column must agree, else it raises), or `'newest'` (the row from the
  newest input). Resident memory is about one batch per input plus what's
  pending at the merge frontier.
- `parquet_batches`: a parquet's row groups as batches (the usual input).
- `row_group_spans`: a written parquet's per-row-group byte span, for sidecars.

Write the stream with `pyrmts.intervals.write_exact_row_groups`, so the
output's bytes depend only on its rows.
"""
from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Literal

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

Agg = Literal['min', 'max']
Reduce = Mapping[str, Agg] | Literal['newest'] | None

SRC = '__src'


def parquet_batches(path: str | Path, columns: Sequence[str] | None = None) -> Iterator[pa.RecordBatch]:
    """`path`'s row groups, in order, as record batches."""
    f = pq.ParquetFile(path)
    for g in range(f.num_row_groups):
        yield from f.read_row_group(g, columns=columns).to_batches()


def row_group_spans(path: str | Path) -> list[dict[str, int]]:
    """Per row group of a written parquet: `{rg, offset, length, rows}`, the
    contiguous byte span of its column chunks (dictionary pages included)."""
    md = pq.ParquetFile(path).metadata
    spans = []
    for g in range(md.num_row_groups):
        rg = md.row_group(g)
        starts = [rg.column(c).dictionary_page_offset or rg.column(c).data_page_offset for c in range(rg.num_columns)]
        ends = [st + rg.column(c).total_compressed_size for c, st in enumerate(starts)]
        spans.append({'rg': g, 'offset': min(starts), 'length': max(ends) - min(starts), 'rows': rg.num_rows})
    return spans


def _decode(t: pa.Table) -> pa.Table:
    """Dictionary columns → their value type, so batches from different files
    (different dictionaries) concatenate, compare and sort by value."""
    cols = [c.cast(c.type.value_type) if pa.types.is_dictionary(c.type) else c for c in t.columns]
    return pa.table(cols, names=t.column_names)


def _key_at(t: pa.Table, i: int, cols: Sequence[str]) -> tuple:
    return tuple(t.column(c)[i].as_py() for c in cols)


def _bisect(t: pa.Table, cols: Sequence[str], bound: tuple) -> int:
    """First row of sorted `t` whose `cols` prefix is ≥ `bound`."""
    lo, hi = 0, t.num_rows
    while lo < hi:
        mid = (lo + hi) // 2
        if _key_at(t, mid, cols) < bound:
            lo = mid + 1
        else:
            hi = mid
    return lo


def _group_starts(t: pa.Table, cols: Sequence[str]) -> tuple[pa.Array, pa.Array]:
    """`(is_start, starts)`: whether each row of sorted `t` differs from the
    previous one on `cols`, and the indices where it does."""
    n = t.num_rows
    diff = None
    for c in cols:
        a = t.column(c)
        ne = pc.not_equal(a.slice(1), a.slice(0, n - 1))
        diff = ne if diff is None else pc.or_(diff, ne)
    is_start = pa.concat_arrays([pa.array([True]), *diff.chunks]) if diff is not None and n else pa.array([True] * n)
    return is_start, pc.indices_nonzero(is_start)


def _reduce(t: pa.Table, identity: Sequence[str], reduce: Reduce) -> pa.Table:
    """`t` sorted by identity (then `SRC` and the rest of the key for
    `'newest'`, or the rest of the key and `SRC` otherwise); one row per
    identity."""
    is_start, starts = _group_starts(t, identity)
    if len(starts) == t.num_rows:
        return t
    if reduce == 'newest':
        ends = pc.subtract(pa.concat_arrays([starts.slice(1), pa.array([t.num_rows], starts.type)]), 1)
        return t.take(ends)
    assert reduce is not None
    gid = pc.subtract(pc.cumulative_sum(pc.cast(is_start, pa.int64())), 1)
    first_of_row = pc.take(starts, gid)
    out = t.take(starts)
    if reduce:
        aggs = t.select(list(reduce)).append_column('__gid', gid) \
            .group_by('__gid', use_threads=False).aggregate([(c, p) for c, p in reduce.items()]) \
            .sort_by('__gid')
        for c, p in reduce.items():
            out = out.set_column(out.column_names.index(c), c, aggs.column(f'{c}_{p}').cast(t.schema.field(c).type))
    for c in t.column_names:
        if c in identity or c == SRC or c in reduce:
            continue
        a = t.column(c)
        b = a.take(first_of_row)
        same = pc.fill_null(pc.equal(a, b), False)
        both_null = pc.and_(pc.is_null(a), pc.is_null(b))
        bad = pc.invert(pc.or_(same, both_null))
        if pc.any(bad).as_py():
            i = pc.index(bad, True).as_py()
            raise ValueError(
                f"merge_sorted: rows with identity {_key_at(t, i, identity)} disagree on {c!r} "
                f"({b[i].as_py()!r} vs {a[i].as_py()!r}) and {c!r} has no reduce policy"
            )
    return out


def merge_sorted(
    inputs: Sequence[Iterable[pa.RecordBatch]],
    key: Sequence[str],
    *,
    identity: Sequence[str] | None = None,
    reduce: Reduce = None,
) -> Iterator[pa.RecordBatch]:
    """Merge `inputs` (oldest first), each sorted by `key` ascending (strings
    in code-point order; no nulls in `key`), into one stream sorted by `key`.

    `reduce`: how rows with equal `identity` (default: all of `key`; must be
    a prefix of it) combine — rows from one input or several:

    - `None`: no reduce; every row is kept (ties ordered by input).
    - `{col: 'min' | 'max'}`: one row per identity, those columns
      aggregated; every other column must agree within the identity, else
      `ValueError`. disky's runs: `{'vt': 'min'}` (suffix rows), and
      `{'vt': 'min', 'op': 'max'}` (version deltas, sorted `(..., vf, op)`,
      identity without `op`).
    - `'newest'`: the row from the newest input holding the identity (the
      last in key order, if that input holds several).

    Streaming: each input is read one batch at a time. At each step the
    frontier is the smallest last-identity over inputs not yet exhausted;
    every buffered row before it is final (all inputs are past it), so it is
    sorted, reduced and emitted, and the inputs at the frontier read their
    next batch. An input whose batches go backwards raises."""
    key = list(key)
    ident = list(identity) if identity is not None else key
    if key[:len(ident)] != ident:
        raise ValueError(f"merge_sorted: identity {ident} must be a prefix of key {key}")
    if isinstance(reduce, Mapping):
        overlap = set(reduce) & set(ident)
        if overlap:
            raise ValueError(f"merge_sorted: reduce columns {sorted(overlap)} are identity columns")
        bad = {c: p for c, p in reduce.items() if p not in ('min', 'max')}
        if bad:
            raise ValueError(f"merge_sorted: reduce policy must be 'min' or 'max', got {bad}")
    elif reduce not in (None, 'newest'):
        raise ValueError(f"merge_sorted: reduce must be a mapping, 'newest' or None, got {reduce!r}")

    its = [iter(x) for x in inputs]
    bufs: list[pa.Table | None] = [None] * len(its)
    last: list[tuple | None] = [None] * len(its)
    done = [False] * len(its)
    schema: pa.Schema | None = None

    def load(j: int) -> None:
        nonlocal schema
        for b in its[j]:
            if b.num_rows == 0:
                continue
            t = _decode(pa.Table.from_batches([b]))
            if schema is None:
                schema = t.schema
            elif not t.schema.equals(schema, check_metadata=False):
                t = t.select(schema.names).cast(schema)
            if any(t.column(c).null_count for c in key):
                raise ValueError(f"merge_sorted: input {j} has nulls in key columns {key}")
            t = t.append_column(SRC, pa.repeat(pa.scalar(j, pa.int32()), t.num_rows))
            first = _key_at(t, 0, key)
            if last[j] is not None and first < last[j]:
                raise ValueError(f"merge_sorted: input {j} is not sorted by {key}: {first} after {last[j]}")
            last[j] = _key_at(t, t.num_rows - 1, key)
            bufs[j] = t if bufs[j] is None else pa.concat_tables([bufs[j], t])
            return
        done[j] = True

    def emit(parts: list[pa.Table]) -> Iterator[pa.RecordBatch]:
        parts = [p for p in parts if p.num_rows]
        if not parts:
            return
        # `'newest'` orders an identity's rows by input before the rest of the
        # key, so the newest input's row is the group's last.
        order = [*ident, SRC, *key[len(ident):]] if reduce == 'newest' else [*key, SRC]
        t = pa.concat_tables(parts).sort_by([(c, 'ascending') for c in order])
        if reduce is not None:
            t = _reduce(t, ident, reduce)
        yield from t.drop_columns([SRC]).combine_chunks().to_batches()

    for j in range(len(its)):
        load(j)
    while True:
        live = [j for j in range(len(its)) if not done[j]]
        if not live:
            yield from emit([b for b in bufs if b is not None])
            return
        frontier = min(last[j][:len(ident)] for j in live)
        parts = []
        for j, b in enumerate(bufs):
            if b is None:
                continue
            cut = _bisect(b, ident, frontier)
            parts.append(b.slice(0, cut))
            bufs[j] = b.slice(cut) if cut < b.num_rows else None
        yield from emit(parts)
        for j in live:
            if last[j][:len(ident)] == frontier:
                load(j)


def merge_parquets(
    paths: Sequence[str | Path],
    key: Sequence[str],
    *,
    identity: Sequence[str] | None = None,
    reduce: Reduce = None,
    columns: Sequence[str] | None = None,
) -> Iterator[pa.RecordBatch]:
    """`merge_sorted` over parquet files (oldest first), row group by row group."""
    return merge_sorted([parquet_batches(p, columns) for p in paths], key, identity=identity, reduce=reduce)

"""Pyramid-shard writer. Lays out parquet for downstream RG pruning by the
JS reader (`pyrmts.fetchShardData`): rows sorted by `binCol` first so each
row group's `binCol` stats are tight, with row-group sizes chosen to balance
decode overhead against per-RG bookkeeping.

See `specs/done/writer-helper-and-arbitrary-col-rg-prune.md` for motivation
(ctbk avail v2 OOM on default pyarrow layout).
"""
from __future__ import annotations

from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import BinaryIO

import pyarrow as pa
import pyarrow.parquet as pq

from .monoids import Row
from .types import Pyramid

_DEFAULT_ROW_GROUP_MIN = 4096
_DEFAULT_ROW_GROUP_MAX = 16384


def _default_row_group_size(total_rows: int) -> int:
    """Pick a row-group size that keeps each RG small enough to decode
    cheaply but large enough that per-RG overhead is amortized.

    Concrete default: `max(4096, min(16384, total_rows // 100))`. Empty
    tables get the minimum.
    """
    return max(_DEFAULT_ROW_GROUP_MIN, min(_DEFAULT_ROW_GROUP_MAX, total_rows // 100))


def _default_sort_cols(pyramid: Pyramid) -> list[str]:
    """Default sort order: bin_col first (tight RG stats for time-axis
    pruning), then dim cols, then the geo cell col (clusters cells within
    a bin for hyparquet's read-time spatial filter)."""
    cols: list[str] = [pyramid.binCol]
    for dim in pyramid.dims:
        if dim.name not in cols:
            cols.append(dim.name)
    if pyramid.geo is not None and pyramid.geo.cellCol not in cols:
        cols.append(pyramid.geo.cellCol)
    return cols


def write_tier_parquet(
    rows: Iterable[Row] | pa.Table,
    pyramid: Pyramid,
    out: BinaryIO | str | Path,
    *,
    row_group_size: int | None = None,
    sort: Sequence[str] | None = None,
    compression: str = 'snappy',
) -> int:
    """Write rows as a tier shard, laid out for read-side RG pruning.

    Defaults are tuned for hyparquet consumption:

    - **Sort**: `(bin_col, *dims, geo.cellCol)`. The bin-col-first sort
      gives each RG tight `bin_col` stats, which is the property
      `fetchShardData` uses to skip RGs.
    - **Row-group size**: `max(4096, min(16384, total_rows // 100))`.
      Small enough that a decoded RG fits in ~1 MB for typical schemas;
      big enough to amortize per-RG overhead.
    - **Compression**: `snappy`. Hyparquet doesn't decode ZSTD (see ctbk's
      earlier breakage in `avail_geo.py` history).

    Args:
        rows: Row iterable or a pre-built `pa.Table`.
        pyramid: Used to derive default `sort` cols (`bin_col`, dim cols,
            optional `geo.cellCol`).
        out: Destination — a binary file-like, path-string, or `Path`.
        row_group_size: Override the default sizing.
        sort: Override the default sort cols. Pass `[]` to skip sorting.
        compression: Passed through to `pq.write_table` (default `snappy`).

    Returns:
        Bytes written.
    """
    if isinstance(rows, pa.Table):
        table = rows
    else:
        table = pa.Table.from_pylist(list(rows))

    sort_cols = list(sort) if sort is not None else _default_sort_cols(pyramid)
    # Tolerate sort cols not present (e.g. dim columns the writer didn't
    # populate); pa.Table.sort_by errors on missing columns.
    schema_names = set(table.schema.names)
    sort_cols = [c for c in sort_cols if c in schema_names]
    if sort_cols and table.num_rows > 0:
        table = table.sort_by([(c, 'ascending') for c in sort_cols])

    rgs = row_group_size if row_group_size is not None else _default_row_group_size(table.num_rows)

    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, row_group_size=rgs, compression=compression)
    data = sink.getvalue()

    if isinstance(out, (str, Path)):
        with open(out, 'wb') as f:
            f.write(data.to_pybytes())
    else:
        out.write(data.to_pybytes())

    return data.size

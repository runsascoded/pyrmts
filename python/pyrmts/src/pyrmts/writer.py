"""Pyramid-shard writer. Lays out parquet for downstream RG pruning by the
JS reader (`pyrmts.fetchShardData`): rows sorted by `binCol` first so each
row group's `binCol` stats are tight, with row-group sizes chosen to balance
decode overhead against per-RG bookkeeping.

See `specs/done/writer-helper-and-arbitrary-col-rg-prune.md` for motivation
(ctbk avail v2 OOM on default pyarrow layout).
"""
from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

import pyarrow as pa
import pyarrow.parquet as pq

from .monoids import Row
from .types import Pyramid

_DEFAULT_ROW_GROUP_MIN = 4096
_DEFAULT_ROW_GROUP_MAX = 16384

#: Parquet key-value metadata stamped on every shard `write_tier_parquet`
#: writes: the effective sort columns (comma-joined) and row-group size, so an
#: in-place rewriter (`canonicalize_shards`, …) reproduces the build's layout
#: without any config plumbing (`specs/canonicalize-preserve-layout.md`).
LAYOUT_SORT_KEY = b'pyrmts.sort'
LAYOUT_ROW_GROUP_SIZE_KEY = b'pyrmts.row_group_size'


@dataclass(frozen=True)
class ShardLayout:
    sort: list[str]
    row_group_size: int


def read_layout(schema_or_metadata) -> ShardLayout | None:
    """The layout stamp from a shard's schema / parquet metadata (a
    `pa.Schema`, `pq.FileMetaData`, or the raw KV dict), or None for a legacy
    shard written without one."""
    md = schema_or_metadata
    if hasattr(md, 'metadata'):
        md = md.metadata
    if not md or LAYOUT_ROW_GROUP_SIZE_KEY not in md:
        return None
    sort_raw = md.get(LAYOUT_SORT_KEY, b'').decode()
    return ShardLayout(
        sort=[c for c in sort_raw.split(',') if c],
        row_group_size=int(md[LAYOUT_ROW_GROUP_SIZE_KEY].decode()),
    )


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
    pyramid: Pyramid | None = None,
    out: BinaryIO | str | Path | None = None,
    *,
    row_group_size: int | None = None,
    sort: Sequence[str] | None = None,
    compression: str = 'snappy',
) -> int:
    """Write rows as a tier shard, laid out for read-side RG pruning.

    Defaults are tuned for hyparquet consumption:

    - **Sort**: when `pyramid` is supplied, defaults to
      `(bin_col, *dims, geo.cellCol)` — the bin-col-first sort gives each
      RG tight `bin_col` stats, which is the property `fetchShardData`
      uses to skip RGs. When `pyramid` is `None`, the caller must pass
      `sort=[...]` explicitly (or `sort=[]` for no sort).
    - **Row-group size**: `max(4096, min(16384, total_rows // 100))`.
      Small enough that a decoded RG fits in ~1 MB for typical schemas;
      big enough to amortize per-RG overhead.
    - **Compression**: `snappy`. Hyparquet doesn't decode ZSTD (see ctbk's
      earlier breakage in `avail_geo.py` history).

    Args:
        rows: Row iterable or a pre-built `pa.Table`.
        pyramid: When given, used only to derive default `sort` cols
            (`bin_col`, dim cols, optional `geo.cellCol`). Optional —
            callers without a Python `Pyramid` declaration can pass
            `sort=[...]` explicitly instead.
        out: Destination — a binary file-like, path-string, or `Path`.
            Required (only typed `None` to allow `pyramid` to be omitted
            positionally; pass via keyword if you don't supply `pyramid`).
        row_group_size: Override the default sizing.
        sort: Override the default sort cols. Pass `[]` to skip sorting.
            Required when `pyramid` is `None`.
        compression: Passed through to `pq.write_table` (default `snappy`).

    The effective `sort` and `row_group_size` are stamped into the parquet
    key-value metadata (`LAYOUT_SORT_KEY` / `LAYOUT_ROW_GROUP_SIZE_KEY`; read
    back with `read_layout`) so in-place rewriters keep the build's layout.

    Returns:
        Bytes written.
    """
    if out is None:
        raise TypeError("write_tier_parquet: `out` is required")
    if pyramid is None and sort is None:
        raise TypeError(
            "write_tier_parquet: `sort` is required when `pyramid` is not "
            "supplied (or pass `sort=[]` for no sort)"
        )

    if isinstance(rows, pa.Table):
        table = rows
    else:
        table = pa.Table.from_pylist(list(rows))

    sort_cols = (
        list(sort) if sort is not None
        else _default_sort_cols(pyramid)  # type: ignore[arg-type]  # pyramid is non-None here
    )
    # Tolerate sort cols not present (e.g. dim columns the writer didn't
    # populate); pa.Table.sort_by errors on missing columns.
    schema_names = set(table.schema.names)
    sort_cols = [c for c in sort_cols if c in schema_names]
    if sort_cols and table.num_rows > 0:
        table = table.sort_by([(c, 'ascending') for c in sort_cols])

    rgs = row_group_size if row_group_size is not None else _default_row_group_size(table.num_rows)

    # Stamp the effective layout so a rewriter can reproduce it (`read_layout`).
    existing = table.schema.metadata or {}
    table = table.replace_schema_metadata({
        **existing,
        LAYOUT_SORT_KEY: ','.join(sort_cols).encode(),
        LAYOUT_ROW_GROUP_SIZE_KEY: str(rgs).encode(),
    })

    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, row_group_size=rgs, compression=compression)
    data = sink.getvalue()

    if isinstance(out, (str, Path)):
        with open(out, 'wb') as f:
            f.write(data.to_pybytes())
    else:
        out.write(data.to_pybytes())

    return data.size

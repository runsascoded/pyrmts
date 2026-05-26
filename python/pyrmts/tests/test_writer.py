"""Tests for `write_tier_parquet` — shard layout for downstream RG pruning."""
from __future__ import annotations

import io
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyrmts import (
    Dim,
    GeoSpec,
    MemStorage,
    Metric,
    Pyramid,
    Tier,
    write_tier_parquet,
)


UTC = timezone.utc


def _ms(t: datetime) -> int:
    return int(t.timestamp() * 1000)


def _awair_pyramid() -> Pyramid:
    return Pyramid(
        storage=MemStorage(),
        keyTemplate='awair-{device_id}/{tier}/{period}.parquet',
        binCol='ts',
        dims=[Dim(name='device_id', type='int')],
        metrics=[Metric(name='temp', monoid='sum')],
        tiers=[
            Tier(name='raw', bin='1min', shard='1mo'),
            Tier(name='h1', bin='1h', shard='1mo'),
        ],
    )


def _geo_pyramid() -> Pyramid:
    p = _awair_pyramid()
    p.geo = GeoSpec(cellCol='h3_cell', resolutions=(9, 7, 5))
    return p


def _read_parquet(buf: bytes) -> tuple[pa.Table, pq.ParquetFile]:
    pf = pq.ParquetFile(io.BytesIO(buf))
    return pf.read(), pf


def test_writes_with_default_sort_bin_first():
    """Rows in (device_id, ts) order get re-sorted to (ts, device_id)."""
    pyramid = _awair_pyramid()
    base = _ms(datetime(2026, 1, 1, tzinfo=UTC))
    # Construct rows in device_id-major order (the wrong order for RG pruning).
    rows = [
        {'ts': base + h * 3_600_000, 'device_id': dev, 'temp_n': 60,
         'temp_sum': 60.0 * (dev + h), 'temp_sumsq': 100.0 * (dev + h)}
        for dev in (99999, 17617)
        for h in range(3)
    ]
    buf = io.BytesIO()
    n = write_tier_parquet(rows, pyramid, buf)
    assert n > 0 and n == len(buf.getvalue())

    table, _ = _read_parquet(buf.getvalue())
    ts_col = table.column('ts').to_pylist()
    dev_col = table.column('device_id').to_pylist()
    # Sorted ts-first, then device_id.
    assert ts_col == sorted(ts_col)
    # Within each ts, device_id is sorted.
    by_ts: dict[int, list[int]] = {}
    for t, d in zip(ts_col, dev_col):
        by_ts.setdefault(t, []).append(d)
    for devs in by_ts.values():
        assert devs == sorted(devs)


def test_per_rg_bin_col_stats_are_tight():
    """Sorted ts → each RG's ts min/max stats are a tight sub-interval."""
    pyramid = _awair_pyramid()
    base = _ms(datetime(2026, 1, 1, tzinfo=UTC))
    n_rows = 50_000  # default rgs picks 500 → 100 RGs
    rows = [
        {'ts': base + i * 60_000, 'device_id': 17617, 'temp_n': 1,
         'temp_sum': float(i), 'temp_sumsq': float(i * i)}
        for i in range(n_rows)
    ]
    buf = io.BytesIO()
    write_tier_parquet(rows, pyramid, buf)
    _, pf = _read_parquet(buf.getvalue())
    meta = pf.metadata
    # Default sizing: max(4096, min(16384, 50000 // 100)) = max(4096, 500) = 4096.
    # 50_000 / 4096 = ~13 RGs.
    assert 8 <= meta.num_row_groups <= 20
    # Walk RGs and verify ts stats are monotone and contiguous (tight per-RG).
    ts_col_idx = pf.schema.names.index('ts')
    prev_max = -1
    for rg_i in range(meta.num_row_groups):
        col = meta.row_group(rg_i).column(ts_col_idx)
        s = col.statistics
        assert s is not None
        assert s.has_min_max
        assert s.min >= prev_max
        assert s.min <= s.max
        prev_max = s.max


def test_default_row_group_size():
    """row_group_size = max(4096, min(16384, total // 100))."""
    pyramid = _awair_pyramid()
    base = _ms(datetime(2026, 1, 1, tzinfo=UTC))

    # Small table (5 rows): clamp up to 4096 → 1 RG.
    rows_small = [{'ts': base + i * 60_000, 'device_id': 1, 'temp_n': 1,
                   'temp_sum': float(i), 'temp_sumsq': 1.0} for i in range(5)]
    buf = io.BytesIO()
    write_tier_parquet(rows_small, pyramid, buf)
    _, pf = _read_parquet(buf.getvalue())
    assert pf.metadata.num_row_groups == 1

    # Huge table (2M rows): clamp down to 16384 → 2M/16384 = ~123 RGs.
    rows_big = pa.table({
        'ts': pa.array([base + i * 60_000 for i in range(2_000_000)], type=pa.int64()),
        'device_id': pa.array([1] * 2_000_000, type=pa.int32()),
        'temp_n': pa.array([1] * 2_000_000, type=pa.int32()),
        'temp_sum': pa.array([0.0] * 2_000_000, type=pa.float64()),
        'temp_sumsq': pa.array([0.0] * 2_000_000, type=pa.float64()),
    })
    buf2 = io.BytesIO()
    write_tier_parquet(rows_big, pyramid, buf2)
    _, pf2 = _read_parquet(buf2.getvalue())
    assert 100 <= pf2.metadata.num_row_groups <= 140


def test_compression_default_is_snappy():
    pyramid = _awair_pyramid()
    base = _ms(datetime(2026, 1, 1, tzinfo=UTC))
    rows = [{'ts': base + i * 60_000, 'device_id': 1, 'temp_n': 1,
             'temp_sum': float(i), 'temp_sumsq': 1.0} for i in range(100)]
    buf = io.BytesIO()
    write_tier_parquet(rows, pyramid, buf)
    _, pf = _read_parquet(buf.getvalue())
    col = pf.metadata.row_group(0).column(0)
    assert col.compression == 'SNAPPY'


def test_geo_cellCol_in_default_sort():
    """Geo pyramids: cell col is appended to the default sort order."""
    pyramid = _geo_pyramid()
    base = _ms(datetime(2026, 1, 1, tzinfo=UTC))
    # Two timestamps × 3 cells, intentionally in wrong order.
    rows = []
    for cell in ('cellC', 'cellA', 'cellB'):
        for h in (1, 0):
            rows.append({
                'ts': base + h * 3_600_000,
                'device_id': 17617,
                'h3_cell': cell,
                'temp_n': 1, 'temp_sum': 1.0, 'temp_sumsq': 1.0,
            })
    buf = io.BytesIO()
    write_tier_parquet(rows, pyramid, buf)
    table, _ = _read_parquet(buf.getvalue())
    # Sort order is (ts, device_id, h3_cell). Within each ts, cells alphabetical.
    by_ts: dict[int, list[str]] = {}
    for t, c in zip(table.column('ts').to_pylist(), table.column('h3_cell').to_pylist()):
        by_ts.setdefault(t, []).append(c)
    for cells in by_ts.values():
        assert cells == ['cellA', 'cellB', 'cellC']


def test_accepts_pa_table_directly():
    pyramid = _awair_pyramid()
    base = _ms(datetime(2026, 1, 1, tzinfo=UTC))
    table = pa.table({
        'ts': pa.array([base + 60_000, base], type=pa.int64()),
        'device_id': pa.array([17617, 17617], type=pa.int32()),
        'temp_n': pa.array([1, 1], type=pa.int32()),
        'temp_sum': pa.array([10.0, 20.0], type=pa.float64()),
        'temp_sumsq': pa.array([100.0, 400.0], type=pa.float64()),
    })
    buf = io.BytesIO()
    n = write_tier_parquet(table, pyramid, buf)
    assert n > 0
    out, _ = _read_parquet(buf.getvalue())
    assert out.column('ts').to_pylist() == [base, base + 60_000]


def test_override_sort_cols():
    """Caller can override the default sort (e.g. legacy `(cell, dt)`)."""
    pyramid = _awair_pyramid()
    base = _ms(datetime(2026, 1, 1, tzinfo=UTC))
    rows = [
        {'ts': base + h * 3_600_000, 'device_id': dev, 'temp_n': 1,
         'temp_sum': 1.0, 'temp_sumsq': 1.0}
        for dev in (99999, 17617)
        for h in range(3)
    ]
    buf = io.BytesIO()
    write_tier_parquet(rows, pyramid, buf, sort=['device_id', 'ts'])
    table, _ = _read_parquet(buf.getvalue())
    # device_id first, then ts.
    devs = table.column('device_id').to_pylist()
    assert devs == sorted(devs)


def test_override_row_group_size():
    pyramid = _awair_pyramid()
    base = _ms(datetime(2026, 1, 1, tzinfo=UTC))
    rows = [{'ts': base + i * 60_000, 'device_id': 1, 'temp_n': 1,
             'temp_sum': float(i), 'temp_sumsq': 1.0} for i in range(1000)]
    buf = io.BytesIO()
    write_tier_parquet(rows, pyramid, buf, row_group_size=100)
    _, pf = _read_parquet(buf.getvalue())
    assert pf.metadata.num_row_groups == 10


def test_writes_to_path(tmp_path):
    pyramid = _awair_pyramid()
    base = _ms(datetime(2026, 1, 1, tzinfo=UTC))
    rows = [{'ts': base + i * 60_000, 'device_id': 1, 'temp_n': 1,
             'temp_sum': float(i), 'temp_sumsq': 1.0} for i in range(10)]
    out = tmp_path / 'shard.parquet'
    n = write_tier_parquet(rows, pyramid, out)
    assert out.exists()
    assert out.stat().st_size == n


def test_empty_rows_writes_valid_parquet():
    pyramid = _awair_pyramid()
    buf = io.BytesIO()
    n = write_tier_parquet([], pyramid, buf)
    assert n > 0
    # Should round-trip as an empty table.
    table, _ = _read_parquet(buf.getvalue())
    assert table.num_rows == 0


def test_skips_sort_cols_missing_from_schema():
    """Pyramid declares a dim col that the rows don't actually carry —
    write_tier_parquet should tolerate that and just skip the missing col."""
    pyramid = _awair_pyramid()  # dims = [device_id]
    base = _ms(datetime(2026, 1, 1, tzinfo=UTC))
    # Rows omit device_id intentionally.
    rows = [{'ts': base + i * 60_000, 'temp_n': 1, 'temp_sum': float(i),
             'temp_sumsq': 1.0} for i in range(5)]
    buf = io.BytesIO()
    n = write_tier_parquet(rows, pyramid, buf)
    assert n > 0
    table, _ = _read_parquet(buf.getvalue())
    assert table.column('ts').to_pylist() == sorted(table.column('ts').to_pylist())


def test_pyramid_optional_when_sort_explicit():
    """Caller without a Python `Pyramid` declaration can still use the
    writer by passing `sort=[...]` explicitly (the ctbk avail_v2 path)."""
    base = _ms(datetime(2026, 1, 1, tzinfo=UTC))
    rows = [
        {'dt': base + h * 3_600_000, 'cell': cell, 'count': 1}
        for cell in ('cellC', 'cellA', 'cellB')
        for h in (1, 0)
    ]
    buf = io.BytesIO()
    n = write_tier_parquet(rows, out=buf, sort=['dt', 'cell'])
    assert n > 0
    table, _ = _read_parquet(buf.getvalue())
    # dt-major, cell-minor.
    dt_col = table.column('dt').to_pylist()
    cell_col = table.column('cell').to_pylist()
    assert dt_col == sorted(dt_col)
    by_dt: dict[int, list[str]] = {}
    for t, c in zip(dt_col, cell_col):
        by_dt.setdefault(t, []).append(c)
    for cells in by_dt.values():
        assert cells == sorted(cells)


def test_no_pyramid_no_sort_raises():
    """Either `pyramid` or `sort` must be supplied; bare `rows + out` errors."""
    rows = [{'ts': 0, 'value': 1}]
    buf = io.BytesIO()
    with pytest.raises(TypeError, match="`sort` is required when `pyramid` is not supplied"):
        write_tier_parquet(rows, out=buf)


def test_missing_out_raises():
    pyramid = _awair_pyramid()
    with pytest.raises(TypeError, match="`out` is required"):
        write_tier_parquet([], pyramid)


def test_pyramid_optional_sort_empty_explicit():
    """`sort=[]` is a valid pyramid-free invocation: skip sorting entirely."""
    base = _ms(datetime(2026, 1, 1, tzinfo=UTC))
    # Rows in an unsorted order; with sort=[] they should stay that way.
    rows = [{'ts': base + 60_000, 'value': 1}, {'ts': base, 'value': 0}]
    buf = io.BytesIO()
    n = write_tier_parquet(rows, out=buf, sort=[])
    assert n > 0
    table, _ = _read_parquet(buf.getvalue())
    assert table.column('value').to_pylist() == [1, 0]

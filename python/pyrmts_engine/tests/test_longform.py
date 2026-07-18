"""wide ↔ long round-trip and canonical hist-JSON output."""
from __future__ import annotations

import polars as pl

from pyrmts_engine import long_to_wide, wide_to_long

from conftest import base_wide_frame, make_pyramid, Q_MS


def _sorted_rows(df: pl.DataFrame) -> list[dict]:
    return sorted(df.to_dicts(), key=lambda r: (r['cell'], r['dt']))


def test_wide_long_round_trip():
    pyramid = make_pyramid()
    start_ms = (1_767_312_000_000 // Q_MS) * Q_MS  # 2026-01-02T00:00Z, 15min-aligned
    wide = base_wide_frame(start_ms, start_ms + 4 * Q_MS)

    long = wide_to_long(wide, pyramid)
    # 4 bins × 2 cells × (2 hist states + 1 count + 3 sum cols) = 48 long rows.
    assert long.height == 48
    rt = long_to_wide(long, pyramid)

    assert _sorted_rows(rt) == _sorted_rows(wide)


def test_long_to_wide_hist_json_is_canonical():
    pyramid = make_pyramid()
    long = pl.DataFrame({
        'cell': ['a', 'a', 'a'],
        'dt': [0, 0, 0],
        'metric': ['bikes', 'bikes', 'bikes'],
        'state': [10, 2, 0],
        'count': [3.0, 1.0, 5.0],
    })
    wide = long_to_wide(long, pyramid)
    # Numerically state-sorted (0 < 2 < 10 — not lexicographic), no spaces.
    assert wide.to_dicts() == [{
        'cell': 'a',
        'dt': 0,
        'bikes': '{"0":5,"2":1,"10":3}',
        'rides': None,
        'temp_n': None,
        'temp_sum': None,
        'temp_sumsq': None,
    }]


def test_wide_to_long_empty_hist_and_nulls_dropped():
    pyramid = make_pyramid()
    wide = pl.DataFrame({
        'cell': ['a', 'b'],
        'dt': [0, 0],
        'bikes': ['{}', None],
        'rides': [None, 3],
        'temp_n': [None, None],
        'temp_sum': [None, None],
        'temp_sumsq': [None, None],
    })
    long = wide_to_long(wide, pyramid)
    assert long.to_dicts() == [
        {'cell': 'b', 'dt': 0, 'metric': 'rides', 'state': None, 'count': 3.0},
    ]

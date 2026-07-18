"""Shared fixture pyramid + deterministic synthetic data.

The fixture exercises all three implemented monoid classes (histogram,
count, sum) and a 3-tier ladder whose preds chain q ← source, h ← q,
d ← h.

Synthetic data is a pure function of the absolute 15-min bin index
`i = ms // 15min` (and cell), so tests can re-derive expected aggregates
independently of any engine/cascade code path:

    bikes hist: {i % 3: 1, (i % 5) + 3: 2}   (state ranges disjoint)
    rides:      (i + cell_idx) % 7
    temp:       n=2, sum=float(i), sumsq=float(i * i)

All values are integer-valued, so float accumulation is exact and window
splits can't perturb output bytes."""
from __future__ import annotations

import io
import json
from datetime import datetime, timezone

import polars as pl
import pytest

from pyrmts import (
    Dim,
    MemStorage,
    Metric,
    Pyramid,
    Tier,
    shard_periods_covering,
    substitute_key,
    write_tier_parquet,
)

Q_MS = 15 * 60_000
CELLS = ['a', 'b']

FROM = datetime(2026, 1, 2, tzinfo=timezone.utc)
TO = datetime(2026, 1, 8, tzinfo=timezone.utc)


def make_pyramid(storage=None) -> Pyramid:
    return Pyramid(
        storage=storage if storage is not None else MemStorage(),
        keyTemplate='pyr/{tier}/{shard}/{period}.parquet',
        binCol='dt',
        dims=[Dim(name='cell', type='string')],
        metrics=[
            Metric(name='bikes', monoid='histogram'),
            Metric(name='rides', monoid='count'),
            Metric(name='temp', monoid='sum'),
        ],
        tiers=[
            Tier(name='q', bin='15min', shards=('6h', '1d')),
            Tier(name='h', bin='1h', shards=('1d', '4d')),
            Tier(name='d', bin='1d', shards=('4d',)),
        ],
    )


def bikes_hist(i: int) -> dict[int, int]:
    return {i % 3: 1, (i % 5) + 3: 2}


def rides_count(i: int, cell_idx: int) -> int:
    return (i + cell_idx) % 7


def hist_json(h: dict[int, int]) -> str:
    return json.dumps({str(k): h[k] for k in sorted(h)}, separators=(',', ':'))


def base_wide_frame(start_ms: int, end_ms: int) -> pl.DataFrame:
    """Wide base-tier (15min-binned) frame for `[start_ms, end_ms)`."""
    rows = []
    for ms in range(start_ms, end_ms, Q_MS):
        i = ms // Q_MS
        for cell_idx, cell in enumerate(CELLS):
            rows.append({
                'cell': cell,
                'dt': ms,
                'bikes': hist_json(bikes_hist(i)),
                'rides': rides_count(i, cell_idx),
                'temp_n': 2,
                'temp_sum': float(i),
                'temp_sumsq': float(i * i),
            })
    return pl.DataFrame(rows)


def write_base_shards(pyramid: Pyramid) -> list[str]:
    """Materialize the base rung (q@6h) wide shards for [FROM, TO)."""
    keys = []
    for period in shard_periods_covering(FROM, TO, '6h'):
        s_ms = int(period.start.timestamp() * 1000)
        e_ms = int(period.end.timestamp() * 1000)
        frame = base_wide_frame(s_ms, e_ms)
        key = substitute_key(
            pyramid.keyTemplate,
            {'tier': 'q', 'shard': '6h', 'period': period.label},
        )
        buf = io.BytesIO()
        write_tier_parquet(frame.to_arrow(), pyramid, out=buf)
        pyramid.storage.put(key, buf.getvalue())
        keys.append(key)
    return keys


@pytest.fixture
def pyramid() -> Pyramid:
    p = make_pyramid()
    write_base_shards(p)
    return p

"""Fixture pyramid for the ops layer — same non-ctbk ladder + synthetic
data as `pyrmts_engine/tests/conftest.py` (values are a pure function of
the 15-min bin index, so engine-vs-engine comparisons are exact)."""
from __future__ import annotations

import io
import json
from dataclasses import replace
from datetime import datetime, timezone

import polars as pl

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
from pyrmts_engine import WideShardSource, build_local

Q_MS = 15 * 60_000
CELLS = ['a', 'b']

FROM = datetime(2026, 1, 2, tzinfo=timezone.utc)
TO = datetime(2026, 1, 8, tzinfo=timezone.utc)

H4D_KEY = 'pyr/h/4d/2026-01-03.parquet'


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


def base_wide_frame(start_ms: int, end_ms: int) -> pl.DataFrame:
    rows = []
    for ms in range(start_ms, end_ms, Q_MS):
        i = ms // Q_MS
        for cell_idx, cell in enumerate(CELLS):
            hist = {i % 3: 1, (i % 5) + 3: 2}
            rows.append({
                'cell': cell,
                'dt': ms,
                'bikes': json.dumps({str(k): hist[k] for k in sorted(hist)}, separators=(',', ':')),
                'rides': (i + cell_idx) % 7,
                'temp_n': 2,
                'temp_sum': float(i),
                'temp_sumsq': float(i * i),
            })
    return pl.DataFrame(rows)


def write_base_shards(pyramid: Pyramid, shard_dur: str = '6h', to: datetime = TO) -> None:
    for period in shard_periods_covering(FROM, to, shard_dur):
        frame = base_wide_frame(
            int(period.start.timestamp() * 1000), int(period.end.timestamp() * 1000))
        key = substitute_key(
            pyramid.keyTemplate,
            {'tier': 'q', 'shard': shard_dur, 'period': period.label},
        )
        buf = io.BytesIO()
        write_tier_parquet(frame.to_arrow(), pyramid, out=buf)
        pyramid.storage.put(key, buf.getvalue())


def make_ladder(storage, *, extended: bool) -> Pyramid:
    """The lambda_shards shape: base ladder stores h only at 1d; the
    extended view adds the 4d consolidation rung."""
    p = make_pyramid(storage=storage)
    tiers = [
        p.tiers[0],
        replace(p.tiers[1], shards=('1d', '4d') if extended else ('1d',)),
        p.tiers[2],
    ]
    return replace(p, tiers=tiers)


def build_base_ladder() -> Pyramid:
    """Build the 1d-only ladder (h materialized as 6 × h@1d tiles), then
    return the extended-view pyramid over the same storage — an
    extension-fill playground missing its two h@4d consolidations."""
    storage = MemStorage()
    base = make_ladder(storage, extended=False)
    write_base_shards(base)
    build_local(
        base, (FROM, TO), WideShardSource(base, shard_dur='6h'), pyramid_name='test',
    )
    return make_ladder(storage, extended=True)

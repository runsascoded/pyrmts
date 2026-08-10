"""Engine calendar builds (`specs/calendar-units.md` acceptance #3): a
ladder with materialized calendar tiers (`1mo`/`3mo`/`1y` bins) cascades
(`1mo ← 15min`, `3mo ← 1mo`, `1y ← 3mo`), window-split-invariant
byte-for-byte, matching an independent hand-derived content anchor, with
`shard_periods_covering`/`format_period` producing the expected keys.

Local synthetic data (not conftest's): values are functions of `i % k` so
float accumulation stays exact over a full year (conftest's `sumsq=i²`
overflows 2^53 exactness at year scale)."""
from __future__ import annotations

import io
import json
from datetime import datetime, timezone

import polars as pl
import pyarrow.parquet as pq

from pyrmts import (
    MemStorage,
    Metric,
    Pyramid,
    Tier,
    shard_periods_covering,
    substitute_key,
    write_tier_parquet,
)
from pyrmts_engine import WideShardSource, build_local

from conftest import CELLS, Q_MS, make_pyramid

FROM = datetime(2024, 1, 1, tzinfo=timezone.utc)   # leap year, Feb included
TO = datetime(2025, 1, 1, tzinfo=timezone.utc)


def _calendar_pyramid() -> Pyramid:
    base = make_pyramid()
    return Pyramid(
        storage=MemStorage(),
        keyTemplate=base.keyTemplate,
        binCol='dt',
        dims=base.dims,
        metrics=base.metrics,
        tiers=[
            Tier(name='q', bin='15min', shards=('1d',)),
            Tier(name='mo', bin='1mo', shards=('1y',)),
            Tier(name='qtr', bin='3mo', shards=('1y',)),
            Tier(name='y', bin='1y', shards=('1y',)),
        ],
    )


def _wide_frame(start_ms: int, end_ms: int) -> pl.DataFrame:
    rows = []
    for ms in range(start_ms, end_ms, Q_MS):
        i = ms // Q_MS
        for cell_idx, cell in enumerate(CELLS):
            rows.append({
                'cell': cell,
                'dt': ms,
                'bikes': json.dumps(
                    {str(k): v for k, v in sorted({i % 3: 1, (i % 5) + 3: 2}.items())},
                    separators=(',', ':'),
                ),
                'rides': (i + cell_idx) % 7,
                'temp_n': 2,
                'temp_sum': float(i % 97),
                'temp_sumsq': float((i % 97) ** 2),
            })
    return pl.DataFrame(rows)


def _write_base(pyramid: Pyramid) -> None:
    for period in shard_periods_covering(FROM, TO, '1d'):
        frame = _wide_frame(
            int(period.start.timestamp() * 1000), int(period.end.timestamp() * 1000),
        )
        key = substitute_key(
            pyramid.keyTemplate, {'tier': 'q', 'shard': '1d', 'period': period.label},
        )
        buf = io.BytesIO()
        write_tier_parquet(frame.to_arrow(), pyramid, out=buf)
        pyramid.storage.put(key, buf.getvalue())


def _build(window: str) -> Pyramid:
    pyramid = _calendar_pyramid()
    _write_base(pyramid)
    build_local(
        pyramid, (FROM, TO), WideShardSource(pyramid, shard_dur='1d'),
        pyramid_name='test', window=window,
    )
    return pyramid


def _expected_row(s_ms: int, e_ms: int, cell_idx: int) -> dict:
    """Hand-derived aggregate for one output bucket — no shared code path
    with the engine (mirrors `_wide_frame`'s value functions directly)."""
    hist: dict[int, int] = {}
    rides = 0
    n = 0
    s = 0.0
    ssq = 0.0
    for ms in range(s_ms, e_ms, Q_MS):
        i = ms // Q_MS
        hist[i % 3] = hist.get(i % 3, 0) + 1
        hist[(i % 5) + 3] = hist.get((i % 5) + 3, 0) + 2
        rides += (i + cell_idx) % 7
        n += 2
        s += float(i % 97)
        ssq += float((i % 97) ** 2)
    return {
        'cell': CELLS[cell_idx], 'dt': s_ms, 'bikes': hist, 'rides': rides,
        'temp_n': n, 'temp_sum': s, 'temp_sumsq': ssq,
    }


def _parse_shard(blob: bytes) -> list[dict]:
    rows = pq.read_table(io.BytesIO(blob)).to_pylist()
    for r in rows:
        r['bikes'] = {int(k): v for k, v in json.loads(r['bikes']).items()}
    return sorted(rows, key=lambda r: (r['cell'], r['dt']))


CALENDAR_KEYS = [
    'pyr/mo/1y/2024.parquet',
    'pyr/qtr/1y/2024.parquet',
    'pyr/y/1y/2024.parquet',
]


def test_calendar_tier_build():
    pyramid = _build('4d')
    keys = [k for k in sorted(pyramid.storage.list('pyr/')) if not k.startswith('pyr/q/')]
    assert keys == CALENDAR_KEYS

    for tier_bin, key in [('1mo', CALENDAR_KEYS[0]), ('3mo', CALENDAR_KEYS[1]), ('1y', CALENDAR_KEYS[2])]:
        expected = [
            _expected_row(
                int(p.start.timestamp() * 1000), int(p.end.timestamp() * 1000), cell_idx,
            )
            for p in shard_periods_covering(FROM, TO, tier_bin)
            for cell_idx in range(len(CELLS))
        ]
        expected.sort(key=lambda r: (r['cell'], r['dt']))
        assert _parse_shard(pyramid.storage.get(key)) == expected


def test_calendar_build_window_invariance():
    """Calendar bins ≫ window: cross-window partial-bin merge yields
    byte-identical shards regardless of window size."""
    a = _build('4d')
    b = _build('32d')
    assert [
        (k, a.storage.get(k) == b.storage.get(k)) for k in CALENDAR_KEYS
    ] == [(k, True) for k in CALENDAR_KEYS]

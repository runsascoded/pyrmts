"""Cascade integration tests for the unified-shard-ladder model.

Each tier declares `shards: tuple[str, ...]` (ascending, divisibility-chain).
The caller materializes `(tiers[0], tiers[0].shards[0])`; cascade fills in
every other (tier, shard_dur) rung."""
from __future__ import annotations

import io
import json
from collections import defaultdict
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyrmts import (
    Dim,
    MemStorage,
    Metric,
    Pyramid,
    Tier,
    cascade_tiers,
    parse_duration,
    floor_to_span,
    shard_periods_covering,
)


UTC = timezone.utc


def _ms(t: datetime) -> int:
    return int(t.timestamp() * 1000)


def _make_pyramid(storage: MemStorage, *, with_dim: bool = True) -> Pyramid:
    """5min → 15min → 1h → 1d ladder, histogram monoid, optional `station_id` dim.

    Each tier has a single shard rung — same shape as the pre-ladder tests, just
    expressed as `shards=('1h',)` etc. Cross-tier promotion still occurs at
    every tier boundary."""
    return Pyramid(
        storage=storage,
        keyTemplate='avail/{tier}/{period}.parquet',
        binCol='dt',
        dims=[Dim(name='station_id', type='string')] if with_dim else [],
        metrics=[Metric(name='bikes', monoid='histogram')],
        tiers=[
            Tier(name='5m',  bin='5min',  shards=('1h',)),
            Tier(name='15m', bin='15min', shards=('1h',)),
            Tier(name='1h',  bin='1h',    shards=('1d',)),
            Tier(name='1d',  bin='1d',    shards=('1mo',)),
        ],
    )


def _write_finest_shards(
    pyramid: Pyramid,
    observations: list[tuple[datetime, str, int]],  # (ts, station_id, state)
) -> None:
    """Write the finest tier's smallest shard rung directly from per-minute
    observations.

    Each observation contributes a 1-minute histogram entry `{state: 1}` to
    the tier-bin bucket covering its timestamp."""
    finest = pyramid.tiers[0]
    bin_span = parse_duration(finest.bin)
    shard_span = parse_duration(finest.shards[0])

    # (shard_label, bin_ms, station_id) -> hist
    accum: dict[tuple[str, int, str], dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for ts, sid, state in observations:
        shard_start = floor_to_span(ts, shard_span)
        shard_label = _format_period(shard_start, shard_span.unit)
        bin_start_ms = _ms(floor_to_span(ts, bin_span))
        accum[(shard_label, bin_start_ms, sid)][str(state)] += 1

    by_shard: dict[str, list] = defaultdict(list)
    for (shard_label, bin_ms, sid), hist in accum.items():
        by_shard[shard_label].append((bin_ms, sid, json.dumps(dict(sorted(hist.items())))))

    for shard_label, rows in by_shard.items():
        rows.sort(key=lambda r: (r[1], r[0]))
        table = pa.table({
            'dt': [r[0] for r in rows],
            'station_id': [r[1] for r in rows],
            'bikes': [r[2] for r in rows],
        })
        buf = io.BytesIO()
        pq.write_table(table, buf, compression='snappy')
        key = f"avail/{finest.name}/{shard_label}.parquet"
        pyramid.storage.put(key, buf.getvalue())


def _format_period(start: datetime, unit: str) -> str:
    if unit == 'h':   return start.strftime('%Y-%m-%dT%H')
    if unit == 'd':   return start.strftime('%Y-%m-%d')
    if unit == 'mo':  return start.strftime('%Y-%m')
    if unit == 'y':   return start.strftime('%Y')
    if unit == 'min': return start.strftime('%Y-%m-%dT%H-%M')
    raise AssertionError(unit)


def _read_hist_rows(storage: MemStorage, key: str) -> list[dict]:
    blob = storage.get(key)
    if blob is None: return []
    table = pq.read_table(io.BytesIO(blob))
    out: list[dict] = []
    for row in table.to_pylist():
        row['bikes'] = json.loads(row['bikes']) if row.get('bikes') else {}
        out.append(row)
    return out


def _expected_hist(
    observations: list[tuple[datetime, str, int]],
    bin: str,
) -> dict[tuple[int, str], dict[str, int]]:
    """Compute the expected (bin_ms, station_id) → histogram directly."""
    bin_span = parse_duration(bin)
    accum: dict[tuple[int, str], dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for ts, sid, state in observations:
        bin_ms = _ms(floor_to_span(ts, bin_span))
        accum[(bin_ms, sid)][str(state)] += 1
    return {k: dict(v) for k, v in accum.items()}


def test_cascade_histogram_5m_to_15m_to_1h():
    storage = MemStorage()
    pyramid = _make_pyramid(storage)
    base = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)

    # 1-minute observations over a 2-hour window for 2 stations, alternating states.
    observations: list[tuple[datetime, str, int]] = []
    for minute in range(120):
        ts = datetime(2026, 5, 10, 12 + minute // 60, minute % 60, tzinfo=UTC)
        for sid in ('s1', 's2'):
            state = (minute + (0 if sid == 's1' else 7)) % 3
            observations.append((ts, sid, state))

    _write_finest_shards(pyramid, observations)

    result = cascade_tiers(
        pyramid,
        time_range=(base, datetime(2026, 5, 10, 14, 0, tzinfo=UTC)),
    )
    assert result.errors == []
    assert len(result.written) > 0

    for tier_name, tier_bin, periods in [
        ('15m', '15min', ['2026-05-10T12', '2026-05-10T13']),
        ('1h',  '1h',    ['2026-05-10']),
        ('1d',  '1d',    ['2026-05']),
    ]:
        actual: dict[tuple[int, str], dict[str, int]] = {}
        for p in periods:
            for row in _read_hist_rows(storage, f"avail/{tier_name}/{p}.parquet"):
                actual[(row['dt'], row['station_id'])] = row['bikes']
        expected = _expected_hist(observations, tier_bin)
        assert actual == expected, f"tier {tier_name} mismatch"


def test_cascade_skip_if_exists():
    storage = MemStorage()
    pyramid = _make_pyramid(storage)
    base = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    observations = [
        (datetime(2026, 5, 10, 12, m, tzinfo=UTC), 's1', m % 2)
        for m in range(60)
    ]
    _write_finest_shards(pyramid, observations)

    r1 = cascade_tiers(pyramid, time_range=(base, datetime(2026, 5, 10, 13, 0, tzinfo=UTC)))
    assert len(r1.written) > 0
    assert len(r1.skipped) == 0

    r2 = cascade_tiers(pyramid, time_range=(base, datetime(2026, 5, 10, 13, 0, tzinfo=UTC)))
    assert r2.written == []
    assert len(r2.skipped) == len(r1.written)


def test_cascade_overwrite():
    storage = MemStorage()
    pyramid = _make_pyramid(storage)
    base = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    observations = [
        (datetime(2026, 5, 10, 12, m, tzinfo=UTC), 's1', m % 2)
        for m in range(60)
    ]
    _write_finest_shards(pyramid, observations)

    cascade_tiers(pyramid, time_range=(base, datetime(2026, 5, 10, 13, 0, tzinfo=UTC)))
    r = cascade_tiers(
        pyramid,
        time_range=(base, datetime(2026, 5, 10, 13, 0, tzinfo=UTC)),
        overwrite=True,
    )
    assert r.skipped == []
    assert len(r.written) > 0


def test_cascade_within_tier_ladder():
    """Same-tier ladder rung: build `(raw, 1d)` from `(raw, 1h)` 24× combine."""
    storage = MemStorage()
    pyramid = Pyramid(
        storage=storage,
        keyTemplate='c/{tier}/{shard}/{period}.parquet',
        binCol='ts',
        dims=[],
        metrics=[Metric(name='n', monoid='count')],
        tiers=[
            Tier(name='raw', bin='1min', shards=('1h', '1d')),
        ],
    )
    base = datetime(2026, 5, 10, 0, 0, tzinfo=UTC)

    # 24 1h shards, each with 60 1min rows.
    for h in range(24):
        rows = [
            {'ts': _ms(datetime(2026, 5, 10, h, m, tzinfo=UTC)), 'n': 1}
            for m in range(60)
        ]
        table = pa.table({'ts': [r['ts'] for r in rows], 'n': [r['n'] for r in rows]})
        buf = io.BytesIO()
        pq.write_table(table, buf)
        period = f"2026-05-10T{h:02d}"
        storage.put(f"c/raw/1h/{period}.parquet", buf.getvalue())

    result = cascade_tiers(
        pyramid,
        time_range=(base, datetime(2026, 5, 11, tzinfo=UTC)),
    )
    assert result.errors == []
    # The `(raw, 1d)` rung is the only output; cross-tier doesn't apply (single tier).
    assert result.written == ['c/raw/1d/2026-05-10.parquet']

    out = pq.read_table(io.BytesIO(storage.get('c/raw/1d/2026-05-10.parquet'))).to_pylist()
    # All 60×24 = 1440 minute-bins survive at the `raw` tier's bin (1min);
    # only the shard size grows (1h → 1d).
    assert len(out) == 24 * 60
    assert sum(r['n'] for r in out) == 24 * 60


def test_cascade_multi_rung_with_cross_tier_promotion():
    """Mixed ladder: finest tier `[5min, 1h]` + coarser tier `[1d]`. Caller
    writes (5m, 5min); cascade builds (5m, 1h) → (1h, 1d)."""
    storage = MemStorage()
    pyramid = Pyramid(
        storage=storage,
        keyTemplate='m/{tier}/{shard}/{period}.parquet',
        binCol='ts',
        dims=[],
        metrics=[Metric(name='temp', monoid='sum')],
        tiers=[
            Tier(name='5m', bin='5min', shards=('5min', '1h')),
            Tier(name='1h', bin='1h',   shards=('1d',)),
        ],
    )

    # Write the finest rung (5m, 5min): 12 5min bins per hour × 2 hours.
    base = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    for h in range(2):
        rows = []
        for i in range(12):
            ts = datetime(2026, 5, 10, 12 + h, i * 5, tzinfo=UTC)
            temp = 20 + h * 12 + i  # monotone over both hours
            rows.append({
                'ts': _ms(ts),
                'temp_n': 5,
                'temp_sum': 5.0 * temp,
                'temp_sumsq': 5.0 * temp * temp,
            })
        # The (5m, 5min) shard is the finest rung; period label = HH-prefixed minute.
        table = pa.table({k: [r[k] for r in rows] for k in rows[0]})
        buf = io.BytesIO()
        pq.write_table(table, buf)
        # Each 5min shard covers one bin; we'd normally have 12 shards per hour,
        # but using 5min as the shard duration makes each shard a single row.
        # Write them per actual 5min period.
        for row in rows:
            ts_dt = datetime.fromtimestamp(row['ts'] / 1000, tz=UTC)
            period = ts_dt.strftime('%Y-%m-%dT%H-%M')
            sub_table = pa.table({k: [v] for k, v in row.items()})
            sub_buf = io.BytesIO()
            pq.write_table(sub_table, sub_buf)
            storage.put(f"m/5m/5min/{period}.parquet", sub_buf.getvalue())

    result = cascade_tiers(
        pyramid,
        time_range=(base, datetime(2026, 5, 10, 14, 0, tzinfo=UTC)),
    )
    assert result.errors == []

    # Same-tier promotion: (5m, 1h) from (5m, 5min).
    out_5m_1h_12 = pq.read_table(
        io.BytesIO(storage.get('m/5m/1h/2026-05-10T12.parquet'))
    ).to_pylist()
    # 12 5-minute bins survive at tier bin=5min.
    assert len(out_5m_1h_12) == 12
    expected_sum_h0 = sum(5.0 * (20 + i) for i in range(12))
    assert sum(r['temp_sum'] for r in out_5m_1h_12) == pytest.approx(expected_sum_h0)

    # Cross-tier promotion: (1h, 1d) from (5m, 1h).
    out_1h_1d = pq.read_table(
        io.BytesIO(storage.get('m/1h/1d/2026-05-10.parquet'))
    ).to_pylist()
    # 1h tier has bin=1h; 2 hours of data → 2 rows.
    assert len(out_1h_1d) == 2
    assert out_1h_1d[0]['ts'] == _ms(base)
    assert out_1h_1d[1]['ts'] == _ms(datetime(2026, 5, 10, 13, tzinfo=UTC))
    # Sums match by-hour totals from the finest data.
    expected_sum_h1 = sum(5.0 * (20 + 12 + i) for i in range(12))
    assert out_1h_1d[0]['temp_sum'] == pytest.approx(expected_sum_h0)
    assert out_1h_1d[1]['temp_sum'] == pytest.approx(expected_sum_h1)


def test_cascade_sum_monoid():
    """Cascade with `sum` monoid (n, sum, sumsq)."""
    storage = MemStorage()
    pyramid = Pyramid(
        storage=storage,
        keyTemplate='m/{tier}/{period}.parquet',
        binCol='ts',
        dims=[],
        metrics=[Metric(name='temp', monoid='sum')],
        tiers=[
            Tier(name='1min', bin='1min', shards=('1h',)),
            Tier(name='1h',   bin='1h',   shards=('1d',)),
            Tier(name='1d',   bin='1d',   shards=('1mo',)),
        ],
    )

    # Write 60 finest rows: temp = 20, 21, 22, …, 79 over an hour.
    rows = []
    base = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    for m in range(60):
        ts = datetime(2026, 5, 10, 12, m, tzinfo=UTC)
        temp = 20 + m
        rows.append({
            'ts':        _ms(ts),
            'temp_n':    1,
            'temp_sum':  float(temp),
            'temp_sumsq': float(temp * temp),
        })

    table = pa.table({
        'ts':         [r['ts'] for r in rows],
        'temp_n':     [r['temp_n'] for r in rows],
        'temp_sum':   [r['temp_sum'] for r in rows],
        'temp_sumsq': [r['temp_sumsq'] for r in rows],
    })
    buf = io.BytesIO()
    pq.write_table(table, buf)
    storage.put('m/1min/2026-05-10T12.parquet', buf.getvalue())

    cascade_tiers(pyramid, time_range=(base, datetime(2026, 5, 10, 13, 0, tzinfo=UTC)))

    blob = storage.get('m/1h/2026-05-10.parquet')
    assert blob is not None
    out = pq.read_table(io.BytesIO(blob)).to_pylist()
    assert len(out) == 1
    expected_sum = sum(20 + m for m in range(60))
    expected_sumsq = sum((20 + m) ** 2 for m in range(60))
    assert out[0]['ts'] == _ms(base)
    assert out[0]['temp_n'] == 60
    assert out[0]['temp_sum'] == expected_sum
    assert out[0]['temp_sumsq'] == expected_sumsq


def test_cascade_count_monoid():
    storage = MemStorage()
    pyramid = Pyramid(
        storage=storage,
        keyTemplate='c/{tier}/{period}.parquet',
        binCol='ts',
        dims=[],
        metrics=[Metric(name='n', monoid='count')],
        tiers=[
            Tier(name='1min', bin='1min', shards=('1h',)),
            Tier(name='1h',   bin='1h',   shards=('1d',)),
        ],
    )
    base = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    rows = [
        {'ts': _ms(datetime(2026, 5, 10, 12, m, tzinfo=UTC)), 'n': m + 1}
        for m in range(60)
    ]
    table = pa.table({'ts': [r['ts'] for r in rows], 'n': [r['n'] for r in rows]})
    buf = io.BytesIO()
    pq.write_table(table, buf)
    storage.put('c/1min/2026-05-10T12.parquet', buf.getvalue())

    cascade_tiers(pyramid, time_range=(base, datetime(2026, 5, 10, 13, 0, tzinfo=UTC)))

    out = pq.read_table(io.BytesIO(storage.get('c/1h/2026-05-10.parquet'))).to_pylist()
    assert len(out) == 1
    assert out[0]['ts'] == _ms(base)
    assert out[0]['n'] == sum(range(1, 61))


def test_cascade_with_filter():
    """`filter` injects extra keyTemplate vars (e.g. device_id for awair)."""
    storage = MemStorage()
    pyramid = Pyramid(
        storage=storage,
        keyTemplate='awair-{device_id}/{tier}/{period}.parquet',
        binCol='ts',
        dims=[],
        metrics=[Metric(name='n', monoid='count')],
        tiers=[
            Tier(name='1min', bin='1min', shards=('1h',)),
            Tier(name='1h',   bin='1h',   shards=('1d',)),
        ],
    )
    base = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    rows = [{'ts': _ms(datetime(2026, 5, 10, 12, m, tzinfo=UTC)), 'n': 1} for m in range(60)]
    table = pa.table({'ts': [r['ts'] for r in rows], 'n': [r['n'] for r in rows]})
    buf = io.BytesIO()
    pq.write_table(table, buf)
    storage.put('awair-17617/1min/2026-05-10T12.parquet', buf.getvalue())

    result = cascade_tiers(
        pyramid,
        time_range=(base, datetime(2026, 5, 10, 13, 0, tzinfo=UTC)),
        filter={'device_id': 17617},
    )
    assert result.errors == []
    assert 'awair-17617/1h/2026-05-10.parquet' in result.written


def test_cascade_no_source_skips_quietly():
    storage = MemStorage()
    pyramid = _make_pyramid(storage)
    result = cascade_tiers(
        pyramid,
        time_range=(
            datetime(2026, 5, 10, 12, 0, tzinfo=UTC),
            datetime(2026, 5, 10, 13, 0, tzinfo=UTC),
        ),
    )
    assert result.errors == []
    assert result.written == []

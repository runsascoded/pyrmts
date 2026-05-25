"""Cascade integration test: build a finest tier directly, cascade up, then
verify outputs match a direct-from-source aggregation."""
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
    """5min → 15min → 1h → 1d ladder, histogram monoid, optional `station_id` dim."""
    return Pyramid(
        storage=storage,
        keyTemplate='avail/{tier}/{period}.parquet',
        binCol='dt',
        dims=[Dim(name='station_id', type='string')] if with_dim else [],
        metrics=[Metric(name='bikes', monoid='histogram')],
        tiers=[
            Tier(name='5m',  bin='5min', shard='1h'),
            Tier(name='15m', bin='15min', shard='1h'),
            Tier(name='1h',  bin='1h',   shard='1d'),
            Tier(name='1d',  bin='1d',   shard='1mo'),
        ],
    )


def _write_finest_shards(
    pyramid: Pyramid,
    observations: list[tuple[datetime, str, int]],  # (ts, station_id, state)
) -> None:
    """Write the 5m@1h finest tier directly from per-minute observations.

    Each observation contributes a 1-minute histogram entry `{state: 1}` to
    the 5-minute bucket covering its timestamp."""
    finest = pyramid.tiers[0]
    bin_span = parse_duration(finest.bin)
    shard_span = parse_duration(finest.shard)

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


def test_cascade_explicit_derive_from():
    storage = MemStorage()
    pyramid = _make_pyramid(storage)
    base = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    observations = [
        (datetime(2026, 5, 10, 12 + h, m, tzinfo=UTC), 's1', (h * 60 + m) % 4)
        for h in range(2) for m in range(60)
    ]
    _write_finest_shards(pyramid, observations)

    # Derive 1h directly from 5m, skipping 15m. Skip 15m and 1d via empty source.
    result = cascade_tiers(
        pyramid,
        time_range=(base, datetime(2026, 5, 10, 14, 0, tzinfo=UTC)),
        derive_from={'1h': '5m'},
    )
    assert result.errors == []

    expected_1h = _expected_hist(observations, '1h')
    actual: dict[tuple[int, str], dict[str, int]] = {}
    for row in _read_hist_rows(storage, 'avail/1h/2026-05-10.parquet'):
        actual[(row['dt'], row['station_id'])] = row['bikes']
    assert actual == expected_1h


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
            Tier(name='1min', bin='1min', shard='1h'),
            Tier(name='1h',   bin='1h',   shard='1d'),
            Tier(name='1d',   bin='1d',   shard='1mo'),
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
            Tier(name='1min', bin='1min', shard='1h'),
            Tier(name='1h',   bin='1h',   shard='1d'),
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
            Tier(name='1min', bin='1min', shard='1h'),
            Tier(name='1h',   bin='1h',   shard='1d'),
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

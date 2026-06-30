"""Tests for `list_expected_shards` — pure enumeration over a Pyramid's
ladders. Mirrors `js/.../gap-discovery.test.ts`'s `listExpectedShards`
acceptance cases (Python has no `ShardIndex` port, so `list_missing_shards`
is JS-only — see `specs/done/python-unified-ladder.md`)."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pyrmts import (
    Dim,
    MemStorage,
    Metric,
    Pyramid,
    Tier,
    list_expected_shards,
)


UTC = timezone.utc


def _make_pyramid() -> Pyramid:
    return Pyramid(
        storage=MemStorage(),
        keyTemplate='avail/{tier}/{shard}/{period}.parquet',
        binCol='ts',
        dims=[],
        metrics=[Metric(name='n', monoid='count')],
        tiers=[
            Tier(name='1m', bin='1min', shards=('5min', '1h')),
        ],
    )


def _make_calendar_pyramid() -> Pyramid:
    return Pyramid(
        storage=MemStorage(),
        keyTemplate='a/{tier}/{shard}/{period}.parquet',
        binCol='ts',
        dims=[],
        metrics=[Metric(name='n', monoid='count')],
        tiers=[
            Tier(name='1d', bin='1d', shards=('1d', '1mo')),
        ],
    )


def _make_filter_pyramid() -> Pyramid:
    return Pyramid(
        storage=MemStorage(),
        keyTemplate='awair-{device_id}/{tier}/{shard}/{period}.parquet',
        binCol='ts',
        dims=[],
        metrics=[Metric(name='n', monoid='count')],
        tiers=[
            Tier(name='1m', bin='1min', shards=('1h',)),
        ],
    )


def test_one_tier_two_rungs_over_1h_returns_13_entries():
    """12 × 5min + 1 × 1h = 13 entries."""
    p = _make_pyramid()
    got = list_expected_shards(p, (
        datetime(2026, 6, 1, 0, 0, tzinfo=UTC),
        datetime(2026, 6, 1, 1, 0, tzinfo=UTC),
    ))
    assert len(got) == 13
    by_rung: dict[str, int] = {}
    for s in got:
        by_rung[s.shard_dur] = by_rung.get(s.shard_dur, 0) + 1
    assert by_rung == {'5min': 12, '1h': 1}


def test_substitutes_tier_shard_period_into_key_template():
    p = _make_pyramid()
    got = list_expected_shards(p, (
        datetime(2026, 6, 1, 0, 0, tzinfo=UTC),
        datetime(2026, 6, 1, 0, 15, tzinfo=UTC),
    ))
    summary = sorted((s.tier, s.shard_dur, s.period_start.isoformat(), s.key) for s in got)
    assert summary == [
        ('1m', '1h', '2026-06-01T00:00:00+00:00', 'avail/1m/1h/2026-06-01T00.parquet'),
        ('1m', '5min', '2026-06-01T00:00:00+00:00', 'avail/1m/5min/2026-06-01T00-00.parquet'),
        ('1m', '5min', '2026-06-01T00:05:00+00:00', 'avail/1m/5min/2026-06-01T00-05.parquet'),
        ('1m', '5min', '2026-06-01T00:10:00+00:00', 'avail/1m/5min/2026-06-01T00-10.parquet'),
    ]


def test_calendar_ladder_over_two_month_range():
    """1d ladder rung + 1mo ladder rung over June+July: 30+31 days + 2 months."""
    p = _make_calendar_pyramid()
    got = list_expected_shards(p, (
        datetime(2026, 6, 1, tzinfo=UTC),
        datetime(2026, 8, 1, tzinfo=UTC),
    ))
    by_rung: dict[str, int] = {}
    for s in got:
        by_rung[s.shard_dur] = by_rung.get(s.shard_dur, 0) + 1
    assert by_rung == {'1d': 61, '1mo': 2}


def test_filter_values_fill_custom_placeholders():
    p = _make_filter_pyramid()
    got = list_expected_shards(
        p,
        (datetime(2026, 6, 1, 0, 0, tzinfo=UTC), datetime(2026, 6, 1, 1, 0, tzinfo=UTC)),
        filter={'device_id': 17617},
    )
    assert len(got) == 1
    assert got[0].key == 'awair-17617/1m/1h/2026-06-01T00.parquet'


def test_raises_on_missing_filter_placeholder():
    p = _make_filter_pyramid()
    with pytest.raises(KeyError, match=r'\{device_id\}'):
        list_expected_shards(
            p,
            (datetime(2026, 6, 1, 0, 0, tzinfo=UTC), datetime(2026, 6, 1, 1, 0, tzinfo=UTC)),
        )


def test_empty_range_returns_no_entries():
    p = _make_pyramid()
    got = list_expected_shards(p, (
        datetime(2026, 6, 1, tzinfo=UTC),
        datetime(2026, 6, 1, tzinfo=UTC),
    ))
    assert got == []

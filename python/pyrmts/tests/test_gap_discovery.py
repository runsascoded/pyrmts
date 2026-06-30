"""Tests for `list_expected_shards` — minimal-cover enumeration. Mirrors
`js/.../gap-discovery.test.ts`'s `listExpectedShards` cases."""
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


def _pyramid(tiers: list[Tier], key_template: str = 'avail/{tier}/{shard}/{period}.parquet') -> Pyramid:
    return Pyramid(
        storage=MemStorage(),
        keyTemplate=key_template,
        binCol='ts',
        dims=[],
        metrics=[Metric(name='n', monoid='count')],
        tiers=tiers,
    )


def test_to_aligned_to_max_shard_emits_one_max_shard():
    """`[00:00, 01:00)` over 1m{5min,1h} → 1 × /1m@1h (no trailing window)."""
    p = _pyramid([Tier(name='1m', bin='1min', shards=('5min', '1h'))])
    got = list_expected_shards(p, (
        datetime(2026, 6, 1, 0, 0, tzinfo=UTC),
        datetime(2026, 6, 1, 1, 0, tzinfo=UTC),
    ))
    assert len(got) == 1
    assert (got[0].tier, got[0].shard_dur) == ('1m', '1h')
    assert got[0].key == 'avail/1m/1h/2026-06-01T00.parquet'


def test_trailing_window_decomposes_into_largest_fitting_rungs():
    """`[00:00, 00:15)` over 1m{5min,1h} → no max-shard (15min < 1h);
    trailing window of 15min tiles as 3 × /1m@5min."""
    p = _pyramid([Tier(name='1m', bin='1min', shards=('5min', '1h'))])
    got = list_expected_shards(p, (
        datetime(2026, 6, 1, 0, 0, tzinfo=UTC),
        datetime(2026, 6, 1, 0, 15, tzinfo=UTC),
    ))
    summary = sorted((s.tier, s.shard_dur, s.period_start.isoformat(), s.key) for s in got)
    assert summary == [
        ('1m', '5min', '2026-06-01T00:00:00+00:00', 'avail/1m/5min/2026-06-01T00-00.parquet'),
        ('1m', '5min', '2026-06-01T00:05:00+00:00', 'avail/1m/5min/2026-06-01T00-05.parquet'),
        ('1m', '5min', '2026-06-01T00:10:00+00:00', 'avail/1m/5min/2026-06-01T00-10.parquet'),
    ]


def test_max_shards_plus_trailing_partial():
    """`[00:00, 02:35)` over 1m{5min,30min,1h} →
       2 × /1m@1h (00:00-02:00) + /1m@30min (02:00-02:30) + /1m@5min (02:30-02:35)."""
    p = _pyramid([Tier(name='1m', bin='1min', shards=('5min', '30min', '1h'))])
    got = list_expected_shards(p, (
        datetime(2026, 6, 1, 0, 0, tzinfo=UTC),
        datetime(2026, 6, 1, 2, 35, tzinfo=UTC),
    ))
    summary = sorted((s.shard_dur, s.period_start.isoformat()) for s in got)
    assert summary == [
        ('1h', '2026-06-01T00:00:00+00:00'),
        ('1h', '2026-06-01T01:00:00+00:00'),
        ('30min', '2026-06-01T02:00:00+00:00'),
        ('5min', '2026-06-01T02:30:00+00:00'),
    ]


def test_avail_v3_one_day_range_minimal_cover():
    """Avail-v3 `/1m` ladder over a full day: a single max-shard at the
    day boundary. (Vs the redundant enumeration that would emit 289
    shards across all 7 rungs.)"""
    p = _pyramid([
        Tier(name='1m', bin='1min', shards=('5min', '10min', '30min', '1h', '3h', '12h', '1d')),
    ])
    got = list_expected_shards(p, (
        datetime(2026, 6, 1, tzinfo=UTC),
        datetime(2026, 6, 2, tzinfo=UTC),
    ))
    assert [(s.shard_dur, s.period_start) for s in got] == [
        ('1d', datetime(2026, 6, 1, tzinfo=UTC)),
    ]


def test_avail_v3_partial_day_trailing_decomposition():
    """`/1m` ladder over `[00:00, 18:35)` on a single day: no max-shard
    (entire range is in the day's open window); trailing 18h35m tiles
    as 12h + 3h + 3h + 30min + 5min = 5 shards."""
    p = _pyramid([
        Tier(name='1m', bin='1min', shards=('5min', '10min', '30min', '1h', '3h', '12h', '1d')),
    ])
    got = list_expected_shards(p, (
        datetime(2026, 6, 1, tzinfo=UTC),
        datetime(2026, 6, 1, 18, 35, tzinfo=UTC),
    ))
    summary = [(s.shard_dur, s.period_start.isoformat()) for s in got]
    assert summary == [
        ('12h', '2026-06-01T00:00:00+00:00'),
        ('3h',  '2026-06-01T12:00:00+00:00'),
        ('3h',  '2026-06-01T15:00:00+00:00'),
        ('30min', '2026-06-01T18:00:00+00:00'),
        ('5min',  '2026-06-01T18:30:00+00:00'),
    ]


def test_from_mid_period_emits_max_shard_starting_before_from():
    """`[T00:30, T+1d 00:00)` over 1m{5min,1d}: max-shard at day boundary
    (starting BEFORE `from`); no trailing (to is day-aligned)."""
    p = _pyramid([Tier(name='1m', bin='1min', shards=('5min', '1d'))])
    got = list_expected_shards(p, (
        datetime(2026, 6, 1, 0, 30, tzinfo=UTC),
        datetime(2026, 6, 2, 0, 0, tzinfo=UTC),
    ))
    assert [(s.shard_dur, s.period_start) for s in got] == [
        ('1d', datetime(2026, 6, 1, tzinfo=UTC)),
    ]


def test_remaining_below_smallest_rung_breaks_loop():
    """When the trailing residual is smaller than the smallest non-max
    rung, no shard is emitted for it — the caller's next-finer tier
    handles that sub-rung resolution."""
    p = _pyramid([Tier(name='1m', bin='1min', shards=('5min', '1h'))])
    got = list_expected_shards(p, (
        datetime(2026, 6, 1, 0, 0, tzinfo=UTC),
        datetime(2026, 6, 1, 0, 7, tzinfo=UTC),  # 7min — 1 × 5min tiles, 2min residual unmet
    ))
    assert [(s.shard_dur, s.period_start) for s in got] == [
        ('5min', datetime(2026, 6, 1, 0, 0, tzinfo=UTC)),
    ]


def test_substitutes_tier_shard_period_into_key_template():
    p = _pyramid([Tier(name='1m', bin='1min', shards=('5min', '1h'))])
    got = list_expected_shards(p, (
        datetime(2026, 6, 1, 1, 0, tzinfo=UTC),
        datetime(2026, 6, 1, 2, 0, tzinfo=UTC),
    ))
    assert len(got) == 1
    assert got[0].key == 'avail/1m/1h/2026-06-01T01.parquet'


def test_calendar_ladder_one_max_shard_per_month():
    """1d{1d,1mo} over `[Jun 1, Aug 1)`: 2 × /1d@1mo (Jun, Jul)."""
    p = _pyramid([Tier(name='1d', bin='1d', shards=('1d', '1mo'))])
    got = list_expected_shards(p, (
        datetime(2026, 6, 1, tzinfo=UTC),
        datetime(2026, 8, 1, tzinfo=UTC),
    ))
    summary = sorted((s.shard_dur, s.period_start.isoformat()) for s in got)
    assert summary == [
        ('1mo', '2026-06-01T00:00:00+00:00'),
        ('1mo', '2026-07-01T00:00:00+00:00'),
    ]


def test_filter_values_fill_custom_placeholders():
    p = _pyramid(
        [Tier(name='1m', bin='1min', shards=('1h',))],
        key_template='awair-{device_id}/{tier}/{shard}/{period}.parquet',
    )
    got = list_expected_shards(
        p,
        (datetime(2026, 6, 1, 0, 0, tzinfo=UTC), datetime(2026, 6, 1, 1, 0, tzinfo=UTC)),
        filter={'device_id': 17617},
    )
    assert len(got) == 1
    assert got[0].key == 'awair-17617/1m/1h/2026-06-01T00.parquet'


def test_raises_on_missing_filter_placeholder():
    p = _pyramid(
        [Tier(name='1m', bin='1min', shards=('1h',))],
        key_template='awair-{device_id}/{tier}/{shard}/{period}.parquet',
    )
    with pytest.raises(KeyError, match=r'\{device_id\}'):
        list_expected_shards(
            p,
            (datetime(2026, 6, 1, 0, 0, tzinfo=UTC), datetime(2026, 6, 1, 1, 0, tzinfo=UTC)),
        )


def test_empty_range_returns_no_entries():
    p = _pyramid([Tier(name='1m', bin='1min', shards=('5min', '1h'))])
    got = list_expected_shards(p, (
        datetime(2026, 6, 1, tzinfo=UTC),
        datetime(2026, 6, 1, tzinfo=UTC),
    ))
    assert got == []

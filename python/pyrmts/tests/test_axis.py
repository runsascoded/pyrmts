from datetime import datetime, timezone

from pyrmts import (
    add_span,
    bins_in_range,
    floor_to_span,
    format_period,
    parse_duration,
    shard_periods_covering,
)
from pyrmts.axis import ParsedTimeSpan


UTC = timezone.utc


def test_parse_duration():
    assert parse_duration('1min') == ParsedTimeSpan(count=1, unit='min')
    assert parse_duration('5min') == ParsedTimeSpan(count=5, unit='min')
    assert parse_duration('2h')   == ParsedTimeSpan(count=2, unit='h')
    assert parse_duration('1mo')  == ParsedTimeSpan(count=1, unit='mo')
    assert parse_duration('1y')   == ParsedTimeSpan(count=1, unit='y')


def test_add_span_fixed_width():
    t = datetime(2026, 5, 10, 12, 34, tzinfo=UTC)
    assert add_span(t, parse_duration('5min')) == datetime(2026, 5, 10, 12, 39, tzinfo=UTC)
    assert add_span(t, parse_duration('1h'))   == datetime(2026, 5, 10, 13, 34, tzinfo=UTC)
    assert add_span(t, parse_duration('1d'))   == datetime(2026, 5, 11, 12, 34, tzinfo=UTC)


def test_add_span_calendar():
    t = datetime(2026, 5, 10, tzinfo=UTC)
    assert add_span(t, parse_duration('1mo'))  == datetime(2026, 6, 10, tzinfo=UTC)
    assert add_span(t, parse_duration('8mo'))  == datetime(2027, 1, 10, tzinfo=UTC)
    assert add_span(t, parse_duration('1y'))   == datetime(2027, 5, 10, tzinfo=UTC)


def test_floor_to_span():
    t = datetime(2026, 5, 10, 12, 34, 56, 789_000, tzinfo=UTC)
    assert floor_to_span(t, parse_duration('1min')) == datetime(2026, 5, 10, 12, 34, tzinfo=UTC)
    assert floor_to_span(t, parse_duration('1h'))   == datetime(2026, 5, 10, 12, tzinfo=UTC)
    assert floor_to_span(t, parse_duration('1d'))   == datetime(2026, 5, 10, tzinfo=UTC)
    assert floor_to_span(t, parse_duration('1mo'))  == datetime(2026, 5, 1, tzinfo=UTC)
    assert floor_to_span(t, parse_duration('1y'))   == datetime(2026, 1, 1, tzinfo=UTC)


def test_floor_to_span_multi():
    t = datetime(2026, 5, 10, 12, 7, 0, tzinfo=UTC)
    assert floor_to_span(t, parse_duration('5min')) == datetime(2026, 5, 10, 12, 5, tzinfo=UTC)
    assert floor_to_span(t, parse_duration('15min')) == datetime(2026, 5, 10, 12, 0, tzinfo=UTC)


def test_bins_in_range():
    f = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
    t = datetime(2026, 5, 10, 13, 0, tzinfo=UTC)
    assert bins_in_range(f, t, '1min') == 60
    assert bins_in_range(f, t, '5min') == 12
    assert bins_in_range(f, t, '1h')   == 1
    assert bins_in_range(f, f, '1min') == 0


def test_shard_periods_covering_calendar():
    f = datetime(2026, 5, 1, tzinfo=UTC)
    t = datetime(2026, 7, 1, tzinfo=UTC)
    periods = shard_periods_covering(f, t, '1mo')
    assert [p.label for p in periods] == ['2026-05', '2026-06']
    assert periods[0].start == datetime(2026, 5, 1, tzinfo=UTC)
    assert periods[0].end   == datetime(2026, 6, 1, tzinfo=UTC)


def test_shard_periods_covering_hourly():
    f = datetime(2026, 5, 10, 23, 45, tzinfo=UTC)
    t = datetime(2026, 5, 11, 1, 15, tzinfo=UTC)
    periods = shard_periods_covering(f, t, '1h')
    assert [p.label for p in periods] == [
        '2026-05-10T23',
        '2026-05-11T00',
        '2026-05-11T01',
    ]


def test_format_period():
    t = datetime(2026, 5, 10, 12, 34, tzinfo=UTC)
    assert format_period(t, parse_duration('1y'))   == '2026'
    assert format_period(t, parse_duration('1mo'))  == '2026-05'
    assert format_period(t, parse_duration('1d'))   == '2026-05-10'
    assert format_period(t, parse_duration('1h'))   == '2026-05-10T12'
    assert format_period(t, parse_duration('1min')) == '2026-05-10T12-34'

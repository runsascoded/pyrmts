"""Time-axis arithmetic. UTC throughout; calendar-correct for `mo`/`y` units
(variable-width) and millisecond-multiplied for fixed-width units. Mirrors
`js/packages/pyrmts/src/axis.ts`."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone

from .types import TimeUnit


_MS: dict[str, int] = {
    'min': 60_000,
    'h':   60 * 60_000,
    'd':   24 * 60 * 60_000,
}

_SPAN_RE = re.compile(r'^(\d+)(min|h|d|mo|y)$')


@dataclass(frozen=True)
class ParsedTimeSpan:
    count: int
    unit: TimeUnit


def parse_duration(s: str) -> ParsedTimeSpan:
    m = _SPAN_RE.match(s)
    if not m:
        raise ValueError(f"Not a valid Duration: {s!r}")
    return ParsedTimeSpan(count=int(m.group(1)), unit=m.group(2))  # type: ignore[arg-type]


def _ensure_utc(t: datetime) -> datetime:
    if t.tzinfo is None:
        return t.replace(tzinfo=timezone.utc)
    return t.astimezone(timezone.utc)


def add_span(t: datetime, span: ParsedTimeSpan) -> datetime:
    t = _ensure_utc(t)
    count, unit = span.count, span.unit
    if unit == 'mo':
        m = t.month - 1 + count
        y = t.year + m // 12
        return t.replace(year=y, month=m % 12 + 1)
    if unit == 'y':
        return t.replace(year=t.year + count)
    delta_ms = count * _MS[unit]
    return datetime.fromtimestamp((t.timestamp() * 1000 + delta_ms) / 1000, tz=timezone.utc)


def floor_to_span(t: datetime, span: ParsedTimeSpan) -> datetime:
    t = _ensure_utc(t)
    count, unit = span.count, span.unit
    if count != 1:
        if unit in ('mo', 'y'):
            raise ValueError(f"Multi-unit calendar bins not supported: {count}{unit}")
        bin_ms = count * _MS[unit]
        ms = int(t.timestamp() * 1000)
        floored = (ms // bin_ms) * bin_ms
        return datetime.fromtimestamp(floored / 1000, tz=timezone.utc)
    if unit == 'min':
        return t.replace(second=0, microsecond=0)
    if unit == 'h':
        return t.replace(minute=0, second=0, microsecond=0)
    if unit == 'd':
        return t.replace(hour=0, minute=0, second=0, microsecond=0)
    if unit == 'mo':
        return t.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if unit == 'y':
        return t.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    raise AssertionError(f"unreachable: {unit}")


def bins_in_range(from_: datetime, to: datetime, bin: str) -> int:
    if to <= from_:
        return 0
    span = parse_duration(bin)
    cursor = floor_to_span(from_, span)
    n = 0
    while cursor < to:
        n += 1
        cursor = add_span(cursor, span)
    return n


@dataclass(frozen=True)
class ShardPeriod:
    start: datetime
    end: datetime
    label: str


def shard_periods_covering(from_: datetime, to: datetime, shard: str) -> list[ShardPeriod]:
    """Enumerate shard periods covering `[from, to)`."""
    if shard == '1run':
        raise NotImplementedError("'1run' shards are step-axis only; not yet supported")
    span = parse_duration(shard)
    out: list[ShardPeriod] = []
    cursor = floor_to_span(from_, span)
    while cursor < to:
        nxt = add_span(cursor, span)
        out.append(ShardPeriod(start=cursor, end=nxt, label=format_period(cursor, span)))
        cursor = nxt
    return out


def format_period(t: datetime, span: ParsedTimeSpan) -> str:
    """Format a UTC instant as a `{period}` substitution string."""
    t = _ensure_utc(t)
    yyyy = f"{t.year:04d}"
    mm = f"{t.month:02d}"
    dd = f"{t.day:02d}"
    hh = f"{t.hour:02d}"
    mi = f"{t.minute:02d}"
    unit = span.unit
    if unit == 'y':   return yyyy
    if unit == 'mo':  return f"{yyyy}-{mm}"
    if unit == 'd':   return f"{yyyy}-{mm}-{dd}"
    if unit == 'h':   return f"{yyyy}-{mm}-{dd}T{hh}"
    if unit == 'min': return f"{yyyy}-{mm}-{dd}T{hh}-{mi}"
    raise AssertionError(f"unreachable: {unit}")

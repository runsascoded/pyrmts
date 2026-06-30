"""Gap discovery: enumerate the minimal cover a pyramid's ladders need
over a time range. Pure axis arithmetic over the YAML; no storage or
index access. Mirrors `js/packages/pyrmts/src/gap-discovery.ts`
`listExpectedShards`.

The cover for `[from, to)` per tier with shards `[s_0 < ... < s_max]`:
- Max-shard tiles fill `[floor(from, s_max), floor(to, s_max))` (the
  closed-history region). The first tile may start before `from` — the
  shard's notional period contains pre-genesis time the materializer
  just leaves empty.
- The trailing partial-max window `[floor(to, s_max), to)` is tiled
  greedily by the largest non-max rung that fits at each step. The
  ladder is divisibility-chained, so `cur` is always rung-aligned. If
  `to - cur` falls below the smallest non-max rung, the residual is
  left for the next-finer tier (whose bin can represent that resolution).

The two-region split mirrors the api-worker planner walk
(largest-rung-first, falling back to smaller rungs). Historical periods
need only the max-shard; redundant smaller-rung copies aren't part of
the cover.

`list_missing_shards` is JS-only for now (no Python `ShardIndex` port —
see `specs/done/python-unified-ladder.md`)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from .axis import add_span, floor_to_span, format_period, parse_duration
from .keys import substitute_key
from .types import Pyramid, Tier


@dataclass(frozen=True)
class ExpectedShard:
    tier: str
    shard_dur: str
    period_start: datetime
    period_end: datetime  # exclusive
    key: str  # pre-substituted keyTemplate path


def list_expected_shards(
    pyramid: Pyramid,
    time_range: tuple[datetime, datetime],
    filter: dict[str, str | int] | None = None,
) -> list[ExpectedShard]:
    """Per-tier minimal cover of `time_range`. See module docstring."""
    from_, to = time_range
    filter = filter or {}
    out: list[ExpectedShard] = []
    for tier in pyramid.tiers:
        out.extend(_cover_for_tier(pyramid, tier, from_, to, filter))
    return out


def _cover_for_tier(
    pyramid: Pyramid,
    tier: Tier,
    from_: datetime,
    to: datetime,
    filter: dict,
) -> list[ExpectedShard]:
    shards = list(tier.shards)
    max_shard = shards[-1]
    max_span = parse_duration(max_shard)
    last_max = floor_to_span(to, max_span)
    first_max = floor_to_span(from_, max_span)
    out: list[ExpectedShard] = []

    # Closed-history region: max-shard tiles.
    cur = first_max
    while cur < last_max:
        nxt = add_span(cur, max_span)
        out.append(_make_expected(pyramid, tier, max_shard, cur, nxt, filter))
        cur = nxt

    # Trailing partial-max window: greedy largest-fitting-rung descent.
    cur = last_max
    non_max = shards[:-1]
    while cur < to:
        chosen = _largest_fitting_rung(non_max, cur, to)
        if chosen is None:
            break
        rung, rung_end = chosen
        out.append(_make_expected(pyramid, tier, rung, cur, rung_end, filter))
        cur = rung_end

    return out


def _largest_fitting_rung(
    rungs: list[str],
    cur: datetime,
    upper: datetime,
) -> tuple[str, datetime] | None:
    """Largest rung r with `cur` r-aligned and `cur + r ≤ upper`."""
    for r in reversed(rungs):
        span = parse_duration(r)
        if floor_to_span(cur, span) != cur:
            continue
        r_end = add_span(cur, span)
        if r_end <= upper:
            return (r, r_end)
    return None


def _make_expected(
    pyramid: Pyramid,
    tier: Tier,
    shard_dur: str,
    start: datetime,
    end: datetime,
    filter: dict,
) -> ExpectedShard:
    span = parse_duration(shard_dur)
    label = format_period(start, span)
    key = substitute_key(
        pyramid.keyTemplate,
        {**filter, 'tier': tier.name, 'shard': shard_dur, 'period': label},
    )
    return ExpectedShard(
        tier=tier.name,
        shard_dur=shard_dur,
        period_start=start,
        period_end=end,
        key=key,
    )

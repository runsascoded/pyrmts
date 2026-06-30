"""Gap discovery: enumerate every shard a pyramid's ladders declare for
a range. Pure axis arithmetic over the YAML; no storage or index access.
Mirrors `js/packages/pyrmts/src/gap-discovery.ts` `listExpectedShards`.

`list_missing_shards` is JS-only for now (no Python `ShardIndex` port —
see `specs/done/python-unified-ladder.md`)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from .axis import shard_periods_covering
from .keys import substitute_key
from .types import Pyramid


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
    """List every shard the pyramid's ladders declare for `time_range`.

    Args:
        pyramid: the Pyramid; tiers ordered finest-first.
        time_range: half-open `(from, to)` UTC interval.
        filter: extra `{...}` values to substitute into `pyramid.keyTemplate`
            (e.g. `{device_id: 17617}` for awair). `{tier}`, `{shard}`, and
            `{period}` are filled internally.

    Returns:
        Flat list of `ExpectedShard`s in (tier-iteration order × ladder
        order × period order). Consumers wanting a different order
        (dependency, calendar, etc.) sort the result themselves.
    """
    from_, to = time_range
    filter = filter or {}
    out: list[ExpectedShard] = []
    for tier in pyramid.tiers:
        for shard_dur in tier.shards:
            for p in shard_periods_covering(from_, to, shard_dur):
                key = substitute_key(
                    pyramid.keyTemplate,
                    {**filter, 'tier': tier.name, 'shard': shard_dur, 'period': p.label},
                )
                out.append(ExpectedShard(
                    tier=tier.name,
                    shard_dur=shard_dur,
                    period_start=p.start,
                    period_end=p.end,
                    key=key,
                ))
    return out

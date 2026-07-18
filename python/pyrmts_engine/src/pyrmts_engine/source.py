"""Source protocol: where the base tier's long-form rows come from.

`read_window(start, end)` returns a long frame (see `longform.long_schema`)
whose bins are already floored to the base tier's bin and lie in
`[start, end)`. App-specific ingesters (e.g. ctbk's GBFS minute parquets,
rides' monthly normalized parquet) implement this directly; pyramids whose
base rung is already materialized as wide shards can use `WideShardSource`.
"""
from __future__ import annotations

import io
from datetime import datetime
from typing import Protocol

import polars as pl
import pyarrow.parquet as pq

from pyrmts import Pyramid, shard_periods_covering, substitute_key
from .longform import empty_long, long_schema, wide_to_long


class Source(Protocol):
    def read_window(self, start: datetime, end: datetime) -> pl.DataFrame: ...


class WideShardSource:
    """Reads a materialized (tier, shard_dur) rung's wide shards from
    `pyramid.storage` and converts to long form. Missing shards are treated
    as empty (pre-genesis / outage windows); rows outside `[start, end)`
    are filtered (shard periods straddle window edges).

    `provides` names the rung so the engine can skip re-writing it."""

    def __init__(
        self,
        pyramid: Pyramid,
        tier_name: str | None = None,
        shard_dur: str | None = None,
        filter: dict[str, str | int] | None = None,
    ) -> None:
        self.pyramid = pyramid
        tier = pyramid.tier(tier_name) if tier_name else pyramid.tiers[0]
        self.tier = tier
        self.shard_dur = shard_dur or tier.shards[0]
        self.filter = filter or {}

    @property
    def provides(self) -> tuple[str, str]:
        return (self.tier.name, self.shard_dur)

    def read_window(self, start: datetime, end: datetime) -> pl.DataFrame:
        pyramid = self.pyramid
        bin_col = pyramid.binCol
        start_ms = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)
        frames: list[pl.DataFrame] = []
        for period in shard_periods_covering(start, end, self.shard_dur):
            key = substitute_key(
                pyramid.keyTemplate,
                {**self.filter, 'tier': self.tier.name, 'shard': self.shard_dur, 'period': period.label},
            )
            blob = pyramid.storage.get(key)
            if blob is None:
                continue
            wide = pl.from_arrow(pq.read_table(io.BytesIO(blob)))
            frames.append(wide_to_long(wide, pyramid))
        if not frames:
            return empty_long(pyramid)
        out = pl.concat(frames).filter(
            (pl.col(bin_col) >= start_ms) & (pl.col(bin_col) < end_ms)
        )
        return out.cast(long_schema(pyramid))

"""Source protocol: where the base tier's long-form rows come from.

`read_window(start, end)` returns a long frame (see `longform.long_schema`)
whose bins are already floored to the base tier's bin and lie in
`[start, end)`. App-specific ingesters (e.g. ctbk's GBFS minute parquets,
rides' monthly normalized parquet) implement this directly; pyramids whose
base rung is already materialized as wide shards can use `WideShardSource`.
"""
from __future__ import annotations

import io
import threading
from datetime import datetime
from typing import Protocol

import polars as pl
import pyarrow.parquet as pq

from pyrmts import Pyramid, shard_periods_covering, substitute_key
from .longform import empty_long, long_schema, wide_to_long


class Source(Protocol):
    def read_window(self, start: datetime, end: datetime) -> pl.DataFrame: ...


class _CacheEntry:
    """A period's parsed frame, or a marker that another thread is loading
    it (waiters block on `event`)."""
    __slots__ = ('end_ms', 'event', 'frame', 'exc')

    def __init__(self, end_ms: int) -> None:
        self.end_ms = end_ms
        self.event = threading.Event()
        self.frame: pl.DataFrame | None = None
        self.exc: BaseException | None = None


class WideShardSource:
    """Reads a materialized (tier, shard_dur) rung's wide shards from
    `pyramid.storage` and converts to long form. Missing shards are treated
    as empty (pre-genesis / outage windows); rows outside `[start, end)`
    are filtered (shard periods straddle window edges).

    Parsed shards are cached across calls, so a window smaller than
    `shard_dur` doesn't re-fetch/re-parse the same blob ⌈shard/window⌉
    times. Concurrent readers of the same period block on one load
    instead of duplicating it (the parallel executor runs many window
    tasks at once). Eviction is engine-driven: the executor calls
    `evict_before(watermark_start_ms)` as its completion watermark
    advances — a period can't self-evict on "this window's start" because
    windows complete out of order. Standalone callers can call it
    themselves, or let the cache grow (bounded by the range).

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
        self._cache: dict[str, _CacheEntry] = {}
        self._n_periods = 0
        self._missing: list[str] = []
        self._lock = threading.Lock()

    @property
    def provides(self) -> tuple[str, str]:
        return (self.tier.name, self.shard_dur)

    def _load(self, label: str, end_ms: int) -> pl.DataFrame:
        key = substitute_key(
            self.pyramid.keyTemplate,
            {**self.filter, 'tier': self.tier.name, 'shard': self.shard_dur, 'period': label},
        )
        blob = self.pyramid.storage.get(key)
        if blob is None:
            with self._lock:
                self._missing.append(key)
            return empty_long(self.pyramid)
        # use_threads=False: worker-level parallelism already saturates
        # cores, and Arrow's internal pool is the suspected racer in the
        # cold-start SIGSEGV (ctbk finding 11) — keep decode on the
        # calling thread.
        wide = pl.from_arrow(pq.read_table(io.BytesIO(blob), use_threads=False))
        return wide_to_long(wide, self.pyramid)

    def coverage(self) -> tuple[int, list[str]]:
        """(periods read, keys that were absent). The engine's
        `max_missing_source` policy consumes this: reads are clamped to
        the build range, so every requested period is post-genesis —
        an absent key is a real hole (a GC'd rung, filter typo, wrong
        metric prefix, …), not a pre-genesis miss. Affirmatively-EMPTY
        shards (outage windows) are present zero-row objects and do NOT
        count as missing."""
        with self._lock:
            return self._n_periods, sorted(self._missing)

    def _period_frame(self, label: str, end_ms: int) -> pl.DataFrame:
        with self._lock:
            entry = self._cache.get(label)
            mine = entry is None
            if mine:
                entry = self._cache[label] = _CacheEntry(end_ms)
                self._n_periods += 1
        if mine:
            try:
                entry.frame = self._load(label, end_ms)
            except BaseException as e:
                entry.exc = e
                with self._lock:
                    self._cache.pop(label, None)
                entry.event.set()
                raise
            entry.event.set()
        else:
            entry.event.wait()
            if entry.exc is not None:
                raise entry.exc
        assert entry.frame is not None
        return entry.frame

    def cache_bytes(self) -> int:
        """Estimated bytes of resident parsed shard frames — feeds the
        engine's byte-aware admission."""
        with self._lock:
            frames = [e.frame for e in self._cache.values() if e.frame is not None]
        return sum(f.estimated_size() for f in frames)

    def evict_before(self, start_ms: int) -> None:
        """Drop cached periods ending at/before `start_ms` (the earliest
        possibly-active window start — i.e. the executor's watermark)."""
        with self._lock:
            dead = [
                label for label, e in self._cache.items()
                if e.end_ms <= start_ms and e.event.is_set()
            ]
            for label in dead:
                del self._cache[label]

    def read_window(self, start: datetime, end: datetime) -> pl.DataFrame:
        pyramid = self.pyramid
        bin_col = pyramid.binCol
        start_ms = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)
        frames = [
            self._period_frame(period.label, int(period.end.timestamp() * 1000))
            for period in shard_periods_covering(start, end, self.shard_dur)
        ]
        if not frames:
            return empty_long(pyramid)
        out = pl.concat(frames).filter(
            (pl.col(bin_col) >= start_ms) & (pl.col(bin_col) < end_ms)
        )
        return out.cast(long_schema(pyramid))

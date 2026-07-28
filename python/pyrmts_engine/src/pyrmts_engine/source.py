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
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
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
    """Reads a materialized source tier's wide shards from
    `pyramid.storage` and converts to long form. Missing shards are treated
    as empty (pre-genesis / outage windows); rows outside `[start, end)`
    are filtered (shard periods straddle window edges).

    **Tile selection** (`specs/engine-min-cover-source.md`): by default
    (`shard_dur=None`) the source reads the tier *as it's actually
    stored* — one LIST of the tier prefix (lazy, once), then at each
    instant the largest rung whose grid-aligned tile is present wins, so
    a maintained min-cover mix (large consolidated history tiles +
    progressively finer tiles toward the live tip) is read exactly as
    laid out, and redundant not-yet-GC'd finer tiles under a present
    larger tile are deterministically ignored. Selection is a pure
    function of the LIST snapshot — build byte-determinism depends on
    that. Instants covered by *no* rung's tile fall back to the finest
    rung, whose read records the miss (so coverage accounting — and the
    engine's `max_missing_source` guard — behave exactly as in pinned
    mode). Passing `shard_dur` pins a single rung as before (the
    seeded-scratch / back-compat case; no LIST happens).

    Parsed tiles are cached across calls, so a window smaller than a
    tile doesn't re-fetch/re-parse the same blob once per window.
    Concurrent readers of the same tile block on one load instead of
    duplicating it (the parallel executor runs many window tasks at
    once). Eviction is engine-driven: the executor calls
    `evict_before(watermark_start_ms)` as its completion watermark
    advances — a tile can't self-evict on "this window's start" because
    windows complete out of order. Standalone callers can call it
    themselves, or let the cache grow (bounded by the range).

    `provides` names the rung so the engine can skip re-writing it; in
    min-cover mode the dur is None, meaning the *whole tier* is
    externally owned (the engine skips every rung of it — same-tier
    consolidation is a separate spec)."""

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
        self.shard_dur = shard_dur  # None → min-cover selection across the tier's rungs
        self.filter = filter or {}
        self._cache: dict[tuple[str, str], _CacheEntry] = {}
        self._n_periods = 0
        self._missing: list[str] = []
        self._listing: set[str] | None = None
        self._lock = threading.Lock()
        self._ra_pool: ThreadPoolExecutor | None = None

    @property
    def provides(self) -> tuple[str, str | None]:
        return (self.tier.name, self.shard_dur)

    def _key(self, dur: str, label: str) -> str:
        return substitute_key(
            self.pyramid.keyTemplate,
            {**self.filter, 'tier': self.tier.name, 'shard': dur, 'period': label},
        )

    def _listed(self) -> set[str]:
        """The tier prefix's LIST result (lazy, once, single-flight)."""
        with self._lock:
            if self._listing is None:
                partial = self.pyramid.keyTemplate
                for k, v in {**self.filter, 'tier': self.tier.name}.items():
                    partial = partial.replace('{' + k + '}', str(v))
                self._listing = set(self.pyramid.storage.list(partial.split('{', 1)[0]))
            return self._listing

    def _tile_at(self, at: datetime):
        """(dur, period) of the tile to read for instant `at`: the pinned
        rung's grid tile, or — min-cover mode — the largest rung whose
        tile is present, falling back to the finest rung (whose read
        records the coverage miss) when nothing covers `at`."""
        def covering(dur: str):
            return shard_periods_covering(at, at + timedelta(milliseconds=1), dur)[0]

        if self.shard_dur is not None:
            return self.shard_dur, covering(self.shard_dur)
        listed = self._listed()
        for dur in reversed(self.tier.shards):
            p = covering(dur)
            if self._key(dur, p.label) in listed:
                return dur, p
        return self.tier.shards[0], covering(self.tier.shards[0])

    def _load(self, dur: str, label: str) -> pl.DataFrame:
        key = self._key(dur, label)
        blob = self.pyramid.storage.get(key)
        if blob is None:
            with self._lock:
                self._missing.append(key)
            return empty_long(self.pyramid)
        # Stream the parse per record batch: whole-shard wide→long
        # transients peaked ~8-10 GB on ctbk avail and alone bust a
        # Lambda-sized (10 GB) container; per-batch conversion bounds the
        # transient at one batch's explode intermediates. `wide_to_long`
        # is row-local, and downstream paths all pass through unordered
        # group_bys + the writer's total sort, so batch boundaries and
        # row order can't reach output bytes. use_threads=False:
        # worker-level parallelism already saturates cores, and Arrow's
        # internal pool is the suspected racer in the cold-start SIGSEGV
        # (ctbk finding 11) — keep decode on the calling thread.
        import pyarrow as pa
        pf = pq.ParquetFile(io.BytesIO(blob))
        parts = [
            wide_to_long(pl.from_arrow(pa.Table.from_batches([batch])), self.pyramid)
            for batch in pf.iter_batches(batch_size=131_072, use_threads=False)
        ]
        parts = [p for p in parts if p.height]
        if not parts:
            return empty_long(self.pyramid)
        return pl.concat(parts, rechunk=False)

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

    def _period_frame(self, dur: str, label: str, end_ms: int) -> pl.DataFrame:
        tile = (dur, label)
        with self._lock:
            entry = self._cache.get(tile)
            mine = entry is None
            if mine:
                entry = self._cache[tile] = _CacheEntry(end_ms)
                self._n_periods += 1
        if mine:
            try:
                entry.frame = self._load(dur, label)
            except BaseException as e:
                entry.exc = e
                with self._lock:
                    self._cache.pop(tile, None)
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

    def close(self) -> None:
        """Stop the readahead pool (a parse in progress is abandoned to
        finish on its own thread; nothing waits on it)."""
        with self._lock:
            pool, self._ra_pool = self._ra_pool, None
        if pool is not None:
            pool.shutdown(wait=False, cancel_futures=True)

    def evict_before(self, start_ms: int) -> None:
        """Drop cached tiles ending at/before `start_ms` (the earliest
        possibly-active window start — i.e. the executor's watermark)."""
        with self._lock:
            dead = [
                tile for tile, e in self._cache.items()
                if e.end_ms <= start_ms and e.event.is_set()
            ]
            for tile in dead:
                del self._cache[tile]

    def prefetch(self, at: datetime) -> None:
        """Background-load the selected tile covering instant `at` (one
        readahead slot; single-flight with foreground loads via the
        cache). The engine calls this with upcoming *in-range* window
        starts, so a prefetch miss is a legitimate coverage miss exactly
        like a foreground one. Without readahead every tile boundary
        stalls all window workers for ~one parse (~30 s on ctbk avail —
        ~25 min serialized across a full-range walk)."""
        dur, period = self._tile_at(at)
        with self._lock:
            if (dur, period.label) in self._cache:
                return
            if self._ra_pool is None:
                self._ra_pool = ThreadPoolExecutor(
                    max_workers=1, thread_name_prefix='source-readahead',
                )
            pool = self._ra_pool
        pool.submit(self._period_frame, dur, period.label, int(period.end.timestamp() * 1000))

    def read_window(self, start: datetime, end: datetime) -> pl.DataFrame:
        pyramid = self.pyramid
        bin_col = pyramid.binCol
        start_ms = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)
        tiles = []
        t = start
        while t < end:
            dur, period = self._tile_at(t)
            tiles.append((dur, period))
            t = period.end
        frames = [
            self._period_frame(dur, period.label, int(period.end.timestamp() * 1000))
            for dur, period in tiles
        ]
        if not frames:
            return empty_long(pyramid)
        out = pl.concat(frames).filter(
            (pl.col(bin_col) >= start_ms) & (pl.col(bin_col) < end_ms)
        )
        return out.cast(long_schema(pyramid))

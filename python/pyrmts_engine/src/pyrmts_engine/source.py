"""Source protocol: where the base tier's long-form rows come from.

`read_window(start, end)` returns a long frame (see `longform.long_schema`)
whose bins are already floored to the base tier's bin and lie in
`[start, end)`. App-specific ingesters implement this directly, but most
should subclass `TiledSource` (`specs/engine-raw-ingest.md`): sources whose
backing store is a set of period-aligned blobs ("tiles" — daily raw
archives, materialized wide shards, …) get the production chassis — parsed
tile cache, single-flight loads, readahead `prefetch`, watermark
`evict_before`, `cache_bytes` admission input, tile-granular `coverage()` —
for the cost of two hooks: `tile_at` and `parse`. `WideShardSource` (a
pyramid whose base rung is already materialized as wide shards) is itself
a `TiledSource` subclass.
"""
from __future__ import annotations

import io
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

import polars as pl
import pyarrow.parquet as pq

from pyrmts import Pyramid, ShardPeriod, shard_periods_covering, substitute_key
from .longform import empty_long, long_schema, wide_to_long


class Source(Protocol):
    def read_window(self, start: datetime, end: datetime) -> pl.DataFrame: ...


@dataclass(frozen=True)
class Tile:
    """One cacheable unit of a `TiledSource`'s storage layout: the blob at
    `key` covers `period`."""
    key: str
    period: ShardPeriod


class _CacheEntry:
    """A tile's parsed frame, or a marker that another thread is loading
    it (waiters block on `event`)."""
    __slots__ = ('end_ms', 'event', 'frame', 'exc')

    def __init__(self, end_ms: int) -> None:
        self.end_ms = end_ms
        self.event = threading.Event()
        self.frame: pl.DataFrame | None = None
        self.exc: BaseException | None = None


class TiledSource:
    """Chassis for sources reading period-aligned blobs. Subclasses
    implement:

    - `tile_at(at)` — the tile whose period covers instant `at`. The
      default `tiles_for(start, end)` walks it across a window (override
      that instead/too for layouts where per-instant selection isn't the
      natural unit).
    - `parse(blob, tile)` — whole-tile blob → long frame. Parsed frames
      are cached across windows, so a window smaller than a tile doesn't
      re-fetch/re-parse the same blob once per window; `read_window`
      clips the concatenated tile frames to `[start, end)` afterward.
      Rows outside the requested window are therefore fine (and
      expected — tile periods straddle window edges).

    The base provides the production chassis:

    - **Single-flight cache**: concurrent readers of the same tile block
      on one load instead of duplicating it (the parallel executor runs
      many window tasks at once).
    - **Eviction** is engine-driven: the executor calls
      `evict_before(watermark_start_ms)` as its completion watermark
      advances — a tile can't self-evict on "this window's start" because
      windows complete out of order. Standalone callers can call it
      themselves, or let the cache grow (bounded by the range).
    - **`prefetch(at)`** background-loads one tile ahead (single-flight
      with foreground loads via the cache); the engine drives it with
      upcoming in-range window starts.
    - **`coverage()`** is tile-granular: a missing tile (`fetch` → None)
      is recorded and read as empty. Whether absence is an error is the
      *caller's* policy — the engine's `max_missing_source` guard, strict
      by default. Absence *within* a present tile (a window with no rows)
      is never a miss: an empty window is a legitimate empty bin.

    `provides` defaults to None — the source owns no rung, so the engine
    writes every rung including the base tier (the raw-ingest case).
    `WideShardSource` overrides it with the rung it reads."""

    provides: tuple[str, str | None] | None = None

    def __init__(self, pyramid: Pyramid) -> None:
        self.pyramid = pyramid
        self._cache: dict[str, _CacheEntry] = {}
        self._n_tiles = 0
        self._missing: list[Tile] = []
        self._lock = threading.Lock()
        self._ra_pool: ThreadPoolExecutor | None = None

    # ---- subclass hooks ----

    def tile_at(self, at: datetime) -> Tile:
        raise NotImplementedError

    def tiles_for(self, start: datetime, end: datetime) -> list[Tile]:
        """Tiles covering `[start, end)`, in order (default: walk
        `tile_at` from `start`; tile periods must advance)."""
        tiles: list[Tile] = []
        t = start
        while t < end:
            tile = self.tile_at(t)
            tiles.append(tile)
            t = tile.period.end
        return tiles

    def parse(self, blob: bytes, tile: Tile) -> pl.DataFrame:
        raise NotImplementedError

    def fetch(self, key: str) -> bytes | None:
        return self.pyramid.storage.get(key)

    # ---- chassis ----

    def _load(self, tile: Tile) -> pl.DataFrame:
        blob = self.fetch(tile.key)
        if blob is None:
            with self._lock:
                self._missing.append(tile)
            return empty_long(self.pyramid)
        return self.parse(blob, tile)

    def _tile_frame(self, tile: Tile) -> pl.DataFrame:
        with self._lock:
            entry = self._cache.get(tile.key)
            mine = entry is None
            if mine:
                end_ms = int(tile.period.end.timestamp() * 1000)
                entry = self._cache[tile.key] = _CacheEntry(end_ms)
                self._n_tiles += 1
        if mine:
            try:
                entry.frame = self._load(tile)
            except BaseException as e:
                entry.exc = e
                with self._lock:
                    self._cache.pop(tile.key, None)
                entry.event.set()
                raise
            entry.event.set()
        else:
            entry.event.wait()
            if entry.exc is not None:
                raise entry.exc
        assert entry.frame is not None
        return entry.frame

    def coverage(self) -> tuple[int, list[str]]:
        """(tiles read, keys that were absent). The engine's
        `max_missing_source` policy consumes this: reads are clamped to
        the build range, so every requested tile is post-genesis —
        an absent key is a real hole (a GC'd rung, filter typo, wrong
        metric prefix, a compaction gap, …), not a pre-genesis miss.
        Affirmatively-EMPTY tiles (outage windows) are present zero-row
        objects and do NOT count as missing."""
        with self._lock:
            return self._n_tiles, sorted(t.key for t in self._missing)

    def missing_tiles(self) -> list[Tile]:
        """Absent tiles with their periods, key-sorted. The engine's
        open-period classification consumes this — an absent tile whose
        period extends past the build range's end hasn't finished
        happening, so its object may legitimately not exist yet
        (`coverage()` keeps the keys-only shape)."""
        with self._lock:
            return sorted(self._missing, key=lambda t: t.key)

    def cache_bytes(self) -> int:
        """Estimated bytes of resident parsed tile frames — feeds the
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
                key for key, e in self._cache.items()
                if e.end_ms <= start_ms and e.event.is_set()
            ]
            for key in dead:
                del self._cache[key]

    def prefetch(self, at: datetime) -> None:
        """Background-load the tile covering instant `at` (one readahead
        slot; single-flight with foreground loads via the cache). The
        engine calls this with upcoming *in-range* window starts, so a
        prefetch miss is a legitimate coverage miss exactly like a
        foreground one. Without readahead every tile boundary stalls all
        window workers for ~one parse (~30 s on ctbk avail — ~25 min
        serialized across a full-range walk)."""
        tile = self.tile_at(at)
        with self._lock:
            if tile.key in self._cache:
                return
            if self._ra_pool is None:
                self._ra_pool = ThreadPoolExecutor(
                    max_workers=1, thread_name_prefix='source-readahead',
                )
            pool = self._ra_pool
        pool.submit(self._tile_frame, tile)

    def read_window(self, start: datetime, end: datetime) -> pl.DataFrame:
        pyramid = self.pyramid
        bin_col = pyramid.binCol
        start_ms = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)
        frames = [self._tile_frame(tile) for tile in self.tiles_for(start, end)]
        if not frames:
            return empty_long(pyramid)
        out = pl.concat(frames).filter(
            (pl.col(bin_col) >= start_ms) & (pl.col(bin_col) < end_ms)
        )
        return out.cast(long_schema(pyramid))


class WideShardSource(TiledSource):
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
        super().__init__(pyramid)
        tier = pyramid.tier(tier_name) if tier_name else pyramid.tiers[0]
        self.tier = tier
        self.shard_dur = shard_dur  # None → min-cover selection across the tier's rungs
        self.filter = filter or {}
        self._listing: set[str] | None = None

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

    def tile_at(self, at: datetime) -> Tile:
        """The tile to read for instant `at`: the pinned rung's grid
        tile, or — min-cover mode — the largest rung whose tile is
        present, falling back to the finest rung (whose read records the
        coverage miss) when nothing covers `at`."""
        def covering(dur: str) -> ShardPeriod:
            return shard_periods_covering(at, at + timedelta(milliseconds=1), dur)[0]

        if self.shard_dur is not None:
            p = covering(self.shard_dur)
            return Tile(key=self._key(self.shard_dur, p.label), period=p)
        listed = self._listed()
        for dur in reversed(self.tier.shards):
            p = covering(dur)
            key = self._key(dur, p.label)
            if key in listed:
                return Tile(key=key, period=p)
        p = covering(self.tier.shards[0])
        return Tile(key=self._key(self.tier.shards[0], p.label), period=p)

    def parse(self, blob: bytes, tile: Tile) -> pl.DataFrame:
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

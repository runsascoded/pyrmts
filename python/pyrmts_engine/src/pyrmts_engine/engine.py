"""`local` executor: windowed streaming outer loop.

One pass over the source, window by window. Per window:

1. Read the window's base long-form frame (`source.read_window`; prefetched
   on a small thread pool so reads overlap compute).
2. Walk tiers finest→coarsest; each tier re-bins its divisibility
   predecessor's window frame (`group_by.sum` — one cheap aggregation per
   tier per window; sibling tiers share the predecessor frame, so fused
   fan-outs like `/2m,/3m,/5m ← /1m` scan their input once).
3. Append each tier's window frame to its WIP buffer.
4. Flush every expected shard whose period has closed (cursor ≥
   `effective_end`): combine the buffer once, slice per shard, materialize
   wide (hist-JSON built exactly once here), write via `write_tier_parquet`,
   single `storage.put`, then `shard_index.record_shard` — registration
   immediately after each PUT.

Bins wider than the window (e.g. `1mo` tiers) accumulate partial-bin rows
across windows; the combine at flush merges them — exact, since long-form
group_by-sum IS the monoid combine. Max-rung WIP buffers accumulate across
the whole run: no scaffold shards, ever.

Zero-row expected shards are written (and registered) as EMPTY shards —
cover-complete + zero rows is a legitimate state (outage windows), not a
gap for fsck to re-chase."""
from __future__ import annotations

import io
import sys
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from typing import Sequence

import polars as pl

from pyrmts import (
    ExpectedShard,
    Pyramid,
    parse_duration,
    shard_periods_covering,
    write_tier_parquet,
)
from .longform import combine_long, long_to_wide, rebin_long
from .plan import UNIT_MS, bin_floor_expr, compile_plan
from .shard_index import NoopShardIndex, ShardIndex, ShardRecord, now_ms
from .source import Source


@dataclass(frozen=True)
class WrittenShard:
    key: str
    tier: str
    shard_dur: str
    rows: int
    bytes: int


@dataclass
class BuildResult:
    written: list[WrittenShard] = field(default_factory=list)
    skipped_rungs: int = 0
    source_rows: int = 0
    windows: int = 0
    wall_seconds: float = 0.0

    def summary(self) -> str:
        total_bytes = sum(w.bytes for w in self.written)
        return (
            f"build_local: {self.windows} windows, {self.source_rows:,} source rows → "
            f"{len(self.written)} shards ({total_bytes:,} bytes), "
            f"{self.skipped_rungs} source-provided rungs skipped, "
            f"wall {self.wall_seconds:.1f}s"
        )


def _ms(t: datetime) -> int:
    return int(t.timestamp() * 1000)


def build_local(
    pyramid: Pyramid,
    time_range: tuple[datetime, datetime],
    source: Source,
    *,
    pyramid_name: str,
    shard_index: ShardIndex | None = None,
    window: str = '1d',
    filter: dict[str, str | int] | None = None,
    skip_rungs: set[tuple[str, str]] | None = None,
    sort: Sequence[str] | None = None,
    prefetch: int = 2,
    verbose: bool = False,
) -> BuildResult:
    """Build every expected shard of `pyramid` over `time_range` from
    `source`, streaming `window`-sized chunks.

    Args:
        pyramid: tiers ordered finest-first.
        time_range: half-open `(from, to)` UTC. `from` is the genesis
            boundary — pre-`from` source data is never read.
        source: yields base-tier long-form frames per window.
        pyramid_name: name shards are registered under in the ShardIndex.
        shard_index: registration sink (default no-op).
        window: outer-loop chunk Duration; fixed-width, a multiple of the
            base tier's bin. Purely a memory/throughput dial.
        filter: extra keyTemplate substitutions (e.g. awair `device_id`).
        skip_rungs: `(tier, shard_dur)` rungs not to write. Defaults to
            `source.provides` when the source declares one (don't re-write
            the rung being read).
        sort: override `write_tier_parquet` sort cols (e.g. cell-first for
            ctbk avail).
        prefetch: source windows to read ahead (thread pool).
        verbose: per-flush progress lines on stderr.
    """
    t0 = time.time()
    from_, to = time_range
    shard_index = shard_index or NoopShardIndex()

    if skip_rungs is None:
        provides = getattr(source, 'provides', None)
        skip_rungs = {provides} if provides is not None else set()

    _validate_window(pyramid, window)
    plan = compile_plan(pyramid, time_range, filter=filter, skip_rungs=skip_rungs)

    result = BuildResult(skipped_rungs=len(plan.skipped_rungs))
    if to <= from_:
        result.wall_seconds = time.time() - t0
        return result

    tiers = pyramid.tiers
    bin_col = pyramid.binCol
    floor_exprs = {t.name: bin_floor_expr(bin_col, t.bin) for t in tiers}
    buffers: dict[str, list[pl.DataFrame]] = {t.name: [] for t in tiers}
    pending: dict[str, deque[ExpectedShard]] = {
        t.name: deque(sorted(plan.outputs_for_tier(t.name), key=lambda e: e.period_start))
        for t in tiers
    }

    def log(msg: str) -> None:
        if verbose:
            print(msg, file=sys.stderr)

    def flush_ready(tier_name: str, cursor: datetime) -> None:
        q = pending[tier_name]
        ready: list[ExpectedShard] = []
        while q and q[0].effective_end <= cursor:
            ready.append(q.popleft())
        if not ready:
            return
        combined = combine_long(buffers[tier_name], pyramid)
        for shard in ready:
            s_ms, e_ms = _ms(shard.period_start), _ms(shard.period_end)
            rows = combined.filter((pl.col(bin_col) >= s_ms) & (pl.col(bin_col) < e_ms))
            _write_shard(shard, rows)
        buffers[tier_name] = [combined.filter(pl.col(bin_col) >= _ms(ready[-1].period_end))]

    def _write_shard(shard: ExpectedShard, rows: pl.DataFrame) -> None:
        t_flush = time.time()
        wide = long_to_wide(rows, pyramid)
        buf = io.BytesIO()
        kwargs = {'sort': list(sort)} if sort is not None else {}
        n_bytes = write_tier_parquet(wide.to_arrow(), pyramid, out=buf, **kwargs)
        pyramid.storage.put(shard.key, buf.getvalue())
        shard_index.record_shard(ShardRecord(
            pyramid=pyramid_name,
            tier=shard.tier,
            shard_dur=shard.shard_dur,
            period_start_ms=_ms(shard.period_start),
            period_end_ms=_ms(shard.period_end),
            key=shard.key,
            written_at_ms=now_ms(),
        ))
        result.written.append(WrittenShard(
            key=shard.key, tier=shard.tier, shard_dur=shard.shard_dur,
            rows=wide.height, bytes=n_bytes,
        ))
        log(f"  flush {shard.tier:6s} {shard.key}: {wide.height:,} rows, "
            f"{n_bytes/1024:.0f} KiB, {time.time() - t_flush:.1f}s")

    def process_window(frame: pl.DataFrame, w_end: datetime) -> None:
        result.source_rows += frame.height
        window_frames: dict[str, pl.DataFrame] = {}
        for tier in tiers:
            pred = plan.preds[tier.name]
            src = frame if pred is None else window_frames[pred]
            tf = rebin_long(src, pyramid, floor_exprs[tier.name])
            window_frames[tier.name] = tf
            # Only buffer for tiers that still have shards to flush —
            # rungs the source provides (or an exhausted pending queue)
            # would otherwise accumulate frames unboundedly.
            if pending[tier.name]:
                buffers[tier.name].append(tf)
        cursor = min(w_end, to)
        for tier in tiers:
            flush_ready(tier.name, cursor)
        result.windows += 1

    periods = shard_periods_covering(from_, to, window)

    def read(p) -> pl.DataFrame:
        return source.read_window(max(p.start, from_), min(p.end, to))

    if prefetch > 1:
        with ThreadPoolExecutor(max_workers=prefetch) as pool:
            futs = deque()
            it = iter(periods)
            for p in it:
                futs.append((pool.submit(read, p), p))
                if len(futs) >= prefetch:
                    break
            while futs:
                fut, p = futs.popleft()
                process_window(fut.result(), p.end)
                nxt = next(it, None)
                if nxt is not None:
                    futs.append((pool.submit(read, nxt), nxt))
    else:
        for p in periods:
            process_window(read(p), p.end)

    leftover = [(t, len(q)) for t, q in pending.items() if q]
    if leftover:
        raise AssertionError(f"build_local: unflushed shards after final window: {leftover}")

    result.wall_seconds = time.time() - t0
    return result


def _validate_window(pyramid: Pyramid, window: str) -> None:
    span = parse_duration(window)
    if span.unit not in UNIT_MS:
        raise ValueError(f"build_local: window must be fixed-width (min/h/d); got {window!r}")
    window_ms = span.count * UNIT_MS[span.unit]
    base = pyramid.tiers[0]
    base_span = parse_duration(base.bin)
    if base_span.unit in UNIT_MS:
        base_ms = base_span.count * UNIT_MS[base_span.unit]
        if window_ms % base_ms != 0:
            raise ValueError(
                f"build_local: window {window!r} is not a multiple of base tier "
                f"bin {base.bin!r}"
            )

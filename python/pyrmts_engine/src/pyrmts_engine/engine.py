"""`local` executor: windowed streaming outer loop.

One pass over the source, window by window. Per window:

1. Read the window's base long-form frame (`source.read_window`; prefetched
   on a small thread pool so reads overlap compute).
2. Walk tiers finest→coarsest; each tier re-bins its divisibility
   predecessor's window frame (`group_by.sum` — one cheap aggregation per
   tier per window; sibling tiers share the predecessor frame, so fused
   fan-outs like `/2m,/3m,/5m ← /1m` scan their input once).
3. Route each tier's window rows to the open expected shards they fall in,
   appending row-groups to per-shard spill files (`SpillBuffer`). Rows
   belonging to no expected shard (residual tails) are dropped immediately.
4. Flush every expected shard whose period has closed (cursor ≥
   `effective_end`): streaming-combine its spill file (partial bins from
   different windows merge here — exact, since long-form group_by-sum IS
   the monoid combine), materialize wide (hist-JSON built exactly once),
   write via `write_tier_parquet`, single `storage.put`, then
   `shard_index.record_shard` — registration immediately after each PUT.

Peak memory ≈ one window's frames + one closing shard's combined long
form — NOT the sum of open max-rung WIP buffers (ctbk measured ~40 GB
that way on a 4-day smoke; see the spill module docstring). Max-rung
shards accumulate on scratch disk across the whole run: no scaffold
shards, ever.

Zero-row expected shards are written (and registered) as EMPTY shards —
cover-complete + zero rows is a legitimate state (outage windows), not a
gap for fsck to re-chase."""
from __future__ import annotations

import io
import sys
import tempfile
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Mapping, Sequence

import polars as pl

from pyrmts import (
    ExpectedShard,
    Pyramid,
    parse_duration,
    shard_periods_covering,
    write_tier_parquet,
)

from .longform import long_to_wide, rebin_long
from .plan import UNIT_MS, bin_floor_expr, compile_plan
from .shard_index import NoopShardIndex, ShardIndex, ShardRecord, now_ms
from .source import Source
from .spill import SpillBuffer


class EmptySourceError(RuntimeError):
    """The source produced zero rows across the entire range — almost
    always a mis-specified source rung, not real data. (The zero-row
    outputs were still written/registered before this raised.)"""


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
    resumed_shards: int = 0
    source_rows: int = 0
    windows: int = 0
    wall_seconds: float = 0.0

    def summary(self) -> str:
        total_bytes = sum(w.bytes for w in self.written)
        resumed = f"{self.resumed_shards} manifested shards skipped, " if self.resumed_shards else ""
        return (
            f"build_local: {self.windows} windows, {self.source_rows:,} source rows → "
            f"{len(self.written)} shards ({total_bytes:,} bytes), "
            f"{self.skipped_rungs} source-provided rungs skipped, {resumed}"
            f"wall {self.wall_seconds:.1f}s"
        )


_libc: object = None


def _trim_allocator() -> None:
    """Return freed heap pages to the OS after shard closes (glibc only —
    a no-op elsewhere). Without this, allocator-retained pages grow the
    container footprint ~monotonically (ctbk measured a 90-120 GB
    extrapolated footprint against a ≤15 GB working set; Fargate has no
    swap, so retention OOMs the cgroup). Only effective for allocations
    made through the system allocator — pair with
    `ARROW_DEFAULT_MEMORY_POOL=system` (baked into the engine image)."""
    global _libc
    if _libc is None:
        try:
            import ctypes
            _libc = ctypes.CDLL('libc.so.6')
        except OSError:
            _libc = False
    if _libc:
        _libc.malloc_trim(0)


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
    row_group_size: int | Mapping[str, int] | None = None,
    spill_dir: str | Path | None = None,
    prefetch: int = 2,
    resume: bool = False,
    allow_empty: bool = False,
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
        row_group_size: output-shard row-group size — an int for all tiers
            or a per-tier-name mapping (e.g. ctbk's 2048); default = the
            writer's row-count heuristic.
        spill_dir: scratch dir for WIP spill files (deleted as shards
            close). Default: a fresh temp dir, removed at the end.
        prefetch: source windows to read ahead (thread pool).
        resume: skip shards already recorded in `shard_index` (which must
            expose `existing_keys()` — the JSONL manifest impls do), and
            skip source windows that only feed skipped shards. Shards are
            deterministic, so a Spot-reclaimed run resumes for the cost of
            the first unfinished shard's windows, not the whole range.
        allow_empty: permit a build whose source produced 0 rows over the
            whole range (all-EMPTY outputs). Off by default: a 0-row
            full-range build is ~always a mis-specified source rung, and
            silently `SUCCEEDED` empty runs clobber real data —
            `EmptySourceError` is raised at end-of-run instead.
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
    pending: dict[str, deque[ExpectedShard]] = {
        t.name: deque(sorted(plan.outputs_for_tier(t.name), key=lambda e: e.period_start))
        for t in tiers
    }

    resume_from = from_
    if resume:
        existing_keys = getattr(shard_index, 'existing_keys', None)
        if existing_keys is None:
            raise ValueError(
                "build_local: resume=True needs a shard_index that can list prior "
                "records (existing_keys()) — e.g. a JSONL manifest index"
            )
        done = existing_keys()
        for name, q in pending.items():
            kept = deque(e for e in q if e.key not in done)
            result.resumed_shards += len(q) - len(kept)
            pending[name] = kept
        remaining_starts = [e.effective_start for q in pending.values() for e in q]
        resume_from = min(remaining_starts, default=to)

    own_spill = spill_dir is None
    spill_root = Path(tempfile.mkdtemp(prefix='pyrmts-engine-')) if own_spill else Path(spill_dir)
    spill = SpillBuffer(spill_root, pyramid)

    def log(msg: str) -> None:
        if verbose:
            print(msg, file=sys.stderr)

    def rg_size_for(tier_name: str) -> int | None:
        if row_group_size is None:
            return None
        if isinstance(row_group_size, int):
            return row_group_size
        return row_group_size.get(tier_name)

    def route(tier_name: str, tf: pl.DataFrame, w_end: datetime) -> None:
        """Append `tf`'s rows to the spill files of the pending shards they
        fall in. Shard periods are tier-bin-aligned and disjoint, so each
        (floored) bin lands in exactly one shard; rows past the last
        pending shard (residual tail) are dropped."""
        if tf.height == 0:
            return
        for shard in pending[tier_name]:
            if shard.period_start >= w_end:
                break
            s_ms, e_ms = _ms(shard.period_start), _ms(shard.period_end)
            rows = tf.filter((pl.col(bin_col) >= s_ms) & (pl.col(bin_col) < e_ms))
            spill.append(shard.key, rows)

    def flush_ready(tier_name: str, cursor: datetime) -> int:
        q = pending[tier_name]
        n = 0
        while q and q[0].effective_end <= cursor:
            shard = q.popleft()
            _write_shard(shard, spill.close_shard(shard.key))
            n += 1
        return n

    def _write_shard(shard: ExpectedShard, rows: pl.DataFrame) -> None:
        t_flush = time.time()
        wide = long_to_wide(rows, pyramid)
        buf = io.BytesIO()
        kwargs: dict = {'sort': list(sort)} if sort is not None else {}
        rgs = rg_size_for(shard.tier)
        if rgs is not None:
            kwargs['row_group_size'] = rgs
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
            route(tier.name, tf, w_end)
        cursor = min(w_end, to)
        flushed = 0
        for tier in tiers:
            flushed += flush_ready(tier.name, cursor)
        if flushed:
            _trim_allocator()
        result.windows += 1

    periods = [
        p for p in shard_periods_covering(from_, to, window)
        if p.end > resume_from
    ]
    if resume and result.resumed_shards:
        log(f"resume: {result.resumed_shards} manifested shards skipped, "
            f"restarting from {resume_from:%Y-%m-%dT%H:%M} ({len(periods)} windows)")

    def read(p) -> pl.DataFrame:
        return source.read_window(max(p.start, from_), min(p.end, to))

    try:
        if prefetch > 1:
            with ThreadPoolExecutor(max_workers=prefetch) as pool:
                futs = deque()
                it = iter(periods)
                for p in it:
                    fut = pool.submit(read, p)
                    if not futs:
                        # Serialize the very first read: concurrent cold-start
                        # `pq.read_table` calls race pyarrow's lazy init
                        # (intermittent SIGSEGV on aarch64 — ctbk finding 6).
                        fut.result()
                    futs.append((fut, p))
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
        if result.windows and not result.source_rows and not allow_empty:
            raise EmptySourceError(
                f"build_local: 0 source rows across {result.windows} windows — "
                f"almost always a mis-specified source rung (the "
                f"{len(result.written)} zero-row shards WERE written/registered); "
                f"pass allow_empty=True / --allow-empty if intentional"
            )
    finally:
        close_index = getattr(shard_index, 'close', None)
        if close_index is not None:
            close_index()
        spill.close()
        if own_spill:
            try:
                spill_root.rmdir()
            except OSError:
                pass  # leftover spill files after an abort — keep for debugging

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

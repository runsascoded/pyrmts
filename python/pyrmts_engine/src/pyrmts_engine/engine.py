"""`local` executor: parallel windowed walk (queue/watermark).

`workers` threads claim windows off a shared forward-marching cursor
(`specs/parallel-window-executor.md`). Each claimed window is one task:

1. Read the window's base long-form frame (`source.read_window`).
2. Walk tiers finest→coarsest; each tier re-bins its divisibility
   predecessor's window frame (`group_by.sum` — one cheap aggregation per
   tier per window; sibling tiers share the predecessor frame, so fused
   fan-outs like `/2m,/3m,/5m ← /1m` scan their input once).
3. Route each tier's window rows to the expected shards they fall in,
   appending run files to per-shard spill (`SpillBuffer` — lock-free
   across workers, one file per append). Rows belonging to no expected
   shard (residual tails) are dropped immediately.

A **completion watermark** (largest prefix of finished windows — not the
claim cursor) gates shard closes: when it passes a shard's
`effective_end`, the coordinator hands the close to a dedicated close
thread — combine the shard's runs (group_by-sum IS the monoid combine),
materialize wide (hist-JSON built exactly once), write via
`write_tier_parquet`, single `storage.put`, then
`shard_index.record_shard` — registration immediately after each PUT,
in deterministic (sequential-equivalent) close order, so the ShardIndex
needs no locking and closes never stall the walk. Out-of-order spill
appends are safe: close combines + sorts, so append order can't reach
output bytes (regression-tested: N-worker builds are byte-identical to
`workers=1`).

`max_inflight` bounds claims (window `i` claimable only while
`i − watermark < max_inflight`), so peak memory ≈ in-flight windows'
frames + resident source-shard cache + one closing shard's combined long
form — NOT the sum of open max-rung WIP buffers (ctbk measured ~40 GB
that way on a 4-day smoke; see the spill module docstring). Max-rung
shards accumulate on scratch disk across the whole run: no scaffold
shards, ever.

On top of the `max_inflight` count bound, claims are **byte-aware**
(ctbk finding 10: `j=16` alone busted a 61 GB box regardless of `K`):
window 0 — already serialized for cold-start safety — measures its own
peak live bytes (window frame + cascade frames, freed eagerly as their
consumers finish), and further claims are admitted only while
`(inflight + 1) × 2 × est + source_cache_bytes ≤ mem_budget` (the ×2
covers group-by/parse transients above retained frames; the estimate is
the max observed across completed windows). The budget defaults to 70%
of the detected memory limit (cgroup v2/v1, then MemTotal), so an
oversized `workers` degrades to throttled parallelism instead of a
silent OOM kill. At least one window is always admitted.

Observability (ctbk finding 9 — two OOM'd Batch runs produced zero log
lines): a startup banner (resolved workers/K/budget, window count,
range, source rung, spill dir) prints to stderr before the first read,
and a progress line (windows done, shards written, in-flight vs
admitted cap, RSS) prints on watermark advance at most every 30 s.

Zero-row expected shards are written (and registered) as EMPTY shards —
cover-complete + zero rows is a legitimate state (outage windows), not a
gap for fsck to re-chase."""
from __future__ import annotations

import io
import os
import sys
import tempfile
import time
from bisect import bisect_right
from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
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


class SourceCoverageError(RuntimeError):
    """Post-genesis source shards were absent beyond `max_missing_source`.
    The scarier sibling of `EmptySourceError`: partial missing (filter
    typo, GC'd keys, wrong metric name) builds a plausible-looking
    pyramid with quietly wrong numbers. Affirmatively-EMPTY source shards
    (outage windows) are present objects and don't count. (Outputs were
    still written/registered before this raised.)"""


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
    missing_source: int = 0
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


_PAGE = os.sysconf('SC_PAGESIZE') if hasattr(os, 'sysconf') else 4096


def _rss_bytes() -> int | None:
    """Current RSS (Linux /proc; None elsewhere)."""
    try:
        with open('/proc/self/statm') as f:
            return int(f.read().split()[1]) * _PAGE
    except (OSError, IndexError, ValueError):
        return None


def _detect_mem_bytes() -> int | None:
    """Effective memory limit: cgroup v2 `memory.max`, then cgroup v1
    `memory.limit_in_bytes` (Batch/Fargate set these on the container),
    then host MemTotal. None when nothing is readable (non-Linux)."""
    for path in (
        '/sys/fs/cgroup/memory.max',
        '/sys/fs/cgroup/memory/memory.limit_in_bytes',
    ):
        try:
            v = Path(path).read_text().strip()
        except OSError:
            continue
        if v.isdigit() and int(v) < 1 << 60:  # v1 reports ~2^63 for "no limit"
            return int(v)
    try:
        with open('/proc/meminfo') as f:
            for line in f:
                if line.startswith('MemTotal:'):
                    return int(line.split()[1]) * 1024
    except OSError:
        pass
    return None


def _warmup_arrow() -> None:
    """One-time in-memory parquet round-trip on the calling (main) thread,
    before any worker pool exists: forces pyarrow's lazy initialization so
    N workers' first concurrent reads can't race it (ctbk finding 11:
    intermittent cold-start SIGSEGV on aarch64, not fixed by serializing
    only window 0's read — every thread's *first* read participated)."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    buf = io.BytesIO()
    pq.write_table(pa.table({'x': [0]}), buf)
    buf.seek(0)
    pq.read_table(buf)


# Admission headroom over retained frame bytes: group-by/parse transients
# roughly double a window task's peak footprint (measured on ctbk avail:
# ~2.5 GB retained → ~4-5 GB task RSS delta).
_ADMIT_HEADROOM = 2


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
    workers: int | None = None,
    max_inflight: int | None = None,
    mem_budget: int | None = None,
    resume: bool = False,
    allow_empty: bool = False,
    max_missing_source: float = 0.0,
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
        workers: window-worker threads (default: `os.cpu_count()` —
            Fargate reports the job's vCPUs). `workers=1` is the
            sequential reference the byte-identity gate compares against.
        max_inflight: claim bound `K` — window `i` is claimable only
            while `i − watermark < K` (default `2 × workers`). Bounds
            watermark lag (and so source-cache span ≈
            `⌈K·window/source_shard_dur⌉` resident shards); byte-level
            memory is governed by `mem_budget`.
        mem_budget: byte budget gating window admission (see module
            docstring). Default: 70% of the detected memory limit
            (cgroup, then MemTotal); pass 0 to disable byte-aware
            admission entirely.
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
        max_missing_source: tolerated fraction of absent source shards
            (when the source exposes `coverage()` — `WideShardSource`
            does). Strict (0.0) by default: reads are range-clamped so
            every miss is post-genesis, and affirmatively-EMPTY shards
            don't count — a miss means a real hole. Raise the threshold
            (up to 1.0) for sources whose outages legitimately have no
            objects at all.
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

    def err(msg: str) -> None:
        print(msg, file=sys.stderr, flush=True)

    def log(msg: str) -> None:
        if verbose:
            err(msg)

    def rg_size_for(tier_name: str) -> int | None:
        if row_group_size is None:
            return None
        if isinstance(row_group_size, int):
            return row_group_size
        return row_group_size.get(tier_name)

    # Immutable routing snapshot (post-resume), per tier, sorted by
    # period_start: window tasks route against this — never against the
    # coordinator-mutated `pending` deques. Shard periods within a tier
    # are disjoint (min-cover), so bisect finds the overlap run. A window
    # can never overlap an already-closed shard: closes require
    # `effective_end <= cursor ≤` every in-flight window's start.
    route_shards = {
        name: [(_ms(e.period_start), _ms(e.period_end), e.key) for e in q]
        for name, q in pending.items()
    }
    route_starts = {name: [s for s, _, _ in lst] for name, lst in route_shards.items()}

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

    periods = [
        p for p in shard_periods_covering(from_, to, window)
        if p.end > resume_from
    ]
    if resume and result.resumed_shards:
        err(f"resume: {result.resumed_shards} manifested shards skipped, "
            f"restarting from {resume_from:%Y-%m-%dT%H:%M} ({len(periods)} windows)")

    # Cascade consumer counts: a tier's window frame is freed as soon as
    # its last consumer has re-binned from it (finding 10: retaining all
    # tiers' frames for the task's whole life ~tripled per-window bytes).
    _BASE = ''  # frames-dict key for the source window frame
    n_succ: dict[str, int] = {_BASE: 0, **{t.name: 0 for t in tiers}}
    for t in tiers:
        n_succ[plan.preds[t.name] or _BASE] += 1

    def window_task(i: int) -> tuple[int, int]:
        """Returns (source rows, peak retained frame bytes) — the latter
        feeds byte-aware admission."""
        p = periods[i]
        frame = source.read_window(max(p.start, from_), min(p.end, to))
        rows = frame.height
        w_start_ms = _ms(max(p.start, from_))
        w_end_ms = _ms(min(p.end, to))
        frames: dict[str, pl.DataFrame] = {_BASE: frame}
        sizes: dict[str, int] = {_BASE: frame.estimated_size()}
        remaining = dict(n_succ)
        del frame
        live = peak = sizes[_BASE]
        for tier in tiers:
            pred = plan.preds[tier.name] or _BASE
            tf = rebin_long(frames[pred], pyramid, floor_exprs[tier.name])
            sz = tf.estimated_size()
            live += sz
            peak = max(peak, live)
            if tf.height:
                shards = route_shards[tier.name]
                lo = max(bisect_right(route_starts[tier.name], w_start_ms) - 1, 0)
                for s_ms, e_ms, key in shards[lo:]:
                    if s_ms >= w_end_ms:
                        break
                    if e_ms <= w_start_ms:
                        continue
                    spill.append(key, tf.filter(
                        (pl.col(bin_col) >= s_ms) & (pl.col(bin_col) < e_ms)
                    ))
            remaining[pred] -= 1
            if remaining[pred] == 0:
                del frames[pred]
                live -= sizes.pop(pred)
            if remaining[tier.name]:
                frames[tier.name] = tf
                sizes[tier.name] = sz
            else:
                live -= sz
            del tf
        return rows, peak

    def close_task(shard: ExpectedShard) -> None:
        _write_shard(shard, spill.close_shard(shard.key))
        _trim_allocator()

    n = len(periods)
    n_workers = workers or os.cpu_count() or 4
    inflight_cap = max_inflight or 2 * n_workers
    if mem_budget is None:
        detected = _detect_mem_bytes()
        mem_budget = int(detected * 0.7) if detected is not None else 0
    completed = [False] * n
    rows_of = [0] * n
    watermark = 0
    evict = getattr(source, 'evict_before', None)
    cache_bytes = getattr(source, 'cache_bytes', None)
    est_window = 0  # max observed per-window peak retained bytes
    last_progress = 0.0

    src_rung = getattr(source, 'provides', None)
    err(
        f"build_local: {n} windows × {window} over "
        f"{from_:%Y-%m-%dT%H:%M}/{to:%Y-%m-%dT%H:%M}, "
        f"{sum(len(q) for q in pending.values())} shards to write "
        f"({len(plan.skipped_rungs)} rung-skipped"
        + (f", {result.resumed_shards} resumed" if result.resumed_shards else '')
        + f"), workers={n_workers}, max_inflight={inflight_cap}, "
        f"mem_budget={mem_budget / 1e9:.1f}GB"
        + (f", source rung={src_rung[0]}/{src_rung[1]}" if src_rung else '')
        + f", spill={spill_root}"
    )
    _warmup_arrow()

    def admitted_cap() -> int:
        """Windows the byte budget currently admits (≥1 for progress)."""
        if not (mem_budget and est_window):
            return inflight_cap
        cached = cache_bytes() if cache_bytes is not None else 0
        return max(1, int((mem_budget - cached) // (_ADMIT_HEADROOM * est_window)))

    def progress(inflight: int) -> None:
        nonlocal last_progress
        if time.time() - last_progress < 30:
            return
        last_progress = time.time()
        rss = _rss_bytes()
        err(
            f"progress: {watermark}/{n} windows, {len(result.written)} shards "
            f"written, {inflight} in-flight (cap {min(inflight_cap, admitted_cap())})"
            + (f", rss {rss / 1e9:.1f}GB" if rss is not None else '')
        )

    window_pool = ThreadPoolExecutor(max_workers=n_workers)
    # Dedicated close thread: closes never stall the walk, and the
    # ShardIndex / result bookkeeping stay single-threaded with
    # deterministic (sequential-equivalent) close order.
    close_pool = ThreadPoolExecutor(max_workers=1)
    close_futs: list[Future] = []

    def advance_watermark() -> None:
        """Count newly-completed prefix windows, evict dead source cache,
        submit ready closes."""
        nonlocal watermark
        moved = False
        while watermark < n and completed[watermark]:
            result.source_rows += rows_of[watermark]
            result.windows += 1
            watermark += 1
            moved = True
        if not moved:
            return
        if evict is not None and watermark < n:
            evict(_ms(max(periods[watermark].start, from_)))
        cursor = min(periods[watermark - 1].end, to)
        for tier in tiers:
            q = pending[tier.name]
            while q and q[0].effective_end <= cursor:
                close_futs.append(close_pool.submit(close_task, q.popleft()))

    try:
        inflight: dict[Future, int] = {}
        next_i = 0

        def claim() -> None:
            nonlocal next_i, est_window
            while (
                next_i < n
                and len(inflight) < n_workers
                and next_i - watermark < inflight_cap
                and (not inflight or len(inflight) < admitted_cap())
            ):
                fut = window_pool.submit(window_task, next_i)
                if next_i == 0:
                    # Serialize the very first read: concurrent cold-start
                    # `pq.read_table` calls race pyarrow's lazy init
                    # (intermittent SIGSEGV on aarch64 — ctbk finding 6);
                    # window 0's result also seeds the admission estimate.
                    _, est0 = fut.result()
                    est_window = max(est_window, est0)
                inflight[fut] = next_i
                next_i += 1

        claim()
        while inflight:
            done, _ = wait(inflight, return_when=FIRST_COMPLETED)
            for fut in done:
                i = inflight.pop(fut)
                rows_of[i], est = fut.result()
                est_window = max(est_window, est)
                completed[i] = True
            advance_watermark()
            claim()
            progress(len(inflight))

        for fut in close_futs:
            fut.result()
        leftover = [(t, len(q)) for t, q in pending.items() if q]
        if leftover:
            raise AssertionError(f"build_local: unflushed shards after final window: {leftover}")
        cov = getattr(source, 'coverage', None)
        if cov is not None:
            n_periods, missing = cov()
            result.missing_source = len(missing)
            if missing:
                err(f"missing source shards ({len(missing)}/{n_periods}): "
                    + ', '.join(missing[:5]) + (', …' if len(missing) > 5 else ''))
            if len(missing) > max_missing_source * n_periods:
                sample = ', '.join(missing[:5]) + (', …' if len(missing) > 5 else '')
                raise SourceCoverageError(
                    f"build_local: {len(missing)}/{n_periods} source shards absent "
                    f"(> max_missing_source={max_missing_source}): {sample} — "
                    f"a real hole (GC'd rung, filter typo, wrong rung), not an "
                    f"outage (outage shards are present-but-EMPTY); raise "
                    f"max_missing_source / --max-missing if such holes are "
                    f"expected here (outputs WERE written/registered)"
                )
        if result.windows and not result.source_rows and not allow_empty:
            raise EmptySourceError(
                f"build_local: 0 source rows across {result.windows} windows — "
                f"almost always a mis-specified source rung (the "
                f"{len(result.written)} zero-row shards WERE written/registered); "
                f"pass allow_empty=True / --allow-empty if intentional"
            )
    finally:
        window_pool.shutdown(wait=True, cancel_futures=True)
        close_pool.shutdown(wait=True, cancel_futures=True)
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

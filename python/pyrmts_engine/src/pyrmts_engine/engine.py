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
`effective_end`, the coordinator hands the close to a small close pool —
combine the shard's runs (group_by-sum IS the monoid combine; chunked by
bin-range when large), materialize wide (hist-JSON built exactly once),
write via `write_tier_parquet`, single `storage.put`, then
`shard_index.record_shard`. Close *compute* (and PUTs) overlap freely;
registration + result bookkeeping pass through an ordered barrier in
submission order, so the ShardIndex sees the deterministic
(sequential-equivalent) sequence without locking of its own, and closes
never stall the walk. Out-of-order spill
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
consumers finish), the estimate is thereafter the max observed across
completed windows, and further claims are admitted only while

    current_rss + (inflight + 1) × 2 × est ≤ mem_budget

Gating on *actual RSS* (not an estimate of committed bytes) makes the
controller feedback-driven: source-cache growth, parse/group-by
transients, close-thread spikes, and allocator slack all show up in RSS
and shrink further admission, while the reservation term covers tasks
that were claimed but haven't materialized their frames yet (a claim's
RSS lands seconds later; the first 61 GB OOM on `e` was exactly
claim-time-only accounting). `est` is retained-frame bytes only — the
true per-task RSS delta measured ~5× that on ctbk avail (group-by hash
tables, per-shard filter + to_arrow copies in spill appends, allocator
churn) — hence ×4 headroom *plus* two burst limiters: at most 2 new
claims per scheduler pass (slow start: RSS reflects each admission
before the next burst), and no claims at all above 85% of budget (the
last 15% absorbs close-thread spikes, which no per-window term models).
Mature in-flight tasks are double-counted (already in RSS *and*
reserved) — deliberately conservative. The budget defaults to 70% of
the detected memory limit (cgroup v2/v1, then MemTotal), so an
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

import hashlib
import io
import os
import sys
import tempfile
import threading
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


# Admission headroom over retained frame bytes. The measured per-task RSS
# delta on ctbk avail was ~5× retained frames (group-by hash tables,
# routing filter + to_arrow copies, allocator churn); 4× suffices because
# slow-start + the 85%-of-budget claim ceiling bound the un-materialized
# claim burst that headroom has to absorb.
_ADMIT_HEADROOM = 4
# No new claims above this fraction of `mem_budget`: leaves room for
# close-thread transients (combine + wide/hist-JSON materialization),
# which per-window reservations don't model.
_ADMIT_RSS_CEILING = 0.85
# Max new claims per scheduler pass (slow start): each admission's RSS
# should be observable before the next burst — 16 claims in one pass at
# cold start is how the first two OOMs got past claim-time gating.
_ADMIT_BURST = 2

# Chunked-close sizing: target combined-long bytes per close chunk, the
# snappy-parquet → in-memory long-frame inflation used to estimate them
# from run-file sizes (measured ~12-13× on ctbk avail spill runs), and a
# chunk-count cap (each chunk rescans the shard's runs — local snappy
# parquet, cheap, but not free).
_CLOSE_CHUNK_BYTES = 1 << 30
_SPILL_LONG_RATIO = 12
_CLOSE_MAX_CHUNKS = 64
# Concurrent close computations (combine/widen/PUT). Registration stays
# in submission order regardless. Chunking bounds each worker's
# transient; the admission RSS ceiling pauses the walk if a burst of
# closes spikes anyway. 2 (down from 3): the first full-range run's
# close pool had slack (92/99 closed before walk-end) while concurrent
# close transients drove the 37 GB peak RSS.
_CLOSE_WORKERS = 2


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
    close_workers: int | None = None,
    close_chunk_bytes: int | None = None,
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
        close_workers: concurrent close computations (default
            `_CLOSE_WORKERS`). More overlaps closes with the walk
            (wall) at the cost of stacked close transients (peak RSS) —
            the par-vs-mem profile dial, with `window`/`mem_budget`.
        close_chunk_bytes: target combined-long bytes per close chunk
            (default `_CLOSE_CHUNK_BYTES`); smaller bounds each close's
            transient tighter.
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
        # No unfinished shards → nothing to walk at all (`resume_from =
        # to` alone would still admit a final window straddling `to`) —
        # a no-op cron top-up must be exactly free, not one-window-ish.
        if not remaining_starts:
            resume_from = None
        else:
            resume_from = min(remaining_starts)

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

    def _write_shard(shard: ExpectedShard, wide: pl.DataFrame, t_flush: float, seq: int) -> None:
        nonlocal next_reg
        buf = io.BytesIO()
        kwargs: dict = {'sort': list(sort)} if sort is not None else {}
        rgs = rg_size_for(shard.tier)
        if rgs is not None:
            kwargs['row_group_size'] = rgs
        n_bytes = write_tier_parquet(wide.to_arrow(), pyramid, out=buf, **kwargs)
        payload = buf.getvalue()
        pyramid.storage.put(shard.key, payload)
        with reg_cond:
            while next_reg != seq:
                reg_cond.wait()
            shard_index.record_shard(ShardRecord(
                pyramid=pyramid_name,
                tier=shard.tier,
                shard_dur=shard.shard_dur,
                period_start_ms=_ms(shard.period_start),
                period_end_ms=_ms(shard.period_end),
                key=shard.key,
                written_at_ms=now_ms(),
                md5=hashlib.md5(payload).hexdigest(),
                n_bytes=len(payload),
            ))
            result.written.append(WrittenShard(
                key=shard.key, tier=shard.tier, shard_dur=shard.shard_dur,
                rows=wide.height, bytes=n_bytes,
            ))
            next_reg += 1
            reg_cond.notify_all()
        log(f"  flush {shard.tier:6s} {shard.key}: {wide.height:,} rows, "
            f"{n_bytes/1024:.0f} KiB, {time.time() - t_flush:.1f}s")

    periods = [] if resume_from is None else [
        p for p in shard_periods_covering(from_, to, window)
        if p.end > resume_from
    ]
    if resume and result.resumed_shards:
        restart = (
            'nothing left to build' if resume_from is None
            else f"restarting from {resume_from:%Y-%m-%dT%H:%M}"
        )
        err(f"resume: {result.resumed_shards} manifested shards skipped, "
            f"{restart} ({len(periods)} windows)")

    # Cascade consumer counts: a tier's window frame is freed as soon as
    # its last consumer has re-binned from it (finding 10: retaining all
    # tiers' frames for the task's whole life ~tripled per-window bytes).
    n_succ: dict[str, int] = {t.name: 0 for t in tiers}
    for t in tiers[1:]:
        n_succ[plan.preds[t.name]] += 1

    def window_task(i: int) -> tuple[int, int]:
        """Returns (source rows, peak retained frame bytes) — the latter
        feeds byte-aware admission."""
        p = periods[i]
        frame = source.read_window(max(p.start, from_), min(p.end, to))
        rows = frame.height
        w_start_ms = _ms(max(p.start, from_))
        w_end_ms = _ms(min(p.end, to))

        def route(tier_name: str, tf: pl.DataFrame) -> None:
            if not tf.height:
                return
            shards = route_shards[tier_name]
            lo = max(bisect_right(route_starts[tier_name], w_start_ms) - 1, 0)
            for s_ms, e_ms, key in shards[lo:]:
                if s_ms >= w_end_ms:
                    break
                if e_ms <= w_start_ms:
                    continue
                spill.append(key, tf.filter(
                    (pl.col(bin_col) >= s_ms) & (pl.col(bin_col) < e_ms)
                ))

        # Base-tier passthrough: the Source contract guarantees bins
        # already floored to the base bin, so a rebin here would be an
        # identity group_by — a full copy plus transients per window.
        # Duplicate keys a source might emit combine at spill close.
        base = tiers[0].name
        remaining = dict(n_succ)
        frames = {base: frame}
        sizes = {base: frame.estimated_size()}
        live = peak = sizes[base]
        route(base, frame)
        if not remaining[base]:
            del frames[base]
            live -= sizes.pop(base)
        del frame
        for tier in tiers[1:]:
            pred = plan.preds[tier.name]
            tf = rebin_long(frames[pred], pyramid, floor_exprs[tier.name])
            sz = tf.estimated_size()
            live += sz
            peak = max(peak, live)
            route(tier.name, tf)
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

    reg_cond = threading.Condition()
    next_reg = 0
    chunk_bytes = close_chunk_bytes or _CLOSE_CHUNK_BYTES

    def close_task(shard: ExpectedShard, seq: int) -> None:
        """Combine a shard's spill runs and write it. Shards whose
        estimated combined long frame exceeds `_CLOSE_CHUNK_BYTES` are
        closed in disjoint bin-range chunks (combine + widen each, concat
        the far-smaller wide frames, one global sort at write): a 4d
        max-rung close was materializing a ~100M-row long frame plus a
        non-streaming pivot (~10 GB transient) on top of the walk. Bins
        partition across chunks and wide rows are unique per (dims, bin),
        so chunking cannot reach output bytes.

        Compute (and the PUT) runs on any of the close pool's threads;
        registration + result bookkeeping wait for submission order
        (`seq`), so the ShardIndex sees the sequential-equivalent order
        with no locking of its own. Deadlock-free: the pool queue is
        FIFO, so task k only ever waits on strictly-earlier tasks that
        are already running or done."""
        nonlocal next_reg
        try:
            t_flush = time.time()
            paths, run_bytes = spill.take_shard(shard.key)
            n_chunks = min(
                _CLOSE_MAX_CHUNKS,
                max(1, -(-run_bytes * _SPILL_LONG_RATIO // chunk_bytes)),
            )
            if n_chunks == 1:
                wide = long_to_wide(spill.combine_runs(paths), pyramid)
            else:
                s_ms, e_ms = _ms(shard.period_start), _ms(shard.period_end)
                edges = [s_ms + (e_ms - s_ms) * k // n_chunks for k in range(n_chunks + 1)]
                wides = [
                    long_to_wide(w, pyramid)
                    for lo, hi in zip(edges, edges[1:])
                    if (w := spill.combine_runs(paths, bin_range=(lo, hi))).height
                ]
                wide = pl.concat(wides) if wides else long_to_wide(spill.combine_runs([]), pyramid)
            for path in paths:
                path.unlink()
            _write_shard(shard, wide, t_flush, seq)
        finally:
            with reg_cond:
                if next_reg == seq:
                    # Exception before registration: release the ordered
                    # wait so later closes don't block behind a dead seq
                    # (the error still surfaces via this task's Future).
                    next_reg += 1
                    reg_cond.notify_all()
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
    prefetch = getattr(source, 'prefetch', None)
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
        """Windows the byte budget currently admits (≥1 for progress):
        actual RSS + a worst-case reservation per in-flight window must
        stay under budget."""
        if not (mem_budget and est_window):
            return inflight_cap
        rss = _rss_bytes()
        if rss is None:
            return inflight_cap
        return max(1, int((mem_budget - rss) // (_ADMIT_HEADROOM * est_window)))

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
            + (f", cache {cache_bytes() / 1e9:.1f}GB" if cache_bytes is not None else '')
            + (f", est {est_window / 1e9:.2f}GB" if est_window else '')
        )

    window_pool = ThreadPoolExecutor(max_workers=n_workers)
    # Close pool: closes never stall the walk, and compute (+PUT) runs on
    # a few threads — the serial close-drain tail was ~half the wall on
    # the 2-week probe. ShardIndex / result bookkeeping still happen in
    # deterministic submission order via the `seq` ordered-registration
    # barrier in `_write_shard`.
    close_pool = ThreadPoolExecutor(max_workers=close_workers or _CLOSE_WORKERS)
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
                close_futs.append(close_pool.submit(close_task, q.popleft(), len(close_futs)))

    try:
        inflight: dict[Future, int] = {}
        next_i = 0

        def _readahead_ok() -> bool:
            """Readahead is a wall optimization that pins an extra parsed
            shard (~one cache entry) — skip it unless the budget has room
            (Lambda-envelope sim peaked 9.7 GB against a 10 GB cap purely
            from the readahead-doubled cache)."""
            if not mem_budget or cache_bytes is None:
                return True
            rss = _rss_bytes()
            if rss is None:
                return True
            return mem_budget - rss > 1.5 * cache_bytes()

        def admit_ok() -> bool:
            if not inflight:
                return True  # progress guarantee
            if mem_budget:
                rss = _rss_bytes()
                if rss is not None and rss >= _ADMIT_RSS_CEILING * mem_budget:
                    return False
            return len(inflight) < admitted_cap()

        def claim() -> None:
            nonlocal next_i, est_window
            burst = 0
            while (
                next_i < n
                and burst < _ADMIT_BURST
                and len(inflight) < n_workers
                and next_i - watermark < inflight_cap
                and admit_ok()
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
                burst += 1
                if prefetch is not None and next_i + inflight_cap < n and _readahead_ok():
                    # Warm the shard the claim frontier will reach next
                    # (in-range by construction, so a miss is a real
                    # coverage miss like any foreground read's).
                    prefetch(max(periods[next_i + inflight_cap].start, from_))

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
        close_source = getattr(source, 'close', None)
        if close_source is not None:
            close_source()
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

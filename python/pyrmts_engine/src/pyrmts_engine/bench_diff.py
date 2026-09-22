"""Bake-off harness: index-free diff *walk* vs. *materialized* pairwise diff,
on real path-index scans (`specs/multi-scan-consolidation.md`, "diff-treemap
engine").

The question: can a diff-treemap view between any two scans be served by a
rendering-bounded best-first walk over two random-access snapshots (no extra
index), and how does that compare to materializing the full pairwise diff?

**Walk** (`walk_diff`): expand pending dirs level by level (or largest-|Δ|
first), list each one's children in both scans (one row-group-pruned read per
side), merge-join by name, push the changed subdirs. It stops at the **render floor** — a dir whose size on both
sides and |Δ| are all below `floor` bytes cannot be drawn, so it is never
expanded — plus an expansion `budget` as a backstop. Levers, each a flag:
`footer_cache` (decoded parquet metadata reused across opens), `rg_cache`
(a decoded row group reused across expansions within one request), `listing`
(`bisect`: per-RG sorted keys, two bisections per listing; `filter`: a
vectorized predicate over the whole RG per listing).

**Materialize** (`materialize_diff`): full outer join of the two scans per
depth (Arrow-native), keep changed rows — what a per-pair diff index stores —
then `slice_view` = the rows a view would render (prefix + floor).

Everything is instrumented: per-stage CPU (`locate` = RG selection from
stats, `read` = row-group decode, `post` = Arrow filter + dict alignment),
row groups read (`requests`) and their compressed `bytes`, range `gets` (a
listing's row groups are one contiguous byte run, so one GET per listing that
misses the RG cache), row groups decoded, listings, expansions. Remote latency
is modelled, not simulated: `wall_model(rtt_ms, parallel)` = CPU + gets /
parallel × rtt, so one local run yields the R2 / GCS numbers for any RTT.
"""
from __future__ import annotations

import heapq
import os
import time
from bisect import bisect_left
from collections.abc import Iterable
from dataclasses import dataclass, field

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


@dataclass(frozen=True)
class Cols:
    """Column mapping for a path-index scan (pyrmts / cw: `b`/`o`; disk-tree:
    `size`/`n_desc`)."""
    path: str = 'path'
    depth: str = 'depth'
    size: str = 'b'
    count: str = 'o'


@dataclass
class Stats:
    requests: int = 0        # row-group reads (RG granularity)
    gets: int = 0            # range GETs: one per listing that reads ≥1 uncached RG (its RGs are one contiguous byte run)
    bytes: int = 0           # compressed bytes of those row groups
    rg_decodes: int = 0      # row groups actually decoded (rg_cache hits excluded)
    rg_cache_hits: int = 0
    footer_parses: int = 0
    listings: int = 0
    expansions: int = 0
    rounds: list[int] = field(default_factory=list)   # GETs issued per dependent round (level-synchronous walk)
    ms: dict[str, float] = field(default_factory=lambda: {'locate': 0.0, 'read': 0.0, 'post': 0.0, 'footer': 0.0})

    @property
    def cpu_ms(self) -> float:
        return sum(self.ms.values())

    def wall_model(self, rtt_ms: float, parallel: int = 8) -> float:
        """Modelled wall time: CPU plus the dependent round trips. A dir can only
        be expanded after its parent's listing returned, so the walk's GETs
        happen in `rounds` (one per tree level in level-synchronous order); a
        round with more GETs than `parallel` in flight takes ⌈n / parallel⌉
        trips. Without recorded rounds (best-first order) the optimistic
        `gets / parallel` is used."""
        if self.rounds:
            trips = sum(-(-n // max(1, parallel)) for n in self.rounds if n)
        else:
            trips = self.gets / max(1, parallel)
        return self.cpu_ms + trips * rtt_ms

    @property
    def round_trips(self) -> int:
        return sum(1 for n in self.rounds if n)

    def as_dict(self) -> dict:
        return {
            'requests': self.requests, 'gets': self.gets, 'bytes': self.bytes, 'rg_decodes': self.rg_decodes,
            'rg_cache_hits': self.rg_cache_hits, 'footer_parses': self.footer_parses,
            'listings': self.listings, 'expansions': self.expansions, 'rounds': list(self.rounds),
            'ms': {k: round(v, 1) for k, v in self.ms.items()}, 'cpu_ms': round(self.cpu_ms, 1),
        }


def _rg_bytes(md: pq.FileMetaData, i: int) -> int:
    rg = md.row_group(i)
    return sum(rg.column(c).total_compressed_size for c in range(rg.num_columns))


class SnapshotReader:
    """Per-directory children reader over one `(depth, path)`-sorted path-index
    parquet: row groups are selected from footer min/max stats on `depth` and
    `path` (the same pruning `fetch.ts` does), decoded, then filtered to exactly
    "depth d+1 under prefix P". `footer_cache` maps `(path, mtime)` → decoded
    `FileMetaData`; `rg_cache` keeps decoded row groups for this reader's life
    (one request)."""

    def __init__(
        self,
        path: str,
        cols: Cols = Cols(),
        *,
        stats: Stats | None = None,
        footer_cache: dict | None = None,
        rg_cache: bool = True,
        listing: str = 'filter',
    ) -> None:
        """`listing`: how a decoded row group is searched — `filter` (a
        vectorized Arrow predicate over the whole RG per listing; wins at small
        RGs: ~0.2 ms/listing on 8K-row RGs) or `bisect` (the RG's (depth, path)
        keys are listed once, then two bisections + a slice per listing; wins
        when RGs are large, e.g. 64K+ rows, and many listings share one)."""
        if listing not in ('bisect', 'filter'):
            raise ValueError(f"SnapshotReader: listing must be 'bisect' or 'filter', got {listing!r}")
        self.path = path
        self.cols = cols
        self.stats = stats if stats is not None else Stats()
        self._rg_cache: dict[int, pa.Table] | None = {} if rg_cache else None
        t0 = time.perf_counter()
        key = (path, os.path.getmtime(path))
        cached = footer_cache.get(key) if footer_cache is not None else None
        if cached is None:
            self.pf = pq.ParquetFile(path)
            self.stats.footer_parses += 1
            if footer_cache is not None:
                footer_cache[key] = self.pf.metadata
        else:
            self.pf = pq.ParquetFile(path, metadata=cached)
        self.md = self.pf.metadata
        self.stats.ms['footer'] += (time.perf_counter() - t0) * 1000
        names = self.pf.schema_arrow.names
        self._depth_idx = names.index(cols.depth)
        self._path_idx = names.index(cols.path)
        self._read_cols = [cols.path, cols.depth, cols.size, cols.count]
        self.listing = listing
        # Row-group key ranges from the footer stats, once per open: RG i
        # covers [(depth_min, path_min), (depth_max, path_max)] and the file is
        # (depth, path)-sorted, so the RGs for a listing are one contiguous run
        # found by bisection — what a per-RG stats table in D1 would answer.
        t0 = time.perf_counter()
        self._rg_lo: list[tuple[int, str]] = []
        self._rg_hi: list[tuple[int, str]] = []
        for i in range(self.md.num_row_groups):
            rg = self.md.row_group(i)
            ds = rg.column(self._depth_idx).statistics
            ps = rg.column(self._path_idx).statistics
            self._rg_lo.append((ds.min, ps.min))
            self._rg_hi.append((ds.max, ps.max))
        self._keys: dict[int, list[tuple[int, str]]] = {}
        self.stats.ms['footer'] += (time.perf_counter() - t0) * 1000

    def _locate(self, depth: int, lo: str, hi: str) -> list[int]:
        """Row groups whose key range intersects `[(depth, lo), (depth, hi))`."""
        first = bisect_left(self._rg_hi, (depth, lo))          # first RG ending at/after the start
        last = bisect_left(self._rg_lo, (depth, hi))           # first RG starting at/after the end
        return list(range(first, max(first, last)))

    def _read_rg(self, i: int) -> pa.Table:
        if self._rg_cache is not None and i in self._rg_cache:
            self.stats.rg_cache_hits += 1
            return self._rg_cache[i]
        self.stats.requests += 1
        self.stats.bytes += _rg_bytes(self.md, i)
        self.stats.rg_decodes += 1
        t = self.pf.read_row_group(i, columns=self._read_cols)
        if self._rg_cache is not None:
            self._rg_cache[i] = t
        return t

    def node(self, path: str) -> tuple[int, int] | None:
        """`(size, count)` of one node, or None if absent."""
        depth = 0 if path in ('', '.') else path.count('/') + 1
        listing = self._list(depth, path, path + '\0')
        return listing.get(path)

    def children(self, prefix: str, depth: int) -> dict[str, tuple[int, int]]:
        """Direct children of `prefix` (a node at `depth`): `{child_path: (size, count)}`."""
        self.stats.listings += 1
        lo = f'{prefix}/' if prefix else ''
        hi = f'{prefix}0' if prefix else '\x7f'
        return self._list(depth + 1, lo, hi)

    def _list(self, depth: int, lo: str, hi: str) -> dict[str, tuple[int, int]]:
        c = self.cols
        t0 = time.perf_counter()
        rgs = self._locate(depth, lo, hi)
        t1 = time.perf_counter()
        before = self.stats.requests
        tables = [self._read_rg(i) for i in rgs]
        if self.stats.requests > before:
            self.stats.gets += 1
        t2 = time.perf_counter()
        out: dict[str, tuple[int, int]] = {}
        for i, t in zip(rgs, tables):
            if self.listing == 'bisect':
                keys = self._keys.get(i)
                if keys is None:
                    keys = list(zip(t[c.depth].to_pylist(), t[c.path].to_pylist()))
                    if self._rg_cache is not None:
                        self._keys[i] = keys
                a = bisect_left(keys, (depth, lo))
                b = bisect_left(keys, (depth, hi))
                if b > a:
                    sub = t.slice(a, b - a)
                    for p, s, n in zip(sub[c.path].to_pylist(), sub[c.size].to_pylist(), sub[c.count].to_pylist()):
                        out[p] = (s or 0, n or 0)
            else:
                mask = pc.and_(
                    pc.equal(t[c.depth], depth),
                    pc.and_(pc.greater_equal(t[c.path], lo), pc.less(t[c.path], hi)),
                )
                sub = t.filter(mask)
                if sub.num_rows:
                    for p, s, n in zip(sub[c.path].to_pylist(), sub[c.size].to_pylist(), sub[c.count].to_pylist()):
                        out[p] = (s or 0, n or 0)
        t3 = time.perf_counter()
        self.stats.ms['locate'] += (t1 - t0) * 1000
        self.stats.ms['read'] += (t2 - t1) * 1000
        self.stats.ms['post'] += (t3 - t2) * 1000
        return out


@dataclass
class DeltaRow:
    path: str
    depth: int
    status: str            # added | removed | changed | unchanged
    size_a: int
    size_b: int
    count_a: int
    count_b: int
    expanded: bool = False
    pruned: bool = False   # differing, not expanded (floor / budget)

    @property
    def delta(self) -> int:
        return self.size_b - self.size_a


@dataclass
class WalkResult:
    rows: list[DeltaRow]
    expansions: int
    truncated: bool


def render_floor(root_size: int, width_px: int, height_px: int, min_cell_px: float = 4.0) -> int:
    """Smallest subtree (bytes) that can be drawn: a node's area ≈ its share of
    the root × the canvas, so `root_size × min_cell_px² / (width × height)`."""
    return int(root_size * (min_cell_px * min_cell_px) / max(1, width_px * height_px))


def walk_diff(
    ra: SnapshotReader,
    rb: SnapshotReader,
    root: str = '',
    *,
    floor: int = 0,
    budget: int = 10_000,
    order: str = 'level',
) -> WalkResult:
    """Pruned recursive diff between two snapshots under `root`, bounded by the
    render `floor` (bytes) and an expansion `budget`. Returns every changed row
    met plus `pruned` marks where change may hide below.

    `order='level'`: level-synchronous — every pending expansion at one depth is
    listed in the same round (they are independent; only a parent → child
    edge is a dependency), so the number of dependent round trips equals the
    depth of the expanded tree. Rounds are recorded in `ra.stats.rounds`.
    `order='bestfirst'`: pop the largest-|Δ| dir first, one at a time — the
    right order under a budget cut, but its round trips are sequential."""
    if order not in ('level', 'bestfirst'):
        raise ValueError(f"walk_diff: order must be 'level' or 'bestfirst', got {order!r}")
    root_depth = 0 if root in ('', '.') else root.count('/') + 1
    rows: list[DeltaRow] = []
    by_path: dict[str, DeltaRow] = {}
    heap: list[tuple[int, int, int, str]] = [(0, root_depth, 0, root)]
    seq = 0
    expansions = 0
    truncated = False
    stats = ra.stats
    stats.rounds = []

    def gets() -> int:
        return stats.gets + (rb.stats.gets if rb.stats is not stats else 0)

    while heap:
        if expansions >= budget:
            truncated = True
            break
        if order == 'level':
            # The whole shallowest level is one round (never past the budget).
            level = min(h[1] for h in heap)
            at_level = [h for h in heap if h[1] == level]
            batch = at_level[:budget - expansions]
            heap = [h for h in heap if h[1] != level] + at_level[len(batch):]
            heapq.heapify(heap)
        else:
            batch = [heapq.heappop(heap)]
        gets_before = gets()
        for _, d, _, rel in batch:
            ca = ra.children(rel, d)
            cb = rb.children(rel, d)
            expansions += 1
            stats.expansions += 1
            if rel in by_path:
                by_path[rel].expanded = True
            for name in sorted(set(ca) | set(cb)):
                sa, na = ca.get(name, (0, 0))
                sb, nb = cb.get(name, (0, 0))
                in_a, in_b = name in ca, name in cb
                row = DeltaRow(name, d + 1, '', sa, sb, na, nb)
                if not in_a:
                    row.status = 'added'
                elif not in_b:
                    row.status = 'removed'
                elif sa != sb or na != nb:
                    row.status = 'changed'
                else:
                    row.status = 'unchanged'
                by_path[name] = row
                if row.status == 'changed':
                    # Only a changed node present on both sides can hide change
                    # below it; added/removed rows already tell the whole story.
                    if max(sa, sb) < floor and abs(row.delta) < floor:
                        row.pruned = True     # below the render floor: not drawable
                    else:
                        # No `kind` column in general (a prefix with one object and
                        # the object itself both carry count 1), so a leaf is found
                        # by an empty listing — one locate, usually no read.
                        seq += 1
                        heapq.heappush(heap, (-abs(row.delta), d + 1, seq, name))
                if row.status != 'unchanged':
                    rows.append(row)
        stats.rounds.append(gets() - gets_before)
    for _, _, _, rel in heap:            # budget-cut: queued but never expanded
        r = by_path.get(rel)
        if r is not None:
            r.pruned = True
            truncated = True
    rows.sort(key=lambda r: (-abs(r.delta), r.path))
    return WalkResult(rows=rows, expansions=expansions, truncated=truncated)


def materialize_diff(path_a: str, path_b: str, cols: Cols = Cols()) -> tuple[pa.Table, dict]:
    """Full pairwise diff as a per-pair index would store it: per depth, full
    outer join on `path`, keep rows whose (size, count) differ or exist on one
    side only. Columns: path, depth, status, size_a, size_b, count_a, count_b.
    Returns `(table, timings_ms)`."""
    c = cols
    t0 = time.perf_counter()
    read = [c.path, c.depth, c.size, c.count]
    ta = pq.read_table(path_a, columns=read)
    tb = pq.read_table(path_b, columns=read)
    t1 = time.perf_counter()
    depths = sorted(set(pc.unique(ta[c.depth]).to_pylist()) | set(pc.unique(tb[c.depth]).to_pylist()))
    parts: list[pa.Table] = []
    for d in depths:
        a = ta.filter(pc.equal(ta[c.depth], d)).select([c.path, c.size, c.count]).rename_columns(['path', 'size_a', 'count_a'])
        b = tb.filter(pc.equal(tb[c.depth], d)).select([c.path, c.size, c.count]).rename_columns(['path', 'size_b', 'count_b'])
        j = a.join(b, keys='path', join_type='full outer')
        in_a = pc.is_valid(j['size_a'])
        in_b = pc.is_valid(j['size_b'])
        sa = pc.fill_null(j['size_a'], 0)
        sb = pc.fill_null(j['size_b'], 0)
        na = pc.fill_null(j['count_a'], 0)
        nb = pc.fill_null(j['count_b'], 0)
        changed = pc.or_(pc.not_equal(sa, sb), pc.not_equal(na, nb))
        keep = pc.or_(pc.or_(pc.invert(in_a), pc.invert(in_b)), changed)
        status = pc.if_else(pc.invert(in_a), 'added', pc.if_else(pc.invert(in_b), 'removed', 'changed'))
        out = pa.table({
            'path': j['path'], 'depth': pa.array([d] * j.num_rows, pa.int64()), 'status': status,
            'size_a': sa, 'size_b': sb, 'count_a': na, 'count_b': nb,
        }).filter(keep)
        parts.append(out)
    diff = pa.concat_tables(parts)
    t2 = time.perf_counter()
    return diff, {'load': (t1 - t0) * 1000, 'join': (t2 - t1) * 1000, 'rows': diff.num_rows}


def slice_view(diff: pa.Table, root: str = '', *, floor: int = 0) -> tuple[pa.Table, float]:
    """The rows a view under `root` would render from a materialized diff: the
    prefix's rows whose size on either side or |Δ| clears the floor, minus rows
    under an added/removed ancestor (that ancestor's row is the whole story —
    the same frontier disk-tree's `diff_index` stores and the walk emits)."""
    t0 = time.perf_counter()
    lo = f'{root}/' if root else ''
    hi = f'{root}0' if root else '\x7f'
    m = pc.and_(pc.greater_equal(diff['path'], lo), pc.less(diff['path'], hi))
    big = pc.or_(
        pc.greater_equal(pc.max_element_wise(diff['size_a'], diff['size_b']), floor),
        pc.greater_equal(pc.abs(pc.subtract(diff['size_b'], diff['size_a'])), floor),
    )
    out = diff.filter(pc.and_(m, big))
    status = out['status'].to_pylist()
    paths = out['path'].to_pylist()
    whole = {p for p, st in zip(paths, status) if st != 'changed'}
    keep = []
    for p in paths:
        parts = p.split('/')
        keep.append(not any('/'.join(parts[:k]) in whole for k in range(1, len(parts))))
    out = out.filter(pa.array(keep, pa.bool_()))
    return out, (time.perf_counter() - t0) * 1000


def walk_rows_table(rows: Iterable[DeltaRow]) -> pa.Table:
    rows = list(rows)
    return pa.table({
        'path': [r.path for r in rows], 'depth': [r.depth for r in rows], 'status': [r.status for r in rows],
        'size_a': [r.size_a for r in rows], 'size_b': [r.size_b for r in rows],
        'count_a': [r.count_a for r in rows], 'count_b': [r.count_b for r in rows],
        'expanded': [r.expanded for r in rows], 'pruned': [r.pruned for r in rows],
    })

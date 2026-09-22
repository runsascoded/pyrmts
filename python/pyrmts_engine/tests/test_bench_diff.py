"""Bake-off harness (`bench_diff.py`): the index-free walk must return exactly
the changed rows a materialized diff's view slice contains (down to the render
floor), stop expanding below the floor, and count what it reads."""
from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from pyrmts_engine.bench_diff import (
    Cols,
    SnapshotReader,
    Stats,
    materialize_diff,
    render_floor,
    slice_view,
    walk_diff,
)

# A small tree with a deep change (big.bin grows), a birth, a death, a rename
# in a net-zero dir, and an untouched sibling. Rows are (path, depth, b, o),
# dirs carry rolled-up totals, written (depth, path)-sorted in 4-row groups.
def _tree_a() -> list[tuple[str, int, int, int]]:
    return [
        ('r', 1, 1000, 6), ('r/a', 2, 400, 2), ('r/b', 2, 600, 2), ('r/z', 2, 0, 0),
        ('r/a/f1', 3, 100, 1), ('r/a/f2', 3, 300, 1), ('r/b/sub', 3, 600, 1),
        ('r/b/sub/big.bin', 4, 600, 1),
    ]


def _tree_b() -> list[tuple[str, int, int, int]]:
    return [
        ('r', 1, 2010, 7), ('r/a', 2, 400, 2), ('r/b', 2, 1600, 2), ('r/z', 2, 10, 1),
        ('r/a/f1', 3, 100, 1), ('r/a/f3', 3, 300, 1),          # f2 → f3: net-zero rename in r/a
        ('r/b/sub', 3, 1600, 1), ('r/z/new', 3, 10, 1),
        ('r/b/sub/big.bin', 4, 1600, 1),
    ]


def _write(rows, path: Path) -> str:
    rows = sorted(rows, key=lambda r: (r[1], r[0]))
    t = pa.table({
        'path': [r[0] for r in rows], 'depth': [r[1] for r in rows],
        'b': [r[2] for r in rows], 'o': [r[3] for r in rows],
    })
    pq.write_table(t, path, row_group_size=4)
    return str(path)


def _key(t: pa.Table) -> list[tuple]:
    d = t.to_pydict()
    return sorted(zip(d['path'], d['status'], d['size_a'], d['size_b']))


def test_walk_matches_materialized_view_slice(tmp_path: Path):
    a = _write(_tree_a(), tmp_path / 'a.parquet')
    b = _write(_tree_b(), tmp_path / 'b.parquet')
    stats = Stats()
    ra = SnapshotReader(a, stats=stats)
    rb = SnapshotReader(b, stats=stats)
    res = walk_diff(ra, rb, '', floor=0)
    got = sorted((r.path, r.status, r.size_a, r.size_b) for r in res.rows)
    diff, _ = materialize_diff(a, b)
    view, _ = slice_view(diff, '', floor=0)
    changed_spines = [
        ('r', 'changed', 1000, 2010),
        ('r/b', 'changed', 600, 1600),
        ('r/b/sub', 'changed', 600, 1600),
        ('r/b/sub/big.bin', 'changed', 600, 1600),
        ('r/z', 'changed', 0, 10),
        ('r/z/new', 'added', 0, 10),
    ]
    hidden_rename = [('r/a/f2', 'removed', 300, 0), ('r/a/f3', 'added', 0, 300)]
    assert got == changed_spines
    # The walk's known blind spot: r/a's totals (size AND count) are unchanged by
    # the f2 → f3 rename, so it is never expanded and the rename is invisible.
    # The materialized diff sees every row. (disk-tree adds mtime to the
    # descend trigger to catch this; a path index without mtime cannot.)
    assert _key(view) == sorted(changed_spines + hidden_rename)
    assert not any(r.path == 'r/a' for r in res.rows)
    # '', r, r/b, r/b/sub, r/b/sub/big.bin (a leaf: found by its empty listing), r/z
    assert res.expansions == 6
    assert not res.truncated
    assert stats.listings == 12 and stats.expansions == 6


def test_render_floor_stops_expansion_and_marks_pruned(tmp_path: Path):
    a = _write(_tree_a(), tmp_path / 'a.parquet')
    b = _write(_tree_b(), tmp_path / 'b.parquet')
    # Floor of 100 bytes: r/z (0 → 10, |Δ| 10) is below it → pruned, never expanded.
    res = walk_diff(SnapshotReader(a), SnapshotReader(b), '', floor=100)
    by = {r.path: r for r in res.rows}
    assert by['r/z'].pruned and not by['r/z'].expanded
    assert 'r/z/new' not in by
    assert by['r/b'].expanded and by['r/b/sub'].expanded
    assert res.expansions == 5                          # '', r, r/b, r/b/sub, big.bin
    assert render_floor(1_000_000, 1000, 100, 10.0) == 1000       # 1e6 × 100 / 1e5


def test_rg_cache_and_footer_cache_are_counted(tmp_path: Path):
    a = _write(_tree_a(), tmp_path / 'a.parquet')
    b = _write(_tree_b(), tmp_path / 'b.parquet')
    footer_cache: dict = {}
    s1 = Stats()
    walk_diff(SnapshotReader(a, stats=s1, footer_cache=footer_cache), SnapshotReader(b, stats=s1, footer_cache=footer_cache), '')
    s2 = Stats()
    walk_diff(SnapshotReader(a, stats=s2, footer_cache=footer_cache), SnapshotReader(b, stats=s2, footer_cache=footer_cache), '')
    assert (s1.footer_parses, s2.footer_parses) == (2, 0)
    assert s2.requests == s1.requests and s2.rg_cache_hits == s1.rg_cache_hits
    assert s1.rg_cache_hits > 0                                    # sibling listings share 4-row groups
    s3 = Stats()
    walk_diff(SnapshotReader(a, stats=s3, rg_cache=False), SnapshotReader(b, stats=s3, rg_cache=False), '')
    assert s3.rg_cache_hits == 0 and s3.requests == s1.requests + s1.rg_cache_hits
    assert s3.bytes > s1.bytes
    assert 0 < s1.gets <= s1.requests
    assert sum(s1.rounds) == s1.gets
    trips = sum(-(-n // 8) for n in s1.rounds if n)
    assert s1.wall_model(30, 8) == s1.cpu_ms + trips * 30


def test_filter_listing_matches_bisect(tmp_path: Path):
    a = _write(_tree_a(), tmp_path / 'a.parquet')
    b = _write(_tree_b(), tmp_path / 'b.parquet')
    r1 = walk_diff(SnapshotReader(a, listing='bisect'), SnapshotReader(b, listing='bisect'), '')
    r2 = walk_diff(SnapshotReader(a, listing='filter'), SnapshotReader(b, listing='filter'), '')
    assert [(r.path, r.status, r.size_a, r.size_b, r.pruned) for r in r1.rows] == \
           [(r.path, r.status, r.size_a, r.size_b, r.pruned) for r in r2.rows]


def test_view_slice_collapses_added_and_removed_subtrees(tmp_path: Path):
    """An added (or removed) dir is one row in the view — its descendants are
    implied — matching what the walk emits (it never descends into them)."""
    a = _write(_tree_a() + [('r/q', 2, 50, 2), ('r/q/x', 3, 50, 1)], tmp_path / 'a.parquet')
    b = _write(_tree_b() + [('r/n', 2, 70, 3), ('r/n/y', 3, 40, 1), ('r/n/z', 3, 30, 1)], tmp_path / 'b.parquet')
    diff, _ = materialize_diff(a, b)
    view, _ = slice_view(diff, '', floor=0)
    paths = view['path'].to_pylist()
    assert 'r/q' in paths and 'r/q/x' not in paths
    assert 'r/n' in paths and 'r/n/y' not in paths and 'r/n/z' not in paths
    walk = walk_diff(SnapshotReader(a), SnapshotReader(b), '')
    assert {r.path for r in walk.rows if r.path.startswith('r/q') or r.path.startswith('r/n')} == {'r/q', 'r/n'}


def test_level_order_rounds_equal_expanded_depth_and_match_bestfirst(tmp_path: Path):
    a = _write(_tree_a(), tmp_path / 'a.parquet')
    b = _write(_tree_b(), tmp_path / 'b.parquet')
    s_level = Stats()
    r_level = walk_diff(SnapshotReader(a, stats=s_level), SnapshotReader(b, stats=s_level), '', order='level')
    s_best = Stats()
    r_best = walk_diff(SnapshotReader(a, stats=s_best), SnapshotReader(b, stats=s_best), '', order='bestfirst')
    key = lambda res: sorted((r.path, r.status, r.size_a, r.size_b, r.pruned) for r in res.rows)
    assert key(r_level) == key(r_best)
    # Expanded levels: '' (0), r (1), r/b + r/z (2), r/b/sub (3), big.bin (4) → 5 rounds, one per depth.
    assert len(s_level.rounds) == 5
    assert s_level.round_trips <= 5
    assert sum(s_level.rounds) == s_level.gets
    # Best-first records one round per expansion: 6 sequential rounds.
    assert len(s_best.rounds) == 6
    assert s_level.round_trips <= s_best.round_trips


def test_node_lookup_and_missing_root(tmp_path: Path):
    b = SnapshotReader(_write(_tree_b(), tmp_path / 'b.parquet'))
    assert b.node('r/b/sub') == (1600, 1)
    assert b.node('nope') is None
    assert b.children('r', 1) == {'r/a': (400, 2), 'r/b': (1600, 2), 'r/z': (10, 1)}

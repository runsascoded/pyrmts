"""Flat-changeset diff-index (`diffindex.py`): a diff between *any* two scans,
composed from disjoint aligned dyadic blocks, must equal the ground-truth
2-snapshot diff for every pair — while storage stays bounded by the events log
(level 0) and reads by the aligned-block count."""
from __future__ import annotations

import pyarrow as pa
import pytest

from pyrmts import (
    Dim,
    MemStorage,
    Metric,
    Pyramid,
    SparseDiffIndex,
    Tier,
    aligned_blocks,
    changeset_between,
    changeset_to_table,
    compose_changesets,
    diff_tables,
)


def _pyr() -> Pyramid:
    return Pyramid(
        storage=MemStorage(),
        keyTemplate='p/{tier}/{shard}/{period}.parquet',
        binCol='dt',
        dims=[Dim(name='path', type='string')],
        metrics=[Metric(name='b', monoid='count'), Metric(name='o', monoid='count')],
        tiers=[Tier(name='base', bin='1d', shards=('1mo',))],
    )


def _shard(rows: list[tuple]) -> pa.Table:
    cols: dict[str, list] = {'dt': [], 'path': [], 'b': [], 'o': []}
    for dt, path, b, o in rows:
        cols['dt'].append(dt); cols['path'].append(path); cols['b'].append(b); cols['o'].append(o)
    return pa.table(cols)


def _chg(t: pa.Table) -> list[tuple]:
    d = t.to_pydict()
    return sorted(zip(d['dt'], d['path'], d['b__a'], d['o__a'], d['b__b'], d['o__b']))


# A churny synthetic history: 11 scans, paths appearing/vanishing/moving,
# including remove-then-re-add (which nets to zero over some spans — the
# non-invertibility the disjoint composition must respect).
def _history() -> list[pa.Table]:
    scans = []
    for k in range(11):
        rows = [(0, 'const', 100, 1)]                       # never changes
        rows.append((0, 'ramp', k * 10, k))                 # changes every scan
        if k % 2 == 0:
            rows.append((0, 'blink', 5, 1))                 # present even scans only (churn)
        if k >= 4:
            rows.append((0, 'late', 7, 2))                  # born at scan 4
        if k < 7:
            rows.append((0, 'early', 3, 1))                 # dies after scan 6
        scans.append(_shard(rows))
    return scans


def _deltas(scans, pyr) -> list:
    return [changeset_between(scans[k], scans[k + 1], pyr) for k in range(len(scans) - 1)]


@pytest.mark.parametrize('levels', [0, 1, 3])
def test_any_pair_diff_matches_two_snapshot_oracle(levels: int):
    """For every (i ≤ j) and every hierarchy cap, the composed diff equals the
    direct 2-snapshot diff — the correctness contract for arbitrary-pair diffs."""
    pyr = _pyr()
    scans = _history()
    index = SparseDiffIndex(_deltas(scans, pyr), levels=levels)
    for i in range(len(scans)):
        for j in range(i, len(scans)):
            got = changeset_to_table(index.diff(i, j), pyr)
            oracle = diff_tables(scans[i], scans[j], pyr)
            assert _chg(got) == _chg(oracle), f"diff({i},{j}) levels={levels}"


def test_aligned_blocks_are_aligned_disjoint_and_capped():
    """Blocks tile `(i, j]` exactly, each start is a multiple of its width, no
    block exceeds the cap, and the count is ≤ 2·log2(j−i)+1 (or j−i at cap 0)."""
    assert aligned_blocks(0, 8, 3) == [(3, 0)]                       # one aligned 2^3 block
    assert aligned_blocks(0, 7, 3) == [(2, 0), (1, 4), (0, 6)]       # 4 + 2 + 1, aligned
    assert aligned_blocks(3, 10, 3) == [(0, 3), (2, 4), (1, 8)]      # 1 + 4 + 2: the odd start first
    assert aligned_blocks(0, 8, 1) == [(1, 0), (1, 2), (1, 4), (1, 6)]  # capped at 2^1
    assert aligned_blocks(3, 10, 0) == [(0, k) for k in range(3, 10)]  # events log only
    assert aligned_blocks(4, 4, 3) == []
    for levels in (0, 1, 2, 3, 4):
        for i in range(0, 20):
            for j in range(i, 20):
                blocks = aligned_blocks(i, j, levels)
                pos = i
                for level, start in blocks:
                    assert start == pos and start % (1 << level) == 0 and level <= levels
                    pos += 1 << level
                assert pos == j
                bound = j - i if levels == 0 else 2 * max(1, (j - i).bit_length()) + 1
                assert len(blocks) <= bound, (i, j, levels, blocks)


def test_storage_is_bounded_by_the_events_log():
    """The point of the aligned layout: every level's stored entries ≤ level 0's
    (a sliding-window layout would grow O(N) per level), and level L holds only
    aligned starts — at most N/2^L nodes."""
    pyr = _pyr()
    scans = _history()
    index = SparseDiffIndex(_deltas(scans, pyr), levels=3)
    entries = index.entries()
    assert len(entries) == 4
    assert all(e <= entries[0] for e in entries[1:]), entries
    n = len(scans) - 1                                              # deltas
    assert [sorted(level) for level in index.table] == [
        list(range(n)),                                             # L0: every adjacency
        [0, 2, 4, 6, 8],                                            # L1: aligned pairs
        [0, 4],                                                     # L2: aligned quads
        [0],                                                        # L3: one aligned octet
    ]


def test_levels_zero_is_the_events_log():
    pyr = _pyr()
    scans = _history()
    index = SparseDiffIndex(_deltas(scans, pyr))
    assert index.levels == 0
    assert index.table[1:] == []
    assert index.blocks_composed(0, 10) == 10
    assert index.blocks_composed(4, 4) == 0
    assert index.diff(4, 4) == {}


def test_diff_touches_few_blocks_with_a_hierarchy():
    pyr = _pyr()
    index = SparseDiffIndex(_deltas(_history(), pyr), levels=3)
    assert index.blocks_composed(0, 8) == 1        # a single aligned 2^3 block
    assert index.blocks_composed(0, 7) == 3        # 4 + 2 + 1
    assert index.blocks_composed(3, 10) == 3       # 1 + 4 + 2


def test_remove_then_readd_nets_out_over_a_span():
    """'blink' is present on even scans, absent on odd — over an even→even span it
    churns but nets to no change, which the disjoint (non-idempotent) composition
    must get right (a naive idempotent merge would double-count)."""
    pyr = _pyr()
    scans = _history()
    index = SparseDiffIndex(_deltas(scans, pyr), levels=3)
    # scans 0 and 2: blink present in both with the same state → not in the diff.
    d02 = index.diff(0, 2)
    assert not any(key[1] == 'blink' for key in d02)
    # scan 0 (present) → scan 1 (absent): blink IS a death.
    assert any(key[1] == 'blink' for key in index.diff(0, 1))


def test_compose_is_associative():
    pyr = _pyr()
    scans = _history()
    d = _deltas(scans, pyr)[:4]
    left = compose_changesets(compose_changesets(d[0], d[1]), compose_changesets(d[2], d[3]))
    right = compose_changesets(d[0], compose_changesets(d[1], compose_changesets(d[2], d[3])))
    assert left == right == changeset_between(scans[0], scans[4], pyr)


def test_append_is_incremental_and_append_only():
    """Appending scans one at a time yields the same index as a from-scratch
    build, creates the level-0 node plus one aligned node per level dividing
    the delta count, and never touches existing nodes — so a persisted store
    is append-only / immutable per node."""
    pyr = _pyr()
    scans = _history()
    deltas = _deltas(scans, pyr)
    full = SparseDiffIndex(deltas, levels=3)
    inc = SparseDiffIndex(levels=3)
    for k, d in enumerate(deltas):
        before = [dict(level) for level in inc.table]        # snapshot existing nodes
        new = inc.append(d)
        m = k + 1                                            # deltas so far
        assert [(lvl, i) for lvl, i, _ in new] == [(0, m - 1)] + [
            (lvl, m - (1 << lvl)) for lvl in range(1, 4) if m % (1 << lvl) == 0
        ]
        for lvl, nodes in enumerate(before):                 # existing nodes untouched
            assert {i: inc.table[lvl][i] for i in nodes} == nodes
    assert inc.table == full.table
    assert inc.n == full.n == len(scans)
    # Amortized ~2 node writes per scan: 10 deltas → 10 + 5 + 2 + 1 = 18 nodes.
    assert sum(len(level) for level in inc.table) == 18


def test_diff_rejects_out_of_range_and_bad_levels():
    index = SparseDiffIndex(_deltas(_history(), _pyr()))
    with pytest.raises(ValueError, match='out of range'):
        index.diff(0, 99)
    with pytest.raises(ValueError, match='levels must be'):
        SparseDiffIndex(levels=-1)

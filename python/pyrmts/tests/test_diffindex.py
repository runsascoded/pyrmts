"""Dyadic changeset-hierarchy (`diffindex.py`): a diff between *any* two scans,
composed from O(log) disjoint power-of-2 blocks, must equal the ground-truth
2-snapshot diff for every pair — while touching only popcount(j−i) blocks."""
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


def _build_index(scans, pyr) -> SparseDiffIndex:
    deltas = [changeset_between(scans[k], scans[k + 1], pyr) for k in range(len(scans) - 1)]
    return SparseDiffIndex(deltas)


def test_any_pair_diff_matches_two_snapshot_oracle():
    """For every (i ≤ j), the hierarchy's composed diff equals the direct
    2-snapshot diff — the correctness contract for arbitrary-pair diffs."""
    pyr = _pyr()
    scans = _history()
    index = _build_index(scans, pyr)
    for i in range(len(scans)):
        for j in range(i, len(scans)):
            got = changeset_to_table(index.diff(i, j), pyr)
            oracle = diff_tables(scans[i], scans[j], pyr)
            assert _chg(got) == _chg(oracle), f"diff({i},{j})"


def test_diff_touches_only_log_many_blocks():
    """The whole point: an arbitrary-pair diff composes popcount(j−i) = O(log)
    disjoint dyadic blocks, not O(j−i)."""
    pyr = _pyr()
    index = _build_index(_history(), pyr)
    assert index.blocks_composed(0, 8) == 1        # a single 2^3 block
    assert index.blocks_composed(0, 7) == 3        # 7 = 4+2+1
    assert index.blocks_composed(3, 10) == 3       # span 7 = 4+2+1
    assert index.diff(4, 4) == {}                  # empty diff


def test_remove_then_readd_nets_out_over_a_span():
    """'blink' is present on even scans, absent on odd — over an even→even span it
    churns but nets to no change, which the disjoint (non-idempotent) composition
    must get right (a naive idempotent merge would double-count)."""
    pyr = _pyr()
    scans = _history()
    index = _build_index(scans, pyr)
    # scans 0 and 2: blink present in both with the same state → not in the diff.
    d02 = index.diff(0, 2)
    assert not any(key[1] == 'blink' for key in d02)
    # scan 0 (present) → scan 1 (absent): blink IS a death.
    assert any(key[1] == 'blink' for key in index.diff(0, 1))


def test_compose_is_associative():
    pyr = _pyr()
    scans = _history()
    d = [changeset_between(scans[k], scans[k + 1], pyr) for k in range(4)]
    left = compose_changesets(compose_changesets(d[0], d[1]), compose_changesets(d[2], d[3]))
    right = compose_changesets(d[0], compose_changesets(d[1], compose_changesets(d[2], d[3])))
    assert left == right == changeset_between(scans[0], scans[4], pyr)


def test_append_is_incremental_and_append_only():
    """Appending scans one at a time yields the same index as a from-scratch
    build, creates exactly one new node per level, and never touches existing
    nodes — so a persisted store is append-only / immutable per node."""
    pyr = _pyr()
    scans = _history()
    deltas = [changeset_between(scans[k], scans[k + 1], pyr) for k in range(len(scans) - 1)]
    full = SparseDiffIndex(deltas)
    inc = SparseDiffIndex()
    for k, d in enumerate(deltas):
        before = [list(level) for level in inc.table]        # snapshot existing nodes
        new = inc.append(d)
        m = k + 1                                            # deltas so far
        # One new node per level L with 2**L ≤ m, at index m − 2**L.
        assert [(lvl, i) for lvl, i, _ in new] == [(lvl, m - (1 << lvl)) for lvl in range(m.bit_length()) if (1 << lvl) <= m]
        for lvl, nodes in enumerate(before):                 # existing nodes untouched
            assert inc.table[lvl][:len(nodes)] == nodes
    assert inc.table == full.table
    assert inc.n == full.n == len(scans)
    for i in range(len(scans)):
        for j in range(i, len(scans)):
            assert inc.diff(i, j) == full.diff(i, j)


def test_diff_rejects_out_of_range():
    index = _build_index(_history(), _pyr())
    with pytest.raises(ValueError, match='out of range'):
        index.diff(0, 99)

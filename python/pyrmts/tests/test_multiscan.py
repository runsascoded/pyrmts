"""Multi-scan consolidation (`specs/multi-scan-consolidation.md`).

Both encoders fold the scan axis into one tile and round-trip losslessly.
Assertions parse the output back into sorted tuples and compare by exact
equality; the round-trip is checked per member scan against the originals and
via the writer-independent `scan_digest`."""
from __future__ import annotations

import io

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyrmts import (
    Dim,
    MemStorage,
    Metric,
    Pyramid,
    Tier,
    consolidate_tables,
    diff_scans,
    diff_tables,
    extract_table,
    scan_digest,
    series_for,
)


def _count_pyramid() -> Pyramid:
    """(b, o) additive tile keyed `(dt, path)` — the origin's age shard shape."""
    return Pyramid(
        storage=MemStorage(),
        keyTemplate='p/{tier}/{shard}/{period}.parquet',
        binCol='dt',
        dims=[Dim(name='path', type='string')],
        metrics=[Metric(name='b', monoid='count'), Metric(name='o', monoid='count')],
        tiers=[Tier(name='base', bin='1d', shards=('1mo',))],
    )


def _shard(rows: list[tuple]) -> pa.Table:
    """rows: (dt, path, b, o)."""
    cols: dict[str, list] = {'dt': [], 'path': [], 'b': [], 'o': []}
    for dt, path, b, o in rows:
        cols['dt'].append(dt)
        cols['path'].append(path)
        cols['b'].append(b)
        cols['o'].append(o)
    return pa.table(cols)


def _rows(t: pa.Table) -> list[tuple]:
    d = t.to_pydict()
    return sorted(zip(d['dt'], d['path'], d['b'], d['o']))


def _ivl(t: pa.Table) -> list[tuple]:
    """Rows as (dt, path, b, o, lo, hi), sorted key-major then by run start."""
    d = t.to_pydict()
    rows = list(zip(d['dt'], d['path'], d['b'], d['o'], d['__scan_lo'], d['__scan_hi']))
    return sorted(rows, key=lambda r: (r[1], r[0], r[4]))


def _dns(t: pa.Table) -> list[tuple]:
    """Rows as (dt, path, b, o, scan), sorted key-major then scan-innermost."""
    d = t.to_pydict()
    rows = list(zip(d['dt'], d['path'], d['b'], d['o'], d['__scan']))
    return sorted(rows, key=lambda r: (r[1], r[0], r[4]))


def _pq_bytes(t: pa.Table) -> int:
    buf = io.BytesIO()
    pq.write_table(t, buf, compression='snappy')
    return len(buf.getvalue())


# Three scans: `a` constant, `b` changes then vanishes, `c` appears late.
SCANS = [
    ('s0', _shard([(0, 'a', 10, 1), (0, 'b', 20, 2)])),
    ('s1', _shard([(0, 'a', 10, 1), (0, 'b', 30, 3)])),
    ('s2', _shard([(0, 'a', 10, 1), (0, 'c', 5, 1)])),
]


def test_interval_rows_are_change_runs():
    pyr = _count_pyramid()
    ms = consolidate_tables(SCANS, pyr, encoder='interval')
    assert ms.scans == ['s0', 's1', 's2']
    # `a` constant across all → one row spanning [0,2]; `b` splits at its
    # change then ends before s2; `c` only s2. O(#changes), not O(#scans).
    assert _ivl(ms.table) == [
        (0, 'a', 10, 1, 0, 2),
        (0, 'b', 20, 2, 0, 0),
        (0, 'b', 30, 3, 1, 1),
        (0, 'c', 5, 1, 2, 2),
    ]


def test_densify_grid_fills_absence_with_identity():
    pyr = _count_pyramid()
    ms = consolidate_tables(SCANS, pyr, encoder='densify')
    # Full 3 keys × 3 scans grid; absent (b@s2, c@s0, c@s1) filled with the
    # count identity 0 so each key's states form a contiguous run.
    assert _dns(ms.table) == [
        (0, 'a', 10, 1, 0),
        (0, 'a', 10, 1, 1),
        (0, 'a', 10, 1, 2),
        (0, 'b', 20, 2, 0),
        (0, 'b', 30, 3, 1),
        (0, 'b', 0, 0, 2),
        (0, 'c', 0, 0, 0),
        (0, 'c', 0, 0, 1),
        (0, 'c', 5, 1, 2),
    ]


@pytest.mark.parametrize('encoder', ['interval', 'densify'])
def test_extract_round_trips_every_scan(encoder: str):
    pyr = _count_pyramid()
    ms = consolidate_tables(SCANS, pyr, encoder=encoder)
    for label, original in SCANS:
        assert _rows(extract_table(ms, label, pyr)) == _rows(original)


@pytest.mark.parametrize('encoder', ['interval', 'densify'])
def test_digest_verifies_round_trip(encoder: str):
    pyr = _count_pyramid()
    ms = consolidate_tables(SCANS, pyr, encoder=encoder)
    for label, original in SCANS:
        assert scan_digest(extract_table(ms, label, pyr), pyr) == scan_digest(original, pyr)


def test_encoders_extract_identical_scans():
    pyr = _count_pyramid()
    ivl = consolidate_tables(SCANS, pyr, encoder='interval')
    dns = consolidate_tables(SCANS, pyr, encoder='densify')
    for label, _ in SCANS:
        assert _rows(extract_table(ivl, label, pyr)) == _rows(extract_table(dns, label, pyr))


def test_consolidate_is_deterministic():
    pyr = _count_pyramid()
    a = consolidate_tables(SCANS, pyr, encoder='interval').table
    b = consolidate_tables(SCANS, pyr, encoder='interval').table
    assert _pq_bytes(a) == _pq_bytes(b)
    assert a.equals(b)


def test_histogram_states_coalesce_on_equality():
    pyr = Pyramid(
        storage=MemStorage(),
        keyTemplate='p/{tier}/{shard}/{period}.parquet',
        binCol='dt',
        dims=[Dim(name='path', type='string')],
        metrics=[Metric(name='h', monoid='histogram')],
        tiers=[Tier(name='base', bin='1d', shards=('1mo',))],
    )
    scans = [
        ('s0', pa.table({'dt': [0], 'path': ['a'], 'h': ['{"x":1}']})),
        ('s1', pa.table({'dt': [0], 'path': ['a'], 'h': ['{"x":1}']})),
        ('s2', pa.table({'dt': [0], 'path': ['a'], 'h': ['{"x":2}']})),
    ]
    ms = consolidate_tables(scans, pyr, encoder='interval')
    d = ms.table.to_pydict()
    # Equal JSON states s0/s1 coalesce; the change at s2 splits.
    assert sorted(zip(d['dt'], d['path'], d['h'], d['__scan_lo'], d['__scan_hi'])) == [
        (0, 'a', '{"x":1}', 0, 1),
        (0, 'a', '{"x":2}', 2, 2),
    ]
    for label, original in scans:
        assert extract_table(ms, label, pyr).to_pydict() == original.to_pydict()


def test_low_churn_interval_is_o_changes_and_beats_baseline():
    """The O(N) claim: on low-churn data (many keys constant over many scans),
    interval stores O(#changes) rows (one per constant key) vs the densified
    grid's O(#keys × #scans), and beats the O(#scans) per-scan baseline in
    bytes. (The interval-vs-densify *byte* crossover is regime-dependent —
    parquet RLE crushes a constant grid — so that is left to the benchmark.)"""
    pyr = _count_pyramid()
    base = _shard([(0, f'p{i}', i * 100, i) for i in range(50)])
    scans = [(f's{j}', base) for j in range(10)]  # 50 keys, all constant, 10 scans
    ivl = consolidate_tables(scans, pyr, encoder='interval')
    dns = consolidate_tables(scans, pyr, encoder='densify')
    baseline = sum(_pq_bytes(t) for _, t in scans)
    assert ivl.table.num_rows == 50            # one row per key, no churn
    assert dns.table.num_rows == 500           # full 50 × 10 grid
    assert _pq_bytes(ivl.table) < baseline     # O(1 scan + deltas) < O(N scans)


def test_consolidate_rejects_bad_input():
    pyr = _count_pyramid()
    with pytest.raises(ValueError, match='unknown encoder'):
        consolidate_tables(SCANS, pyr, encoder='rle')
    with pytest.raises(ValueError, match='at least one scan'):
        consolidate_tables([], pyr, encoder='interval')
    with pytest.raises(ValueError, match='duplicate scan labels'):
        consolidate_tables([SCANS[0], SCANS[0]], pyr, encoder='interval')


def test_extract_rejects_non_member_scan():
    pyr = _count_pyramid()
    ms = consolidate_tables(SCANS, pyr, encoder='interval')
    with pytest.raises(ValueError, match='not a member scan'):
        extract_table(ms, 's9', pyr)


def _chg(t: pa.Table) -> list[tuple]:
    d = t.to_pydict()
    return sorted(zip(d['dt'], d['path'], d['b__a'], d['o__a'], d['b__b'], d['o__b']))


def test_diff_tables_changeset_with_births_and_deaths():
    pyr = _count_pyramid()
    s0, s1, s2 = (t for _, t in SCANS)
    # b's value moved 20→30; a unchanged (absent from the changeset).
    assert _chg(diff_tables(s0, s1, pyr)) == [(0, 'b', 20, 2, 30, 3)]
    # b died (→ identity 0), c was born (identity 0 →); a still unchanged.
    assert _chg(diff_tables(s0, s2, pyr)) == [
        (0, 'b', 20, 2, 0, 0),   # death
        (0, 'c', 0, 0, 5, 1),    # birth
    ]


@pytest.mark.parametrize('encoder', ['interval', 'densify'])
def test_diff_scans_matches_extract_diff(encoder: str):
    pyr = _count_pyramid()
    ms = consolidate_tables(SCANS, pyr, encoder=encoder)
    for a in ms.scans:
        for b in ms.scans:
            expected = diff_tables(extract_table(ms, a, pyr), extract_table(ms, b, pyr), pyr)
            assert _chg(diff_scans(ms, a, b, pyr)) == _chg(expected)


def test_diff_scans_reads_only_changed_keys_in_span():
    """One key changes at s3; a window that straddles the change reports just
    that key, and a window ending before it reports nothing."""
    pyr = _count_pyramid()
    scans = []
    for j in range(5):
        rows = [(0, f'p{i}', i * 10, i) for i in range(100)]
        if j >= 3:
            rows[0] = (0, 'p0', 999, 9)  # p0 moves at s3
        scans.append((f's{j}', _shard(rows)))
    ms = consolidate_tables(scans, pyr, encoder='interval')
    assert _chg(diff_scans(ms, 's0', 's4', pyr)) == [(0, 'p0', 0, 0, 999, 9)]
    assert _chg(diff_scans(ms, 's0', 's2', pyr)) == []   # change is after s2
    assert _chg(diff_scans(ms, 's2', 's3', pyr)) == [(0, 'p0', 0, 0, 999, 9)]


@pytest.mark.parametrize('encoder', ['interval', 'densify'])
def test_series_for_is_the_over_time_line(encoder: str):
    pyr = _count_pyramid()
    ms = consolidate_tables(SCANS, pyr, encoder=encoder)
    # `a` constant; `b` present then absent (→ identity 0); `c` absent then born.
    assert series_for(ms, (0, 'a'), pyr) == [('s0', (10, 1)), ('s1', (10, 1)), ('s2', (10, 1))]
    assert series_for(ms, (0, 'b'), pyr) == [('s0', (20, 2)), ('s1', (30, 3)), ('s2', (0, 0))]
    assert series_for(ms, (0, 'c'), pyr) == [('s0', (0, 0)), ('s1', (0, 0)), ('s2', (5, 1))]

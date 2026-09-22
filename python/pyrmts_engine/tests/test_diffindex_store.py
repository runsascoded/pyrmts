"""Persistent dyadic diff-index (`DiffIndexStore`): append-only ingest + an
O(log)-node serve path. Every any-pair diff must equal the 2-snapshot oracle,
incremental ingest must equal a one-shot build, and a query must read only the
popcount(j−i) jump nodes — never a snapshot."""
from __future__ import annotations

import pyarrow as pa
import pytest

from pyrmts import Dim, MemStorage, Metric, Pyramid, Tier, diff_tables
from pyrmts_engine.diffindex_store import DiffIndexStore


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


def _history() -> list[tuple[str, pa.Table]]:
    out = []
    for k in range(11):
        rows = [(0, 'const', 100, 1), (0, 'ramp', k * 10, k)]
        if k % 2 == 0:
            rows.append((0, 'blink', 5, 1))
        if k >= 4:
            rows.append((0, 'late', 7, 2))
        if k < 7:
            rows.append((0, 'early', 3, 1))
        out.append((f's{k}', _shard(rows)))
    return out


class _CountingStorage(MemStorage):
    """MemStorage that counts `get`s per key prefix, to prove the serve path
    reads only jump nodes."""
    def __init__(self) -> None:
        super().__init__()
        self.gets: list[str] = []

    def get(self, key: str):
        self.gets.append(key)
        return super().get(key)


def test_store_any_pair_diff_matches_oracle_both_directions():
    pyr = _pyr()
    scans = _history()
    store = DiffIndexStore(MemStorage(), 'diffidx/dt', pyr, 'dt')
    assert store.update(scans) == [label for label, _ in scans]
    for i, (a, ta) in enumerate(scans):
        for j, (b, tb) in enumerate(scans):
            assert _chg(store.diff_table(a, b)) == _chg(diff_tables(ta, tb, pyr)), f"{a}->{b}"


def test_store_update_is_incremental_and_idempotent():
    pyr = _pyr()
    scans = _history()
    one_shot = DiffIndexStore(MemStorage(), 'diffidx/dt', pyr, 'dt')
    one_shot.update(scans)
    inc_storage = MemStorage()
    inc = DiffIndexStore(inc_storage, 'diffidx/dt', pyr, 'dt')
    assert inc.update(scans[:5]) == ['s0', 's1', 's2', 's3', 's4']
    assert inc.update(scans) == ['s5', 's6', 's7', 's8', 's9', 's10']   # appends only the new tail
    assert inc.update(scans) == []                                        # no-op when nothing is new
    assert inc.scans() == one_shot.scans()
    # Identical node set + bytes-equal manifest — append-only ingest is canonical.
    assert sorted(inc_storage.list('diffidx/')) == sorted(one_shot.storage.list('diffidx/'))
    for key in inc_storage.list('diffidx/'):
        assert inc_storage.get(key) == one_shot.storage.get(key), key


def test_store_diff_reads_only_jump_nodes_never_snapshots():
    pyr = _pyr()
    storage = _CountingStorage()
    store = DiffIndexStore(storage, 'diffidx/dt', pyr, 'dt')
    store.update(_history())
    def node_reads(a: str, b: str) -> list[str]:
        storage.gets.clear()
        store.diff(a, b)
        return [k for k in storage.gets if '/L' in k]
    # s0→s8: one 2^3 node. s0→s7 (7 = 4+2+1): three nodes. Manifest read aside.
    assert node_reads('s0', 's8') == ['diffidx/dt/L3/0.parquet']
    assert node_reads('s0', 's7') == ['diffidx/dt/L0/0.parquet', 'diffidx/dt/L1/1.parquet', 'diffidx/dt/L2/3.parquet']
    assert all('/L' in k or k.endswith('index.json') for k in storage.gets)  # no snapshot reads


def test_store_rejects_diverged_scan_order():
    pyr = _pyr()
    scans = _history()
    store = DiffIndexStore(MemStorage(), 'diffidx/dt', pyr, 'dt')
    store.update(scans[:3])
    with pytest.raises(ValueError, match='scan order diverges'):
        store.update([scans[0], scans[2], scans[1]])
    with pytest.raises(ValueError, match="not in the index"):
        store.diff('s0', 's99')

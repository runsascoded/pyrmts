"""Scan-location manifest (`multiscan_index`) + its driver integration: the
routing overlay records one row per consolidated tile, `resolve_scan` routes a
scan to its archive (with fold index), and `drop_consolidated_scans` deletes the
individuals only after digest-verifying recovery."""
from __future__ import annotations

import io
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyrmts import (
    Dim,
    FsStorage,
    Metric,
    MultiScanPolicy,
    Pyramid,
    Tier,
    from_arrow,
    substitute_key,
    to_arrow,
)
from pyrmts_engine.multiscan_driver import (
    consolidate_groups,
    consolidate_range,
    drop_consolidated_scans,
    extract_scan,
    seal_dyadic,
    seal_new_groups,
)
from pyrmts_engine.multiscan_index import (
    MemMultiScanIndex,
    MultiScanRecord,
    StorageJsonlMultiScanIndex,
    dyadic_decompose,
    multiscan_d1_ddl,
    multiscan_d1_row,
    resolve_scan,
)

KEY_TEMPLATE = 'p/{tier}/{shard}/{period}.parquet'
RANGE = (datetime(2026, 1, 1, tzinfo=timezone.utc), datetime(2026, 2, 1, tzinfo=timezone.utc))
SCANS = {
    's0': [(0, 'a', 10, 1), (0, 'b', 20, 2)],
    's1': [(0, 'a', 10, 1), (0, 'b', 30, 3)],
    's2': [(0, 'a', 10, 1), (0, 'c', 5, 1)],
}


def _pyramid(root: Path) -> Pyramid:
    return Pyramid(
        storage=FsStorage(root),
        keyTemplate=KEY_TEMPLATE,
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


def _rows(t: pa.Table) -> list[tuple]:
    d = t.to_pydict()
    return sorted(zip(d['dt'], d['path'], d['b'], d['o']))


def _key() -> str:
    return substitute_key(KEY_TEMPLATE, {'tier': 'base', 'shard': '1mo', 'period': '2026-01'})


def _seed(tmp_path: Path) -> tuple[Path, list[tuple[str, FsStorage]]]:
    root = tmp_path / 'scans'
    for label, rows in SCANS.items():
        buf = io.BytesIO()
        pq.write_table(_shard(rows), buf)
        FsStorage(root / label).put(_key(), buf.getvalue())
    scans = [(label, FsStorage(root / label)) for label in SCANS]
    return root, scans


# ── The index itself.


def test_mem_index_scopes_by_dataset_and_resolves_folds():
    idx = MemMultiScanIndex()
    rec = MultiScanRecord(
        dataset='usage', tier='base', shard_dur='1mo',
        period_start_ms=0, period_end_ms=1, key='k', scans=['s0', 's1', 's2'],
        encoder='interval', written_at_ms=0,
    )
    idx.record_multiscan(rec)
    idx.record_multiscan(MultiScanRecord('other', 'base', '1mo', 0, 1, 'k2', ['x'], 'interval', 0))
    assert idx.list_multiscans('usage') == [rec]          # dataset-scoped
    got = resolve_scan(idx.list_multiscans('usage'), 's1')
    assert got is rec and got.fold_index('s1') == 1        # routes + fold index
    assert resolve_scan(idx.list_multiscans('usage'), 's9') is None  # not consolidated → fall back


def test_storage_jsonl_index_persists_across_instances(tmp_path: Path):
    store = FsStorage(tmp_path / 'idx')
    rec = MultiScanRecord('usage', 'base', '1mo', 0, 1, 'k', ['s0', 's1'], 'interval', 7, {'s0': 'd0', 's1': 'd1'})
    StorageJsonlMultiScanIndex(store, 'ms.jsonl').record_multiscan(rec)
    # A fresh instance loads the existing manifest (round-trips digests too).
    assert StorageJsonlMultiScanIndex(store, 'ms.jsonl').list_multiscans('usage') == [rec]


# ── Driver integration: record on consolidate, then safe drop.


def test_consolidate_records_manifest_row(tmp_path: Path):
    root, scans = _seed(tmp_path)
    pyr = _pyramid(root)
    idx = MemMultiScanIndex()
    consolidate_range(
        scans, pyr, 'base', '1mo', RANGE, FsStorage(tmp_path / 'ms'),
        ms_index=idx, dataset='usage',
    )
    (rec,) = idx.list_multiscans('usage')
    assert rec.key == _key()
    assert rec.scans == ['s0', 's1', 's2']
    assert rec.encoder == 'interval'
    assert set(rec.digests) == {'s0', 's1', 's2'}          # digests carried for verify-before-drop
    assert resolve_scan([rec], 's2').fold_index('s2') == 2


def test_consolidate_range_requires_both_index_and_dataset(tmp_path: Path):
    root, scans = _seed(tmp_path)
    pyr = _pyramid(root)
    with pytest.raises(ValueError, match='both ms_index and dataset'):
        consolidate_range(scans, pyr, 'base', '1mo', RANGE, FsStorage(tmp_path / 'ms'), dataset='usage')


def test_drop_deletes_individuals_only_after_verifying(tmp_path: Path):
    root, scans = _seed(tmp_path)
    pyr = _pyramid(root)
    out = FsStorage(tmp_path / 'ms')
    consolidate_range(scans, pyr, 'base', '1mo', RANGE, out)
    key = _key()
    # Individuals present before drop.
    assert all(storage.get(key) is not None for _, storage in scans)
    dropped = drop_consolidated_scans(scans, out, key, pyr)
    assert dropped == ['s0', 's1', 's2']
    # Individuals gone; the archive remains and still round-trips.
    assert all(storage.get(key) is None for _, storage in scans)
    ms = from_arrow(pq.read_table(io.BytesIO(out.get(key))))
    assert ms.scans == ['s0', 's1', 's2']


def test_drop_refuses_on_digest_mismatch(tmp_path: Path):
    """A corrupted archive is caught before any individual is deleted."""
    root, scans = _seed(tmp_path)
    pyr = _pyramid(root)
    out = FsStorage(tmp_path / 'ms')
    consolidate_range(scans, pyr, 'base', '1mo', RANGE, out)
    key = _key()
    ms = from_arrow(pq.read_table(io.BytesIO(out.get(key))))
    d = ms.table.to_pydict()
    bcol = list(d['b']); bcol[bcol.index(20)] = 99; d['b'] = bcol   # corrupt a value
    out.put(key, _to_bytes(to_arrow(type(ms)(pa.table(d), ms.scans, ms.encoder, ms.digests))))
    with pytest.raises(ValueError, match='digest mismatch'):
        drop_consolidated_scans(scans, out, key, pyr)
    # Nothing deleted — s0 (the first checked) survives, so the drop is atomic-ish.
    assert scans[0][1].get(key) is not None


def _to_bytes(t: pa.Table) -> bytes:
    buf = io.BytesIO(); pq.write_table(t, buf); return buf.getvalue()


# ── Capped-K grouping.


def test_consolidate_groups_seals_capped_k(tmp_path: Path):
    """`--group-size 2` over 3 scans → two sealed archives ([s0,s1], [s2]) at
    distinct keys, each with its own manifest row; routing sends a scan to its
    group; each archive extracts its members."""
    root, scans = _seed(tmp_path)
    pyr = _pyramid(root)
    out = FsStorage(tmp_path / 'ms')
    idx = MemMultiScanIndex()
    written = consolidate_groups(
        scans, pyr, 'base', '1mo', RANGE, out,
        group_size=2, ms_index=idx, dataset='usage',
    )
    assert [n for _, _, n in written] == [2, 1]              # group sizes: [s0,s1], [s2]
    recs = idx.list_multiscans('usage')
    assert [r.scans for r in recs] == [['s0', 's1'], ['s2']]
    assert len({r.key for r in recs}) == 2                   # distinct group keys, no collision
    # Routing: each scan resolves to its own group's archive.
    assert resolve_scan(recs, 's1').scans == ['s0', 's1']
    assert resolve_scan(recs, 's2').scans == ['s2']
    # Each archive extracts its members (b's 20→30 change lives inside group 0).
    g0 = resolve_scan(recs, 's0').key
    assert _rows(extract_scan(out, g0, 's1', pyr)) == _rows(_shard(SCANS['s1']))


# ── D1 schema + row shape (pyrmts owns; the consumer writes via its CF-D1 path).


def _seed_extra(root: Path, labels: list[str]) -> None:
    for label in labels:
        buf = io.BytesIO()
        pq.write_table(_shard([(0, 'a', 10, 1), (0, label, 1, 1)]), buf)
        FsStorage(root / label).put(_key(), buf.getvalue())


def test_seal_new_groups_is_incremental(tmp_path: Path):
    """Policy-driven capped-K seal: seals each complete group of K not-yet-sealed
    scans, skips sealed ones, leaves the remainder — idempotent across cron-like
    reruns."""
    root, scans = _seed(tmp_path)                        # s0, s1, s2
    pyr = _pyramid(root)
    pyr.multi_scan = MultiScanPolicy(dataset='usage', tier='base', shard='1mo', group_size=2)
    out = FsStorage(tmp_path / 'ms')
    idx = StorageJsonlMultiScanIndex(out, 'ms.jsonl')

    # Run 1: 3 scans, K=2 → seal [s0,s1]; s2 (remainder) stays individual.
    w1 = seal_new_groups(scans, pyr, RANGE, out, idx)
    assert [n for _, _, n in w1] == [2]
    assert [r.scans for r in idx.list_multiscans('usage')] == [['s0', 's1']]

    # Run 2: same 3 scans → nothing new (s2 alone < K). Idempotent no-op.
    assert seal_new_groups(scans, pyr, RANGE, out, StorageJsonlMultiScanIndex(out, 'ms.jsonl')) == []

    # Run 3: two more scans arrive → [s2,s3] fills; s4 remainder stays.
    _seed_extra(root, ['s3', 's4'])
    scans5 = [(l, FsStorage(root / l)) for l in ['s0', 's1', 's2', 's3', 's4']]
    idx3 = StorageJsonlMultiScanIndex(out, 'ms.jsonl')
    w3 = seal_new_groups(scans5, pyr, RANGE, out, idx3)
    assert [r.scans for r in idx3.list_multiscans('usage')] == [['s0', 's1'], ['s2', 's3']]
    assert resolve_scan(idx3.list_multiscans('usage'), 's3').scans == ['s2', 's3']


def test_dyadic_decompose():
    assert dyadic_decompose(1) == [(0, 1)]
    assert dyadic_decompose(2) == [(0, 2)]
    assert dyadic_decompose(3) == [(0, 2), (2, 1)]
    assert dyadic_decompose(4) == [(0, 4)]
    assert dyadic_decompose(13) == [(0, 8), (8, 4), (12, 1)]      # popcount(13)=3 blocks
    assert dyadic_decompose(9, base=3) == [(0, 9)]                # base-3: 9 = 3^2
    assert dyadic_decompose(4, base=3) == [(0, 3), (3, 1)]


def test_seal_exponential_coalesces_logarithmically(tmp_path: Path):
    """The logarithmic method: as scans accumulate, old ones merge into
    base-power blocks so the archive count stays O(log N) (= popcount). Merges
    read from the current (smaller) archives after individuals are dropped, and
    every scan still extracts losslessly."""
    root = tmp_path / 'scans'
    payload = {}
    for i in range(4):
        rows = [(0, 'a', 10, 1), (0, f'k{i}', i + 1, 1)]         # each scan distinct
        payload[f's{i}'] = rows
        buf = io.BytesIO(); pq.write_table(_shard(rows), buf)
        FsStorage(root / f's{i}').put(_key(), buf.getvalue())
    pyr = _pyramid(root)
    pyr.multi_scan = MultiScanPolicy('dt', 'base', '1mo', scheme='exponential', base=2, drop=True)
    out = FsStorage(tmp_path / 'ms')

    def seal_n(n: int):
        scans = [(f's{i}', FsStorage(root / f's{i}')) for i in range(n)]
        idx = StorageJsonlMultiScanIndex(out, 'ms.jsonl')
        seal_dyadic(scans, pyr, RANGE, out, idx)
        return idx.list_multiscans('dt')

    for n in (1, 2, 3, 4):                                        # a cron firing each cycle
        recs = seal_n(n)
        assert len(recs) == len(dyadic_decompose(n))             # archive count = popcount(n)
        for i in range(n):                                       # every sealed scan routes + extracts
            rec = resolve_scan(recs, f's{i}')
            assert rec is not None
            assert _rows(extract_scan(out, rec.key, f's{i}', pyr)) == _rows(_shard(payload[f's{i}']))
    # N=4 fully coalesced into a single size-4 block; re-running is a no-op.
    assert [r.scans for r in seal_n(4)] == [['s0', 's1', 's2', 's3']]
    assert seal_dyadic(
        [(f's{i}', FsStorage(root / f's{i}')) for i in range(4)],
        pyr, RANGE, out, StorageJsonlMultiScanIndex(out, 'ms.jsonl'),
    ) == []                                                       # idempotent: nothing rebuilt


def test_multiscan_d1_ddl_and_row_shape():
    ddl = multiscan_d1_ddl()
    assert ddl.startswith('CREATE TABLE IF NOT EXISTS "pyramid_multiscans"')
    assert 'PRIMARY KEY (dataset, key)' in ddl               # unique per sealed group
    rec = MultiScanRecord('usage', 'base', '1mo', 0, 1, 'k', ['s0', 's1'], 'interval', 7, {'s0': 'd0'})
    assert multiscan_d1_row(rec) == {
        'dataset': 'usage', 'tier': 'base', 'shard_dur': '1mo',
        'period_start': 0, 'period_end': 1, 'key': 'k',
        'scans': '["s0", "s1"]',                             # JSON array (D1 has no array type)
        'encoder': 'interval', 'digests': '{"s0": "d0"}', 'written_at': 7,
    }
    # digests omitted → SQL NULL.
    assert multiscan_d1_row(MultiScanRecord('u', 'base', '1mo', 0, 1, 'k', ['s0'], 'interval', 7))['digests'] is None

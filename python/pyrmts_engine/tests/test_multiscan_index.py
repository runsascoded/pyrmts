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

from pyrmts import Dim, FsStorage, Metric, Pyramid, Tier, from_arrow, substitute_key, to_arrow
from pyrmts_engine.multiscan_driver import (
    consolidate_range,
    drop_consolidated_scans,
)
from pyrmts_engine.multiscan_index import (
    MemMultiScanIndex,
    MultiScanRecord,
    StorageJsonlMultiScanIndex,
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

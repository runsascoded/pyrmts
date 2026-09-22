"""DuckDB multi-scan backend (`multiscan_duckdb`): the fleet-scale path must be
**byte-identical** to `pyrmts.consolidate_scans` (the Python oracle) — same
interval table, same per-scan digests. Verified on the hand-checkable fixture,
a histogram fixture, and a synthetic-churn sweep; both the arrow and the
out-of-core `read_parquet` entries."""
from __future__ import annotations

import io
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyrmts import (
    Dim,
    MemStorage,
    Metric,
    Pyramid,
    Tier,
    consolidate_scans,
    extract_table,
)
from pyrmts_engine.cli import _synth_scans
from pyrmts_engine.multiscan_duckdb import (
    consolidate_arrow_duckdb,
    consolidate_parquet_duckdb,
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


SCANS = [
    ('s0', _shard([(0, 'a', 10, 1), (0, 'b', 20, 2)])),
    ('s1', _shard([(0, 'a', 10, 1), (0, 'b', 30, 3)])),
    ('s2', _shard([(0, 'a', 10, 1), (0, 'c', 5, 1)])),
]


def _assert_oracle_parity(scans, pyr):
    """The DuckDB arrow backend equals the Python fold exactly — table + digests."""
    oracle = consolidate_scans(iter(scans), pyr)
    got = consolidate_arrow_duckdb(scans, pyr)
    assert got.table.equals(oracle.table)          # byte-identical interval table
    assert got.scans == oracle.scans
    assert got.digests == oracle.digests
    return got


def test_duckdb_arrow_matches_oracle_on_fixture():
    pyr = _pyr()
    got = _assert_oracle_parity(SCANS, pyr)
    for label, original in SCANS:
        assert extract_table(got, label, pyr).to_pydict() == original.to_pydict()


@pytest.mark.parametrize('churn', [0.0, 0.02, 0.1])
def test_duckdb_arrow_matches_oracle_on_synthetic_churn(churn: float):
    pyr = _pyr()
    scans = _synth_scans(keys=2000, scans=8, churn=churn, births=0.01, seed=1)
    _assert_oracle_parity(scans, pyr)


def test_duckdb_matches_oracle_on_histogram_states():
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
        ('s1', pa.table({'dt': [0], 'path': ['a'], 'h': ['{"x":1}']})),  # coalesces with s0
        ('s2', pa.table({'dt': [0], 'path': ['a'], 'h': ['{"x":2}']})),  # splits
    ]
    assert consolidate_arrow_duckdb(scans, pyr).table.equals(consolidate_scans(iter(scans), pyr).table)


def test_duckdb_parquet_reads_out_of_core_and_matches_oracle(tmp_path: Path):
    pyr = _pyr()
    files = []
    for label, table in SCANS:
        path = tmp_path / f'{label}.parquet'
        pq.write_table(table, path)
        files.append((label, str(path)))
    got = consolidate_parquet_duckdb(files, pyr)
    assert got.table.equals(consolidate_scans(iter(SCANS), pyr).table)
    assert got.digests == {label: consolidate_scans(iter(SCANS), pyr).digests[label] for label, _ in SCANS}
    for label, original in SCANS:
        assert extract_table(got, label, pyr).to_pydict() == original.to_pydict()


def test_duckdb_rejects_bad_input():
    pyr = _pyr()
    with pytest.raises(ValueError, match='at least one scan'):
        consolidate_arrow_duckdb([], pyr)
    with pytest.raises(ValueError, match='duplicate scan labels'):
        consolidate_arrow_duckdb([SCANS[0], SCANS[0]], pyr)

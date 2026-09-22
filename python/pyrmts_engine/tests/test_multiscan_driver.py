"""Multi-scan storage driver (`multiscan_driver` + the `multiscan` CLI group):
consolidate a range of per-scan shards from storage into interval shards, then
extract any member scan back — digest-verified. Assertions parse rows back into
sorted tuples and compare by exact equality."""
from __future__ import annotations

import io
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from pyrmts import (
    Dim,
    FsStorage,
    Metric,
    Pyramid,
    Tier,
    from_arrow,
    shard_periods_covering,
    substitute_key,
)
from pyrmts_engine.cli import cli
from pyrmts_engine.multiscan_driver import consolidate_range, extract_scan

KEY_TEMPLATE = 'p/{tier}/{shard}/{period}.parquet'
RANGE = (datetime(2026, 1, 1, tzinfo=timezone.utc), datetime(2026, 2, 1, tzinfo=timezone.utc))
# Snapshot indices have no event-time axis → a constant `binCol` (dt=0), the
# mapping the spec prescribes for snapshot → constant-bin pyramid.
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


def _period() -> str:
    (p,) = shard_periods_covering(*RANGE, '1mo')
    return p.label


def _seed_scans(tmp_path: Path) -> Path:
    """Write each scan as a subdir `<root>/<label>/` in the keyTemplate layout."""
    root = tmp_path / 'scans'
    key = substitute_key(KEY_TEMPLATE, {'tier': 'base', 'shard': '1mo', 'period': _period()})
    for label, rows in SCANS.items():
        buf = io.BytesIO()
        pq.write_table(_shard(rows), buf)
        FsStorage(root / label).put(key, buf.getvalue())
    return root


def test_consolidate_range_writes_self_describing_shard(tmp_path: Path):
    root = _seed_scans(tmp_path)
    out = tmp_path / 'ms'
    pyr = _pyramid(root)
    scans = [(label, FsStorage(root / label)) for label in SCANS]
    written = consolidate_range(scans, pyr, 'base', '1mo', RANGE, FsStorage(out))
    key = substitute_key(KEY_TEMPLATE, {'tier': 'base', 'shard': '1mo', 'period': _period()})
    # One tile; interval rows = the 4 change-runs (a spans all; b splits then
    # dies; c born late) — O(#changes), verified against the encoder's shape.
    assert written == [(key, 4, 3)]
    ms = from_arrow(pq.read_table(io.BytesIO(FsStorage(out).get(key))))
    assert ms.encoder == 'interval'
    assert ms.scans == ['s0', 's1', 's2']
    assert set(ms.digests) == {'s0', 's1', 's2'}


def test_extract_scan_round_trips_and_verifies(tmp_path: Path):
    root = _seed_scans(tmp_path)
    out = tmp_path / 'ms'
    pyr = _pyramid(root)
    scans = [(label, FsStorage(root / label)) for label in SCANS]
    consolidate_range(scans, pyr, 'base', '1mo', RANGE, FsStorage(out))
    key = substitute_key(KEY_TEMPLATE, {'tier': 'base', 'shard': '1mo', 'period': _period()})
    for label, rows in SCANS.items():
        got = extract_scan(FsStorage(out), key, label, pyr, verify=True)
        assert _rows(got) == _rows(_shard(rows))


def test_extract_scan_detects_a_tampered_shard(tmp_path: Path):
    """If the consolidated shard is corrupted, digest verification catches it
    rather than silently returning wrong rows."""
    root = _seed_scans(tmp_path)
    out = tmp_path / 'ms'
    pyr = _pyramid(root)
    scans = [(label, FsStorage(root / label)) for label in SCANS]
    consolidate_range(scans, pyr, 'base', '1mo', RANGE, FsStorage(out))
    key = substitute_key(KEY_TEMPLATE, {'tier': 'base', 'shard': '1mo', 'period': _period()})
    ms = from_arrow(pq.read_table(io.BytesIO(FsStorage(out).get(key))))
    # Corrupt one value (b's s0 run: 20 → 99) but keep the stored digest.
    d = ms.table.to_pydict()
    bcol = list(d['b'])
    bcol[bcol.index(20)] = 99
    d['b'] = bcol
    from pyrmts import to_arrow
    tampered = to_arrow(type(ms)(pa.table(d), ms.scans, ms.encoder, ms.digests))
    FsStorage(out).put(key, _to_bytes(tampered))
    with pytest.raises(ValueError, match='digest mismatch'):
        extract_scan(FsStorage(out), key, 's0', pyr, verify=True)


def _to_bytes(t: pa.Table) -> bytes:
    buf = io.BytesIO()
    pq.write_table(t, buf)
    return buf.getvalue()


def test_consolidate_range_duckdb_engine_matches_python(tmp_path: Path):
    """The out-of-core `duckdb` engine writes the same shard as the in-memory
    `python` engine (byte-identical), through the storage driver."""
    root = _seed_scans(tmp_path)
    pyr = _pyramid(root)
    scans = [(label, FsStorage(root / label)) for label in SCANS]
    key = substitute_key(KEY_TEMPLATE, {'tier': 'base', 'shard': '1mo', 'period': _period()})

    consolidate_range(scans, pyr, 'base', '1mo', RANGE, FsStorage(tmp_path / 'py'), engine='python')
    consolidate_range(scans, pyr, 'base', '1mo', RANGE, FsStorage(tmp_path / 'dd'), engine='duckdb')
    py = from_arrow(pq.read_table(io.BytesIO(FsStorage(tmp_path / 'py').get(key))))
    dd = from_arrow(pq.read_table(io.BytesIO(FsStorage(tmp_path / 'dd').get(key))))
    assert dd.table.equals(py.table)
    assert dd.scans == py.scans
    assert dd.digests == py.digests
    for label, rows in SCANS.items():
        assert _rows(extract_scan(FsStorage(tmp_path / 'dd'), key, label, pyr)) == _rows(_shard(rows))


def test_consolidate_range_rejects_unknown_engine(tmp_path: Path):
    root = _seed_scans(tmp_path)
    pyr = _pyramid(root)
    scans = [(label, FsStorage(root / label)) for label in SCANS]
    with pytest.raises(ValueError, match='unknown engine'):
        consolidate_range(scans, pyr, 'base', '1mo', RANGE, FsStorage(tmp_path / 'x'), engine='rust')


def test_consolidate_range_errors_on_missing_tile(tmp_path: Path):
    root = _seed_scans(tmp_path)
    pyr = _pyramid(root)
    # A member scan with no shard for the tile is an error (a range is homogeneous).
    scans = [(label, FsStorage(root / label)) for label in SCANS] + [('s9', FsStorage(tmp_path / 'nope'))]
    with pytest.raises(ValueError, match="scan 's9' is missing tile"):
        consolidate_range(scans, pyr, 'base', '1mo', RANGE, FsStorage(tmp_path / 'ms'))


CONFIG_YAML = """\
storage:
  type: s3
  bucket: unused
  key: "p/{tier}/{shard}/{period}.parquet"
binCol: dt
dims:
  - name: path
    type: string
metrics:
  - name: b
    monoid: count
  - name: o
    monoid: count
tiers:
  - name: base
    bin: 1d
    shards: [1mo]
"""


def test_multiscan_cli_consolidate_then_extract(tmp_path: Path):
    """The `multiscan consolidate` → `multiscan extract` CLI round-trip: extract
    reports the row count and digest-verifies each member scan."""
    root = _seed_scans(tmp_path)
    config = tmp_path / 'pyr.yaml'
    config.write_text(CONFIG_YAML)
    out = tmp_path / 'ms'
    rng = '2026-01-01T00:00/2026-02-01T00:00'

    args = ['multiscan', 'consolidate', '-o', str(out), '-r', rng, '-R', str(root),
            '-s', 's0', '-s', 's1', '-s', 's2', '-S', '1mo', '-t', 'base', str(config)]
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 0, result.output
    key = substitute_key(KEY_TEMPLATE, {'tier': 'base', 'shard': '1mo', 'period': _period()})
    assert result.stdout.rstrip().split('\n') == [f'{key}\t4 rows\t3 scans']

    ext = tmp_path / 's1.parquet'
    result = CliRunner().invoke(cli, [
        'multiscan', 'extract', '-o', str(ext), '-p', _period(), '-R', str(out),
        '-s', 's1', '-S', '1mo', '-t', 'base', str(config),
    ])
    assert result.exit_code == 0, result.output
    assert result.stdout.strip() == 's1: 2 rows, digest verified'
    assert _rows(pq.read_table(ext)) == _rows(_shard(SCANS['s1']))

"""`pyrmts-engine diffindex update|diff`: the idempotent ingest stage + the
aligned-node query, end to end over an FsStorage scan root."""
from __future__ import annotations

import io
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from click.testing import CliRunner

from pyrmts import Dim, FsStorage, Metric, Pyramid, Tier, diff_tables, substitute_key
from pyrmts_engine.cli import cli

KEY_TEMPLATE = 'p/{tier}/{shard}/{period}.parquet'
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
SCANS = {
    's0': [(0, 'a', 10, 1), (0, 'b', 20, 2)],
    's1': [(0, 'a', 10, 1), (0, 'b', 30, 3)],
    's2': [(0, 'a', 10, 1), (0, 'c', 5, 1)],
    's3': [(0, 'a', 11, 1), (0, 'c', 5, 1)],
}


def _shard(rows: list[tuple]) -> pa.Table:
    cols: dict[str, list] = {'dt': [], 'path': [], 'b': [], 'o': []}
    for dt, path, b, o in rows:
        cols['dt'].append(dt); cols['path'].append(path); cols['b'].append(b); cols['o'].append(o)
    return pa.table(cols)


def _chg(t: pa.Table) -> list[tuple]:
    d = t.to_pydict()
    return sorted(zip(d['dt'], d['path'], d['b__a'], d['o__a'], d['b__b'], d['o__b']))


def _key() -> str:
    return substitute_key(KEY_TEMPLATE, {'tier': 'base', 'shard': '1mo', 'period': '2026-01'})


def _seed(tmp_path: Path) -> tuple[Path, Path]:
    root = tmp_path / 'scans'
    for label, rows in SCANS.items():
        buf = io.BytesIO(); pq.write_table(_shard(rows), buf)
        FsStorage(root / label).put(_key(), buf.getvalue())
    config = tmp_path / 'pyr.yaml'
    config.write_text(CONFIG_YAML)
    return root, config


def test_diffindex_cli_update_then_diff(tmp_path: Path):
    root, config = _seed(tmp_path)
    out = tmp_path / 'idx'
    common = ['-D', 'dt', '-o', str(out), str(config)]

    # First update appends every scan (labels printed in order); a rerun is a no-op.
    result = CliRunner().invoke(cli, ['diffindex', 'update', '-k', _key(), '-L', '1', '-R', str(root), *common])
    assert result.exit_code == 0, result.output
    assert result.stdout.split('\n') == ['s0', 's1', 's2', 's3', '']
    result = CliRunner().invoke(cli, ['diffindex', 'update', '-k', _key(), '-R', str(root), *common])
    assert result.exit_code == 0, result.output
    assert result.stdout == ''
    # Aligned layout: L0 has the 3 adjacency nodes; L1 only the aligned pair at 0
    # (a sliding layout would also have L1/1). The manifest records the cap.
    assert sorted(p.name for p in (out / 'diffidx/dt/L0').iterdir()) == ['0.parquet', '1.parquet', '2.parquet']
    assert sorted(p.name for p in (out / 'diffidx/dt/L1').iterdir()) == ['0.parquet']
    assert json.loads((out / 'diffidx/dt/index.json').read_text()) == {'dataset': 'dt', 'scans': ['s0', 's1', 's2', 's3'], 'levels': 1}
    # A conflicting cap on an existing index is refused.
    result = CliRunner().invoke(cli, ['diffindex', 'update', '-k', _key(), '-L', '2', '-R', str(root), *common])
    assert result.exit_code != 0
    assert 'has levels=1, not 2' in str(result.exception)

    # Any-pair diff equals the 2-snapshot oracle; the changeset is written out.
    written = tmp_path / 'd.parquet'
    result = CliRunner().invoke(cli, ['diffindex', 'diff', '-a', 's0', '-b', 's3', '-w', str(written), *common])
    assert result.exit_code == 0, result.output
    assert result.stdout.strip() == 's0 -> s3: 3 changed keys'          # a moved, b died, c born
    pyr = Pyramid(
        storage=FsStorage(out), keyTemplate=KEY_TEMPLATE, binCol='dt',
        dims=[Dim(name='path', type='string')],
        metrics=[Metric(name='b', monoid='count'), Metric(name='o', monoid='count')],
        tiers=[Tier(name='base', bin='1d', shards=('1mo',))],
    )
    assert _chg(pq.read_table(written)) == _chg(diff_tables(_shard(SCANS['s0']), _shard(SCANS['s3']), pyr))

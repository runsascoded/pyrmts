"""CLI: plan output, s3:// config parsing, `--source module:attr` hook."""
from __future__ import annotations

import re
from pathlib import Path

import polars as pl
from click.testing import CliRunner

from pyrmts import FsStorage
from pyrmts_engine import empty_long
from pyrmts_engine.cli import cli

from conftest import FROM, TO, make_pyramid, write_base_shards

CONFIG_YAML = """\
storage:
  type: s3
  bucket: unused
  key: "pyr/{tier}/{shard}/{period}.parquet"
binCol: dt
dims:
  - name: cell
    type: string
metrics:
  - name: bikes
    monoid: histogram
  - name: rides
    monoid: count
  - name: temp
    monoid: sum
tiers:
  - name: q
    bin: 15min
    shards: [6h, 1d]
  - name: h
    bin: 1h
    shards: [1d, 4d]
  - name: d
    bin: 1d
    shards: [4d]
"""

RANGE = '2026-01-02T00:00/2026-01-08T00:00'


def _setup(tmp_path: Path) -> tuple[Path, Path]:
    config = tmp_path / 'pyr.yaml'
    config.write_text(CONFIG_YAML)
    data = tmp_path / 'data'
    write_base_shards(make_pyramid(storage=FsStorage(data)))
    return config, data


def test_plan_output(tmp_path: Path):
    config, data = _setup(tmp_path)
    result = CliRunner().invoke(cli, ['plan', '-r', RANGE, '-R', str(data), str(config)])
    assert result.exit_code == 0, result.output
    assert result.output.split('\n') == [
        'q        bin=15min ← <source> shards=6',
        'h        bin=1h    ← q        shards=3',
        'd        bin=1d    ← h        shards=2',
        'total expected shards: 11',
        '',
    ]


# `--source` factory the test below points the CLI at.
def null_source_factory(pyramid, filter):
    class NullSource:
        def read_window(self, start, end):
            return empty_long(pyramid)
    return NullSource()


def test_build_with_source_factory(tmp_path: Path):
    """--source module:attr resolves and is used: a null source yields all
    11 cover shards as EMPTY (and no rung is source-skipped, since the
    factory declares no `provides`). Without -e/--allow-empty the same
    build exits 3 (zero-source-rows guard)."""
    config, data = _setup(tmp_path)
    args = [
        'build',
        '-n', 'null',
        '-r', RANGE,
        '-R', str(data),
        '-x', 'test_cli:null_source_factory',
        str(config),
    ]
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 3

    result = CliRunner().invoke(cli, [*args, '-e'])
    assert result.exit_code == 0, result.output
    normalized = re.sub(r'\([\d,]+ bytes\)', '(<bytes>)', result.output)
    normalized = re.sub(r'wall \d+\.\ds', 'wall <t>', normalized)
    assert normalized.split('\n') == [
        'build_local: 6 windows, 0 source rows → 11 shards (<bytes>), '
        '0 source-provided rungs skipped, wall <t>',
        '',
    ]
    empty = pl.read_parquet(data / 'pyr/q/1d/2026-01-04.parquet')
    assert empty.height == 0


def test_build_source_rung_flags(tmp_path: Path):
    """ctbk footgun repro: the durable base rung is the LARGEST shard dur
    (q@1d), the default (smallest, q@6h) doesn't exist. Without -t/-d the
    build is all-empty → exit 3; with them it reads q@1d and writes the 5
    remaining cover shards. (Separate data dirs: the mis-sourced run
    overwrites the seeded q@1d rung with empties — the finding-2 clobber.)"""
    config = tmp_path / 'pyr.yaml'
    config.write_text(CONFIG_YAML)

    def seeded(name: str) -> Path:
        data = tmp_path / name
        write_base_shards(make_pyramid(storage=FsStorage(data)), shard_dur='1d')
        return data

    args = ['build', '-n', 'rung', '-r', RANGE, str(config)]
    result = CliRunner().invoke(cli, [*args, '-R', str(seeded('missourced'))])
    assert result.exit_code == 3

    result = CliRunner().invoke(cli, [*args, '-R', str(seeded('data')), '-t', 'q', '-d', '1d'])
    assert result.exit_code == 0, result.output
    normalized = re.sub(r'\([\d,]+ bytes\)', '(<bytes>)', result.output)
    normalized = re.sub(r'wall \d+\.\ds', 'wall <t>', normalized)
    # 6,912 = 192 wide rows/day (96 15-min bins × 2 cells) × 6 long rows
    # each (2 hist states + 1 count + 3 sum cols) × 6 days; "6 rungs
    # skipped" counts the provided rung's 6 q@1d cover tiles.
    assert normalized.split('\n') == [
        'build_local: 6 windows, 6,912 source rows → 5 shards (<bytes>), '
        '6 source-provided rungs skipped, wall <t>',
        '',
    ]

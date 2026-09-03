"""`pyrmts-ops d1` — schema emit / verify / apply
(`specs/d1-schema-drift.md`).

No network: `verify`/`apply` reach D1 through `pyrmts.d1.d1_query`, which is
substituted here with a recorded-call fake, in the same style as
`test_aws.py`'s `FakeLam`."""
from __future__ import annotations

import json

from click.testing import CliRunner

from pyrmts import d1 as d1_mod
from pyrmts_ops.cli import cli

WATERMARKS_DDL_HEAD = 'CREATE TABLE IF NOT EXISTS "pyramid_watermarks" ('
PERIOD_INDEX_DDL = (
    'CREATE INDEX IF NOT EXISTS "pyramid_shards_period" '
    'ON "pyramid_shards" (pyramid, period_end)'
)

FULL_SCHEMA = {
    'sqlite_master': [
        {'type': 'table', 'name': 'pyramid_watermarks'},
        {'type': 'table', 'name': 'pyramid_shards'},
        {'type': 'index', 'name': 'pyramid_shards_period'},
    ],
    'pragmas': {
        'pyramid_watermarks': [
            'pyramid', 'tier', 'shard_dur', 'latest_period_end', 'updated_at',
        ],
        'pyramid_shards': [
            'pyramid', 'tier', 'shard_dur', 'period_start', 'period_end',
            'key', 'written_at',
        ],
        'pyramid_shards_period': ['pyramid', 'period_end'],
    },
}


def _install_fake_query(monkeypatch, sqlite_master, pragmas) -> list:
    """Replace `pyrmts.d1.d1_query` — which the CLI reaches via the default
    `query=None` path — and record every (sql, params)."""
    calls: list = []

    def fake(sql: str, params=None, **kwargs):
        calls.append((sql, params or []))
        if sql.startswith('SELECT type, name FROM sqlite_master'):
            return sqlite_master
        for name, cols in pragmas.items():
            if f'"{name}"' in sql:
                return [{'name': c} for c in cols]
        return []

    monkeypatch.setattr(d1_mod, 'd1_query', fake)
    return calls


def test_schema_prints_a_runnable_script_of_semicolon_terminated_statements():
    result = CliRunner().invoke(cli, ['d1', 'schema'])
    assert result.exit_code == 0
    statements = [s.strip() for s in result.output.split(';\n') if s.strip()]
    assert [s.splitlines()[0] for s in statements] == [
        WATERMARKS_DDL_HEAD,
        'CREATE TABLE IF NOT EXISTS "pyramid_shards" (',
        PERIOD_INDEX_DDL,
    ]


def test_schema_skip_inventory_emits_only_the_watermarks_table():
    result = CliRunner().invoke(cli, ['d1', 'schema', '-i'])
    assert result.exit_code == 0
    assert result.output.count('CREATE TABLE') == 1
    assert result.output.count('CREATE INDEX') == 0
    assert result.output.rstrip().endswith(') WITHOUT ROWID;')


def test_schema_custom_table_names_reach_the_emitted_ddl():
    result = CliRunner().invoke(cli, ['d1', 'schema', '-s', 't1_shards'])
    assert result.exit_code == 0
    assert result.output.rstrip().split('\n')[-1] == (
        'CREATE INDEX IF NOT EXISTS "t1_shards_period" ON "t1_shards" (pyramid, period_end);'
    )


def test_verify_clean_database_prints_up_to_date_and_exits_zero(monkeypatch):
    calls = _install_fake_query(monkeypatch, **FULL_SCHEMA)
    result = CliRunner().invoke(cli, ['d1', 'verify'])
    assert result.exit_code == 0
    assert result.output == 'schema up to date\n'
    assert [sql for sql, _ in calls] == [
        'SELECT type, name FROM sqlite_master WHERE name IN (?, ?, ?)',
        'PRAGMA table_info("pyramid_watermarks")',
        'PRAGMA table_info("pyramid_shards")',
        'PRAGMA index_info("pyramid_shards_period")',
    ]


def test_verify_exits_nonzero_when_the_period_index_is_absent(monkeypatch):
    """A deployment provisioned before the index landed — the state ctbk and
    awair were both in. This exit code is what makes the command a CI gate."""
    pragmas = {k: v for k, v in FULL_SCHEMA['pragmas'].items() if k != 'pyramid_shards_period'}
    _install_fake_query(
        monkeypatch,
        sqlite_master=[r for r in FULL_SCHEMA['sqlite_master'] if r['type'] == 'table'],
        pragmas=pragmas,
    )
    result = CliRunner().invoke(cli, ['d1', 'verify'])
    assert result.exit_code == 1
    assert result.output == 'missing: pyramid_shards_period\n'


def test_verify_json_emits_the_full_diff(monkeypatch):
    pragmas = {k: v for k, v in FULL_SCHEMA['pragmas'].items() if k != 'pyramid_shards_period'}
    _install_fake_query(
        monkeypatch,
        sqlite_master=[r for r in FULL_SCHEMA['sqlite_master'] if r['type'] == 'table'],
        pragmas=pragmas,
    )
    result = CliRunner().invoke(cli, ['d1', 'verify', '-j'])
    assert result.exit_code == 1
    assert json.loads(result.output) == {
        'ok': False,
        'missing': ['pyramid_shards_period'],
        'mismatched': [],
    }


_STATS_SCHEMA = {
    'sqlite_master': FULL_SCHEMA['sqlite_master'],
    'pragmas': {
        **FULL_SCHEMA['pragmas'],
        'pyramid_shards': FULL_SCHEMA['pragmas']['pyramid_shards'] + [
            'n_rows', 'n_rgs', 'size_bytes', 'footer_bytes',
        ],
    },
}


def test_verify_registered_extra_columns_pass(monkeypatch):
    """`-x` lets a consumer register app-owned columns (awair's stats/footer
    caches on `pyramid_shards`) so the CI gate agrees with what `/health`
    shows — no false drift."""
    _install_fake_query(monkeypatch, **_STATS_SCHEMA)
    result = CliRunner().invoke(cli, [
        'd1', 'verify', '-x', 'pyramid_shards:n_rows,n_rgs,size_bytes,footer_bytes',
    ])
    assert result.exit_code == 0
    assert result.output == 'schema up to date\n'


def test_verify_unregistered_extra_column_still_drifts(monkeypatch):
    _install_fake_query(monkeypatch, **_STATS_SCHEMA)
    # Register only three of the four live extras → the fourth is drift.
    result = CliRunner().invoke(cli, [
        'd1', 'verify', '-j', '-x', 'pyramid_shards:n_rows,n_rgs,size_bytes',
    ])
    assert result.exit_code == 1
    assert json.loads(result.output) == {
        'ok': False,
        'missing': [],
        'mismatched': [
            "pyramid_shards: expected=['key', 'n_rgs', 'n_rows', 'period_end', "
            "'period_start', 'pyramid', 'shard_dur', 'size_bytes', 'tier', "
            "'written_at'] actual=['footer_bytes', 'key', 'n_rgs', 'n_rows', "
            "'period_end', 'period_start', 'pyramid', 'shard_dur', 'size_bytes', "
            "'tier', 'written_at']",
        ],
    }


def test_verify_malformed_extra_column_spec_errors(monkeypatch):
    _install_fake_query(monkeypatch, **FULL_SCHEMA)
    result = CliRunner().invoke(cli, ['d1', 'verify', '-x', 'no_colon_here'])
    assert (result.exit_code, str(result.exception)) == (
        1, "d1 verify: expected TABLE:COL[,COL…], got 'no_colon_here'",
    )


def test_apply_runs_every_statement_and_reports_each(monkeypatch):
    calls = _install_fake_query(monkeypatch, sqlite_master=[], pragmas={})
    result = CliRunner().invoke(cli, ['d1', 'apply'])
    assert result.exit_code == 0
    assert [sql.splitlines()[0] for sql, _ in calls] == [
        WATERMARKS_DDL_HEAD,
        'CREATE TABLE IF NOT EXISTS "pyramid_shards" (',
        PERIOD_INDEX_DDL,
    ]


def test_apply_dry_run_touches_nothing(monkeypatch):
    calls = _install_fake_query(monkeypatch, sqlite_master=[], pragmas={})
    result = CliRunner().invoke(cli, ['d1', 'apply', '-n'])
    assert result.exit_code == 0
    assert calls == []

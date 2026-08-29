"""`pyrmts.d1` — CF REST client request/response shape (no network; the
transport is monkeypatched and every request field asserted exactly)."""
from __future__ import annotations

import json
import urllib.request

import pytest

from pyrmts.d1 import (
    SchemaDiff,
    apply_schema,
    d1_query,
    quote_ident,
    register_shard,
    schema_sql,
    verify_schema,
)


class _Resp:
    def __init__(self, payload: dict) -> None:
        self._body = json.dumps(payload).encode()

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> '_Resp':
        return self

    def __exit__(self, *exc) -> bool:
        return False


def _env(monkeypatch) -> None:
    monkeypatch.setenv('CLOUDFLARE_ACCOUNT_ID', 'acct')
    monkeypatch.setenv('CLOUDFLARE_API_TOKEN', 'tok')
    monkeypatch.setenv('D1_DATABASE_ID', 'db1')


def _capture(monkeypatch, payload: dict) -> list:
    calls: list = []

    def fake_urlopen(req, timeout=None):
        calls.append((req, timeout))
        return _Resp(payload)

    monkeypatch.setattr(urllib.request, 'urlopen', fake_urlopen)
    return calls


def test_d1_query_request_and_rows(monkeypatch):
    _env(monkeypatch)
    calls = _capture(monkeypatch, {'success': True, 'result': [{'results': [{'key': 'k1'}]}]})
    rows = d1_query('SELECT key FROM pyramid_shards WHERE pyramid = ?', ['p'])
    assert rows == [{'key': 'k1'}]
    (req, timeout), = calls
    assert req.full_url == 'https://api.cloudflare.com/client/v4/accounts/acct/d1/database/db1/query'
    assert req.get_header('Authorization') == 'Bearer tok'
    assert req.get_header('Content-type') == 'application/json'
    assert json.loads(req.data) == {
        'sql': 'SELECT key FROM pyramid_shards WHERE pyramid = ?',
        'params': ['p'],
    }
    assert timeout == 60


def test_d1_query_explicit_ids_override_env(monkeypatch):
    _env(monkeypatch)
    calls = _capture(monkeypatch, {'success': True, 'result': []})
    assert d1_query('SELECT 1', database_id='db2', account_id='a2', api_token='t2') == []
    (req, _), = calls
    assert req.full_url == 'https://api.cloudflare.com/client/v4/accounts/a2/d1/database/db2/query'
    assert req.get_header('Authorization') == 'Bearer t2'


def test_d1_query_error_raises(monkeypatch):
    _env(monkeypatch)
    _capture(monkeypatch, {'success': False, 'errors': [{'code': 7500, 'message': 'no such table'}]})
    with pytest.raises(RuntimeError) as exc:
        d1_query('SELECT 1')
    assert str(exc.value) == 'D1 query failed: [{"code": 7500, "message": "no such table"}]'


def test_register_shard_row_shape(monkeypatch):
    """The exact `pyramid_shards` row the CFW cascade writes."""
    _env(monkeypatch)
    calls = _capture(monkeypatch, {'success': True, 'result': [{'results': []}]})
    register_shard(
        pyramid='avail-v5',
        tier='1m',
        shard_dur='2d',
        period_start_ms=1000,
        period_end_ms=2000,
        key='avail-v5/1m/2d/2026-07-01.parquet',
        written_at_ms=3000,
    )
    (req, _), = calls
    assert json.loads(req.data) == {
        'sql': (
            'INSERT OR REPLACE INTO pyramid_shards '
            '(pyramid, tier, shard_dur, period_start, period_end, key, written_at) '
            'VALUES (?, ?, ?, ?, ?, ?, ?)'
        ),
        'params': ['avail-v5', '1m', '2d', 1000, 2000, 'avail-v5/1m/2d/2026-07-01.parquet', 3000],
    }


# ---- schema twin (`specs/d1-schema-drift.md`) ----
#
# These literals are one half of a cross-language contract: the same three
# statements are asserted verbatim in
# `js/packages/pyrmts-cfw/src/shard-index.test.ts`. Editing the DDL on one
# side without the other fails that side's test — the twins can't drift
# silently, which is the whole point of the pair.

WATERMARKS_DDL = (
    'CREATE TABLE IF NOT EXISTS "pyramid_watermarks" (\n'
    '  pyramid TEXT NOT NULL,\n'
    '  tier TEXT NOT NULL,\n'
    '  shard_dur TEXT NOT NULL,\n'
    '  latest_period_end INTEGER NOT NULL,\n'
    '  updated_at INTEGER NOT NULL,\n'
    '  PRIMARY KEY (pyramid, tier, shard_dur)\n'
    ') WITHOUT ROWID'
)
SHARDS_DDL = (
    'CREATE TABLE IF NOT EXISTS "pyramid_shards" (\n'
    '  pyramid TEXT NOT NULL,\n'
    '  tier TEXT NOT NULL,\n'
    '  shard_dur TEXT NOT NULL,\n'
    '  period_start INTEGER NOT NULL,\n'
    '  period_end INTEGER NOT NULL,\n'
    '  key TEXT NOT NULL,\n'
    '  written_at INTEGER NOT NULL,\n'
    '  PRIMARY KEY (pyramid, tier, shard_dur, period_start)\n'
    ') WITHOUT ROWID'
)
PERIOD_INDEX_DDL = (
    'CREATE INDEX IF NOT EXISTS "pyramid_shards_period" '
    'ON "pyramid_shards" (pyramid, period_end)'
)


def test_schema_sql_matches_the_typescript_emitter():
    assert schema_sql() == [WATERMARKS_DDL, SHARDS_DDL, PERIOD_INDEX_DDL]


def test_schema_sql_skip_inventory_drops_shards_table_and_its_index():
    assert schema_sql(skip_inventory=True) == [WATERMARKS_DDL]


def test_schema_sql_custom_table_names_propagate_into_ddl_and_index_name():
    assert schema_sql(watermarks_table='t1_watermarks', shards_table='t1_shards') == [
        WATERMARKS_DDL.replace('"pyramid_watermarks"', '"t1_watermarks"'),
        SHARDS_DDL.replace('"pyramid_shards"', '"t1_shards"'),
        'CREATE INDEX IF NOT EXISTS "t1_shards_period" ON "t1_shards" (pyramid, period_end)',
    ]


def test_quote_ident_escapes_embedded_double_quotes():
    assert quote_ident('w"ater') == '"w""ater"'


def _fake_query(sqlite_master: list[dict], pragmas: dict[str, list[str]]):
    """Query stub over a declared live schema. `sqlite_master` is what the
    existence query returns; `pragmas` maps object name → its column names.
    Records every (sql, params) so tests assert the exact wire calls."""
    calls: list[tuple[str, list]] = []

    def query(sql: str, params: list, **kwargs):
        calls.append((sql, params))
        if sql.startswith('SELECT type, name FROM sqlite_master'):
            return sqlite_master
        for name, cols in pragmas.items():
            if f'({quote_ident(name)})' in sql:
                return [{'name': c} for c in cols]
        raise AssertionError(f'unexpected query: {sql}')

    return query, calls


def test_verify_schema_clean_database_is_ok_and_issues_exact_queries():
    query, calls = _fake_query(
        sqlite_master=[
            {'type': 'table', 'name': 'pyramid_watermarks'},
            {'type': 'table', 'name': 'pyramid_shards'},
            {'type': 'index', 'name': 'pyramid_shards_period'},
        ],
        pragmas={
            'pyramid_watermarks': [
                'pyramid', 'tier', 'shard_dur', 'latest_period_end', 'updated_at',
            ],
            'pyramid_shards': [
                'pyramid', 'tier', 'shard_dur', 'period_start', 'period_end',
                'key', 'written_at',
            ],
            'pyramid_shards_period': ['pyramid', 'period_end'],
        },
    )
    diff = verify_schema(query=query)
    assert diff == SchemaDiff()
    assert diff.ok is True
    assert diff.summary() == 'schema up to date'
    assert calls == [
        (
            'SELECT type, name FROM sqlite_master WHERE name IN (?, ?, ?)',
            ['pyramid_watermarks', 'pyramid_shards', 'pyramid_shards_period'],
        ),
        ('PRAGMA table_info("pyramid_watermarks")', []),
        ('PRAGMA table_info("pyramid_shards")', []),
        ('PRAGMA index_info("pyramid_shards_period")', []),
    ]


def test_verify_schema_reports_the_index_a_pre_index_deployment_lacks():
    """The observed failure: tables provisioned before `pyramid_shards_period`
    existed. ctbk and awair were both in this state."""
    query, _ = _fake_query(
        sqlite_master=[
            {'type': 'table', 'name': 'pyramid_watermarks'},
            {'type': 'table', 'name': 'pyramid_shards'},
        ],
        pragmas={
            'pyramid_watermarks': [
                'pyramid', 'tier', 'shard_dur', 'latest_period_end', 'updated_at',
            ],
            'pyramid_shards': [
                'pyramid', 'tier', 'shard_dur', 'period_start', 'period_end',
                'key', 'written_at',
            ],
        },
    )
    diff = verify_schema(query=query)
    assert diff == SchemaDiff(missing=('pyramid_shards_period',))
    assert diff.ok is False
    assert diff.summary() == 'missing: pyramid_shards_period'


def test_verify_schema_flags_a_table_missing_a_column():
    query, _ = _fake_query(
        sqlite_master=[
            {'type': 'table', 'name': 'pyramid_watermarks'},
            {'type': 'table', 'name': 'pyramid_shards'},
            {'type': 'index', 'name': 'pyramid_shards_period'},
        ],
        pragmas={
            'pyramid_watermarks': ['pyramid', 'tier', 'shard_dur', 'latest_period_end'],
            'pyramid_shards': [
                'pyramid', 'tier', 'shard_dur', 'period_start', 'period_end',
                'key', 'written_at',
            ],
            'pyramid_shards_period': ['pyramid', 'period_end'],
        },
    )
    diff = verify_schema(query=query)
    assert diff == SchemaDiff(mismatched=(
        "pyramid_watermarks: expected=['latest_period_end', 'pyramid', 'shard_dur', "
        "'tier', 'updated_at'] actual=['latest_period_end', 'pyramid', 'shard_dur', 'tier']",
    ))


def test_verify_schema_flags_an_index_on_the_wrong_columns():
    """Column *order* is load-bearing for an index — `(period_end, pyramid)`
    can't serve the windowed `listShards` seek that `(pyramid, period_end)`
    does, so it must not read as equivalent."""
    query, _ = _fake_query(
        sqlite_master=[
            {'type': 'table', 'name': 'pyramid_watermarks'},
            {'type': 'table', 'name': 'pyramid_shards'},
            {'type': 'index', 'name': 'pyramid_shards_period'},
        ],
        pragmas={
            'pyramid_watermarks': [
                'pyramid', 'tier', 'shard_dur', 'latest_period_end', 'updated_at',
            ],
            'pyramid_shards': [
                'pyramid', 'tier', 'shard_dur', 'period_start', 'period_end',
                'key', 'written_at',
            ],
            'pyramid_shards_period': ['period_end', 'pyramid'],
        },
    )
    diff = verify_schema(query=query)
    assert diff == SchemaDiff(mismatched=(
        "pyramid_shards_period: expected=['pyramid', 'period_end'] "
        "actual=['period_end', 'pyramid']",
    ))


def test_apply_schema_runs_every_statement_with_no_params():
    calls: list[tuple[str, list]] = []

    def query(sql: str, params: list, **kwargs):
        calls.append((sql, params))
        return []

    applied = apply_schema(query=query)
    assert applied == [WATERMARKS_DDL, SHARDS_DDL, PERIOD_INDEX_DDL]
    assert calls == [(WATERMARKS_DDL, []), (SHARDS_DDL, []), (PERIOD_INDEX_DDL, [])]

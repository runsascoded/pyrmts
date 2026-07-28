"""`pyrmts.d1` — CF REST client request/response shape (no network; the
transport is monkeypatched and every request field asserted exactly)."""
from __future__ import annotations

import json
import urllib.request

import pytest

from pyrmts.d1 import d1_query, register_shard


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

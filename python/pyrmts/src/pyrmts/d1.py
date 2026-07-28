"""D1 access over Cloudflare's REST API — the Python peer of
`js/packages/pyrmts-cfw/src/d1.ts`, writing the identical `pyramid_shards`
schema (`specs/pyrmts-ops-adoption.md` phase 1).

Talks to `POST /client/v4/accounts/{acct}/d1/database/{db}/query` directly
(no wrangler), so it works in Lambdas and other headless runners.

Env: `CLOUDFLARE_ACCOUNT_ID`, `CLOUDFLARE_API_TOKEN` (D1 edit scope), and
`D1_DATABASE_ID` — each overridable per call."""
from __future__ import annotations

import json
import os
import urllib.request
from urllib.error import HTTPError


def d1_query(
    sql: str,
    params: list | None = None,
    *,
    database_id: str | None = None,
    account_id: str | None = None,
    api_token: str | None = None,
    timeout: float = 60,
) -> list[dict]:
    """Run one statement; return its result rows. Raises on any error
    (HTTP or D1-level) — callers treat registration as must-succeed."""
    acct = account_id or os.environ['CLOUDFLARE_ACCOUNT_ID']
    token = api_token or os.environ['CLOUDFLARE_API_TOKEN']
    db = database_id or os.environ['D1_DATABASE_ID']
    url = f'https://api.cloudflare.com/client/v4/accounts/{acct}/d1/database/{db}/query'
    body = json.dumps({'sql': sql, 'params': params or []}).encode()
    req = urllib.request.Request(url, data=body, headers={
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read())
    except HTTPError as e:
        raise RuntimeError(f'D1 HTTP {e.code}: {e.read().decode(errors="replace")[:300]}') from e
    if not payload.get('success'):
        raise RuntimeError(f'D1 query failed: {json.dumps(payload.get("errors"))[:300]}')
    results = payload['result']
    return results[0].get('results', []) if results else []


def register_shard(
    *,
    pyramid: str,
    tier: str,
    shard_dur: str,
    period_start_ms: int,
    period_end_ms: int,
    key: str,
    written_at_ms: int,
    database_id: str | None = None,
    table: str = 'pyramid_shards',
) -> None:
    """INSERT OR REPLACE one shard row — the same shape the CFW cascade
    (`pyrmts-cfw/src/d1.ts`) writes."""
    d1_query(
        f'INSERT OR REPLACE INTO {table} '
        f'(pyramid, tier, shard_dur, period_start, period_end, key, written_at) '
        f'VALUES (?, ?, ?, ?, ?, ?, ?)',
        [pyramid, tier, shard_dur, period_start_ms, period_end_ms, key, written_at_ms],
        database_id=database_id,
    )

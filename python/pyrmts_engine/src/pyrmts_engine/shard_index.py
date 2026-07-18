"""ShardIndex protocol + impls. The engine calls `record_shard` immediately
after each shard PUT — a crash leaves at worst an unregistered (invisible)
object that a resumed run overwrites idempotently, never a
registered-but-absent key.

Impls:
- `NoopShardIndex`: tests / dry-runs.
- `JsonlShardIndex`: local manifest, one JSON object per line. Doubles as
  the "register later" mode — a driver can replay the manifest into D1.
- `D1ShardIndex`: Cloudflare D1 REST, same `pyramid_shards` row shape the
  CFW cascade and ctbk's Lambda executor write.
"""
from __future__ import annotations

import json
import os
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol
from urllib.error import HTTPError


@dataclass(frozen=True)
class ShardRecord:
    pyramid: str
    tier: str
    shard_dur: str
    period_start_ms: int
    period_end_ms: int
    key: str
    written_at_ms: int


class ShardIndex(Protocol):
    def record_shard(self, record: ShardRecord) -> None: ...


class NoopShardIndex:
    def record_shard(self, record: ShardRecord) -> None:
        return None


@dataclass
class MemShardIndex:
    records: list[ShardRecord] = field(default_factory=list)

    def record_shard(self, record: ShardRecord) -> None:
        self.records.append(record)


class JsonlShardIndex:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def record_shard(self, record: ShardRecord) -> None:
        row = {
            'pyramid': record.pyramid,
            'tier': record.tier,
            'shard_dur': record.shard_dur,
            'period_start': record.period_start_ms,
            'period_end': record.period_end_ms,
            'key': record.key,
            'written_at': record.written_at_ms,
        }
        with open(self.path, 'a') as f:
            f.write(json.dumps(row) + '\n')


class D1ShardIndex:
    """Registers into the `pyramid_shards` D1 table over Cloudflare's REST
    API. Env: `CLOUDFLARE_ACCOUNT_ID`, `CLOUDFLARE_API_TOKEN`, and
    `D1_DATABASE_ID` (unless passed explicitly). Registration is
    must-succeed: any HTTP or D1-level error raises."""

    def __init__(self, database_id: str | None = None) -> None:
        self.database_id = database_id or os.environ['D1_DATABASE_ID']

    def record_shard(self, record: ShardRecord) -> None:
        self._query(
            'INSERT OR REPLACE INTO pyramid_shards '
            '(pyramid, tier, shard_dur, period_start, period_end, key, written_at) '
            'VALUES (?, ?, ?, ?, ?, ?, ?)',
            [
                record.pyramid, record.tier, record.shard_dur,
                record.period_start_ms, record.period_end_ms,
                record.key, record.written_at_ms,
            ],
        )

    def _query(self, sql: str, params: list) -> None:
        acct = os.environ['CLOUDFLARE_ACCOUNT_ID']
        token = os.environ['CLOUDFLARE_API_TOKEN']
        url = f'https://api.cloudflare.com/client/v4/accounts/{acct}/d1/database/{self.database_id}/query'
        body = json.dumps({'sql': sql, 'params': params}).encode()
        req = urllib.request.Request(url, data=body, headers={
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json',
        })
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                payload = json.loads(resp.read())
        except HTTPError as e:
            raise RuntimeError(f'D1 HTTP {e.code}: {e.read().decode(errors="replace")[:300]}') from e
        if not payload.get('success'):
            raise RuntimeError(f'D1 query failed: {json.dumps(payload.get("errors"))[:300]}')


def now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)

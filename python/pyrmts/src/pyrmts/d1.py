"""D1 access over Cloudflare's REST API — the Python peer of
`js/packages/pyrmts-cfw/src/d1.ts`, writing the identical `pyramid_shards`
schema (`specs/pyrmts-ops-adoption.md` phase 1).

Talks to `POST /client/v4/accounts/{acct}/d1/database/{db}/query` directly
(no wrangler), so it works in Lambdas and other headless runners.

Env: `CLOUDFLARE_ACCOUNT_ID`, `CLOUDFLARE_API_TOKEN` (D1 edit scope), and
`D1_DATABASE_ID` — each overridable per call.

Also carries the **schema twin**: `schema_objects` / `schema_sql` emit the
same DDL as `D1ShardIndex.schemaSql()` in `pyrmts-cfw`, and `verify_schema`
diffs a live database against it. Python writes these tables
(`register_shard`, `pyrmts_engine.materialize`, `pyrmts_ops.gc`) but had no
way to create or check the schema it writes, and a DDL change upstream
reached an existing deployment only if a human remembered
(`specs/d1-schema-drift.md`)."""
from __future__ import annotations

import json
import os
import urllib.request
from dataclasses import dataclass, field
from urllib.error import HTTPError

WATERMARKS_TABLE = 'pyramid_watermarks'
SHARDS_TABLE = 'pyramid_shards'


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


# ---- schema twin (`pyrmts-cfw` `D1ShardIndex.schemaSql()`) ----
#
# The DDL below is a hand-maintained twin of the TypeScript emitter, in the
# same style as the `BuildProgress` doc contract (`specs/pyrmts-ops-adoption.md`
# phase 3/4): two implementations, each locked by an exact test, with the
# contract stated once in a spec. `test_d1.py` asserts these strings verbatim;
# `js/packages/pyrmts-cfw/src/shard-index.test.ts` asserts the same strings on
# the TS side. Change one, and the other's test fails — which is the point.


def quote_ident(name: str) -> str:
    """SQLite identifier quoting — the twin of `quoteIdent` in `shard-index.ts`."""
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


@dataclass(frozen=True)
class SchemaObject:
    """One object `D1ShardIndex` expects to exist, with the columns that make
    it correct. `columns` is what a live-vs-expected diff compares: for a
    table, its column names (`PRAGMA table_info`); for an index, the columns
    it indexes, in order (`PRAGMA index_info`)."""
    name: str
    kind: str  # 'table' | 'index'
    sql: str
    columns: tuple[str, ...] = field(default=())


def schema_objects(
    *,
    watermarks_table: str = WATERMARKS_TABLE,
    shards_table: str = SHARDS_TABLE,
    skip_inventory: bool = False,
) -> list[SchemaObject]:
    """The objects `D1ShardIndex` needs, in dependency order (tables before
    the indexes on them). `skip_inventory=True` drops the shards table and
    its index, matching the `skipInventory` construction option."""
    w = quote_ident(watermarks_table)
    s = quote_ident(shards_table)
    out = [
        SchemaObject(
            name=watermarks_table,
            kind='table',
            sql=(
                f'CREATE TABLE IF NOT EXISTS {w} (\n'
                f'  pyramid TEXT NOT NULL,\n'
                f'  tier TEXT NOT NULL,\n'
                f'  shard_dur TEXT NOT NULL,\n'
                f'  latest_period_end INTEGER NOT NULL,\n'
                f'  updated_at INTEGER NOT NULL,\n'
                f'  PRIMARY KEY (pyramid, tier, shard_dur)\n'
                f') WITHOUT ROWID'
            ),
            columns=('pyramid', 'tier', 'shard_dur', 'latest_period_end', 'updated_at'),
        ),
    ]
    if not skip_inventory:
        out.append(SchemaObject(
            name=shards_table,
            kind='table',
            sql=(
                f'CREATE TABLE IF NOT EXISTS {s} (\n'
                f'  pyramid TEXT NOT NULL,\n'
                f'  tier TEXT NOT NULL,\n'
                f'  shard_dur TEXT NOT NULL,\n'
                f'  period_start INTEGER NOT NULL,\n'
                f'  period_end INTEGER NOT NULL,\n'
                f'  key TEXT NOT NULL,\n'
                f'  written_at INTEGER NOT NULL,\n'
                f'  PRIMARY KEY (pyramid, tier, shard_dur, period_start)\n'
                f') WITHOUT ROWID'
            ),
            columns=(
                'pyramid', 'tier', 'shard_dur', 'period_start', 'period_end',
                'key', 'written_at',
            ),
        ))
        out.append(SchemaObject(
            name=f'{shards_table}_period',
            kind='index',
            sql=(
                f'CREATE INDEX IF NOT EXISTS {quote_ident(f"{shards_table}_period")} '
                f'ON {s} (pyramid, period_end)'
            ),
            columns=('pyramid', 'period_end'),
        ))
    return out


def schema_sql(
    *,
    watermarks_table: str = WATERMARKS_TABLE,
    shards_table: str = SHARDS_TABLE,
    skip_inventory: bool = False,
) -> list[str]:
    """DDL statements, byte-identical to `D1ShardIndex.schemaSql()`. Every
    statement is `IF NOT EXISTS`, so applying them to a live database is the
    migration — and re-applying is a no-op."""
    return [o.sql for o in schema_objects(
        watermarks_table=watermarks_table,
        shards_table=shards_table,
        skip_inventory=skip_inventory,
    )]


@dataclass(frozen=True)
class SchemaDiff:
    """Live-vs-expected difference. `missing` is an absent object (the
    observed failure mode: a deployment provisioned before an index was
    added); `mismatched` is present-but-wrong-columns, listed as
    `name: expected=[…] actual=[…]`."""
    missing: tuple[str, ...] = field(default=())
    mismatched: tuple[str, ...] = field(default=())

    @property
    def ok(self) -> bool:
        return not self.missing and not self.mismatched

    def summary(self) -> str:
        if self.ok:
            return 'schema up to date'
        parts = []
        if self.missing:
            parts.append(f"missing: {', '.join(self.missing)}")
        if self.mismatched:
            parts.append(f"mismatched: {'; '.join(self.mismatched)}")
        return ' — '.join(parts)


def verify_schema(
    *,
    watermarks_table: str = WATERMARKS_TABLE,
    shards_table: str = SHARDS_TABLE,
    skip_inventory: bool = False,
    query=None,
    **kwargs,
) -> SchemaDiff:
    """Diff a live D1 database against `schema_objects()`, read-only.

    Uses `sqlite_master` for existence and `PRAGMA table_info` /
    `PRAGMA index_info` for columns — all three are supported by D1's query
    API (`PRAGMA page_count` and `dbstat` are not, and aren't needed).

    `query` overrides the query callable (defaults to `d1_query`); remaining
    kwargs (`database_id`, `account_id`, `api_token`) pass through."""
    q = query if query is not None else d1_query
    expected = schema_objects(
        watermarks_table=watermarks_table,
        shards_table=shards_table,
        skip_inventory=skip_inventory,
    )
    placeholders = ', '.join('?' for _ in expected)
    rows = q(
        f'SELECT type, name FROM sqlite_master WHERE name IN ({placeholders})',
        [o.name for o in expected],
        **kwargs,
    )
    live = {r['name'] for r in rows}
    missing: list[str] = []
    mismatched: list[str] = []
    for o in expected:
        if o.name not in live:
            missing.append(o.name)
            continue
        pragma = 'table_info' if o.kind == 'table' else 'index_info'
        info = q(f'PRAGMA {pragma}({quote_ident(o.name)})', [], **kwargs)
        actual = tuple(r['name'] for r in info)
        if o.kind == 'table':
            # Column order is not load-bearing for a table (SELECTs name
            # their columns); an index's order is.
            if set(actual) != set(o.columns):
                mismatched.append(
                    f'{o.name}: expected={sorted(o.columns)} actual={sorted(actual)}'
                )
        elif actual != o.columns:
            mismatched.append(f'{o.name}: expected={list(o.columns)} actual={list(actual)}')
    return SchemaDiff(missing=tuple(missing), mismatched=tuple(mismatched))


def apply_schema(
    *,
    watermarks_table: str = WATERMARKS_TABLE,
    shards_table: str = SHARDS_TABLE,
    skip_inventory: bool = False,
    query=None,
    **kwargs,
) -> list[str]:
    """Apply every `schema_sql()` statement, returning those applied.

    Each is `IF NOT EXISTS`, so this is safe to run against a live database
    and is a no-op when the schema is current. Note `CREATE INDEX` on a
    populated table builds the index in one pass (63 ms over 60k rows on
    ctbk's `pyramid_shards`), and D1 bills the index build as row writes."""
    q = query if query is not None else d1_query
    stmts = schema_sql(
        watermarks_table=watermarks_table,
        shards_table=shards_table,
        skip_inventory=skip_inventory,
    )
    for sql in stmts:
        q(sql, [], **kwargs)
    return stmts

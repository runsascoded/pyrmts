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
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol


@dataclass(frozen=True)
class ShardRecord:
    pyramid: str
    tier: str
    shard_dur: str
    period_start_ms: int
    period_end_ms: int
    key: str
    written_at_ms: int
    # Content MD5 (hex) + byte length of the written object: makes
    # cross-run RGIP checks a manifest diff (runs on e overwrote each
    # other's objects, so byte equality was otherwise unprovable
    # after the fact). Optional: absent in pre-2026-07 manifests; D1
    # rows don't carry them.
    md5: str | None = None
    n_bytes: int | None = None


class ShardIndex(Protocol):
    def record_shard(self, record: ShardRecord) -> None: ...


Slot = tuple[str, str, str, int]   # (pyramid, tier, shard_dur, period_start_ms) — the D1 table's PK


def _slot(record: ShardRecord) -> Slot:
    return (record.pyramid, record.tier, record.shard_dur, record.period_start_ms)


def _current(records: list[ShardRecord], pyramid: str | None = None) -> dict[Slot, ShardRecord]:
    """Latest record per slot (append-only manifests: the last row wins, which
    is the `INSERT OR REPLACE` semantics of the D1 table, keyed exactly like
    its PK — so one manifest can hold several pyramids, and multi-tenant
    layouts that put a dim in the key must use one pyramid name per tenant, as
    D1 already requires). `pyramid` scopes the result to one pyramid."""
    out: dict[Slot, ShardRecord] = {}
    for r in records:
        if pyramid is None or r.pyramid == pyramid:
            out[_slot(r)] = r
    return out


def _lookup(records: list[ShardRecord], tier: str, shard_dur: str, period_start_ms: int, pyramid: str | None) -> ShardRecord | None:
    current = _current(records, pyramid)
    if pyramid is not None:
        return current.get((pyramid, tier, shard_dur, period_start_ms))
    hits = [r for (p, t, d, ps), r in current.items() if (t, d, ps) == (tier, shard_dur, period_start_ms)]
    if len(hits) > 1:
        raise ValueError(
            f"lookup({tier!r}, {shard_dur!r}, {period_start_ms}): rows for {len(hits)} pyramids "
            f"({sorted(r.pyramid for r in hits)}) — pass `pyramid`"
        )
    return hits[0] if hits else None


def _record_from_row(row: dict, pyramid: str | None = None) -> ShardRecord:
    return ShardRecord(
        pyramid=row.get('pyramid', pyramid or ''),
        tier=row['tier'],
        shard_dur=row['shard_dur'],
        period_start_ms=int(row['period_start']),
        period_end_ms=int(row['period_end']),
        key=row['key'],
        written_at_ms=int(row.get('written_at') or 0),
        md5=row.get('md5'),
        n_bytes=row.get('bytes'),
    )


@dataclass(frozen=True)
class RegistryResolver:
    """`pyrmts.KeyResolver` over a registry: a slot's current key is its row's
    `key` — the only truth once keys carry a content hash. `pyramid` scopes
    the lookup (required when the index holds several pyramids)."""
    index: object   # any ShardIndex with `lookup`
    pyramid: str | None = None

    def resolve(self, tier: str, shard_dur: str, period_start_ms: int, period_label: str, filter) -> str | None:
        rec = self.index.lookup(tier, shard_dur, period_start_ms, pyramid=self.pyramid)
        return None if rec is None else rec.key


class NoopShardIndex:
    def record_shard(self, record: ShardRecord) -> None:
        return None


def _row(record: ShardRecord) -> dict:
    row = {
        'pyramid': record.pyramid,
        'tier': record.tier,
        'shard_dur': record.shard_dur,
        'period_start': record.period_start_ms,
        'period_end': record.period_end_ms,
        'key': record.key,
        'written_at': record.written_at_ms,
    }
    if record.md5 is not None:
        row['md5'] = record.md5
    if record.n_bytes is not None:
        row['bytes'] = record.n_bytes
    return row


@dataclass
class MemShardIndex:
    records: list[ShardRecord] = field(default_factory=list)

    def record_shard(self, record: ShardRecord) -> None:
        self.records.append(record)

    def current_records(self, pyramid: str | None = None) -> list[ShardRecord]:
        return list(_current(self.records, pyramid).values())

    def existing_keys(self, pyramid: str | None = None) -> set[str]:
        return {r.key for r in self.current_records(pyramid)}

    def lookup(self, tier: str, shard_dur: str, period_start_ms: int, pyramid: str | None = None) -> ShardRecord | None:
        return _lookup(self.records, tier, shard_dur, period_start_ms, pyramid)


class JsonlShardIndex:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._cache = None

    def record_shard(self, record: ShardRecord) -> None:
        with open(self.path, 'a') as f:
            f.write(json.dumps(_row(record)) + '\n')
        if self._cache is not None:
            self._cache.append(record)

    _cache: list[ShardRecord] | None = None

    def _records(self) -> list[ShardRecord]:
        # Parsed once per index instance (a canonicalize / cascade pass looks
        # up every slot); `record_shard` appends to the cache, so it stays
        # exact for this writer. Another writer's rows appear on the next instance.
        if self._cache is None:
            if not self.path.exists():
                self._cache = []
            else:
                self._cache = [_record_from_row(json.loads(line)) for line in self.path.read_text().splitlines() if line]
        return self._cache

    def current_records(self, pyramid: str | None = None) -> list[ShardRecord]:
        return list(_current(self._records(), pyramid).values())

    def existing_keys(self, pyramid: str | None = None) -> set[str]:
        return {r.key for r in self.current_records(pyramid)}

    def lookup(self, tier: str, shard_dur: str, period_start_ms: int, pyramid: str | None = None) -> ShardRecord | None:
        return _lookup(self._records(), tier, shard_dur, period_start_ms, pyramid)


class StorageJsonlShardIndex:
    """JSONL manifest written through a pyrmts `Storage` (S3/R2/fs/mem) —
    for ephemeral runners (Batch/Fargate) where local disk dies with the
    container. Re-PUTs the full manifest every `flush_every` records
    (object stores can't append; default 1 — a manifest PUT is noise next
    to the shard write it records, and per-close cadence is what makes
    `resume` trustworthy after a Spot reclaim), plus a final PUT from
    `close()` (which `build_local` calls when the index has one).

    An existing manifest at `key` is loaded on init, so records survive
    across resumed runs and `existing_keys()` reflects prior attempts."""

    def __init__(self, storage, key: str, flush_every: int = 1) -> None:
        self.storage = storage
        self.key = key
        self.flush_every = flush_every
        existing = storage.get(key)
        self._lines: list[str] = (
            existing.decode().rstrip('\n').split('\n') if existing else []
        )
        self._unflushed = 0

    def _records(self) -> list[ShardRecord]:
        return [_record_from_row(json.loads(line)) for line in self._lines]

    def current_records(self, pyramid: str | None = None) -> list[ShardRecord]:
        return list(_current(self._records(), pyramid).values())

    def existing_keys(self, pyramid: str | None = None) -> set[str]:
        return {r.key for r in self.current_records(pyramid)}

    def lookup(self, tier: str, shard_dur: str, period_start_ms: int, pyramid: str | None = None) -> ShardRecord | None:
        return _lookup(self._records(), tier, shard_dur, period_start_ms, pyramid)

    def record_shard(self, record: ShardRecord) -> None:
        self._lines.append(json.dumps(_row(record)))
        self._unflushed += 1
        if self._unflushed >= self.flush_every:
            self._flush()

    def _flush(self) -> None:
        self.storage.put(self.key, ('\n'.join(self._lines) + '\n').encode())
        self._unflushed = 0

    def close(self) -> None:
        if self._lines and self._unflushed:
            self._flush()


class D1ShardIndex:
    """Registers into the `pyramid_shards` D1 table via `pyrmts.d1` (the
    REST client). Env: `CLOUDFLARE_ACCOUNT_ID`, `CLOUDFLARE_API_TOKEN`,
    and `D1_DATABASE_ID` (unless passed explicitly). Registration is
    must-succeed: any HTTP or D1-level error raises.

    `pyramid` scopes `existing_keys()` (the reconcile/resume read path);
    registration alone doesn't need it (each record carries its own)."""

    def __init__(
        self,
        database_id: str | None = None,
        table: str = 'pyramid_shards',
        pyramid: str | None = None,
    ) -> None:
        self.database_id = database_id or os.environ['D1_DATABASE_ID']
        self.table = table
        self.pyramid = pyramid

    def current_records(self, pyramid: str | None = None) -> list[ShardRecord]:
        pyramid = pyramid or self.pyramid
        if pyramid is None:
            raise ValueError("D1ShardIndex.current_records() needs `pyramid` (row scope)")
        from pyrmts.d1 import d1_query
        rows = d1_query(f'SELECT * FROM {self.table} WHERE pyramid = ?', [pyramid], database_id=self.database_id)
        return [_record_from_row(r, pyramid) for r in rows]

    def existing_keys(self, pyramid: str | None = None) -> set[str]:
        return {r.key for r in self.current_records(pyramid)}

    def lookup(self, tier: str, shard_dur: str, period_start_ms: int, pyramid: str | None = None) -> ShardRecord | None:
        pyramid = pyramid or self.pyramid
        if pyramid is None:
            raise ValueError("D1ShardIndex.lookup() needs `pyramid` (row scope)")
        from pyrmts.d1 import d1_query
        rows = d1_query(
            f'SELECT * FROM {self.table} WHERE pyramid = ? AND tier = ? AND shard_dur = ? AND period_start = ?',
            [pyramid, tier, shard_dur, period_start_ms],
            database_id=self.database_id,
        )
        return _record_from_row(rows[0], pyramid) if rows else None

    def record_shard(self, record: ShardRecord) -> None:
        from pyrmts.d1 import register_shard
        register_shard(
            pyramid=record.pyramid,
            tier=record.tier,
            shard_dur=record.shard_dur,
            period_start_ms=record.period_start_ms,
            period_end_ms=record.period_end_ms,
            key=record.key,
            written_at_ms=record.written_at_ms,
            database_id=self.database_id,
            table=self.table,
        )


def now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)

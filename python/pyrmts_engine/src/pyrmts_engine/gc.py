"""Orphan handling for content-hashed shard keys
(`specs/done/content-addressed-shards.md`): with `{hash}` in the keyTemplate a
rewrite never overwrites — it writes a new blob and swaps the registry row —
so storage accumulates **orphans** (hashed blobs no registry row references).
Two passes deal with them:

- `adopt_unregistered`: a write that died between `put` and register leaves a
  blob the registry doesn't point at — either a slot with no row at all, or a
  slot whose row is older than the blob (a rewrite whose registration was
  lost). Adopt it: the newest such blob whose content still hashes to its key
  gets registered.
- `gc_orphans`: delete blobs no row references, once older than a grace
  period (in-flight reads of the previous version, edge cache TTL). Dry-run
  by default; `apply=True` deletes, re-reading the registry right before
  each delete so a slot re-pointed at an old version since the listing is
  never deleted.

Both are LIST-driven (that is what they are for); everything else in the
engine takes "what is built" from the registry. Only keys in the template's
*hashed* shape are ever candidates: legacy-shaped blobs are left alone, and a
hashless template is refused outright.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from functools import partial

from pyrmts import Pyramid, content_hash, hash_width, list_expected_shards, parse_key
from pyrmts.gc import DEFAULT_GRACE, GcResult, Orphan, require_hashed
from pyrmts.gc import gc_orphans as _gc_orphans
from pyrmts.gc import list_orphans as _list_orphans

from .discovery import slot_for_record
from .shard_index import ShardRecord, now_ms

err = partial(print, file=sys.stderr, flush=True)

__all__ = ['DEFAULT_GRACE', 'GcResult', 'Orphan', 'adopt_unregistered', 'gc_orphans', 'list_orphans']


def _registry_keys(shard_index, pyramid_name: str | None) -> set[str]:
    current_records = getattr(shard_index, 'current_records', None)
    if current_records is None:
        raise ValueError("a registry that can list its rows (current_records()) is required")
    return {r.key for r in current_records(pyramid_name)}


def list_orphans(pyramid: Pyramid, registry_keys: set[str], prefix: str | None = None) -> list[Orphan]:
    """`pyrmts.gc.list_orphans` over a `Pyramid`'s storage + keyTemplate."""
    return _list_orphans(pyramid.storage, pyramid.keyTemplate, registry_keys, prefix)


def gc_orphans(
    pyramid: Pyramid,
    shard_index,
    pyramid_name: str | None = None,
    *,
    grace: timedelta = DEFAULT_GRACE,
    now: datetime | None = None,
    apply: bool = False,
    prefix: str | None = None,
) -> GcResult:
    """`pyrmts.gc.gc_orphans` with the registry's current keys (re-read right
    before deleting) as the referenced set. An empty registry for this
    pyramid is refused: every blob would be an orphan."""
    def referenced() -> set[str]:
        keys = _registry_keys(shard_index, pyramid_name)
        if not keys:
            raise ValueError(
                "gc_orphans: the registry has no rows for this pyramid — refusing (every blob would be an orphan); "
                "run `adopt` first, or check the registry / pyramid name"
            )
        return keys

    return _gc_orphans(
        pyramid.storage, pyramid.keyTemplate, referenced,
        grace=grace, now=now, apply=apply, prefix=prefix,
    )


def adopt_unregistered(
    pyramid: Pyramid,
    shard_index,
    pyramid_name: str,
    time_range: tuple[datetime, datetime],
    *,
    filter: dict | None = None,
) -> list[ShardRecord]:
    """Register, for every expected slot, the newest listed hashed blob that
    the registry does not point at and that is newer than the slot's row (or
    the slot has no row), if its bytes still hash to its key. Heals both a
    first write and a rewrite whose registration was lost. Returns the records
    adopted."""
    require_hashed(pyramid.keyTemplate, 'adopt_unregistered')
    template = pyramid.keyTemplate
    width = hash_width(template)
    assert width is not None
    current_records = getattr(shard_index, 'current_records', None)
    if current_records is None:
        raise ValueError("adopt_unregistered: a registry that can list its rows (current_records()) is required")
    rows = {slot_for_record(pyramid, r, filter): r for r in current_records(pyramid_name)}
    registered = {r.key for r in rows.values()}
    orphans_by_slot: dict[str, list[Orphan]] = {}
    for o in list_orphans(pyramid, registered):
        orphans_by_slot.setdefault(o.slot, []).append(o)
    epoch = datetime.min.replace(tzinfo=timezone.utc)
    adopted: list[ShardRecord] = []
    for e in list_expected_shards(pyramid, time_range, filter=filter):
        candidates = orphans_by_slot.get(e.key)
        if not candidates:
            continue
        row = rows.get(e.key)
        floor = _dt(row.written_at_ms) if row is not None and row.written_at_ms else epoch
        newer = [o for o in candidates if o.mtime is not None and o.mtime > floor] if row is not None else candidates
        for o in sorted(newer, key=lambda o: o.mtime or epoch, reverse=True):
            blob = pyramid.storage.get(o.key)
            if blob is None:
                continue
            md5 = content_hash(blob)
            if parse_key(template, o.key)['hash'][:width] != md5[:width]:
                err(f"  adopt: {o.key} content does not hash to its key; skipped")
                continue
            rec = ShardRecord(
                pyramid=pyramid_name, tier=e.tier, shard_dur=e.shard_dur,
                period_start_ms=int(e.period_start.timestamp() * 1000),
                period_end_ms=int(e.period_end.timestamp() * 1000),
                key=o.key, written_at_ms=now_ms(), md5=md5, n_bytes=len(blob),
            )
            shard_index.record_shard(rec)
            adopted.append(rec)
            break
    if adopted:
        err(f"  adopt: registered {len(adopted)} present-but-unregistered shard(s)")
    return adopted


def _dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, timezone.utc)

"""Orphan handling for content-hashed shard keys
(`specs/content-addressed-shards.md`): with `{hash}` in the keyTemplate a
rewrite never overwrites — it writes a new blob and swaps the registry row —
so storage accumulates **orphans** (blobs no registry row references). Two
passes deal with them:

- `adopt_unregistered`: a write-then-die between `put` and register leaves a
  blob whose slot has *no* registry row at all. Adopt it: the newest listed
  version whose content still hashes to its key gets registered.
- `gc_orphans`: delete blobs no row references, once older than a grace
  period (in-flight reads of the previous version, edge cache TTL). Dry-run
  by default; `apply=True` deletes.

Both are LIST-driven (that is what they are for); everything else in the
engine takes "what is built" from the registry.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from functools import partial

from pyrmts import ExpectedShard, Pyramid, content_hash, hash_width, list_expected_shards, parse_key, slot_of

from .shard_index import ShardRecord, now_ms

err = partial(print, file=sys.stderr, flush=True)

DEFAULT_GRACE = timedelta(hours=24)


@dataclass
class Orphan:
    key: str
    slot: str
    mtime: datetime | None


def list_orphans(pyramid: Pyramid, registry_keys: set[str], prefix: str | None = None) -> list[Orphan]:
    """Blobs under `prefix` (default: the template's static prefix) that the
    template can produce and no registry row references."""
    template = pyramid.keyTemplate
    if prefix is None:
        prefix = template.split('{')[0]
    out: list[Orphan] = []
    for key, mtime in pyramid.storage.list_with_mtime(prefix):
        if key in registry_keys:
            continue
        slot = slot_of(template, key)
        if slot is None:
            continue
        out.append(Orphan(key=key, slot=slot, mtime=mtime))
    return out


@dataclass
class GcResult:
    deleted: list[str] = field(default_factory=list)
    kept_young: list[str] = field(default_factory=list)   # orphans inside the grace period
    dry_run: bool = True

    def summary(self) -> str:
        verb = 'would delete' if self.dry_run else 'deleted'
        return f"gc: {verb} {len(self.deleted)} orphan(s), kept {len(self.kept_young)} inside the grace period"


def gc_orphans(
    pyramid: Pyramid,
    registry_keys: set[str],
    *,
    grace: timedelta = DEFAULT_GRACE,
    now: datetime | None = None,
    apply: bool = False,
    prefix: str | None = None,
) -> GcResult:
    """Delete (or, dry-run, list) orphans older than `grace`. A blob without
    an mtime is never deleted (can't prove it is old)."""
    now = now or datetime.now(timezone.utc)
    result = GcResult(dry_run=not apply)
    for o in list_orphans(pyramid, registry_keys, prefix):
        if o.mtime is None or now - o.mtime < grace:
            result.kept_young.append(o.key)
            continue
        if apply:
            pyramid.storage.delete(o.key)
        result.deleted.append(o.key)
    return result


def adopt_unregistered(
    pyramid: Pyramid,
    shard_index,
    pyramid_name: str,
    time_range: tuple[datetime, datetime],
    *,
    filter: dict | None = None,
) -> list[ShardRecord]:
    """Register, for every expected slot with no registry row, the newest
    listed blob of that slot whose bytes still hash to its key (a partial or
    foreign object is skipped). Returns the records adopted."""
    template = pyramid.keyTemplate
    width = hash_width(template)
    if width is None:
        raise ValueError("adopt_unregistered: the keyTemplate has no {hash} token; use reconcile_registrations")
    registered = shard_index.existing_keys()
    orphans_by_slot: dict[str, list[Orphan]] = {}
    for o in list_orphans(pyramid, registered):
        orphans_by_slot.setdefault(o.slot, []).append(o)
    registered_slots = {slot_of(template, k) for k in registered}
    adopted: list[ShardRecord] = []
    for e in list_expected_shards(pyramid, time_range, filter=filter):
        if e.key in registered_slots or e.key not in orphans_by_slot:
            continue
        candidates = sorted(orphans_by_slot[e.key], key=lambda o: (o.mtime or datetime.min.replace(tzinfo=timezone.utc)), reverse=True)
        for o in candidates:
            blob = pyramid.storage.get(o.key)
            if blob is None:
                continue
            md5 = content_hash(blob)
            if parse_key(template, o.key)[HASH_GROUP][:width] != md5[:width]:
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


HASH_GROUP = 'hash'


def _slot_of_expected(e: ExpectedShard) -> str:
    return e.key

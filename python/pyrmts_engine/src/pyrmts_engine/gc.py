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
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from functools import partial

from pyrmts import Pyramid, content_hash, hash_width, list_expected_shards, parse_key, slot_of, template_has_hash

from .discovery import slot_for_record
from .shard_index import ShardRecord, now_ms

err = partial(print, file=sys.stderr, flush=True)

DEFAULT_GRACE = timedelta(hours=24)


@dataclass
class Orphan:
    key: str
    slot: str
    mtime: datetime | None


def _require_hashed(pyramid: Pyramid, what: str) -> None:
    if not template_has_hash(pyramid.keyTemplate):
        raise ValueError(
            f"{what}: keyTemplate {pyramid.keyTemplate!r} has no {{hash}} token — its keys are mutable and "
            f"template-derived, so a listing minus a registry is not a set of orphans"
        )
    if not any(m in pyramid.keyTemplate for m in ('{tier}', '{shard}', '{period}')):
        raise ValueError(
            f"{what}: keyTemplate {pyramid.keyTemplate!r} has no slot placeholders — a pure content-addressed "
            f"layout can be shared by several pyramids, so one registry cannot say what is orphaned; not supported"
        )


def _registry_keys(shard_index, pyramid_name: str | None) -> set[str]:
    current_records = getattr(shard_index, 'current_records', None)
    if current_records is None:
        raise ValueError("a registry that can list its rows (current_records()) is required")
    return {r.key for r in current_records(pyramid_name)}


def list_orphans(pyramid: Pyramid, registry_keys: set[str], prefix: str | None = None) -> list[Orphan]:
    """Hashed-shape blobs under `prefix` (default: the template's static
    prefix) that no registry row references."""
    _require_hashed(pyramid, 'list_orphans')
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
    kept_young: list[str] = field(default_factory=list)      # orphans inside the grace period
    kept_repointed: list[str] = field(default_factory=list)  # re-referenced between the listing and the delete
    dry_run: bool = True

    def summary(self) -> str:
        verb = 'would delete' if self.dry_run else 'deleted'
        return (
            f"gc: {verb} {len(self.deleted)} orphan(s), kept {len(self.kept_young)} inside the grace period"
            + (f", {len(self.kept_repointed)} re-pointed since the listing" if self.kept_repointed else '')
        )


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
    """Delete (or, dry-run, list) orphans older than `grace`.

    Safety: a hashless template is refused; an empty registry is refused
    (nothing would be an orphan except everything); a blob without an mtime is
    never deleted; and because `put_shard` can re-point a slot at an *old*
    orphan (identical bytes → the existing object is reused, its mtime
    untouched), the registry is re-read right before deleting and any key it
    now references is kept. The residual window is the moment between that
    re-read and the delete — keep `grace` well above a fill's duration."""
    _require_hashed(pyramid, 'gc_orphans')
    now = now or datetime.now(timezone.utc)
    registered = _registry_keys(shard_index, pyramid_name)
    if not registered:
        raise ValueError(
            "gc_orphans: the registry has no rows for this pyramid — refusing (every blob would be an orphan); "
            "run `adopt` first, or check the registry / pyramid name"
        )
    result = GcResult(dry_run=not apply)
    candidates = [o for o in list_orphans(pyramid, registered, prefix)]
    old = [o for o in candidates if o.mtime is not None and now - o.mtime >= grace]
    result.kept_young = [o.key for o in candidates if o not in old]
    if not old:
        return result
    if apply:
        registered = _registry_keys(shard_index, pyramid_name)   # fresh: catch a re-point since the listing
    for o in old:
        if o.key in registered:
            result.kept_repointed.append(o.key)
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
    """Register, for every expected slot, the newest listed hashed blob that
    the registry does not point at and that is newer than the slot's row (or
    the slot has no row), if its bytes still hash to its key. Heals both a
    first write and a rewrite whose registration was lost. Returns the records
    adopted."""
    _require_hashed(pyramid, 'adopt_unregistered')
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

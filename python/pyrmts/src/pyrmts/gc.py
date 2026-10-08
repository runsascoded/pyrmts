"""Orphan GC for content-hashed shard keys (`specs/done/content-addressed-shards.md`),
registry-agnostic: stdlib + `pyrmts.keys` only, no `Pyramid`, no polars.

With `{hash}` in a keyTemplate a rewrite never overwrites — it writes a new
blob and re-points whatever says which blob is current (a `ShardIndex` row, a
manifest, …). Storage therefore accumulates **orphans**: hashed-shape blobs
nothing references. `gc_orphans` deletes them once older than a grace period
(in-flight reads of the previous version, edge-cache TTL).

What "referenced" means is the caller's: `pyrmts_engine.gc` passes the
registry's current keys; crashes passes the union of its retained manifests'.
`referenced` is a callable because it is read twice — once to plan, and again
right before deleting, since `put_shard` can re-point a slot at an *old*
orphan (identical bytes → the existing object is reused, its mtime untouched).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable

from .keys import HASH, _PLACEHOLDER, slot_of, template_has_hash

DEFAULT_GRACE = timedelta(hours=24)


def require_hashed(template: str, what: str = 'gc') -> None:
    """Orphan GC needs a hash token (keys are immutable, so a listing minus the
    referenced set is the orphan set) and at least one other placeholder (a
    pure content-addressed layout can be shared by several pyramids, so no one
    referenced-set says what is orphaned there)."""
    if not template_has_hash(template):
        raise ValueError(
            f"{what}: keyTemplate {template!r} has no {{hash}} token — its keys are mutable and "
            f"template-derived, so a listing minus a registry is not a set of orphans"
        )
    if not any(m.group(1) != HASH for m in _PLACEHOLDER.finditer(template)):
        raise ValueError(
            f"{what}: keyTemplate {template!r} has no placeholder besides {{hash}} — a pure content-addressed "
            f"layout can be shared by several pyramids, so one referenced-set cannot say what is orphaned; not supported"
        )


@dataclass
class Orphan:
    key: str
    slot: str
    mtime: datetime | None


def list_orphans(
    storage,
    template: str,
    referenced: set[str],
    prefix: str | None = None,
) -> list[Orphan]:
    """Hashed-shape blobs under `prefix` (default: the template's static
    prefix) that `referenced` doesn't contain. Keys not in the template's
    hashed shape (legacy blobs, sidecars, manifests) are never candidates."""
    require_hashed(template, 'list_orphans')
    if prefix is None:
        prefix = template.split('{')[0]
    out: list[Orphan] = []
    for key, mtime in storage.list_with_mtime(prefix):
        if key in referenced:
            continue
        slot = slot_of(template, key)
        if slot is None:
            continue
        out.append(Orphan(key=key, slot=slot, mtime=mtime))
    return out


@dataclass
class GcResult:
    deleted: list[str] = field(default_factory=list)
    kept_young: list[str] = field(default_factory=list)      # orphans inside the grace period (or mtime-less)
    kept_repointed: list[str] = field(default_factory=list)  # re-referenced between the listing and the delete
    dry_run: bool = True

    def summary(self) -> str:
        verb = 'would delete' if self.dry_run else 'deleted'
        return (
            f"gc: {verb} {len(self.deleted)} orphan(s), kept {len(self.kept_young)} inside the grace period"
            + (f", {len(self.kept_repointed)} re-pointed since the listing" if self.kept_repointed else '')
        )


def gc_orphans(
    storage,
    template: str,
    referenced: Callable[[], set[str]],
    *,
    grace: timedelta = DEFAULT_GRACE,
    now: datetime | None = None,
    apply: bool = False,
    prefix: str | None = None,
) -> GcResult:
    """Delete (or, dry-run, list) orphans older than `grace`.

    Safety: a template without a hash token (or with nothing but one) is
    refused; an empty referenced-set is refused (every blob would be an
    orphan — almost always a wrong registry, pyramid name or prefix); a blob
    without an mtime is never deleted; and with `apply`, `referenced()` is
    re-read right before deleting, and any key it now contains is kept. The
    residual window is the moment between that re-read and the delete — keep
    `grace` well above a writer's run time."""
    require_hashed(template, 'gc_orphans')
    now = now or datetime.now(timezone.utc)
    refs = referenced()
    if not refs:
        raise ValueError(
            "gc_orphans: nothing is referenced — refusing (every blob would be an orphan); "
            "check the registry / manifests / prefix, or adopt existing blobs first"
        )
    result = GcResult(dry_run=not apply)
    candidates = list_orphans(storage, template, refs, prefix)
    old = [o for o in candidates if o.mtime is not None and now - o.mtime >= grace]
    old_keys = {o.key for o in old}
    result.kept_young = [o.key for o in candidates if o.key not in old_keys]
    if not old:
        return result
    if apply:
        refs = referenced()   # fresh: catch a re-point since the listing
        if not refs:
            raise ValueError("gc_orphans: the referenced-set came back empty on re-read — refusing to delete")
    for o in old:
        if o.key in refs:
            result.kept_repointed.append(o.key)
            continue
        if apply:
            storage.delete(o.key)
        result.deleted.append(o.key)
    return result

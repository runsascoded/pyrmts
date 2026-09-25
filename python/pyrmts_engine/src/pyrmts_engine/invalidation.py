"""Shard-invalidation — reader-side (`specs/shard-invalidation.md`):
staleness resolution and journal pruning, consumed by the extension-fill
tick (rebuilds overlapping shards **in place**, dependency-ordered, then
prunes spent entries).

The write-side (`Invalidation`, `journal_key`, `load_invalidations`,
`invalidate`) lives in `pyrmts.invalidation` — a lightweight producer
primitive next to `Pyramid`/`Storage`, appendable by streaming-tip
writers without the engine's polars dep tree
(`specs/streaming-tip-writer.md`). Re-exported here for back-compat.
"""
from __future__ import annotations

from datetime import datetime, timezone

from pyrmts import ExpectedShard, Pyramid, template_has_hash
from pyrmts.invalidation import (  # noqa: F401 — back-compat re-exports
    CAS_ATTEMPTS,
    JOURNAL_BASENAME,
    Invalidation,
    _encode,
    invalidate,
    journal_key,
    load_invalidations,
)
from pyrmts.storage import EtagConflict


def overlaps(inv: Invalidation, shard: ExpectedShard) -> bool:
    """Half-open interval overlap — edge-touching periods are excluded."""
    return shard.period_start < inv.end and inv.start < shard.period_end


def stale_keys_for(
    expected: list[ExpectedShard],
    mtimes: dict[str, datetime | None],
    invalidations: list[Invalidation],
) -> set[str]:
    """Keys of expected shards that exist on storage and are overlapped
    by a journal entry newer than their last build. Staleness applies to
    EXPECTED shards only — superseded/stray keys are GC's concern, not
    the fill's. Unknown mtimes are fresh (backends that can't report
    mtimes shouldn't trigger rebuilds — same rule as `split_stale`)."""
    if not invalidations:
        return set()
    return {
        e.key
        for e in expected
        if (mtime := mtimes.get(e.key)) is not None
        and any(overlaps(inv, e) and mtime < inv.requested_at for inv in invalidations)
    }


def slot_write_times(
    pyramid: Pyramid,
    registry_records=None,
    filter: dict | None = None,
) -> dict[str, datetime | None]:
    """When each built slot was last written, keyed by slot key (what
    `ExpectedShard.key` carries) — the clock a journal entry's
    `requested_at` is compared against.

    - Hashless template: the listing's mtime (key == slot; a rebuild
      overwrites in place, so the mtime advances).
    - Hashed template: the registry row's `written_at_ms`. Not the listing:
      it holds orphans, and a rebuild whose bytes come out identical reuses
      the existing object (`put_shard` is put-if-absent), so that object's
      mtime never advances — only re-registration marks the slot rebuilt."""
    if not template_has_hash(pyramid.keyTemplate):
        from .discovery import list_existing_with_mtime
        return list_existing_with_mtime(pyramid)
    if registry_records is None:
        raise ValueError(
            "slot_write_times: a keyTemplate with {hash} needs `registry_records` — the registry "
            "row's written_at, not a listing mtime, is when a slot was last built"
        )
    from .discovery import slot_for_record
    out: dict[str, datetime | None] = {}
    for r in registry_records:
        slot = slot_for_record(pyramid, r, filter)
        if slot is not None:
            out[slot] = (
                datetime.fromtimestamp(r.written_at_ms / 1000, timezone.utc)
                if r.written_at_ms else None
            )
    return out


def prune_spent(
    pyramid: Pyramid,
    expected: list[ExpectedShard],
    *,
    mtimes: dict[str, datetime | None] | None = None,
    within: tuple[datetime, datetime] | None = None,
) -> tuple[int, int]:
    """Drop journal entries with no remaining stale overlap (idempotent
    by construction: replaying a spent entry finds nothing stale). Called
    by the fill driver after it writes. `mtimes` are per-slot write times
    (`slot_write_times`); re-listed unless provided (hashless only — a
    hashed template must pass registry-derived ones).

    `within`: the range `expected` was enumerated over. An entry not
    contained in it is kept — slots it overlaps outside the range were
    never considered, so "nothing stale in `expected`" doesn't make it
    spent. Returns `(n_pruned, n_remaining)`."""
    key = journal_key(pyramid)
    if mtimes is None:
        mtimes = slot_write_times(pyramid)
    for attempt in range(CAS_ATTEMPTS):
        invs, etag = load_invalidations(pyramid)
        if not invs:
            return 0, 0
        keep = [
            inv for inv in invs
            if (within is not None and not (within[0] <= inv.start and inv.end <= within[1]))
            or stale_keys_for(expected, mtimes, [inv])
        ]
        if len(keep) == len(invs):
            return 0, len(invs)
        try:
            pyramid.storage.put_if_match(key, _encode(keep), etag)
        except EtagConflict:
            if attempt == CAS_ATTEMPTS - 1:
                raise
            continue
        return len(invs) - len(keep), len(keep)
    raise AssertionError('unreachable')

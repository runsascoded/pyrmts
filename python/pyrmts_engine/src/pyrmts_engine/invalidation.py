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

from datetime import datetime

from pyrmts import ExpectedShard, Pyramid
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


def prune_spent(
    pyramid: Pyramid,
    expected: list[ExpectedShard],
    *,
    mtimes: dict[str, datetime | None] | None = None,
) -> tuple[int, int]:
    """Drop journal entries with no remaining stale overlap (idempotent
    by construction: replaying a spent entry finds nothing stale). Called
    by the fill driver after it writes. Fresh mtimes are re-listed unless
    provided. Returns `(n_pruned, n_remaining)`."""
    key = journal_key(pyramid)
    if mtimes is None:
        from .discovery import list_existing_with_mtime
        mtimes = list_existing_with_mtime(pyramid)
    for attempt in range(CAS_ATTEMPTS):
        invs, etag = load_invalidations(pyramid)
        if not invs:
            return 0, 0
        keep = [inv for inv in invs if stale_keys_for(expected, mtimes, [inv])]
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

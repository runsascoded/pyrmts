"""First-class shard invalidation (`specs/shard-invalidation.md`): mark
every built shard overlapping an interval stale; the extension-fill tick
rebuilds them **in place** (dependency-ordered, so coarse rebuilds read
repaired fine tiles) and prunes spent entries.

The journal is a small JSON doc next to the shards
(`<pyramid_prefix>_invalidations.json`): a list of
`{start, end, requested_at}` entries (epoch seconds). It lives in object
storage — not the registry — because storage is the source of truth for
discovery (registry rows are reconciled after the fact, and D1 has forked
before: ctbk `docs/incidents/2026-07-28-d1-rest-split-brain.md`).

All journal writes are etag-CAS'd (`Storage.put_if_match`) with a bounded
retry, so a fill-driver prune racing an admin-CLI append can never drop
the append. The journal is emptied in place, never deleted — object
deletes can't be made conditional, so a delete racing an append could
lose it.
"""
from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial

from pyrmts import ExpectedShard, Pyramid
from pyrmts.storage import EtagConflict

err = partial(print, file=sys.stderr, flush=True)

JOURNAL_BASENAME = '_invalidations.json'
CAS_ATTEMPTS = 5


@dataclass(frozen=True)
class Invalidation:
    """One repair request: shards overlapping `[start, end)` that were
    last built before `requested_at` are stale."""
    start: datetime
    end: datetime
    requested_at: datetime


def journal_key(pyramid: Pyramid) -> str:
    """`_invalidations.json` under the keyTemplate's static prefix — next
    to the shards, under the same LIST namespace (inert for gap diffing:
    it never matches an expected key)."""
    return pyramid.keyTemplate.split('{')[0] + JOURNAL_BASENAME


def _utc(epoch_s: float) -> datetime:
    return datetime.fromtimestamp(epoch_s, tz=timezone.utc)


def _encode(invs: list[Invalidation]) -> bytes:
    doc = [
        {
            'start': inv.start.timestamp(),
            'end': inv.end.timestamp(),
            'requested_at': inv.requested_at.timestamp(),
        }
        for inv in invs
    ]
    return (json.dumps(doc) + '\n').encode()


def load_invalidations(pyramid: Pyramid) -> tuple[list[Invalidation], str | None]:
    """`(entries, etag)` — etag for CAS'ing a subsequent rewrite (None
    when the journal doesn't exist yet, i.e. create-only)."""
    blob, etag = pyramid.storage.get_with_etag(journal_key(pyramid))
    if blob is None:
        return [], etag
    return [
        Invalidation(start=_utc(e['start']), end=_utc(e['end']), requested_at=_utc(e['requested_at']))
        for e in json.loads(blob)
    ], etag


def overlaps(inv: Invalidation, shard: ExpectedShard) -> bool:
    """Half-open interval overlap — edge-touching periods are excluded."""
    return shard.period_start < inv.end and inv.start < shard.period_end


def invalidate(
    pyramid: Pyramid,
    interval: tuple[datetime, datetime],
    *,
    now: datetime | None = None,
) -> int:
    """Append `[start, end)` to the pyramid's invalidation journal; the
    next extension-fill tick rebuilds every overlapping built shard in
    place. Returns the journal entry count after the append. (Spent
    entries are pruned by the fill driver, which has the expected-cover
    context this function lacks — deviation from the spec sketch's
    "append+prune".)"""
    start, end = interval
    if not start < end:
        raise ValueError(f"invalidate: empty interval [{start.isoformat()}, {end.isoformat()})")
    entry = Invalidation(
        start=start, end=end,
        requested_at=now or datetime.now(timezone.utc),
    )
    key = journal_key(pyramid)
    for attempt in range(CAS_ATTEMPTS):
        invs, etag = load_invalidations(pyramid)
        try:
            pyramid.storage.put_if_match(key, _encode([*invs, entry]), etag)
        except EtagConflict:
            if attempt == CAS_ATTEMPTS - 1:
                raise
            continue
        return len(invs) + 1
    raise AssertionError('unreachable')


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

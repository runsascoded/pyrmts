"""Shard-invalidation journal — write-side (`specs/shard-invalidation.md`,
`specs/streaming-tip-writer.md`): append `[start, end)` repair requests;
the engine's extension-fill tick rebuilds every overlapping built shard
**in place** and prunes spent entries (reader-side lives in
`pyrmts_engine.invalidation` — it needs the engine's discovery context).

The write-side lives here in core — not the engine package — because any
producer appends to the journal: streaming-tip writers (awair's Lambda,
a CFW cron) mark the intervals they touch without pulling the engine's
polars dep tree.

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
from dataclasses import dataclass
from datetime import datetime, timezone

from .storage import EtagConflict
from .types import Pyramid

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

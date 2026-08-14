"""Shard-invalidation journal, write-side (`specs/streaming-tip-writer.md`
primary ask): a streaming producer appends journal entries with only
`pyrmts` core imports — no engine/polars. This file deliberately imports
nothing from `pyrmts_engine` (awair's Lambda is the reference consumer;
it packages pyrmts without the engine). Reader-side (staleness, prune,
in-place repair) stays in `pyrmts_engine/tests/test_invalidation.py`."""
from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from pyrmts import MemStorage, Metric, Pyramid, Tier
from pyrmts.invalidation import (
    Invalidation,
    _encode,
    invalidate,
    journal_key,
    load_invalidations,
)


def utc(*a: int) -> datetime:
    return datetime(*a, tzinfo=timezone.utc)


T_REQ = utc(2026, 1, 2, 21)
W_START, W_END = utc(2026, 1, 2, 9), utc(2026, 1, 2, 9, 30)
J_KEY = 'pyr/_invalidations.json'


def make_pyramid(storage=None) -> Pyramid:
    return Pyramid(
        storage=storage if storage is not None else MemStorage(),
        keyTemplate='pyr/{tier}/{shard}/{period}.parquet',
        binCol='dt',
        dims=[],
        metrics=[Metric(name='rides', monoid='count')],
        tiers=[Tier(name='q', bin='15min', shards=('2h',))],
    )


def test_invalidate_appends_and_journal_roundtrip():
    pyramid = make_pyramid()
    assert journal_key(pyramid) == J_KEY
    assert invalidate(pyramid, (W_START, W_END), now=T_REQ) == 1
    t2 = utc(2026, 1, 2, 21, 5)
    assert invalidate(pyramid, (utc(2026, 1, 2, 12), utc(2026, 1, 2, 13)), now=t2) == 2
    invs, etag = load_invalidations(pyramid)
    assert invs == [
        Invalidation(start=W_START, end=W_END, requested_at=T_REQ),
        Invalidation(start=utc(2026, 1, 2, 12), end=utc(2026, 1, 2, 13), requested_at=t2),
    ]
    assert etag is not None
    assert json.loads(pyramid.storage.get(J_KEY)) == [
        {'start': W_START.timestamp(), 'end': W_END.timestamp(), 'requested_at': T_REQ.timestamp()},
        {'start': utc(2026, 1, 2, 12).timestamp(), 'end': utc(2026, 1, 2, 13).timestamp(), 'requested_at': t2.timestamp()},
    ]

    with pytest.raises(ValueError) as exc:
        invalidate(pyramid, (W_END, W_START), now=T_REQ)
    assert str(exc.value) == (
        'invalidate: empty interval [2026-01-02T09:30:00+00:00, 2026-01-02T09:00:00+00:00)'
    )


def test_invalidate_cas_retry_preserves_concurrent_append():
    """A prune/append racing this append conflicts the CAS; the retry
    re-reads and lands on top — the concurrent entry is never dropped."""
    other = Invalidation(start=utc(2026, 1, 2, 3), end=utc(2026, 1, 2, 4), requested_at=T_REQ)

    class RacingStorage(MemStorage):
        raced = False

        def put_if_match(self, key, data, etag):
            if key == J_KEY and not self.raced:
                self.raced = True
                # Concurrent writer lands first → our etag is stale.
                self.put(key, _encode([other]))
            super().put_if_match(key, data, etag)

    pyramid = make_pyramid(RacingStorage())
    mine = (utc(2026, 1, 2, 9), utc(2026, 1, 2, 10))
    assert invalidate(pyramid, mine, now=T_REQ) == 2
    invs, _ = load_invalidations(pyramid)
    assert invs == [other, Invalidation(start=mine[0], end=mine[1], requested_at=T_REQ)]

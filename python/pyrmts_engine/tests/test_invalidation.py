"""First-class shard invalidation (`specs/shard-invalidation.md`):
journal append/CAS, overlap staleness, in-place dependency-ordered
repair through `run_extension_fill`, journal prune, expected-only
scoping, byte-identity of repaired shards."""
from __future__ import annotations

import hashlib
import io
import json
from dataclasses import replace
from datetime import datetime, timezone

import polars as pl
import pytest

from pyrmts import EtagConflict, MemStorage, list_expected_shards, write_tier_parquet
from pyrmts_engine import (
    Invalidation,
    MemShardIndex,
    invalidate,
    journal_key,
    load_invalidations,
    run_extension_fill,
    stale_keys_for,
)
from pyrmts_engine.invalidation import _encode

from conftest import FROM, base_wide_frame, make_pyramid, write_base_shards


def utc(*a: int) -> datetime:
    return datetime(*a, tzinfo=timezone.utc)


T18 = utc(2026, 1, 2, 18)
T_INIT = utc(2026, 1, 2, 19)       # mtime of the genesis build
T_REQ = utc(2026, 1, 2, 21)        # invalidation requested
T_REBUILD = utc(2026, 1, 2, 22)    # mtime of the repair tick

# The late-arriving window: inside q@2h [08,10) and h@3h [09,12);
# edge-touching h@3h [06,09) (ends exactly at 09:00) must NOT match.
W_START, W_END = utc(2026, 1, 2, 9), utc(2026, 1, 2, 9, 30)
Q_KEY = 'pyr/q/2h/2026-01-02T08.parquet'
H_KEY = 'pyr/h/3h/2026-01-02T09.parquet'
J_KEY = 'pyr/_invalidations.json'


def make_ladder(storage: MemStorage):
    """2-tier q(15min)@2h / h(1h)@3h ladder over `storage`."""
    p = make_pyramid(storage=storage)
    return replace(p, tiers=[
        replace(p.tiers[0], shards=('2h',)),
        replace(p.tiers[1], shards=('3h',)),
    ])


def raw_fill(hole, now):
    s, e = hole
    return base_wide_frame(
        int(s.timestamp() * 1000), int(e.timestamp() * 1000),
    ).to_arrow()


def build_pyramid(clock: list[datetime], *, late_window_absent: bool):
    """Base q@2h shards over [FROM, 18:00) + h@3h fills. With
    `late_window_absent`, the [08,10) base shard lacks the rows in
    [`W_START`, `W_END`) — the datum that later 'arrives late'."""
    storage = MemStorage(clock=lambda: clock[0])
    pyramid = make_ladder(storage)
    write_base_shards(pyramid, shard_dur='2h', to=T18)
    if late_window_absent:
        frame = base_wide_frame(
            int(utc(2026, 1, 2, 8).timestamp() * 1000),
            int(utc(2026, 1, 2, 10).timestamp() * 1000),
        ).filter(
            ~(pl.col('dt').is_between(
                int(W_START.timestamp() * 1000),
                int(W_END.timestamp() * 1000),
                closed='left',
            ))
        )
        buf = io.BytesIO()
        write_tier_parquet(frame.to_arrow(), pyramid, out=buf)
        storage.put(Q_KEY, buf.getvalue())
    results = run_extension_fill(
        pyramid, genesis=FROM, now=T18, pyramid_name='test', fill_all=True,
    )
    assert [(r.gap.key, r.status) for r in results] == [
        (f'pyr/h/3h/2026-01-02T{h:02d}.parquet', 'wrote') for h in range(0, 18, 3)
    ]
    return pyramid


def test_overlap_staleness_edge_touching():
    """Only shards genuinely overlapping the interval — and built before
    `requested_at` — are stale; edge-touching periods are excluded, as
    are unknown mtimes and post-request builds."""
    pyramid = make_ladder(MemStorage())
    expected = list_expected_shards(pyramid, (FROM, T18))
    inv = Invalidation(start=W_START, end=W_END, requested_at=T_REQ)
    mtimes = {e.key: T_INIT for e in expected}
    assert stale_keys_for(expected, mtimes, [inv]) == {Q_KEY, H_KEY}
    # Built after the request → fresh.
    assert stale_keys_for(expected, {e.key: T_REBUILD for e in expected}, [inv]) == set()
    # Unknown mtimes → fresh (can't confirm staleness).
    assert stale_keys_for(expected, {e.key: None for e in expected}, [inv]) == set()
    assert stale_keys_for(expected, mtimes, []) == set()


def test_invalidate_appends_and_journal_roundtrip():
    clock = [T_INIT]
    pyramid = make_ladder(MemStorage(clock=lambda: clock[0]))
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

    pyramid = make_ladder(RacingStorage())
    mine = (utc(2026, 1, 2, 9), utc(2026, 1, 2, 10))
    assert invalidate(pyramid, mine, now=T_REQ) == 2
    invs, _ = load_invalidations(pyramid)
    assert invs == [other, Invalidation(start=mine[0], end=mine[1], requested_at=T_REQ)]


def test_invalidation_repair_end_to_end():
    """The full loop: late datum → `invalidate` its interval → the next
    fill tick rebuilds exactly the overlapping shards, in place,
    fine-before-coarse — byte-identical to a from-scratch build with the
    datum present — prunes the spent journal entry, touches nothing
    else (including non-expected stray keys), and re-runs as a no-op."""
    ref = build_pyramid([T_INIT], late_window_absent=False)
    clock = [T_INIT]
    pyramid = build_pyramid(clock, late_window_absent=True)
    storage = pyramid.storage
    assert storage.get(Q_KEY) != ref.storage.get(Q_KEY)
    assert storage.get(H_KEY) != ref.storage.get(H_KEY)
    initial_keys = set(storage.list('pyr/'))

    # A key outside the expected cover (1d isn't a ladder rung) that
    # overlaps the interval with a pre-request mtime: staleness applies
    # to expected shards only — it must be left alone (GC's concern).
    stray_key = 'pyr/h/1d/2026-01-02.parquet'
    storage.put(stray_key, b'stray')

    index = MemShardIndex()
    invalidate(pyramid, (W_START, W_END), now=T_REQ)

    clock[0] = T_REBUILD
    results = run_extension_fill(
        pyramid, genesis=FROM, now=T18, pyramid_name='test',
        fill_all=True, raw_fill=raw_fill, shard_index=index,
    )
    assert [(r.gap.key, r.status) for r in results] == [
        (Q_KEY, 'wrote'),   # fine before coarse: the h rebuild reads the repaired q tile
        (H_KEY, 'wrote'),
    ]
    assert storage.get(Q_KEY) == ref.storage.get(Q_KEY)
    assert storage.get(H_KEY) == ref.storage.get(H_KEY)

    # In-place re-registration: same keys, fresh md5s matching storage.
    assert [(r.key, r.md5) for r in index.records] == [
        (Q_KEY, hashlib.md5(storage.get(Q_KEY)).hexdigest()),
        (H_KEY, hashlib.md5(storage.get(H_KEY)).hexdigest()),
    ]

    # Spent entry pruned (journal emptied in place, never deleted); every
    # other object untouched (bytes and mtimes).
    invs, _ = load_invalidations(pyramid)
    assert invs == []
    assert storage.get(J_KEY) == b'[]\n'
    assert dict(storage.list_with_mtime('pyr/')) == {
        **{k: T_INIT for k in initial_keys - {Q_KEY, H_KEY}},
        Q_KEY: T_REBUILD,
        H_KEY: T_REBUILD,
        J_KEY: T_REBUILD,
        stray_key: T_INIT,
    }
    assert storage.get(stray_key) == b'stray'

    # Idempotent: replaying finds nothing stale, writes nothing.
    clock[0] = utc(2026, 1, 2, 23)
    assert run_extension_fill(
        pyramid, genesis=FROM, now=T18, pyramid_name='test',
        fill_all=True, raw_fill=raw_fill, shard_index=index,
    ) == []
    assert dict(storage.list_with_mtime('pyr/'))[Q_KEY] == T_REBUILD
    assert [r.key for r in index.records] == [Q_KEY, H_KEY]


def test_repair_registers_changed_md5_vs_initial_build():
    """Across the initial build and the repair, the h shard registers
    twice: same key, different md5 — the registry-side version signal
    consumers cache-bust on."""
    clock = [T_INIT]
    storage = MemStorage(clock=lambda: clock[0])
    pyramid = make_ladder(storage)
    write_base_shards(pyramid, shard_dur='2h', to=T18)
    index = MemShardIndex()
    run_extension_fill(
        pyramid, genesis=FROM, now=T18, pyramid_name='test',
        fill_all=True, shard_index=index,
    )
    invalidate(pyramid, (W_START, W_END), now=T_REQ)
    clock[0] = T_REBUILD
    # The 'late datum': the q source shard's content changes on rebuild
    # (raw_fill doubles the rides count), so the h consolidation's bytes
    # change too.
    def raw_fill_v2(hole, now):
        s, e = hole
        return base_wide_frame(
            int(s.timestamp() * 1000), int(e.timestamp() * 1000),
        ).with_columns(pl.col('rides') * 2).to_arrow()

    run_extension_fill(
        pyramid, genesis=FROM, now=T18, pyramid_name='test',
        fill_all=True, raw_fill=raw_fill_v2, shard_index=index,
    )
    h_recs = [r for r in index.records if r.key == H_KEY]
    assert [(r.key, r.md5 == hashlib.md5(storage.get(H_KEY)).hexdigest()) for r in h_recs] == [
        (H_KEY, False),   # initial build (superseded content)
        (H_KEY, True),    # repair
    ]

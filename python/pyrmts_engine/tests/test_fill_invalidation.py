"""`build_local(fill=True)` honors the invalidation journal
(`specs/build-local-honor-invalidations.md`): a built expected shard
overlapping an entry newer than its last write is rebuilt — every rung, so
the refold reaches coarse tiers — and spent entries are pruned. Before this,
only `run_extension_fill` read the journal, so ctbk's Batch `-f` fills
silently skipped the month-boundary spillback its `rides-extend` journaled."""
from __future__ import annotations

import io
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import polars as pl

from pyrmts import MemStorage, put_shard, slot_of, write_tier_parquet
from pyrmts_engine import MemShardIndex, WideShardSource, build_local, invalidate, load_invalidations

from conftest import FROM, TO, base_wide_frame, make_pyramid, write_base_shards
from test_raw_source import TO_OPEN_DAY, DailyEventSource, _write_raw_days


def utc(*a: int) -> datetime:
    return datetime(*a, tzinfo=timezone.utc)


T_INIT = utc(2026, 1, 9)       # the original build
T_REQ = utc(2026, 1, 10)       # the invalidation is journaled
T_REBUILD = utc(2026, 1, 11)   # the fill that should honor it

# Every non-source expected shard of the fixture ladder over [FROM, TO) whose
# period overlaps [Jan 4, Jan 5): the fine rung and the coarse ones above it.
JAN4 = (utc(2026, 1, 4), utc(2026, 1, 5))
JAN4_KEYS = [
    'pyr/d/4d/2026-01-03.parquet',
    'pyr/h/4d/2026-01-03.parquet',
    'pyr/q/1d/2026-01-04.parquet',
]


def _clocked_build(clock: list[datetime], **kw):
    pyramid = make_pyramid(storage=MemStorage(clock=lambda: clock[0]))
    write_base_shards(pyramid)
    result = build_local(pyramid, (FROM, TO), WideShardSource(pyramid, shard_dur='6h'), pyramid_name='test', **kw)
    return pyramid, result


def _fill(pyramid, **kw):
    return build_local(pyramid, (FROM, TO), WideShardSource(pyramid, shard_dur='6h'), pyramid_name='test', fill=True, **kw)


def test_hashless_fill_rebuilds_overlapping_shards_and_prunes_the_entry():
    clock = [T_INIT]
    pyramid, full = _clocked_build(clock)
    before = {k: pyramid.storage.get(k) for k in pyramid.storage.list('pyr/')}
    invalidate(pyramid, JAN4, now=T_REQ)

    clock[0] = T_REBUILD
    result = _fill(pyramid)
    assert sorted(w.key for w in result.written) == JAN4_KEYS
    assert (result.invalidated, result.present_shards) == (3, len(full.written) - 3)
    mtimes = dict(pyramid.storage.list_with_mtime('pyr/'))
    assert sorted(k for k, t in mtimes.items() if t == T_REBUILD and k.endswith('.parquet')) == JAN4_KEYS
    # Same source → same bytes; the point is that they were rewritten.
    assert {k: pyramid.storage.get(k) for k in before} == before
    assert load_invalidations(pyramid)[0] == []

    # Spent: a second fill rebuilds nothing.
    again = _fill(pyramid)
    assert (again.written, again.invalidated) == ([], 0)


def test_entry_requested_before_the_last_write_rebuilds_nothing():
    """The shards were (re)built after the entry was journaled — already
    refolded — so the entry is spent as-is and pruned without a walk."""
    clock = [T_REQ]
    pyramid = make_pyramid(storage=MemStorage(clock=lambda: clock[0]))
    write_base_shards(pyramid)
    invalidate(pyramid, JAN4, now=T_INIT)
    build_local(pyramid, (FROM, TO), WideShardSource(pyramid, shard_dur='6h'), pyramid_name='test')

    clock[0] = T_REBUILD
    result = _fill(pyramid)
    assert (result.written, result.invalidated, result.windows) == ([], 0, 0)
    assert load_invalidations(pyramid)[0] == []


def test_ignoring_invalidations_rebuilds_nothing_and_keeps_the_entry():
    clock = [T_INIT]
    pyramid, _ = _clocked_build(clock)
    invalidate(pyramid, JAN4, now=T_REQ)
    clock[0] = T_REBUILD
    result = _fill(pyramid, honor_invalidations=False)
    assert (result.written, result.invalidated) == ([], 0)
    assert [(i.start, i.end) for i in load_invalidations(pyramid)[0]] == [JAN4]


def test_entry_reaching_outside_the_range_is_rebuilt_but_kept():
    """A fill over [FROM, TO) can't vouch for slots past `to`, so an entry
    straddling `to` stays journaled even though its in-range shards were
    rebuilt."""
    clock = [T_INIT]
    pyramid, _ = _clocked_build(clock)
    straddle = (utc(2026, 1, 7), utc(2026, 1, 9))
    invalidate(pyramid, straddle, now=T_REQ)
    clock[0] = T_REBUILD
    result = _fill(pyramid)
    # (d@4d over Jan 7 runs past `to`, so it isn't expected in this range.)
    assert sorted(w.key for w in result.written) == [
        'pyr/h/1d/2026-01-07.parquet',
        'pyr/q/1d/2026-01-07.parquet',
    ]
    assert [(i.start, i.end) for i in load_invalidations(pyramid)[0]] == [straddle]


def test_hashed_fill_registers_new_keys_for_exactly_the_overlapping_slots():
    """Hashed template: "built when" is the registry row's `written_at`, not
    the listing. The same source rebuilds identical bytes, so the keys don't
    change — only re-registration marks the slots rebuilt (which is exactly
    why a listing mtime can't be the clock: an identical-bytes put is a
    no-op). A second, changed-content rebuild then leaves the old keys as
    orphans for `gc`."""
    pyramid = make_pyramid()
    pyramid.keyTemplate = pyramid.keyTemplate.replace('.parquet', '.{hash:12}.parquet')
    write_base_shards(pyramid)
    index = MemShardIndex()
    build_local(pyramid, (FROM, TO), WideShardSource(pyramid, shard_dur='6h'), pyramid_name='test', shard_index=index)
    # Back-date the build so the (past-dated) entry postdates it.
    t_init_ms = int(T_INIT.timestamp() * 1000)
    index.records = [replace(r, written_at_ms=t_init_ms) for r in index.records]
    keys_before = {(r.tier, r.shard_dur, r.period_start_ms): r.key for r in index.current_records('test')}
    invalidate(pyramid, JAN4, now=T_REQ)

    result = _fill(pyramid, shard_index=index)
    rebuilt = sorted(w.key.split('.')[0] + '.parquet' for w in result.written)
    assert (rebuilt, result.invalidated) == (JAN4_KEYS, 3)
    current = index.current_records('test')
    fresh = sorted(r.key.split('.')[0] + '.parquet' for r in current if r.written_at_ms > t_init_ms)
    assert fresh == JAN4_KEYS
    assert {(r.tier, r.shard_dur, r.period_start_ms): r.key for r in current} == keys_before
    assert load_invalidations(pyramid)[0] == []


def test_hashed_rebuild_with_new_content_orphans_the_old_keys():
    pyramid = make_pyramid()
    pyramid.keyTemplate = pyramid.keyTemplate.replace('.parquet', '.{hash:12}.parquet')
    write_base_shards(pyramid)
    index = MemShardIndex()
    build_local(pyramid, (FROM, TO), WideShardSource(pyramid, shard_dur='6h'), pyramid_name='test', shard_index=index)
    t_init_ms = int(T_INIT.timestamp() * 1000)
    index.records = [replace(r, written_at_ms=t_init_ms) for r in index.records]
    old = {r.key for r in index.current_records('test')}
    # Late data: the Jan-4 00h base tile is replaced by one missing cell 'b'.
    slot = 'pyr/q/6h/2026-01-04T00.{hash:12}.parquet'
    stale_base, = [k for k in pyramid.storage.list('pyr/q/6h/') if slot_of(pyramid.keyTemplate, k) == slot]
    pyramid.storage._data.pop(stale_base)
    s_ms, e_ms = (int(t.timestamp() * 1000) for t in (utc(2026, 1, 4), utc(2026, 1, 4, 6)))
    buf = io.BytesIO()
    write_tier_parquet(base_wide_frame(s_ms, e_ms).filter(pl.col('cell') == 'a').to_arrow(), pyramid, out=buf)
    put_shard(pyramid.storage, pyramid.keyTemplate, {'tier': 'q', 'shard': '6h', 'period': '2026-01-04T00'}, buf.getvalue())
    invalidate(pyramid, JAN4, now=T_REQ)

    result = _fill(pyramid, shard_index=index)
    new = {w.key for w in result.written}
    assert sorted(k.split('.')[0] + '.parquet' for k in new) == JAN4_KEYS
    assert new.isdisjoint(old)
    current = {r.key for r in index.current_records('test')}
    orphans = sorted(k.split('.')[0] + '.parquet' for k in old - current)
    assert orphans == JAN4_KEYS
    assert set(pyramid.storage.list('pyr/')) >= old   # orphans stay on storage for `gc`


def test_overlapping_shard_over_an_absent_open_tile_defers_and_keeps_its_entry():
    """Open-period deferral still applies to invalidated shards: a stale shard
    whose source tile is absent-and-open is not rebuilt (that would write 0
    rows over it) and its entry stays journaled; a second entry whose shards
    all rebuilt is pruned alongside."""
    clock = [T_INIT]
    pyramid = make_pyramid(storage=MemStorage(clock=lambda: clock[0]))
    _write_raw_days(pyramid.storage, to=TO + timedelta(days=1))
    src = DailyEventSource(pyramid)
    first = build_local(pyramid, (FROM, TO_OPEN_DAY), src, pyramid_name='test', fill=True, window='3h')
    assert len(first.written) == 13
    pyramid.storage._data.pop('raw/2026-01-08.json')
    open_entry = (TO, TO_OPEN_DAY)
    invalidate(pyramid, (utc(2026, 1, 3), utc(2026, 1, 3, 6)), now=T_REQ)
    invalidate(pyramid, open_entry, now=T_REQ)

    clock[0] = T_REBUILD
    result = build_local(pyramid, (FROM, TO_OPEN_DAY), src, pyramid_name='test', fill=True, window='3h')
    assert sorted(w.key for w in result.written) == [
        'pyr/d/4d/2026-01-03.parquet',
        'pyr/h/4d/2026-01-03.parquet',
        'pyr/q/1d/2026-01-03.parquet',
    ]
    assert (result.invalidated, result.deferred) == (5, 2)   # 3 rebuilt + 2 deferred
    assert [(i.start, i.end) for i in load_invalidations(pyramid)[0]] == [open_entry]


def test_filtered_fill_rebuilds_but_never_prunes():
    """The journal sits under the template's static prefix, shared by every
    filter value, so a filtered fill can't know the entry is spent for the
    others: it rebuilds its own shards and leaves the entry."""
    clock = [T_INIT]
    pyramid, _ = _clocked_build(clock)
    invalidate(pyramid, JAN4, now=T_REQ)
    clock[0] = T_REBUILD
    # The fixture template has no filter placeholder; a filter value is
    # accepted (unused in substitution), which is all this needs.
    result = _fill(pyramid, filter={'device': 'a'})
    assert sorted(w.key for w in result.written) == JAN4_KEYS
    assert [(i.start, i.end) for i in load_invalidations(pyramid)[0]] == [JAN4]


def test_hashed_extension_fill_keeps_an_entry_it_did_not_rebuild():
    """The same clock on the Lambda path: `run_extension_fill`'s prune used a
    LIST keyed by storage key, which never matches a hashed slot key, so every
    entry looked spent and was dropped — even with nothing rebuilt."""
    from pyrmts_engine import run_extension_fill
    pyramid = make_pyramid()
    pyramid.keyTemplate = pyramid.keyTemplate.replace('.parquet', '.{hash:12}.parquet')
    write_base_shards(pyramid)
    index = MemShardIndex()
    build_local(pyramid, (FROM, TO), WideShardSource(pyramid, shard_dur='6h'), pyramid_name='test', shard_index=index)
    t_init_ms = int(T_INIT.timestamp() * 1000)
    index.records = [replace(r, written_at_ms=t_init_ms) for r in index.records]
    invalidate(pyramid, JAN4, now=T_REQ)

    run_extension_fill(pyramid, genesis=FROM, pyramid_name='test', now=TO, shard_index=index, fill_limit=0)
    assert [(i.start, i.end) for i in load_invalidations(pyramid)[0]] == [JAN4]

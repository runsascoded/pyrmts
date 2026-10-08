"""`pyrmts.gc`: registry-agnostic orphan GC over content-hashed keys
(`specs/done/core-gc-and-pyarrow-range.md`). The referenced set is a
callable, so any "what is live" source works — the engine's registry, or a
consumer's retained manifests (crashes)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from pyrmts import MemStorage, put_shard
from pyrmts.gc import GcResult, Orphan, gc_orphans, list_orphans, require_hashed

# crashes' shape: no time-slot placeholders, just `{level}` / `{shard}` + hash.
TEMPLATE = 's2_pyramid/s2_l{level}/{shard}.{hash:12}.parquet'
T0 = datetime(2026, 9, 1, tzinfo=timezone.utc)
LATER = T0 + timedelta(days=3)


def _store(clock: list[datetime]) -> tuple[MemStorage, dict[str, str]]:
    """Two levels × two versions of shard `0`, plus a manifest and a legacy
    (hashless-shape) blob that must never be GC candidates."""
    storage = MemStorage(clock=lambda: clock[0])
    keys = {}
    for level in (5, 6):
        for version in ('v1', 'v2'):
            w = put_shard(storage, TEMPLATE, {'level': level, 'shard': '0'}, f'{level}-{version}'.encode())
            keys[f'l{level}-{version}'] = w.key
    storage.put('s2_pyramid/manifest.json', b'{}')
    storage.put('s2_pyramid/s2_l5/0.parquet', b'legacy')
    return storage, keys


def test_require_hashed_wants_a_hash_and_another_placeholder():
    require_hashed(TEMPLATE)
    require_hashed('pyr/{tier}/{shard}/{period}.{hash}.parquet')
    with pytest.raises(ValueError, match=r'has no \{hash\} token'):
        require_hashed('pyr/{tier}/{shard}/{period}.parquet')
    with pytest.raises(ValueError, match='no placeholder besides'):
        require_hashed('cas/{hash}.parquet')


def test_list_orphans_skips_referenced_and_non_hashed_shape_keys():
    clock = [T0]
    storage, k = _store(clock)
    live = {k['l5-v2'], k['l6-v2']}
    assert sorted(list_orphans(storage, TEMPLATE, live), key=lambda o: o.key) == sorted([
        Orphan(key=k['l5-v1'], slot='s2_pyramid/s2_l5/0.{hash:12}.parquet', mtime=T0),
        Orphan(key=k['l6-v1'], slot='s2_pyramid/s2_l6/0.{hash:12}.parquet', mtime=T0),
    ], key=lambda o: o.key)


def test_gc_dry_run_then_apply_deletes_only_old_unreferenced_hashed_blobs():
    clock = [T0]
    storage, k = _store(clock)
    clock[0] = T0 + timedelta(days=2, hours=12)   # 12h before LATER: inside a 1-day grace
    young = put_shard(storage, TEMPLATE, {'level': 5, 'shard': '1'}, b'young').key
    live = {k['l5-v2'], k['l6-v2']}

    dry = gc_orphans(storage, TEMPLATE, lambda: live, grace=timedelta(days=1), now=LATER)
    assert dry == GcResult(deleted=[k['l5-v1'], k['l6-v1']], kept_young=[young], dry_run=True)
    assert sorted(storage.list('s2_pyramid/')) == sorted([*k.values(), young, 's2_pyramid/manifest.json', 's2_pyramid/s2_l5/0.parquet'])

    applied = gc_orphans(storage, TEMPLATE, lambda: live, grace=timedelta(days=1), now=LATER, apply=True)
    assert applied == GcResult(deleted=[k['l5-v1'], k['l6-v1']], kept_young=[young], dry_run=False)
    assert sorted(storage.list('s2_pyramid/')) == sorted([*live, young, 's2_pyramid/manifest.json', 's2_pyramid/s2_l5/0.parquet'])


def test_gc_keeps_a_blob_re_referenced_before_the_delete():
    """`put_shard` can re-point a slot at an old orphan (identical bytes reuse
    the existing object), so the referenced set is re-read before deleting."""
    clock = [T0]
    storage, k = _store(clock)
    reads = []

    def referenced() -> set[str]:
        reads.append(1)
        live = {k['l5-v2'], k['l6-v2']}
        return live if len(reads) == 1 else live | {k['l5-v1']}

    res = gc_orphans(storage, TEMPLATE, referenced, grace=timedelta(days=1), now=LATER, apply=True)
    assert (len(reads), res) == (2, GcResult(deleted=[k['l6-v1']], kept_repointed=[k['l5-v1']], dry_run=False))
    assert storage.get(k['l5-v1']) == b'5-v1'


def test_gc_never_deletes_an_mtime_less_blob():
    class NoMtimes(MemStorage):
        def list_with_mtime(self, prefix):
            return [(key, None) for key, _ in super().list_with_mtime(prefix)]

    storage = NoMtimes()
    old = put_shard(storage, TEMPLATE, {'level': 5, 'shard': '0'}, b'old').key
    live = put_shard(storage, TEMPLATE, {'level': 5, 'shard': '0'}, b'new').key
    res = gc_orphans(storage, TEMPLATE, lambda: {live}, grace=timedelta(0), now=LATER, apply=True)
    assert res == GcResult(kept_young=[old], dry_run=False)


def test_gc_refuses_an_empty_referenced_set_on_either_read():
    clock = [T0]
    storage, k = _store(clock)
    with pytest.raises(ValueError, match='nothing is referenced'):
        gc_orphans(storage, TEMPLATE, set, apply=True)
    reads = []

    def flaky() -> set[str]:
        reads.append(1)
        return {k['l5-v2']} if len(reads) == 1 else set()

    with pytest.raises(ValueError, match='came back empty on re-read'):
        gc_orphans(storage, TEMPLATE, flaky, grace=timedelta(0), now=LATER, apply=True)
    assert sorted(storage.list('s2_pyramid/s2_l6/')) == sorted([k['l6-v1'], k['l6-v2']])

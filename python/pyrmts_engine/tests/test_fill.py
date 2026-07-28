"""`fill` mode (specs/engine-fill-mode.md): LIST-diff against the expected
min-cover, build exactly the missing shards (byte-identical to a full
rebuild), walk only the windows feeding them; missing shards the source
can't cover are reported + skipped, not an error."""
from __future__ import annotations

from datetime import datetime, timezone

from pyrmts_engine import MemShardIndex, WideShardSource, build_local

from conftest import FROM, TO, make_pyramid, write_base_shards
from test_engine import _run_engine


def test_fill_rebuilds_exactly_the_deleted_shards():
    """Delete scattered shards across tiers; fill rebuilds exactly those,
    byte-identical, walking only the (non-contiguous) windows that feed
    them."""
    pyramid, _, _ = _run_engine()
    deleted = [
        'pyr/d/4d/2025-12-30.parquet',  # effective [Jan 2, Jan 3)
        'pyr/q/1d/2026-01-02.parquet',  # [Jan 2, Jan 3)
        'pyr/q/1d/2026-01-04.parquet',  # [Jan 4, Jan 5)
    ]
    orig = {k: pyramid.storage.get(k) for k in deleted}
    for k in deleted:
        pyramid.storage._data.pop(k)

    result = build_local(
        pyramid, (FROM, TO), WideShardSource(pyramid),
        pyramid_name='test', fill=True,
    )
    assert sorted(w.key for w in result.written) == deleted
    # Only the Jan-2 and Jan-4 windows feed the gaps — the Jan-3/5/6/7
    # windows are never walked.
    assert (result.windows, result.present_shards, result.unfillable) == (2, 8, 0)
    for k in deleted:
        assert pyramid.storage.get(k) == orig[k], k


def test_fill_extends_the_range():
    """Extend: a complete [FROM, Jan 7) build + fill over [FROM, TO)
    builds only the wider range's new cover tiles, from one window,
    byte-identical to a from-scratch full-range build."""
    t1 = datetime(2026, 1, 7, tzinfo=timezone.utc)
    pyramid = make_pyramid()
    write_base_shards(pyramid)  # base rung covers [FROM, TO)
    build_local(pyramid, (FROM, t1), WideShardSource(pyramid), pyramid_name='test')

    result = build_local(
        pyramid, (FROM, TO), WideShardSource(pyramid),
        pyramid_name='test', fill=True,
    )
    new = ['pyr/h/1d/2026-01-07.parquet', 'pyr/q/1d/2026-01-07.parquet']
    assert sorted(w.key for w in result.written) == new
    assert (result.windows, result.present_shards, result.unfillable) == (1, 9, 0)

    ref, _, _ = _run_engine()
    for k in new:
        assert pyramid.storage.get(k) == ref.storage.get(k), k


def test_fill_unfillable_beyond_source_coverage():
    """Source rung materialized only through Jan 6: missing shards whose
    effective range extends past the source's coverage end are reported +
    skipped (no error — the run still succeeds), the covered ones are
    built byte-identically, and no uncovered window is ever read (so the
    coverage guard sees no misses)."""
    cov_end = datetime(2026, 1, 6, tzinfo=timezone.utc)
    pyramid = make_pyramid()
    write_base_shards(pyramid, to=cov_end)

    result = build_local(
        pyramid, (FROM, TO), WideShardSource(pyramid),
        pyramid_name='test', fill=True,
    )
    assert sorted(w.key for w in result.written) == [
        'pyr/d/4d/2025-12-30.parquet',
        'pyr/h/4d/2025-12-30.parquet',
        'pyr/q/1d/2026-01-02.parquet',
        'pyr/q/1d/2026-01-03.parquet',
        'pyr/q/1d/2026-01-04.parquet',
        'pyr/q/1d/2026-01-05.parquet',
    ]
    assert (result.windows, result.present_shards, result.unfillable) == (4, 0, 5)
    assert result.missing_source == 0

    ref, _, _ = _run_engine()
    for w in result.written:
        assert pyramid.storage.get(w.key) == ref.storage.get(w.key), w.key


def test_fill_noop_on_complete_pyramid():
    """Fill over a complete pyramid is a fast no-op: LIST + no walk, no
    source reads (and no EmptySourceError despite 0 rows)."""
    pyramid, _, _ = _run_engine()
    result = build_local(
        pyramid, (FROM, TO), WideShardSource(pyramid),
        pyramid_name='test', fill=True,
    )
    assert (result.windows, result.written, result.present_shards, result.source_rows) == (0, [], 11, 0)


def test_fill_unions_manifest_into_done_set():
    """A key recorded in the manifest counts as done even when its object
    is gone from storage — `resume` semantics compose with the listing."""
    pyramid, _, index = _run_engine()
    manifested = 'pyr/q/1d/2026-01-04.parquet'
    unmanifested = 'pyr/q/1d/2026-01-02.parquet'
    for k in (manifested, unmanifested):
        pyramid.storage._data.pop(k)
    prior = MemShardIndex(records=[r for r in index.records if r.key == manifested])

    result = build_local(
        pyramid, (FROM, TO), WideShardSource(pyramid),
        pyramid_name='test', shard_index=prior, fill=True,
    )
    assert [w.key for w in result.written] == [unmanifested]
    assert (result.windows, result.present_shards) == (1, 10)


def test_fill_sub_source_tier_and_absent_source_rung_are_unfillable():
    """With a coarser source rung (h@1d): a missing q shard (its bin is
    finer than the source's) and a missing tile of the source rung itself
    are both unfillable — reported, skipped, nothing walked."""
    pyramid, _, _ = _run_engine()
    for k in ('pyr/q/1d/2026-01-04.parquet', 'pyr/h/1d/2026-01-07.parquet'):
        pyramid.storage._data.pop(k)

    result = build_local(
        pyramid, (FROM, TO),
        WideShardSource(pyramid, tier_name='h', shard_dur='1d'),
        pyramid_name='test', fill=True,
    )
    assert (result.written, result.windows, result.unfillable) == ([], 0, 2)

"""Content-equality validation harness (`specs/pyrmts-ops-adoption.md`
phase 3): aligned ranges, streaming compares, covering-shard fallback via
`compare_manifest`."""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

import pytest

from pyrmts import MemStorage
from pyrmts_engine import (
    JsonlShardIndex,
    WideShardSource,
    aligned_range,
    build_local,
    compare_manifest,
    compare_streaming,
    empty_long,
    long_to_wide,
)

from conftest import FROM, TO, make_pyramid, write_base_shards
from test_engine import _run_engine

JAN3 = datetime(2026, 1, 3, tzinfo=timezone.utc)
JAN7 = datetime(2026, 1, 7, tzinfo=timezone.utc)
JAN11 = datetime(2026, 1, 11, tzinfo=timezone.utc)


def test_aligned_range():
    # The 4d grid tile at genesis (Dec 30) straddles it → dropped; the
    # first fully-aligned period starts Jan 3.
    assert aligned_range('4d', 1, FROM, now=TO) == (JAN3, JAN7)
    assert aligned_range('4d', 2, FROM, now=TO) == (JAN3, JAN11)
    with pytest.raises(ValueError) as exc:
        aligned_range('4d', 3, FROM, now=TO)
    assert str(exc.value) == 'only 2 full 4d periods since genesis; wanted 3'


def test_compare_streaming():
    pyramid, _, _ = _run_engine()
    a = pyramid.storage.get('pyr/q/1d/2026-01-04.parquet')
    b = pyramid.storage.get('pyr/q/1d/2026-01-05.parquet')
    assert compare_streaming(a, a, pyramid) == ('equal', '')
    # Same shape (192 rows), different day's content.
    assert compare_streaming(a, b, pyramid) == ('diff', 'content diverges in rows [0, 192)')

    import io
    from pyrmts import write_tier_parquet
    buf = io.BytesIO()
    write_tier_parquet(long_to_wide(empty_long(pyramid), pyramid).to_arrow(), pyramid, out=buf)
    empty = buf.getvalue()
    assert compare_streaming(empty, empty, pyramid) == ('empty_both', '')
    assert compare_streaming(a, empty, pyramid) == ('diff', 'row counts: 192 vs 0')


def test_compare_manifest_with_covering_fallback(tmp_path):
    """A base-ladder build (h stored as 1d tiles) validated against the
    standard fixture build (h min-cover = 4d tiles): exact-key shards
    compare `equal`; h@1d tiles inside a 4d reference tile compare
    `equal_via_cover` through the bin-filtered covering shard."""
    ref, _, _ = _run_engine()

    storage = MemStorage()
    p = make_pyramid(storage=storage)
    base = replace(p, tiers=[p.tiers[0], replace(p.tiers[1], shards=('1d',)), p.tiers[2]])
    write_base_shards(base)
    manifest = tmp_path / 'manifest.jsonl'
    build_local(
        base, (FROM, TO), WideShardSource(base, shard_dur='6h'),
        pyramid_name='test', shard_index=JsonlShardIndex(manifest),
    )

    buckets = compare_manifest(manifest, base, ref)
    assert {k: sorted(v) for k, v in buckets.items()} == {
        'equal': [
            'pyr/d/4d/2025-12-30.parquet',
            'pyr/d/4d/2026-01-03.parquet',
            'pyr/h/1d/2026-01-07.parquet',
            'pyr/q/1d/2026-01-02.parquet',
            'pyr/q/1d/2026-01-03.parquet',
            'pyr/q/1d/2026-01-04.parquet',
            'pyr/q/1d/2026-01-05.parquet',
            'pyr/q/1d/2026-01-06.parquet',
            'pyr/q/1d/2026-01-07.parquet',
        ],
        'equal_via_cover': [
            'pyr/h/4d/2025-12-30.parquet',
            'pyr/h/4d/2026-01-03.parquet',
            'pyr/h/4d/2026-01-03.parquet',
            'pyr/h/4d/2026-01-03.parquet',
            'pyr/h/4d/2026-01-03.parquet',
        ],
        'diff': [],
        'missing': [],
        'empty_both': [],
    }

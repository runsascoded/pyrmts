"""Strict-cascade per-shard materialization (`specs/pyrmts-ops-adoption.md`
phase 2): source-tier selection, cover planning, byte-identical rebuilds,
invariant violations, raw-ingest seam, D1 SQL emission."""
from __future__ import annotations

import hashlib

import pytest

from pyrmts_engine import (
    MaterializeResult,
    discover_gaps,
    emit_d1_insert_sql,
    materialize_shard,
    source_tier_for,
    wide_to_long,
)

from conftest import FROM, TO, base_wide_frame
from test_engine import _run_engine


def test_source_tier_for():
    pyramid, _, _ = _run_engine()
    assert source_tier_for(pyramid, 'q') is None       # base tier: raw territory
    assert source_tier_for(pyramid, 'h').name == 'q'   # 1h % 15min == 0
    assert source_tier_for(pyramid, 'd').name == 'h'   # largest divisor wins


def test_materialize_shard_rebuilds_byte_identical():
    pyramid, _, _ = _run_engine()
    key = 'pyr/h/4d/2026-01-03.parquet'
    orig = pyramid.storage.get(key)
    pyramid.storage._data.pop(key)

    gaps, existing, expected_by_tier = discover_gaps(pyramid, (FROM, TO))
    (gap,) = gaps
    res = materialize_shard(
        pyramid, gap,
        genesis=FROM, key_set=existing, expected_by_tier=expected_by_tier,
    )
    assert (res.status, res.inputs_present, res.inputs_expected, res.source_desc) == (
        'wrote', 4, 4, '/q@1d×4',
    )
    blob = pyramid.storage.get(key)
    assert blob == orig
    assert (res.rows, res.bytes_written, res.md5) == (
        192, len(orig), hashlib.md5(orig).hexdigest(),
    )


def test_materialize_shard_skips_existing():
    pyramid, _, _ = _run_engine()
    gaps, existing, expected_by_tier = discover_gaps(pyramid, (FROM, TO))
    assert gaps == []
    # Force-materialize an already-present shard: key_set short-circuits.
    from pyrmts import list_expected_shards
    gap = next(e for e in list_expected_shards(pyramid, (FROM, TO))
               if e.key == 'pyr/h/4d/2026-01-03.parquet')
    assert materialize_shard(
        pyramid, gap, genesis=FROM, key_set=existing,
    ) == MaterializeResult(gap=gap, status='exists')


def test_materialize_strict_cascade_violation():
    """A missing source-tier shard under the gap raises rather than
    silently under-populating (the pre-strict-cascade incident class)."""
    pyramid, _, _ = _run_engine()
    for k in ('pyr/h/4d/2026-01-03.parquet', 'pyr/q/1d/2026-01-04.parquet'):
        pyramid.storage._data.pop(k)
    gaps, existing, expected_by_tier = discover_gaps(pyramid, (FROM, TO))
    gap = next(g for g in gaps if g.tier == 'h')
    res = materialize_shard(
        pyramid, gap,
        genesis=FROM, key_set=existing, expected_by_tier=expected_by_tier,
    )
    assert res.status == 'error'
    assert res.error == (
        "source: RuntimeError('strict-cascade invariant violation for "
        "/h@4d 2026-01-03: source tier /q has 1 uncovered segment(s): "
        "[2026-01-04T00:00:00+00:00, 2026-01-05T00:00:00+00:00). "
        "Ensure all /q shards in the range are materialized before /h "
        "shards are scheduled.')"
    )


def test_materialize_base_tier_uses_raw_ingest_seam():
    """Base-tier gaps need the injected raw ingester; with one, the
    rebuild is byte-identical to the engine's build of the same shard."""
    pyramid, _, _ = _run_engine()
    key = 'pyr/q/1d/2026-01-04.parquet'
    orig = pyramid.storage.get(key)
    pyramid.storage._data.pop(key)
    gaps, existing, expected_by_tier = discover_gaps(pyramid, (FROM, TO))
    (gap,) = gaps

    res = materialize_shard(
        pyramid, gap,
        genesis=FROM, key_set=existing, expected_by_tier=expected_by_tier,
    )
    assert (res.status, res.source_desc) == ('no_inputs', 'no-raw-ingester')

    def raw_ingest(start, end):
        s_ms = int(start.timestamp() * 1000)
        e_ms = int(end.timestamp() * 1000)
        return wide_to_long(base_wide_frame(s_ms, e_ms), pyramid)

    res = materialize_shard(
        pyramid, gap,
        genesis=FROM, key_set=existing, expected_by_tier=expected_by_tier,
        raw_ingest=raw_ingest,
    )
    assert (res.status, res.source_desc) == ('wrote', 'raw')
    assert pyramid.storage.get(key) == orig


def test_emit_d1_insert_sql(tmp_path):
    pyramid, _, _ = _run_engine()
    from pyrmts import list_expected_shards
    gap = next(e for e in list_expected_shards(pyramid, (FROM, TO))
               if e.key == 'pyr/q/1d/2026-01-04.parquet')
    results = [
        MaterializeResult(gap=gap, status='wrote', bytes_written=10, rows=2),
        MaterializeResult(gap=gap, status='no_inputs'),
    ]
    path = tmp_path / 'd1.sql'
    assert emit_d1_insert_sql('test', results, str(path)) == 1
    assert path.read_text() == (
        "INSERT INTO pyramid_shards "
        "(pyramid, tier, shard_dur, period_start, period_end, key, written_at) "
        "VALUES ('test', 'q', '1d', 1767484800000, 1767571200000, "
        "'pyr/q/1d/2026-01-04.parquet', unixepoch()*1000) "
        "ON CONFLICT (pyramid, tier, shard_dur, period_start) DO UPDATE SET "
        "period_end=excluded.period_end, key=excluded.key, written_at=excluded.written_at;\n"
        "INSERT INTO pyramid_watermarks "
        "(pyramid, tier, shard_dur, latest_period_end, updated_at) "
        "VALUES ('test', 'q', '1d', 1767571200000, unixepoch()*1000) "
        "ON CONFLICT (pyramid, tier, shard_dur) DO UPDATE SET "
        "latest_period_end=MAX(excluded.latest_period_end, pyramid_watermarks.latest_period_end), "
        "updated_at=excluded.updated_at;\n"
    )

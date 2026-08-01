"""Strict-cascade per-shard materialization (`specs/pyrmts-ops-adoption.md`
phase 2): source-tier selection, cover planning, byte-identical rebuilds,
invariant violations, raw-ingest seam, D1 SQL emission. Plus shard
readiness (`buildable_at` — `specs/source-readiness-pending.md`)."""
from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from pyrmts import Tier, shard_periods_covering
from pyrmts_engine import (
    MaterializeResult,
    buildable_at,
    discover_gaps,
    emit_d1_insert_sql,
    materialize_shard,
    source_tier_for,
    wide_to_long,
)

from conftest import FROM, TO, base_wide_frame, make_pyramid
from test_engine import _run_engine


def _ladder(*tiers: Tier):
    """Fixture pyramid with the given tiers (data machinery unused)."""
    return replace(make_pyramid(), tiers=list(tiers))


def test_source_tier_for():
    pyramid, _, _ = _run_engine()
    assert source_tier_for(pyramid, 'q') is None       # base tier: raw territory
    assert source_tier_for(pyramid, 'h').name == 'q'   # 1h % 15min == 0
    assert source_tier_for(pyramid, 'd').name == 'h'   # largest divisor wins


def test_source_tier_for_malformed_ladder():
    pyramid = _ladder(
        Tier(name='2m', bin='2min', shards=('1h',)),
        Tier(name='3m', bin='3min', shards=('1h',)),   # 3min % 2min != 0
    )
    with pytest.raises(AssertionError) as exc:
        source_tier_for(pyramid, '3m')
    assert str(exc.value) == 'no source tier for /3m — pyramid ladder is malformed'


def test_buildable_at():
    utc = lambda *a: datetime(*a, tzinfo=timezone.utc)

    # Fixture ladder is fully aligned: every rung ending is a multiple of
    # its source tier's smallest rung → buildable_at == period_end.
    pyramid, _, _ = _run_engine()
    for tier, end in [
        ('q', utc(2026, 1, 3)),          # base tier: raw territory
        ('h', utc(2026, 1, 3)),          # 1d ends ≡ 0 mod q's 6h
        ('h', utc(2026, 1, 2, 6)),       # sub-day ending, still 6h-aligned
        ('d', utc(2026, 1, 3)),          # 4d ends ≡ 0 mod h's 1d
    ]:
        assert buildable_at(pyramid, tier, end) == end

    # The incident shape (/1h@3h ← /30m, r_min=2h): odd-hour endings wait
    # for the next even hour; midnight endings are aligned.
    incident = _ladder(
        Tier(name='30m', bin='30min', shards=('2h', '6h')),
        Tier(name='1h', bin='1h', shards=('3h', '6h')),
    )
    assert buildable_at(incident, '1h', utc(2026, 7, 31, 21)) == utc(2026, 7, 31, 22)
    assert buildable_at(incident, '1h', utc(2026, 8, 1)) == utc(2026, 8, 1)

    # Two levels of misalignment compound: ceil to 2h (22:00), then the
    # source tile's own source cover ceils to 45min (22:30).
    two_level = _ladder(
        Tier(name='15m', bin='15min', shards=('45min',)),
        Tier(name='30m', bin='30min', shards=('2h',)),
        Tier(name='1h', bin='1h', shards=('3h',)),
    )
    assert buildable_at(two_level, '1h', utc(2026, 7, 31, 21)) == utc(2026, 7, 31, 22, 30)


# The avail-v5 ladder (ctbk `configs/pyramids/avail-v5.yaml`, extended view:
# `shards` + `lambda_shards`) — the parity fixture shared with
# `js/packages/pyrmts/src/cascade-source.test.ts`.
AVAIL_V5_TIERS = [
    ('1m', '1min', ('5min', '10min', '30min', '1h', '3h', '6h', '12h', '1d', '2d')),
    ('2m', '2min', ('10min', '30min', '1h', '3h', '6h', '12h', '1d', '2d', '4d')),
    ('3m', '3min', ('15min', '30min', '1h', '3h', '6h', '12h', '1d', '2d', '4d', '8d')),
    ('5m', '5min', ('15min', '30min', '1h', '3h', '6h', '12h', '1d', '2d', '4d', '8d')),
    ('10m', '10min', ('30min', '1h', '3h', '6h', '12h', '1d', '2d', '4d', '8d', '16d')),
    ('15m', '15min', ('1h', '3h', '6h', '12h', '1d', '2d', '4d', '8d', '16d', '32d')),
    ('30m', '30min', ('2h', '6h', '12h', '1d', '2d', '4d', '8d', '16d', '32d', '64d')),
    ('1h', '1h', ('3h', '6h', '12h', '1d', '2d', '4d', '8d', '16d', '32d', '64d', '128d')),
    ('2h', '2h', ('6h', '12h', '1d', '2d', '4d', '8d', '16d', '32d', '64d', '128d', '256d')),
    ('3h', '3h', ('12h', '1d', '2d', '4d', '8d', '16d', '32d', '64d', '128d', '256d', '512d')),
    ('6h', '6h', ('1d', '2d', '4d', '8d', '16d', '32d', '64d', '128d', '256d', '512d', '1024d')),
    ('12h', '12h', ('2d', '4d', '8d', '16d', '32d', '64d', '128d', '256d', '512d', '1024d', '2048d')),
    ('1d', '1d', ('4d', '8d', '16d', '32d', '64d', '128d', '256d', '512d', '1024d', '2048d')),
    ('3d', '3d', ('12d', '24d', '48d', '96d', '192d', '384d', '768d', '1536d', '3072d')),
    ('7d', '7d', ('28d', '56d', '112d', '224d', '448d', '896d', '1792d', '3584d', '7168d')),
]


def test_buildable_at_avail_v5_enumeration():
    """Sweep every (tier, rung, ending) over 4 days of the avail-v5
    ladder: the ONLY structurally-lagged class is /1h@3h at odd-hour
    endings (source /30m's smallest rung is 2h), each waiting +1h."""
    pyramid = _ladder(*[
        Tier(name=name, bin=bin, shards=shards)
        for name, bin, shards in AVAIL_V5_TIERS
    ])
    start = datetime(2026, 7, 28, tzinfo=timezone.utc)
    stop = start + timedelta(days=4)
    lags = {}
    for name, _, shards in AVAIL_V5_TIERS:
        for rung in shards:
            for period in shard_periods_covering(start, stop, rung):
                at = buildable_at(pyramid, name, period.end)
                if at != period.end:
                    lags[(name, rung, period.end)] = at - period.end
    assert lags == {
        ('1h', '3h', datetime(2026, 7, 28 + d, h, tzinfo=timezone.utc)): timedelta(hours=1)
        for d in range(4)
        for h in (3, 9, 15, 21)
    }


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

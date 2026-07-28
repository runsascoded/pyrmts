"""Min-cover source (specs/engine-min-cover-source.md): `WideShardSource`
with no pinned `shard_dur` reads the source tier as it's actually stored —
largest present tile wins at each instant — and the whole source tier is
treated as externally owned (no rung of it is written)."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pyrmts import shard_periods_covering
from pyrmts_engine import SourceCoverageError, WideShardSource, build_local

from conftest import FROM, TO, make_pyramid, write_base_shards
from test_engine import _run_engine

# The engine's outputs under a min-cover source: every q rung is
# source-owned, so only the h/d tiers are written.
MIN_COVER_KEYS = [
    'pyr/d/4d/2025-12-30.parquet',
    'pyr/d/4d/2026-01-03.parquet',
    'pyr/h/1d/2026-01-07.parquet',
    'pyr/h/4d/2025-12-30.parquet',
    'pyr/h/4d/2026-01-03.parquet',
]


def test_min_cover_mixed_rungs_byte_identical():
    """Base tier stored as a min-cover mix (1d tiles for history + 6h
    tiles toward the tip): the un-pinned source reads it as laid out and
    the h/d outputs are byte-identical to a single-rung build's."""
    ref, _, _ = _run_engine()

    jan5 = datetime(2026, 1, 5, tzinfo=timezone.utc)
    p_mix = make_pyramid()
    write_base_shards(p_mix, '1d', to=jan5)
    write_base_shards(p_mix, '6h', start=jan5)
    src = WideShardSource(p_mix)
    assert src.provides == ('q', None)

    result = build_local(p_mix, (FROM, TO), src, pyramid_name='test')
    assert sorted(w.key for w in result.written) == MIN_COVER_KEYS
    assert result.skipped_rungs == 6  # the q-tier cover (6 × q@1d)
    # 3 × 1d tiles + 12 × 6h tiles selected, none missing.
    assert src.coverage() == (15, [])
    for key in MIN_COVER_KEYS:
        assert p_mix.storage.get(key) == ref.storage.get(key), key


def test_min_cover_ignores_redundant_finer_tiles():
    """A stale finer tile fully covered by a present larger one is
    deterministically ignored: the 1d tile wins its whole day (4 fewer
    6h reads), bytes unchanged."""
    ref, _, _ = _run_engine()

    jan3 = datetime(2026, 1, 3, tzinfo=timezone.utc)
    jan4 = datetime(2026, 1, 4, tzinfo=timezone.utc)
    p_red = make_pyramid()
    write_base_shards(p_red)  # full 6h rung (24 tiles)
    write_base_shards(p_red, '1d', start=jan3, to=jan4)  # redundant larger tile
    src = WideShardSource(p_red)

    result = build_local(p_red, (FROM, TO), src, pyramid_name='test')
    assert sorted(w.key for w in result.written) == MIN_COVER_KEYS
    # 20 × 6h + 1 × 1d — not 24 × 6h.
    assert src.coverage() == (21, [])
    for key in MIN_COVER_KEYS:
        assert p_red.storage.get(key) == ref.storage.get(key), key


def test_min_cover_tip_fill():
    """Source stored as [1d…, 6h, 6h] reaching Jan 6 12:00: fill's
    coverage end tracks the finest tip tile, only shards needing later
    data (plus the source tier's own absent cover tiles) are unfillable,
    and no uncovered window is read."""
    jan6 = datetime(2026, 1, 6, tzinfo=timezone.utc)
    tip = datetime(2026, 1, 6, 12, tzinfo=timezone.utc)
    p_tip = make_pyramid()
    write_base_shards(p_tip, '1d', to=jan6)
    write_base_shards(p_tip, '6h', start=jan6, to=tip)

    result = build_local(
        p_tip, (FROM, TO), WideShardSource(p_tip),
        pyramid_name='test', fill=True,
    )
    assert sorted(w.key for w in result.written) == [
        'pyr/d/4d/2025-12-30.parquet',
        'pyr/h/4d/2025-12-30.parquet',
    ]
    # Unfillable: h/4d/2026-01-03, h/1d/2026-01-07, d/4d/2026-01-03 (all
    # need data past Jan 6 12:00) + the 2 absent q@1d cover tiles
    # (Jan 6/7 — the source tier itself).
    assert (result.windows, result.present_shards, result.unfillable) == (1, 0, 5)
    assert result.missing_source == 0

    ref, _, _ = _run_engine()
    for w in result.written:
        assert p_tip.storage.get(w.key) == ref.storage.get(w.key), w.key


def test_min_cover_mid_range_hole_trips_guard():
    """An instant covered by no rung's tile (Jan 3 absent at 1d AND 6h)
    falls back to finest-rung reads whose misses trip the strict
    coverage guard — holes are never silently clamped."""
    jan3 = datetime(2026, 1, 3, tzinfo=timezone.utc)
    jan4 = datetime(2026, 1, 4, tzinfo=timezone.utc)
    p_hole = make_pyramid()
    write_base_shards(p_hole, '1d', to=jan3)
    write_base_shards(p_hole, '1d', start=jan4)

    missing = [
        f'pyr/q/6h/{p.label}.parquet'
        for p in shard_periods_covering(jan3, jan4, '6h')
    ]
    assert len(missing) == 4
    with pytest.raises(SourceCoverageError) as exc:
        build_local(
            p_hole, (FROM, TO), WideShardSource(p_hole), pyramid_name='test',
        )
    assert str(exc.value) == (
        f"build_local: 4/9 source shards absent (> max_missing_source=0.0): "
        f"{', '.join(missing)} — a real hole (GC'd rung, filter typo, wrong "
        f"rung), not an outage (outage shards are present-but-EMPTY); raise "
        f"max_missing_source / --max-missing if such holes are expected here "
        f"(outputs WERE written/registered)"
    )

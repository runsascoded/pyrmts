"""Same-tier consolidation / extension fill (`specs/pyrmts-ops-adoption.md`
phase 2): sub-rung tiling, overlap covers, hole-fill strategies, the
extension-fill driver + registration, gap wire format."""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

from pyrmts import ExpectedShard, MemStorage, list_expected_shards
from pyrmts_engine import (
    MemShardIndex,
    WideShardSource,
    build_local,
    decode_gap,
    encode_gap,
    materialize_extension_shard,
    overlap_cover,
    run_extension_fill,
    run_single_gap,
    tile_from_existing,
)

from conftest import FROM, TO, base_wide_frame, make_pyramid, write_base_shards
from test_engine import _run_engine

H4D_KEY = 'pyr/h/4d/2026-01-03.parquet'


def make_ladder(storage, *, extended: bool):
    """The lambda_shards shape: base ladder stores h only at 1d; the
    extended view adds the 4d consolidation rung."""
    p = make_pyramid(storage=storage)
    tiers = [
        p.tiers[0],
        replace(p.tiers[1], shards=('1d', '4d') if extended else ('1d',)),
        p.tiers[2],
    ]
    return replace(p, tiers=tiers)


def build_base_ladder():
    """Build the 1d-only ladder (h materialized as 6 × h@1d tiles), then
    return the extended-view pyramid over the same storage."""
    storage = MemStorage()
    base = make_ladder(storage, extended=False)
    write_base_shards(base)
    build_local(
        base, (FROM, TO), WideShardSource(base, shard_dur='6h'), pyramid_name='test',
    )
    return make_ladder(storage, extended=True)


def _h4d_gap(pyramid) -> ExpectedShard:
    return next(e for e in list_expected_shards(pyramid, (FROM, TO)) if e.key == H4D_KEY)


def test_tile_from_existing_picks_sub_rung_tiles():
    pyramid = build_base_ladder()
    key_set = set(pyramid.storage.list('pyr/'))
    tier = pyramid.tier('h')
    picks, holes = tile_from_existing(pyramid, tier, _h4d_gap(pyramid), key_set, genesis=FROM)
    assert picks == [
        ('1d', 'pyr/h/1d/2026-01-03.parquet'),
        ('1d', 'pyr/h/1d/2026-01-04.parquet'),
        ('1d', 'pyr/h/1d/2026-01-05.parquet'),
        ('1d', 'pyr/h/1d/2026-01-06.parquet'),
    ]
    assert holes == []


def test_extension_concat_byte_identical_to_engine_build():
    """The consolidated 4d shard (concat of four engine-built 1d tiles)
    is byte-identical to the engine building the 4d shard directly."""
    ref, _, _ = _run_engine()
    pyramid = build_base_ladder()
    key_set = set(pyramid.storage.list('pyr/'))
    res = materialize_extension_shard(
        pyramid, _h4d_gap(pyramid), key_set=key_set, genesis=FROM,
    )
    assert (res.status, res.inputs_present, res.source_desc) == (
        'wrote', 4, 'same-tier cover ×4',
    )
    assert pyramid.storage.get(H4D_KEY) == ref.storage.get(H4D_KEY)
    assert H4D_KEY in key_set


def test_extension_hole_filled_by_generic_cross_tier_rebin():
    """A missing sub-tile (dust scar) inside the gap is re-binned from
    the finer source tier by the default `cross_tier_rebin` — output
    still byte-identical to the engine's direct build."""
    ref, _, _ = _run_engine()
    pyramid = build_base_ladder()
    pyramid.storage._data.pop('pyr/h/1d/2026-01-04.parquet')
    key_set = set(pyramid.storage.list('pyr/'))
    res = materialize_extension_shard(
        pyramid, _h4d_gap(pyramid), key_set=key_set, genesis=FROM,
    )
    assert (res.status, res.inputs_present, res.inputs_expected) == ('wrote', 4, 4)
    assert pyramid.storage.get(H4D_KEY) == ref.storage.get(H4D_KEY)


def test_overlap_cover_clips_partial_tiles():
    """A source tile merely overlapping the interval is used, clipped to
    its assigned subinterval (the 2026-07-13 evening-wedge class)."""
    pyramid = build_base_ladder()
    key_set = set(pyramid.storage.list('pyr/'))
    jan4_12 = datetime(2026, 1, 4, 12, tzinfo=timezone.utc)
    jan5_12 = datetime(2026, 1, 5, 12, tzinfo=timezone.utc)
    picks, uncovered = overlap_cover(pyramid, pyramid.tier('q'), jan4_12, jan5_12, key_set)
    assert uncovered == []
    assert picks == [
        ('pyr/q/1d/2026-01-04.parquet', jan4_12, datetime(2026, 1, 5, tzinfo=timezone.utc)),
        ('pyr/q/1d/2026-01-05.parquet', datetime(2026, 1, 5, tzinfo=timezone.utc), jan5_12),
    ]


def test_finest_tier_hole_needs_raw_fill():
    """A finest-tier gap whose sub-tiles are gone falls to the raw-fill
    seam: without a strategy → `no_inputs`; with one → byte-identical."""
    pyramid, _, _ = _run_engine()
    key = 'pyr/q/1d/2026-01-04.parquet'
    orig = pyramid.storage.get(key)
    pyramid.storage._data.pop(key)
    for h in (0, 6, 12, 18):  # the day's 6h sub-tiles
        pyramid.storage._data.pop(f'pyr/q/6h/2026-01-04T{h:02d}.parquet')
    key_set = set(pyramid.storage.list('pyr/'))
    gap = next(e for e in list_expected_shards(pyramid, (FROM, TO)) if e.key == key)

    res = materialize_extension_shard(pyramid, gap, key_set=set(key_set), genesis=FROM)
    assert (res.status, res.source_desc) == ('no_inputs', 'no-raw-ingester')

    def raw_fill(hole, now):
        s, e = hole
        return base_wide_frame(
            int(s.timestamp() * 1000), int(e.timestamp() * 1000),
        ).to_arrow()

    res = materialize_extension_shard(
        pyramid, gap, key_set=key_set, genesis=FROM, raw_fill=raw_fill,
    )
    assert res.status == 'wrote'
    assert pyramid.storage.get(key) == orig


def test_run_extension_fill_driver_registers_and_reconciles():
    pyramid = build_base_ladder()
    index = MemShardIndex()
    results = run_extension_fill(
        pyramid,
        genesis=FROM, now=TO, pyramid_name='test',
        shard_index=index, reconcile=True,
    )
    # The extended ladder's h cover wants two 4d consolidations; all
    # other expected shards are present from the base-ladder build.
    assert [(r.gap.key, r.status) for r in results] == [
        ('pyr/h/4d/2025-12-30.parquet', 'wrote'),
        ('pyr/h/4d/2026-01-03.parquet', 'wrote'),
    ]
    ref, _, _ = _run_engine()
    for r in results:
        assert pyramid.storage.get(r.gap.key) == ref.storage.get(r.gap.key), r.gap.key
    # Registry ends complete: 9 present shards reconciled + 2 written.
    assert sorted(rec.key for rec in index.records) == sorted(
        e.key for e in list_expected_shards(pyramid, (FROM, TO))
    )


def test_run_single_gap():
    pyramid = build_base_ladder()
    index = MemShardIndex()
    res = run_single_gap(
        pyramid, _h4d_gap(pyramid),
        genesis=FROM, pyramid_name='test', shard_index=index,
    )
    assert res.status == 'wrote'
    assert [rec.key for rec in index.records] == [H4D_KEY]


def test_encode_decode_gap_roundtrip():
    pyramid, _, _ = _run_engine()
    gap = next(e for e in list_expected_shards(pyramid, (FROM, TO))
               if e.key == 'pyr/d/4d/2025-12-30.parquet')
    payload = encode_gap(gap)
    assert payload == {
        'tier': 'd',
        'shard_dur': '4d',
        'period_start': '2025-12-30T00:00:00+00:00',
        'period_end': '2026-01-03T00:00:00+00:00',
        'key': 'pyr/d/4d/2025-12-30.parquet',
    }
    decoded = decode_gap(payload, FROM)
    # Genesis-clipped effective start (the wire format omits eff bounds).
    assert decoded == replace(gap, effective_end=gap.period_end)
    assert decoded.effective_start == FROM

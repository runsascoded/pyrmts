"""Same-tier consolidation / extension fill (`specs/pyrmts-ops-adoption.md`
phase 2): sub-rung tiling, overlap covers, hole-fill strategies, the
extension-fill driver + registration, gap wire format."""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

from pyrmts import ExpectedShard, MemStorage, list_expected_shards, shard_periods_covering
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


def make_calendar_ladder(storage, *, extended: bool):
    """awair's raw shape (`specs/calendar-rung-consolidation.md`): base
    tier stores 1d tip shards; the extended view adds the 1mo
    consolidation rung. Single tier so expected covers stay q-only."""
    p = make_pyramid(storage=storage)
    return replace(p, tiers=[
        replace(p.tiers[0], shards=('1d', '1mo') if extended else ('1d',)),
    ])


FEB = datetime(2026, 2, 1, tzinfo=timezone.utc)
MAR = datetime(2026, 3, 1, tzinfo=timezone.utc)
AUG = datetime(2026, 8, 1, tzinfo=timezone.utc)
SEP = datetime(2026, 9, 1, tzinfo=timezone.utc)


def _mo_gap(pyramid, month_start: datetime, month_end: datetime) -> ExpectedShard:
    gaps = list_expected_shards(pyramid, (month_start, month_end))
    assert [(g.shard_dur, g.period_start, g.period_end) for g in gaps] == [
        ('1mo', month_start, month_end),
    ]
    return gaps[0]


def test_tile_from_existing_calendar_rung():
    """A 1mo gap tiles from daily shards — 28 for Feb 2026, 31 for Aug —
    with calendar (not epoch-modulo) slot bounds."""
    storage = MemStorage()
    pyramid = make_calendar_ladder(storage, extended=True)
    for start, end in ((FEB, MAR), (AUG, SEP)):
        write_base_shards(pyramid, shard_dur='1d', start=start, to=end)
    key_set = set(pyramid.storage.list('pyr/'))
    tier = pyramid.tier('q')
    for start, end, n_days in ((FEB, MAR, 28), (AUG, SEP, 31)):
        picks, holes = tile_from_existing(
            pyramid, tier, _mo_gap(pyramid, start, end), key_set, genesis=FEB,
        )
        assert picks == [
            ('1d', f'pyr/q/1d/{p.label}.parquet')
            for p in shard_periods_covering(start, end, '1d')
        ]
        assert len(picks) == n_days
        assert holes == []


def test_calendar_consolidation_byte_identical_to_engine_build():
    """`specs/calendar-rung-consolidation.md` acceptance #2: consolidated
    1mo shards (concat of 28/31 engine-built 1d tiles) are byte-identical
    to the engine building each 1mo shard directly from the 1d source,
    and re-consolidation is idempotent / RGIP."""
    # Reference: engine builds q@1mo directly (wide→long→rebin→wide).
    ref = make_calendar_ladder(MemStorage(), extended=True)
    for start, end in ((FEB, MAR), (AUG, SEP)):
        write_base_shards(ref, shard_dur='1d', start=start, to=end)
        build_local(
            ref, (start, end), WideShardSource(ref, shard_dur='1d'),
            pyramid_name='test',
        )

    pyramid = make_calendar_ladder(MemStorage(), extended=True)
    for start, end in ((FEB, MAR), (AUG, SEP)):
        write_base_shards(pyramid, shard_dur='1d', start=start, to=end)
    key_set = set(pyramid.storage.list('pyr/'))
    for start, end, n_days in ((FEB, MAR, 28), (AUG, SEP, 31)):
        gap = _mo_gap(pyramid, start, end)
        res = materialize_extension_shard(pyramid, gap, key_set=key_set, genesis=FEB)
        assert (res.status, res.inputs_present, res.source_desc) == (
            'wrote', n_days, f'same-tier cover ×{n_days}',
        )
        assert pyramid.storage.get(gap.key) == ref.storage.get(gap.key)

        # Idempotent: the fresh key short-circuits a re-run…
        again = materialize_extension_shard(pyramid, gap, key_set=key_set, genesis=FEB)
        assert again.status == 'exists'
        # …and a forced rebuild regenerates the same bytes (RGIP).
        pyramid.storage._data.pop(gap.key)
        key_set.discard(gap.key)
        rebuilt = materialize_extension_shard(pyramid, gap, key_set=key_set, genesis=FEB)
        assert (rebuilt.status, rebuilt.md5) == ('wrote', res.md5)


def test_overlap_cover_calendar_rung():
    """A calendar-month source tile merely overlapping the interval is
    used, clipped — the coarse-history case for a `[1d, 1mo]` source tier
    whose dailies have been GC'd."""
    storage = MemStorage()
    pyramid = make_calendar_ladder(storage, extended=True)
    key = 'pyr/q/1mo/2026-02.parquet'
    storage.put(key, b'placeholder')
    key_set = {key}
    feb10 = datetime(2026, 2, 10, tzinfo=timezone.utc)
    feb20 = datetime(2026, 2, 20, tzinfo=timezone.utc)
    picks, uncovered = overlap_cover(pyramid, pyramid.tier('q'), feb10, feb20, key_set)
    assert uncovered == []
    assert picks == [(key, feb10, feb20)]


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


def test_run_extension_fill_source_readiness():
    """`specs/source-readiness-pending.md`: an expected shard whose source
    cover is still open (/h@3h ending 21:00 ← /q's smallest rung 2h, so
    buildable at 22:00) is excluded as not-ready — zero attempts, no
    `no_inputs` churn — then source and target both fill in one
    dependency-ordered pass once the source closes."""
    def raw_fill(hole, now):
        s, e = hole
        return base_wide_frame(
            int(s.timestamp() * 1000), int(e.timestamp() * 1000),
        ).to_arrow()

    storage = MemStorage()
    p = make_pyramid(storage=storage)
    pyramid = replace(p, tiers=[
        replace(p.tiers[0], shards=('2h',)),
        replace(p.tiers[1], shards=('3h',)),
    ])
    t20 = datetime(2026, 1, 2, 20, tzinfo=timezone.utc)
    write_base_shards(pyramid, shard_dur='2h', to=t20)
    # Bring /h up to date as of 18:30: fills [00,03) … [15,18).
    t18_30 = datetime(2026, 1, 2, 18, 30, tzinfo=timezone.utc)
    results = run_extension_fill(
        pyramid, genesis=FROM, now=t18_30, pyramid_name='test', fill_all=True,
    )
    assert [(r.gap.key, r.status) for r in results] == [
        (f'pyr/h/3h/2026-01-02T{h:02d}.parquet', 'wrote') for h in range(0, 18, 3)
    ]

    # 21:30: /h@3h [18,21) is expected but its covering /q@2h tile
    # [20,22) is still open → not ready, zero attempts.
    t21_30 = datetime(2026, 1, 2, 21, 30, tzinfo=timezone.utc)
    assert run_extension_fill(
        pyramid, genesis=FROM, now=t21_30, pyramid_name='test',
        fill_all=True, raw_fill=raw_fill,
    ) == []

    # 22:05: source closed → the /q@2h [20,22) gap and the /h@3h [18,21)
    # gap fill in one dependency-ordered pass.
    t22_05 = datetime(2026, 1, 2, 22, 5, tzinfo=timezone.utc)
    results = run_extension_fill(
        pyramid, genesis=FROM, now=t22_05, pyramid_name='test',
        fill_all=True, raw_fill=raw_fill,
    )
    assert [(r.gap.key, r.status) for r in results] == [
        ('pyr/q/2h/2026-01-02T20.parquet', 'wrote'),
        ('pyr/h/3h/2026-01-02T18.parquet', 'wrote'),
    ]


def test_run_extension_fill_reports_not_ready(monkeypatch):
    storage = MemStorage()
    p = make_pyramid(storage=storage)
    pyramid = replace(p, tiers=[
        replace(p.tiers[0], shards=('2h',)),
        replace(p.tiers[1], shards=('3h',)),
    ])
    t20 = datetime(2026, 1, 2, 20, tzinfo=timezone.utc)
    write_base_shards(pyramid, shard_dur='2h', to=t20)
    t18_30 = datetime(2026, 1, 2, 18, 30, tzinfo=timezone.utc)
    run_extension_fill(pyramid, genesis=FROM, now=t18_30, pyramid_name='test', fill_all=True)

    from pyrmts_engine import consolidate
    lines: list[str] = []
    monkeypatch.setattr(consolidate, 'err', lambda *a: lines.append(' '.join(map(str, a))))
    t21_30 = datetime(2026, 1, 2, 21, 30, tzinfo=timezone.utc)
    assert run_extension_fill(
        pyramid, genesis=FROM, now=t21_30, pyramid_name='test',
        fill_all=True, dry_run=True,
    ) == []
    # Dry run: the census line and nothing else (no would-fill lines —
    # the one gap is not-ready).
    assert lines == ['fillable gaps: 0 of 1 total missing (1 not ready — source cover open)']


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


def test_tiling_parity_fixture():
    """`fixtures/tiling-parity.json` (`specs/js-calendar-same-tier-tiling.md`):
    the normative same-tier tiling table, generated by Python
    `tile_from_existing` (the deployed reference) and asserted verbatim by
    both suites (JS twin: `tile-from-existing.test.ts`). Pins the mixed-rung
    `[1d, 1mo, 1y]` walk: calendar recursion, pre-genesis drop, per-day holes."""
    import json
    from pathlib import Path

    from pyrmts import Pyramid
    from pyrmts.types import Metric, Tier

    fixture = json.loads(
        (Path(__file__).parents[3] / 'fixtures' / 'tiling-parity.json').read_text()
    )
    tier = Tier(name=fixture['tier']['name'], bin=fixture['tier']['bin'],
                shards=tuple(fixture['tier']['shards']))
    pyramid = Pyramid(
        storage=MemStorage(),
        keyTemplate=fixture['keyTemplate'],
        binCol='dt',
        dims=[],
        metrics=[Metric(name='n', monoid='count')],
        tiers=[tier],
    )
    iso = lambda s: datetime.fromisoformat(s.replace('Z', '+00:00'))
    fmt = lambda t: t.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    start, end = iso(fixture['gap']['periodStart']), iso(fixture['gap']['periodEnd'])
    gap = ExpectedShard(
        tier=tier.name, shard_dur=fixture['gap']['shardDur'],
        period_start=start, period_end=end,
        effective_start=start, effective_end=end,
        key='unused',
    )
    picks, holes = tile_from_existing(
        pyramid, tier, gap, set(fixture['keySet']), genesis=iso(fixture['genesis']),
    )
    assert [{'rung': rung, 'key': key} for rung, key in picks] == fixture['picks']
    assert [{'start': fmt(s), 'end': fmt(e)} for s, e in holes] == fixture['holes']

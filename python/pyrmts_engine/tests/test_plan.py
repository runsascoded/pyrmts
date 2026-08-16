"""DAG compilation: divisibility predecessors, skip rungs, DOT export."""
from __future__ import annotations

import pytest

from pyrmts import Dim, MemStorage, Metric, Pyramid, Tier
from pyrmts_engine import compile_plan

from conftest import FROM, TO, make_pyramid


def _pyramid_with_tiers(tiers: list[Tier]) -> Pyramid:
    return Pyramid(
        storage=MemStorage(),
        keyTemplate='pyr/{tier}/{shard}/{period}.parquet',
        binCol='dt',
        dims=[Dim(name='cell', type='string')],
        metrics=[Metric(name='rides', monoid='count')],
        tiers=tiers,
    )


def test_fixture_preds_chain():
    plan = compile_plan(make_pyramid(), (FROM, TO))
    assert plan.preds == {'q': None, 'h': 'q', 'd': 'h'}


def test_coprime_fanout_preds():
    """ctbk's ladder shape: 2,3,5 are pairwise coprime so they all source
    /1m; 10 and 15 source /5m (their coarsest divisor)."""
    tiers = [
        Tier(name='m1', bin='1min', shards=('1h',)),
        Tier(name='m2', bin='2min', shards=('1h',)),
        Tier(name='m3', bin='3min', shards=('1h',)),
        Tier(name='m5', bin='5min', shards=('1h',)),
        Tier(name='m10', bin='10min', shards=('1h',)),
        Tier(name='m15', bin='15min', shards=('1h',)),
    ]
    plan = compile_plan(_pyramid_with_tiers(tiers), (FROM, TO))
    assert plan.preds == {
        'm1': None,
        'm2': 'm1',
        'm3': 'm1',
        'm5': 'm1',
        'm10': 'm5',
        'm15': 'm5',
    }


def test_calendar_preds():
    tiers = [
        Tier(name='h1', bin='1h', shards=('1d',)),
        Tier(name='d1', bin='1d', shards=('32d',)),
        Tier(name='mo1', bin='1mo', shards=('1y',)),
        Tier(name='y1', bin='1y', shards=('1y',)),
    ]
    plan = compile_plan(_pyramid_with_tiers(tiers), (FROM, TO))
    assert plan.preds == {'h1': None, 'd1': 'h1', 'mo1': 'd1', 'y1': 'mo1'}


def test_no_predecessor_raises():
    tiers = [
        Tier(name='m2', bin='2min', shards=('1h',)),
        Tier(name='m3', bin='3min', shards=('1h',)),
    ]
    with pytest.raises(ValueError, match=r"tier 'm3' \(bin '3min'\) has no divisibility predecessor"):
        compile_plan(_pyramid_with_tiers(tiers), (FROM, TO))


def test_skip_rungs_partition():
    # NB: (q, 6h) is not in the cover at all — [FROM, TO) is 1d-aligned so
    # the q tier needs no trailing sub-1d tiles.
    plan = compile_plan(make_pyramid(), (FROM, TO), skip_rungs={('q', '1d')})
    assert sorted({(e.tier, e.shard_dur) for e in plan.outputs}) == [
        ('d', '4d'), ('h', '1d'), ('h', '4d'),
    ]
    assert {(e.tier, e.shard_dur) for e in plan.skipped_rungs} == {('q', '1d')}


def test_to_dot():
    plan = compile_plan(make_pyramid(), (FROM, TO))
    assert plan.to_dot().split('\n') == [
        'digraph pyramid_build {',
        '  rankdir=BT;',
        '  source [shape=box];',
        '  "q" [label="q\\nbin=15min shards=6h/1d\\nout=6"];',
        '  source -> "q";',
        '  "h" [label="h\\nbin=1h shards=1d/4d\\nout=3"];',
        '  "q" -> "h";',
        '  "d" [label="d\\nbin=1d shards=4d\\nout=2"];',
        '  "h" -> "d";',
        '}',
    ]


def test_divides_calendar_multi_unit():
    """`specs/calendar-units.md` § Python changes #2: the `_divides` truth
    table for multi-unit calendar spans."""
    from pyrmts_engine.plan import _divides

    table = [
        ('1d', '1mo'), ('1h', '1mo'), ('1mo', '3mo'), ('1mo', '1y'),
        ('3mo', '6mo'), ('6mo', '1y'), ('1y', '4y'),
        ('3d', '1mo'), ('2mo', '3mo'), ('1mo', '1d'), ('4y', '1y'),
    ]
    assert [(p, t, _divides(p, t)) for p, t in table] == [
        ('1d', '1mo', True), ('1h', '1mo', True), ('1mo', '3mo', True), ('1mo', '1y', True),
        ('3mo', '6mo', True), ('6mo', '1y', True), ('1y', '4y', True),
        ('3d', '1mo', False), ('2mo', '3mo', False), ('1mo', '1d', False), ('4y', '1y', False),
    ]


def test_bin_floor_expr_calendar_parity():
    """`bin_floor_expr` reproduces the normative calendar-floor fixture for
    every calendar span — the regression pin for the polars `Ny` anchor
    divergence (`dt.truncate('4y')` is epoch-anchored: 1970-01-01 would
    floor to 1970, the contract says 1968)."""
    import json
    from datetime import datetime
    from pathlib import Path

    import polars as pl

    from pyrmts_engine.plan import bin_floor_expr

    fixture = json.loads(
        (Path(__file__).parents[3] / 'fixtures' / 'calendar-floors.json').read_text()
    )
    by_span: dict[str, list[dict]] = {}
    for c in fixture['cases']:
        by_span.setdefault(c['span'], []).append(c)
    for span, cases in by_span.items():
        df = pl.DataFrame({
            'dt': [int(datetime.fromisoformat(c['t']).timestamp() * 1000) for c in cases],
        })
        floored = df.select(bin_floor_expr('dt', span).alias('f'))['f'].to_list()
        expected = [int(datetime.fromisoformat(c['floor']).timestamp() * 1000) for c in cases]
        assert (span, floored) == (span, expected)


def test_bin_floor_expr_non_year_dividing_months():
    """`Nmo` widths that don't divide 12 floor on the year-0 month grid —
    the regression pin for the polars `Nmo` anchor divergence
    (`dt.truncate('7mo')` is epoch-anchored: 2026-05-10 would floor to
    2026-01, the contract says 2025-12;
    `specs/calendar-composition-and-query-limits.md` §1)."""
    from datetime import datetime, timezone

    import polars as pl

    from pyrmts_engine.plan import bin_floor_expr

    ts = lambda *args: int(datetime(*args, tzinfo=timezone.utc).timestamp() * 1000)
    df = pl.DataFrame({'dt': [ts(2026, 1, 15), ts(2026, 5, 10), ts(2026, 9, 30)]})
    assert df.select(bin_floor_expr('dt', '5mo').alias('f'))['f'].to_list() == [
        ts(2025, 11, 1), ts(2026, 4, 1), ts(2026, 9, 1),
    ]
    assert df.select(bin_floor_expr('dt', '7mo').alias('f'))['f'].to_list() == [
        ts(2025, 12, 1), ts(2025, 12, 1), ts(2026, 7, 1),
    ]

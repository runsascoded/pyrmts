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

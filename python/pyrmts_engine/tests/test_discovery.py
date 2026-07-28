"""Storage-state gap discovery (`specs/pyrmts-ops-adoption.md` phase 2):
expected-vs-LIST diff, dependency ordering, staleness partitioning."""
from __future__ import annotations

from datetime import datetime, timezone

from pyrmts import list_expected_shards
from pyrmts_engine import (
    discover_gaps,
    group_by_tier_rung,
    list_existing_keys,
    sort_by_dependency,
    split_stale,
)

from conftest import FROM, TO
from test_engine import EXPECTED_KEYS, _run_engine

BASE_KEYS = [f'pyr/q/6h/{d}T{h:02d}.parquet' for d in (
    '2026-01-02', '2026-01-03', '2026-01-04', '2026-01-05', '2026-01-06', '2026-01-07',
) for h in (0, 6, 12, 18)]


def test_discover_gaps_diffs_and_orders():
    pyramid, _, _ = _run_engine()
    deleted = [
        'pyr/d/4d/2026-01-03.parquet',
        'pyr/h/4d/2026-01-03.parquet',
        'pyr/q/1d/2026-01-04.parquet',
    ]
    for k in deleted:
        pyramid.storage._data.pop(k)

    gaps, existing, expected_by_tier = discover_gaps(pyramid, (FROM, TO))
    # Dependency order: finest tier first (q feeds h feeds d).
    assert [g.key for g in gaps] == [
        'pyr/q/1d/2026-01-04.parquet',
        'pyr/h/4d/2026-01-03.parquet',
        'pyr/d/4d/2026-01-03.parquet',
    ]
    assert existing == set(BASE_KEYS) | (set(EXPECTED_KEYS) - set(deleted))
    assert {t: len(es) for t, es in expected_by_tier.items()} == {'q': 6, 'h': 3, 'd': 2}


def test_list_existing_keys_prefix():
    pyramid, _, _ = _run_engine()
    assert list_existing_keys(pyramid) == set(BASE_KEYS) | set(EXPECTED_KEYS)
    assert sorted(list_existing_keys(pyramid, 'pyr/d/')) == [
        'pyr/d/4d/2025-12-30.parquet',
        'pyr/d/4d/2026-01-03.parquet',
    ]


def test_sort_by_dependency_orders_tier_then_rung_then_period():
    pyramid, _, _ = _run_engine()
    expected = list_expected_shards(pyramid, (FROM, TO))
    by_key = {e.key: e for e in expected}
    shuffled = [
        by_key['pyr/d/4d/2026-01-03.parquet'],
        by_key['pyr/h/4d/2025-12-30.parquet'],
        by_key['pyr/q/1d/2026-01-03.parquet'],
        by_key['pyr/h/1d/2026-01-07.parquet'],
        by_key['pyr/q/1d/2026-01-02.parquet'],
    ]
    assert [s.key for s in sort_by_dependency(pyramid, shuffled)] == [
        'pyr/q/1d/2026-01-02.parquet',
        'pyr/q/1d/2026-01-03.parquet',
        'pyr/h/1d/2026-01-07.parquet',   # 1d rung before 4d within tier h
        'pyr/h/4d/2025-12-30.parquet',
        'pyr/d/4d/2026-01-03.parquet',
    ]


def test_split_stale():
    t1 = datetime(2026, 1, 5, tzinfo=timezone.utc)
    t2 = datetime(2026, 1, 6, tzinfo=timezone.utc)
    existing = {'a': t1, 'b': t2, 'c': None}
    assert split_stale(existing, None) == ({'a', 'b', 'c'}, set())
    # Unknown mtimes are fresh (backends without mtimes mustn't rebuild).
    assert split_stale(existing, t2) == ({'b', 'c'}, {'a'})


def test_group_by_tier_rung():
    pyramid, _, _ = _run_engine()
    expected = list_expected_shards(pyramid, (FROM, TO))
    ordered = sort_by_dependency(pyramid, [e for e in expected if e.tier in ('h', 'd')])
    groups = [(t, s, [e.key for e in es]) for t, s, es in group_by_tier_rung(ordered)]
    assert groups == [
        ('h', '1d', ['pyr/h/1d/2026-01-07.parquet']),
        ('h', '4d', ['pyr/h/4d/2025-12-30.parquet', 'pyr/h/4d/2026-01-03.parquet']),
        ('d', '4d', ['pyr/d/4d/2025-12-30.parquet', 'pyr/d/4d/2026-01-03.parquet']),
    ]

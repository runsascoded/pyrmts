"""Tests for `parse_pyramid_yaml` — unified-shard-ladder model."""
from __future__ import annotations

from textwrap import dedent

import pytest

from pyrmts import Tier, parse_pyramid_yaml


def test_parses_shards_ladder():
    text = dedent("""
        storage:
          type: r2
          bucket: 380nwk
          key: 'avail/{tier}/{shard}/{period}.parquet'
        binCol: ts
        dims:
          - { name: device_id, type: int }
        metrics:
          - { name: temp, monoid: sum }
        tiers:
          - { name: raw, bin: 1min, shards: [5min, 1h, 1d] }
          - { name: h1,  bin: 1h,   shards: [1d, 1mo] }
    """).strip()
    cfg = parse_pyramid_yaml(text)
    assert cfg.tiers == [
        Tier(name='raw', bin='1min', shards=('5min', '1h', '1d')),
        Tier(name='h1',  bin='1h',   shards=('1d', '1mo')),
    ]
    assert cfg.keyTemplate == 'avail/{tier}/{shard}/{period}.parquet'


def test_rejects_old_singular_shard():
    text = dedent("""
        storage:
          type: r2
          bucket: 380nwk
          key: 'a/{tier}/{period}.parquet'
        binCol: ts
        dims: []
        metrics:
          - { name: n, monoid: count }
        tiers:
          - { name: raw, bin: 1min, shard: 1d }
    """).strip()
    with pytest.raises(ValueError, match=r"old singular `shard:`"):
        parse_pyramid_yaml(text)


def test_rejects_missing_shards():
    text = dedent("""
        storage:
          type: r2
          bucket: 380nwk
          key: 'a/{tier}/{period}.parquet'
        binCol: ts
        dims: []
        metrics:
          - { name: n, monoid: count }
        tiers:
          - { name: raw, bin: 1min }
    """).strip()
    with pytest.raises(ValueError, match=r"shards must be a non-empty list"):
        parse_pyramid_yaml(text)


def test_rejects_non_divisible_chain():
    """1h does not divide 1d? It does. But 1h does not divide 25min in either direction.
    Use a deliberately broken chain: shards: [1h, 25h] (25h is 25*60min = 1500min, 1h=60min, 1500%60=0 OK).
    Pick a genuinely-broken one: [1h, 90min] — 90min doesn't even parse as valid since 90/60=1.5 and
    we want 1h to divide 90min, but 90 % 60 != 0."""
    text = dedent("""
        storage:
          type: r2
          bucket: 380nwk
          key: 'a/{tier}/{period}.parquet'
        binCol: ts
        dims: []
        metrics:
          - { name: n, monoid: count }
        tiers:
          - { name: raw, bin: 1min, shards: [1h, 90min] }
    """).strip()
    with pytest.raises(ValueError, match=r"does not divide"):
        parse_pyramid_yaml(text)


def test_rejects_bin_not_dividing_shards_zero():
    text = dedent("""
        storage:
          type: r2
          bucket: 380nwk
          key: 'a/{tier}/{period}.parquet'
        binCol: ts
        dims: []
        metrics:
          - { name: n, monoid: count }
        tiers:
          - { name: raw, bin: 7min, shards: [1h] }
    """).strip()
    with pytest.raises(ValueError, match=r"bin '7min' does not divide shards\[0\] '1h'"):
        parse_pyramid_yaml(text)


def test_allows_mixed_fixed_and_calendar_units():
    """Calendar (mo/y) durations live in a separate universe from fixed-width
    durations (variable-length months); mixing them in one ladder is allowed
    and divisibility checks are skipped at the boundary. Mirrors JS."""
    text = dedent("""
        storage:
          type: r2
          bucket: 380nwk
          key: 'a/{tier}/{period}.parquet'
        binCol: ts
        dims: []
        metrics:
          - { name: n, monoid: count }
        tiers:
          - { name: raw, bin: 1min, shards: [1d, 1mo] }
    """).strip()
    cfg = parse_pyramid_yaml(text)
    assert cfg.tiers[0].shards == ('1d', '1mo')


def test_allows_calendar_chain():
    text = dedent("""
        storage:
          type: r2
          bucket: 380nwk
          key: 'a/{tier}/{period}.parquet'
        binCol: ts
        dims: []
        metrics:
          - { name: n, monoid: count }
        tiers:
          - { name: mo, bin: 1mo, shards: [1mo, 1y] }
    """).strip()
    cfg = parse_pyramid_yaml(text)
    assert cfg.tiers[0].shards == ('1mo', '1y')


def test_allows_single_rung_ladder():
    """Backward-shape sanity: tiers can declare a one-rung ladder."""
    text = dedent("""
        storage:
          type: r2
          bucket: 380nwk
          key: 'a/{tier}/{period}.parquet'
        binCol: ts
        dims: []
        metrics:
          - { name: n, monoid: count }
        tiers:
          - { name: raw, bin: 1min, shards: [1h] }
          - { name: h1,  bin: 1h,   shards: [1d] }
    """).strip()
    cfg = parse_pyramid_yaml(text)
    assert cfg.tiers[0].shards == ('1h',)
    assert cfg.tiers[1].shards == ('1d',)

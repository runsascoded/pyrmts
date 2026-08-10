"""Tests for `parse_pyramid_yaml` — unified-shard-ladder model."""
from __future__ import annotations

from textwrap import dedent

import pytest

from pyrmts import Tier, merge_lambda_shards, parse_pyramid_yaml


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


EXTRAS_YAML = dedent("""
    storage:
      type: s3
      bucket: b
      key: 'a/{tier}/{shard}/{period}.parquet'
    binCol: ts
    defaults:
      rg_size: 2048
    dims:
      - { name: cell, type: s2 }
    metrics:
      - { name: n, monoid: count }
    tiers:
      - { name: raw, bin: 1min, shards: [1h, 1d], rg_size: 4096, lambda_shards: [2d, 4d] }
      - { name: h1,  bin: 1h,   shards: [1d] }
""").strip()


def test_tier_extras_rg_size_and_lambda_shards():
    """Per-tier extras (`specs/pyrmts-ops-adoption.md` phase 1) survive the
    parse first-class: `rg_size` (tier override > defaults.rg_size) and
    `lambda_shards`; `s2` is a valid dim type."""
    cfg = parse_pyramid_yaml(EXTRAS_YAML)
    assert cfg.tiers == [
        Tier(name='raw', bin='1min', shards=('1h', '1d'), rg_size=4096, lambda_shards=('2d', '4d')),
        Tier(name='h1', bin='1h', shards=('1d',), rg_size=2048),
    ]
    assert [(d.name, d.type) for d in cfg.dims] == [('cell', 's2')]


def test_merge_lambda_shards():
    """The extended-ladder view folds `lambda_shards` into `shards` (and
    clears them); the input config is untouched."""
    cfg = parse_pyramid_yaml(EXTRAS_YAML)
    merged = merge_lambda_shards(cfg)
    assert merged.tiers == [
        Tier(name='raw', bin='1min', shards=('1h', '1d', '2d', '4d'), rg_size=4096),
        Tier(name='h1', bin='1h', shards=('1d',), rg_size=2048),
    ]
    assert cfg.tiers[0].shards == ('1h', '1d')
    assert cfg.tiers[0].lambda_shards == ('2d', '4d')


def test_lambda_shards_must_continue_chain():
    """`lambda_shards` are chain-validated as one combined ladder with
    `shards` — a rung that breaks divisibility fails the parse."""
    text = EXTRAS_YAML.replace('lambda_shards: [2d, 4d]', 'lambda_shards: [36h]')
    with pytest.raises(ValueError, match=r"shards\[1\] '1d' does not divide shards\[2\] '36h'"):
        parse_pyramid_yaml(text)


def _one_tier_yaml(tier_line: str) -> str:
    return dedent(f"""
        storage:
          type: r2
          bucket: 380nwk
          key: 'a/{{tier}}/{{period}}.parquet'
        binCol: ts
        dims: []
        metrics:
          - {{ name: n, monoid: count }}
        tiers:
          - {tier_line}
    """).strip()


def test_allows_multi_unit_calendar_chain():
    """`specs/calendar-units.md`: `[1mo, 3mo, 1y]`-style multi-unit calendar
    ladders divide in months."""
    cfg = parse_pyramid_yaml(_one_tier_yaml('{ name: mo, bin: 1mo, shards: [1mo, 3mo, 1y] }'))
    assert cfg.tiers[0].shards == ('1mo', '3mo', '1y')
    cfg = parse_pyramid_yaml(_one_tier_yaml('{ name: y, bin: 1y, shards: [1y, 4y] }'))
    assert cfg.tiers[0].shards == ('1y', '4y')


def test_rejects_non_dividing_calendar_rungs():
    with pytest.raises(ValueError) as exc:
        parse_pyramid_yaml(_one_tier_yaml('{ name: mo, bin: 1mo, shards: [2mo, 3mo] }'))
    assert str(exc.value) == (
        "parse_pyramid_yaml: tiers[0] ('mo'): shards[0] '2mo' does not divide "
        "shards[1] '3mo' (in months)"
    )


def test_rejects_month_span_not_tiling_year():
    with pytest.raises(ValueError) as exc:
        parse_pyramid_yaml(_one_tier_yaml('{ name: mo, bin: 5mo, shards: [1y] }'))
    assert str(exc.value) == (
        "parse_pyramid_yaml: tiers[0] ('mo'): month-span '5mo' doesn't tile a "
        "year evenly (12 % 5 !== 0)"
    )


def test_rejects_calendar_bin_not_dividing_calendar_shard():
    with pytest.raises(ValueError) as exc:
        parse_pyramid_yaml(_one_tier_yaml('{ name: mo, bin: 6mo, shards: [4mo] }'))
    assert str(exc.value) == (
        "parse_pyramid_yaml: tiers[0] ('mo'): shards[0] '4mo' is smaller than "
        "bin '6mo' (in months)"
    )


def test_rejects_descending_mixed_pair_by_nominal_width():
    with pytest.raises(ValueError) as exc:
        parse_pyramid_yaml(_one_tier_yaml('{ name: raw, bin: 1d, shards: [1mo, 14d] }'))
    assert str(exc.value) == (
        "parse_pyramid_yaml: tiers[0] ('raw'): shards not ascending "
        "(shards[1] '14d' <= shards[0] '1mo' by nominal width)"
    )

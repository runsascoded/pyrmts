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
          key: 'a/{tier}/{shard}/{period}.parquet'
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
          key: 'a/{tier}/{shard}/{period}.parquet'
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
          key: 'a/{{tier}}/{{shard}}/{{period}}.parquet'
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


# The awair Aug-2026 outage: `key: '.../{tier}/{period}.parquet'` (no
# `{shard}`) paired with a multi-rung tier (`[1d, 4d, 32d]`) — two rungs
# starting on the same day collided on one R2 key and silently corrupted
# downstream reads for ~1 month. `parse_pyramid_yaml` must reject the
# config at parse time so this never lands in production.

def _multi_rung_yaml(key: str, tier_line: str) -> str:
    return dedent(f"""
        storage:
          type: r2
          bucket: 380nwk
          key: '{key}'
        binCol: ts
        dims: [{{ name: device_id, type: int }}]
        metrics:
          - {{ name: temp, monoid: sum }}
        tiers:
          - {{ name: raw, bin: 1min, shards: [1h] }}
          - {tier_line}
    """).strip()


def test_rejects_multi_rung_ladder_missing_shard_placeholder():
    text = _multi_rung_yaml(
        'pyramid/awair-{device_id}/{tier}/{period}.parquet',
        '{ name: m3, bin: 3min, shards: [1d, 4d, 32d] }',
    )
    with pytest.raises(ValueError) as exc:
        parse_pyramid_yaml(text)
    assert str(exc.value) == (
        "parse_pyramid_yaml: tier 'm3' has a multi-rung ladder "
        "(['1d', '4d', '32d']) but keyTemplate "
        "'pyramid/awair-{device_id}/{tier}/{period}.parquet' is missing "
        "the '{shard}' placeholder — rungs starting on the same period "
        "would collide on one key. Add '{shard}' to the template "
        "(e.g. '.../{tier}/{shard}/{period}.parquet') or collapse the "
        "tier to a single shard rung."
    )


def test_accepts_multi_rung_ladder_with_shard_placeholder():
    text = _multi_rung_yaml(
        'pyramid/awair-{device_id}/{tier}/{shard}/{period}.parquet',
        '{ name: m3, bin: 3min, shards: [1d, 4d, 32d] }',
    )
    cfg = parse_pyramid_yaml(text)
    assert cfg.tiers[1] == Tier(name='m3', bin='3min', shards=('1d', '4d', '32d'))


def test_accepts_all_single_rung_ladders_without_shard_placeholder():
    """Every tier is single-rung → each period gets a unique key
    regardless of `{shard}`. This is the pre-multi-rung awair shape and
    must not false-positive."""
    text = dedent("""
        storage:
          type: r2
          bucket: 380nwk
          key: 'pyramid/awair-{device_id}/{tier}/{period}.parquet'
        binCol: ts
        dims: [{ name: device_id, type: int }]
        metrics:
          - { name: temp, monoid: sum }
        tiers:
          - { name: raw, bin: 1min, shards: [1h] }
          - { name: h1,  bin: 1h,   shards: [1d] }
    """).strip()
    cfg = parse_pyramid_yaml(text)
    assert len(cfg.tiers) == 2


def test_accepts_single_rung_with_shard_placeholder():
    """awair's canonical `raw` tier + `.../{tier}/{shard}/{period}`
    template — a placeholder is not required for single-rung tiers, but
    it's not rejected either (substitutes to the single shard's label)."""
    text = dedent("""
        storage:
          type: r2
          bucket: 380nwk
          key: 'pyramid/awair-{device_id}/{tier}/{shard}/{period}.parquet'
        binCol: ts
        dims: [{ name: device_id, type: int }]
        metrics:
          - { name: temp, monoid: sum }
        tiers:
          - { name: raw, bin: 1min, shards: [1h] }
    """).strip()
    cfg = parse_pyramid_yaml(text)
    assert cfg.tiers[0] == Tier(name='raw', bin='1min', shards=('1h',))


def test_rejects_lambda_shards_extending_single_rung_without_placeholder():
    """`lambda_shards` fold into the runtime ladder view
    (`merge_lambda_shards`) — a tier with `shards: [1d]` +
    `lambda_shards: [4d]` is multi-rung at runtime and needs the
    placeholder just as much as a bare `shards: [1d, 4d]` tier."""
    text = dedent("""
        storage:
          type: r2
          bucket: 380nwk
          key: 'pyramid/awair-{device_id}/{tier}/{period}.parquet'
        binCol: ts
        dims: [{ name: device_id, type: int }]
        metrics:
          - { name: temp, monoid: sum }
        tiers:
          - { name: raw, bin: 1min, shards: [1h] }
          - { name: m3,  bin: 3min, shards: [1d], lambda_shards: [4d] }
    """).strip()
    with pytest.raises(ValueError, match=r"tier 'm3' has a multi-rung ladder"):
        parse_pyramid_yaml(text)


def test_pyramid_from_config_revalidates_shard_placeholder():
    """A hand-built `PyramidConfig` (bypassing `parse_pyramid_yaml`) must
    also be rejected — defense in depth against non-yaml config paths."""
    from pyrmts import MemStorage, PyramidConfig, pyramid_from_config
    cfg = PyramidConfig(
        storage={'type': 'mem'},
        keyTemplate='pyramid/awair-{device_id}/{tier}/{period}.parquet',
        binCol='ts',
        dims=[],
        metrics=[],
        tiers=[Tier(name='m3', bin='3min', shards=('1d', '4d', '32d'))],
    )
    with pytest.raises(ValueError, match=r"tier 'm3' has a multi-rung ladder"):
        pyramid_from_config(cfg, MemStorage())


def test_allows_mixed_fixed_calendar_chains():
    """`specs/calendar-rung-consolidation.md`: awair's `[1d, 1mo]` shape
    (Lambda tip 1d shards, month-close consolidation), plus the other
    accept cases from the spec's acceptance #4."""
    cfg = parse_pyramid_yaml(_one_tier_yaml('{ name: raw, bin: 1min, shards: [1d, 1mo] }'))
    assert cfg.tiers[0].shards == ('1d', '1mo')
    cfg = parse_pyramid_yaml(_one_tier_yaml('{ name: raw, bin: 1min, shards: [3d, 1mo] }'))
    assert cfg.tiers[0].shards == ('3d', '1mo')
    cfg = parse_pyramid_yaml(_one_tier_yaml('{ name: raw, bin: 1min, shards: [1d, 3mo] }'))
    assert cfg.tiers[0].shards == ('1d', '3mo')


def test_rejects_mixed_pair_not_dividing_by_nominal_width():
    """`[7d, 1mo]`: 30d nominal % 7d ≠ 0 — 7d epoch slots can never chain
    into calendar-month periods."""
    with pytest.raises(ValueError) as exc:
        parse_pyramid_yaml(_one_tier_yaml('{ name: raw, bin: 1d, shards: [7d, 1mo] }'))
    assert str(exc.value) == (
        "parse_pyramid_yaml: tiers[0] ('raw'): shards[0] '7d' does not divide "
        "shards[1] '1mo' (by nominal width)"
    )

"""Parse a pyramid YAML config. Mirrors `js/packages/pyrmts/src/yaml.ts`.

`PyramidConfig` is storage-less; the caller wires `Storage` separately and
constructs a `Pyramid` via `pyramid_from_config`."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import yaml as _yaml

from .axis import parse_duration
from .types import (
    Axis,
    Dim,
    DimType,
    GeoSpec,
    Metric,
    MonoidName,
    Pyramid,
    Storage,
    Tier,
)


_VALID_AXES: set[str] = {'time', 'step'}
_VALID_DIM_TYPES: set[str] = {'int', 'string', 'h3', 'geohash'}
_VALID_MONOIDS: set[str] = {
    'sum', 'count', 'histogram', 'topk', 'botk', 'hll', 'tdigest',
}


@dataclass
class PyramidConfig:
    storage: dict[str, Any]
    keyTemplate: str
    binCol: str
    dims: list[Dim]
    metrics: list[Metric]
    tiers: list[Tier]
    axis: Axis = 'time'
    geo: GeoSpec | None = None


def parse_pyramid_yaml(text: str) -> PyramidConfig:
    raw = _yaml.safe_load(text)
    if not isinstance(raw, dict):
        raise ValueError("parse_pyramid_yaml: top-level must be a mapping")

    storage_meta, key_template = _parse_storage_block(raw.get('storage'))

    axis = raw.get('axis', 'time')
    if axis not in _VALID_AXES:
        raise ValueError(f"parse_pyramid_yaml: invalid axis {axis!r} (want 'time' or 'step')")

    bin_col = raw.get('binCol', 'ts')
    if not isinstance(bin_col, str):
        raise ValueError("parse_pyramid_yaml: binCol must be a string")

    cfg = PyramidConfig(
        storage=storage_meta,
        keyTemplate=key_template,
        axis=axis,
        binCol=bin_col,
        dims=_parse_dims(raw.get('dims')),
        metrics=_parse_metrics(raw.get('metrics')),
        tiers=_parse_tiers(raw.get('tiers')),
    )
    if 'geo' in raw and raw['geo'] is not None:
        cfg.geo = _parse_geo(raw['geo'])
    return cfg


def pyramid_from_config(cfg: PyramidConfig, storage: Storage) -> Pyramid:
    return Pyramid(
        storage=storage,
        keyTemplate=cfg.keyTemplate,
        axis=cfg.axis,
        binCol=cfg.binCol,
        dims=cfg.dims,
        metrics=cfg.metrics,
        tiers=cfg.tiers,
        geo=cfg.geo,
    )


def _parse_storage_block(raw: Any) -> tuple[dict[str, Any], str]:
    if not isinstance(raw, dict):
        raise ValueError("parse_pyramid_yaml: `storage` must be a mapping")
    if not isinstance(raw.get('type'), str):
        raise ValueError("parse_pyramid_yaml: `storage.type` must be a string")
    if not isinstance(raw.get('key'), str):
        raise ValueError("parse_pyramid_yaml: `storage.key` (key template) must be a string")
    key = raw['key']
    meta = {k: v for k, v in raw.items() if k != 'key'}
    return meta, key


def _parse_dims(raw: Any) -> list[Dim]:
    if not isinstance(raw, list):
        raise ValueError("parse_pyramid_yaml: `dims` must be a list")
    out: list[Dim] = []
    for i, d in enumerate(raw):
        if not isinstance(d, dict):
            raise ValueError(f"parse_pyramid_yaml: dims[{i}] must be a mapping")
        name = d.get('name')
        type_ = d.get('type')
        if not isinstance(name, str):
            raise ValueError(f"parse_pyramid_yaml: dims[{i}].name must be a string")
        if type_ not in _VALID_DIM_TYPES:
            raise ValueError(
                f"parse_pyramid_yaml: dims[{i}].type {type_!r} invalid "
                f"(want one of int/string/h3/geohash)"
            )
        out.append(Dim(name=name, type=type_))
    return out


def _parse_metrics(raw: Any) -> list[Metric]:
    if not isinstance(raw, list):
        raise ValueError("parse_pyramid_yaml: `metrics` must be a list")
    out: list[Metric] = []
    for i, m in enumerate(raw):
        if not isinstance(m, dict):
            raise ValueError(f"parse_pyramid_yaml: metrics[{i}] must be a mapping")
        name = m.get('name')
        monoid = m.get('monoid')
        if not isinstance(name, str):
            raise ValueError(f"parse_pyramid_yaml: metrics[{i}].name must be a string")
        if monoid not in _VALID_MONOIDS:
            raise ValueError(f"parse_pyramid_yaml: metrics[{i}].monoid {monoid!r} invalid")
        config = m.get('config')
        if config is not None and not isinstance(config, dict):
            raise ValueError(f"parse_pyramid_yaml: metrics[{i}].config must be a mapping")
        out.append(Metric(name=name, monoid=monoid, config=config))
    return out


def _parse_tiers(raw: Any) -> list[Tier]:
    if not isinstance(raw, list) or not raw:
        raise ValueError("parse_pyramid_yaml: `tiers` must be a non-empty list")
    out: list[Tier] = []
    for i, t in enumerate(raw):
        if not isinstance(t, dict):
            raise ValueError(f"parse_pyramid_yaml: tiers[{i}] must be a mapping")
        name = t.get('name')
        bin_ = t.get('bin')
        if not isinstance(name, str):
            raise ValueError(f"parse_pyramid_yaml: tiers[{i}].name must be a string")
        if not isinstance(bin_, str):
            raise ValueError(f"parse_pyramid_yaml: tiers[{i}].bin must be a string")
        if 'shard' in t:
            raise ValueError(
                f"parse_pyramid_yaml: tiers[{i}] uses the old singular `shard:` form; "
                f"use `shards: [...]` (a divisibility-chained ladder)."
            )
        shards_raw = t.get('shards')
        if not isinstance(shards_raw, list) or not shards_raw:
            raise ValueError(
                f"parse_pyramid_yaml: tiers[{i}].shards must be a non-empty list of Duration strings"
            )
        shards: list[str] = []
        for j, s in enumerate(shards_raw):
            if not isinstance(s, str):
                raise ValueError(
                    f"parse_pyramid_yaml: tiers[{i}].shards[{j}] must be a string (got {s!r})"
                )
            shards.append(s)
        _validate_shard_ladder(i, name, bin_, shards)
        out.append(Tier(name=name, bin=bin_, shards=tuple(shards)))
    return out


def _validate_shard_ladder(
    tier_idx: int,
    tier_name: str,
    bin_: str,
    shards: list[str],
) -> None:
    """`shards` must be ascending; each fixed-width rung must divide the next
    fixed-width rung. `bin_` (if fixed-width) must divide `shards[0]` (if
    fixed-width). Calendar-variable durations (`mo`/`y`) skip divisibility
    checks against fixed-width neighbors — they live in a separate universe
    where month-length variation makes integer-ms comparisons meaningless.
    Mirrors JS `validateLadders` (`js/.../ladder.ts`)."""
    bin_ms = _fixed_ms_or_none(bin_)
    shard_ms_list = [_fixed_ms_or_none(s) for s in shards]

    if shard_ms_list[0] is not None and bin_ms is not None and shard_ms_list[0] < bin_ms:
        raise ValueError(
            f"parse_pyramid_yaml: tiers[{tier_idx}] ({tier_name!r}): "
            f"shards[0] {shards[0]!r} ({shard_ms_list[0]}ms) is smaller than "
            f"bin {bin_!r} ({bin_ms}ms)"
        )
    if shard_ms_list[0] is not None and bin_ms is not None and shard_ms_list[0] % bin_ms != 0:
        raise ValueError(
            f"parse_pyramid_yaml: tiers[{tier_idx}] ({tier_name!r}): "
            f"bin {bin_!r} does not divide shards[0] {shards[0]!r}"
        )
    prev_ms = shard_ms_list[0]
    for j in range(1, len(shards)):
        cur_ms = shard_ms_list[j]
        if cur_ms is None or prev_ms is None:
            prev_ms = cur_ms
            continue
        if cur_ms <= prev_ms:
            raise ValueError(
                f"parse_pyramid_yaml: tiers[{tier_idx}] ({tier_name!r}): "
                f"shards not ascending (shards[{j}] {shards[j]!r} ({cur_ms}ms) "
                f"<= shards[{j-1}] {shards[j-1]!r} ({prev_ms}ms))"
            )
        if cur_ms % prev_ms != 0:
            raise ValueError(
                f"parse_pyramid_yaml: tiers[{tier_idx}] ({tier_name!r}): "
                f"shards[{j-1}] {shards[j-1]!r} does not divide shards[{j}] {shards[j]!r}"
            )
        prev_ms = cur_ms


_FIXED_UNITS: set[str] = {'min', 'h', 'd'}


def _fixed_ms_or_none(s: str) -> int | None:
    """Return the duration's length in milliseconds, or `None` for
    calendar-variable units (`mo`/`y`) that don't have a fixed ms width."""
    from .axis import _MS  # local import to avoid cycle on module load
    span = parse_duration(s)
    if span.unit not in _FIXED_UNITS:
        return None
    return span.count * _MS[span.unit]


def _parse_geo(raw: Any) -> GeoSpec:
    if not isinstance(raw, dict):
        raise ValueError("parse_pyramid_yaml: `geo` must be a mapping")
    cell_col = raw.get('cellCol', 'h3_cell')
    if not isinstance(cell_col, str):
        raise ValueError("parse_pyramid_yaml: `geo.cellCol` must be a string")
    resolutions = raw.get('resolutions')
    if not isinstance(resolutions, list) or not resolutions:
        raise ValueError("parse_pyramid_yaml: `geo.resolutions` must be a non-empty list of ints")
    for i, r in enumerate(resolutions):
        if not isinstance(r, int) or r < 0 or r > 15:
            raise ValueError(
                f"parse_pyramid_yaml: geo.resolutions[{i}] must be an int in 0-15 (got {r!r})"
            )
    for i in range(1, len(resolutions)):
        if resolutions[i] >= resolutions[i - 1]:
            raise ValueError(
                "parse_pyramid_yaml: geo.resolutions must be finest-first (descending); "
                f"got {resolutions}"
            )
    return GeoSpec(cellCol=cell_col, resolutions=tuple(resolutions))

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol


Axis = Literal['time', 'step']
TimeUnit = Literal['min', 'h', 'd', 'mo', 'y']
StepUnit = Literal['step', 'steps', 'ksteps', 'msteps']

MonoidName = Literal[
    'sum',
    'count',
    'histogram',
    'topk',
    'botk',
    'hll',
    'tdigest',
]

DimType = Literal['int', 'string', 'h3', 'geohash', 's2']


@dataclass(frozen=True)
class Tier:
    name: str
    bin: str
    shards: tuple[str, ...]
    # Per-tier extras (`specs/pyrmts-ops-adoption.md` phase 1) — preserved
    # first-class from the YAML instead of consumer-side re-parses.
    # Output-shard parquet row-group size; None = the writer's heuristic.
    rg_size: int | None = None
    # Ladder-extension rungs materialized incrementally (e.g. by a cron
    # Lambda) rather than by bulk builds; must continue the tier's
    # divisibility chain. `merge_lambda_shards` folds them into `shards`
    # for executors that want the extended view.
    lambda_shards: tuple[str, ...] = ()


@dataclass(frozen=True)
class Dim:
    name: str
    type: DimType


@dataclass(frozen=True)
class Metric:
    name: str
    monoid: MonoidName
    config: dict | None = None


@dataclass(frozen=True)
class GeoSpec:
    cellCol: str
    resolutions: tuple[int, ...]


class Storage(Protocol):
    def head(self, key: str) -> dict | None: ...
    def get(self, key: str) -> bytes | None: ...
    def put(self, key: str, data: bytes) -> None: ...
    def list(self, prefix: str): ...


@dataclass
class Pyramid:
    storage: Storage
    keyTemplate: str
    binCol: str
    dims: list[Dim]
    metrics: list[Metric]
    tiers: list[Tier]
    axis: Axis = 'time'
    geo: GeoSpec | None = None

    def tier(self, name: str) -> Tier:
        for t in self.tiers:
            if t.name == name:
                return t
        raise KeyError(f"No tier named {name!r}; have {[t.name for t in self.tiers]}")

    def tier_index(self, name: str) -> int:
        for i, t in enumerate(self.tiers):
            if t.name == name:
                return i
        raise KeyError(f"No tier named {name!r}; have {[t.name for t in self.tiers]}")

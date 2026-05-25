from __future__ import annotations

from dataclasses import dataclass, field
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

DimType = Literal['int', 'string', 'h3', 'geohash']


@dataclass(frozen=True)
class Tier:
    name: str
    bin: str
    shard: str


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

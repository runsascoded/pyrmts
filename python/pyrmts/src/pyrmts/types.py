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


@dataclass(frozen=True)
class IdentityRollup:
    """Declares an id-map-keyed canonical identity level over a ragged-vocab
    column (`specs/pyrmts-identity-rollup.md`). `col` holds `s:<raw>` station
    leaves alongside s2 cells; a `{raw_token: canonical_token}` map rolls the
    leaves up into `canonicalPrefix`-namespaced canonical rows. `map` is a
    declared input (storage key / path) the engine loads and the harness
    treats as a DVX dependency of the canonical rows."""
    col: str
    map: str
    canonicalPrefix: str = 'c:'


@dataclass(frozen=True)
class MultiScanPolicy:
    """Declarative multi-scan consolidation policy (`specs/multi-scan-consolidation.md`,
    Phase 2c). The *parameters* of automatic sealing — which `(tier, shard)` tile
    to fold the scan axis of, the routing `dataset` scope, the grouping `scheme`,
    the interval `encoder`, and whether to `drop` individuals after digest-verify.
    The *trigger/cadence* stays with the consumer (a cron or an end-of-scan-emit
    stage invokes `multiscan seal`); pyrmts is pure mechanism. Write-side only —
    the reader routes via the manifest and never needs this.

    Two schemes:
    - `'fixed'` — seal every `group_size` scans into an immutable capped-K
      archive. Archive count grows O(N/K) — simplest; good for steady churn.
    - `'exponential'` — the logarithmic method (Bentley–Saxe / LSM leveling):
      old scans coalesce into `base`-power-sized archives (recent scans in small
      blocks), so the archive count grows only O(log N). Best for low-churn data
      kept indefinitely; costs O(log N) rewrites per scan as it climbs levels."""
    dataset: str
    tier: str
    shard: str
    scheme: str = 'fixed'
    group_size: int = 0
    base: int = 2
    encoder: str = 'interval'
    drop: bool = False


class Storage(Protocol):
    def head(self, key: str) -> dict | None: ...
    def get(self, key: str) -> bytes | None: ...
    def put(self, key: str, data: bytes) -> None: ...
    def delete(self, key: str) -> None: ...
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
    identity_rollup: IdentityRollup | None = None
    multi_scan: MultiScanPolicy | None = None

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

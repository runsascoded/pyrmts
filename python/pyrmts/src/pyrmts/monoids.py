"""Monoid catalog. Each monoid defines (a) the column-suffix layout for how
its state is stored alongside a metric, and (b) an associative+commutative
combine that merges two states. Mirrors `js/packages/pyrmts/src/monoids.ts`."""
from __future__ import annotations

import json
from abc import ABC, abstractmethod

from .types import MonoidName


Row = dict[str, object]


class Monoid(ABC):
    state_suffixes: tuple[str, ...]

    def state_columns(self, metric_name: str) -> tuple[str, ...]:
        return tuple(f"{metric_name}{suf}" for suf in self.state_suffixes)

    @abstractmethod
    def combine(self, target: Row, source: Row, metric_name: str) -> None: ...

    def init(self, target: Row, metric_name: str) -> None:
        """Normalize a freshly-created aggregate row for this metric. Default no-op."""
        return None


class _Sum(Monoid):
    state_suffixes = ('_n', '_sum', '_sumsq')

    def combine(self, target: Row, source: Row, metric_name: str) -> None:
        for suf in self.state_suffixes:
            col = f"{metric_name}{suf}"
            t = target.get(col) or 0
            s = source.get(col) or 0
            target[col] = t + s


class _Count(Monoid):
    state_suffixes = ('',)

    def combine(self, target: Row, source: Row, metric_name: str) -> None:
        t = target.get(metric_name) or 0
        s = source.get(metric_name) or 0
        target[metric_name] = t + s


class _Histogram(Monoid):
    state_suffixes = ('',)

    def init(self, target: Row, metric_name: str) -> None:
        target[metric_name] = _parse_hist(target.get(metric_name))

    def combine(self, target: Row, source: Row, metric_name: str) -> None:
        t = _parse_hist(target.get(metric_name))
        s = _parse_hist(source.get(metric_name))
        for k, v in s.items():
            t[k] = t.get(k, 0) + v
        target[metric_name] = t


def _parse_hist(v: object) -> dict[str, int]:
    if v is None: return {}
    if isinstance(v, str): return json.loads(v)
    if isinstance(v, dict): return dict(v)
    raise TypeError(f"histogram: unexpected value type {type(v).__name__} ({v!r})")


_MONOIDS: dict[str, Monoid] = {
    'sum': _Sum(),
    'count': _Count(),
    'histogram': _Histogram(),
}


def get_monoid(name: MonoidName) -> Monoid:
    m = _MONOIDS.get(name)
    if m is None:
        raise NotImplementedError(f"Monoid {name!r} not yet implemented")
    return m


def state_columns(monoid: MonoidName, metric_name: str) -> tuple[str, ...]:
    return get_monoid(monoid).state_columns(metric_name)

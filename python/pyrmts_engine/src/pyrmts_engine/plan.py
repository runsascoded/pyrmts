"""Build plan: the ladder compiled to an explicit DAG.

- Nodes = tiers; each non-source tier's edge points at its
  **divisibility predecessor**: the coarsest finer tier whose bin divides
  its bin (e.g. ctbk's `/2m,/3m,/5m ← /1m` since 2,3,5 are pairwise
  coprime; `/{10,15}m ← /5m`). All tiers sourcing the same predecessor
  share one cached long-form frame per window — fusion by construction.
- Output set = `list_expected_shards(pyramid, range)` (min-cover,
  genesis-clipped), minus any rung the source itself provides.

Divisibility across the fixed/calendar boundary: a calendar `mo`/`y` bin
is divisible by any fixed bin that divides one day (month boundaries are
day boundaries), and `y` by `mo`; fixed bins are never divisible by
calendar bins."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

import polars as pl

from pyrmts import ExpectedShard, Pyramid, list_expected_shards, nominal_delta_ms, parse_duration
from pyrmts.types import Tier


UNIT_MS = {'min': 60_000, 'h': 3_600_000, 'd': 86_400_000}
_DAY_MS = 24 * 60 * 60_000


def _bin_ms_or_none(dur: str) -> int | None:
    span = parse_duration(dur)
    if span.unit in ('mo', 'y'):
        return None
    return span.count * UNIT_MS[span.unit]


def _approx_ms(dur: str) -> int:
    """Approximate width for ordering only (calendar units use nominal
    30d/365d). Alias of `pyrmts.nominal_delta_ms` kept for its many
    engine/ops importers."""
    return nominal_delta_ms(dur)


def _divides(p_bin: str, t_bin: str) -> bool:
    """Can `t_bin`-binned aggregates be exactly derived from `p_bin`-binned
    ones (every p-bin lies entirely within one t-bin)?"""
    p_span = parse_duration(p_bin)
    t_span = parse_duration(t_bin)
    p_ms = _bin_ms_or_none(p_bin)
    t_ms = _bin_ms_or_none(t_bin)
    if p_ms is not None and t_ms is not None:
        return t_ms % p_ms == 0
    if p_ms is not None:
        # t is calendar: month/year boundaries are day boundaries.
        return _DAY_MS % p_ms == 0
    if t_ms is not None:
        return False
    # Both calendar: mo divides y (and mo/mo, y/y by count).
    if p_span.unit == 'mo':
        t_months = t_span.count * (12 if t_span.unit == 'y' else 1)
        return t_months % p_span.count == 0
    return t_span.unit == 'y' and t_span.count % p_span.count == 0


def bin_floor_expr(col: str, bin_dur: str) -> pl.Expr:
    """Polars expression flooring an epoch-ms column to `bin_dur` starts."""
    span = parse_duration(bin_dur)
    if span.unit == 'mo':
        # NOT truncate('Nmo'): polars anchors multi-month at the epoch
        # (1970-01), the contract (JS `floorToSpan`) is year-0 anchored —
        # they agree only when 12 % N == 0. Floor months-since-year-0
        # (`M = 12*year + (month-1)`) instead, which is correct for every N
        # (`specs/calendar-composition-and-query-limits.md` §1).
        dt = pl.col(col).cast(pl.Datetime('ms'))
        m = (dt.dt.year() * 12 + dt.dt.month() - 1) // span.count * span.count
        return pl.datetime(m // 12, m % 12 + 1, 1, time_unit='ms').cast(pl.Int64)
    if span.unit == 'y':
        # NOT truncate('Ny'): polars anchors multi-year at the epoch (1970),
        # the contract (JS `floorToSpan`) is year-0 anchored — `4y` → 2024,
        # not 2026. Floor via year arithmetic instead.
        dt = pl.col(col).cast(pl.Datetime('ms'))
        return (
            pl.datetime(dt.dt.year() // span.count * span.count, 1, 1, time_unit='ms')
            .cast(pl.Int64)
        )
    bin_ms = span.count * UNIT_MS[span.unit]
    return (pl.col(col) // bin_ms) * bin_ms


@dataclass
class BuildPlan:
    pyramid: Pyramid
    time_range: tuple[datetime, datetime]
    # tier name → predecessor tier name; None = fed directly by the source.
    preds: dict[str, str | None] = field(default_factory=dict)
    outputs: list[ExpectedShard] = field(default_factory=list)
    skipped_rungs: list[ExpectedShard] = field(default_factory=list)

    def outputs_for_tier(self, tier_name: str) -> list[ExpectedShard]:
        return [e for e in self.outputs if e.tier == tier_name]

    def to_dot(self) -> str:
        lines = ['digraph pyramid_build {', '  rankdir=BT;', '  source [shape=box];']
        n_out: dict[str, int] = {}
        for e in self.outputs:
            n_out[e.tier] = n_out.get(e.tier, 0) + 1
        for t in self.pyramid.tiers:
            label = f"{t.name}\\nbin={t.bin} shards={'/'.join(t.shards)}\\nout={n_out.get(t.name, 0)}"
            lines.append(f'  "{t.name}" [label="{label}"];')
            pred = self.preds[t.name]
            src = 'source' if pred is None else f'"{pred}"'
            lines.append(f'  {src} -> "{t.name}";')
        lines.append('}')
        return '\n'.join(lines)


def compile_plan(
    pyramid: Pyramid,
    time_range: tuple[datetime, datetime],
    filter: dict[str, str | int] | None = None,
    skip_rungs: set[tuple[str, str]] | None = None,
) -> BuildPlan:
    """Compile the ladder + range into a BuildPlan. Tiers must be ordered
    finest-first (existing pyrmts convention); every non-base tier must have
    a divisibility predecessor among finer tiers."""
    skip_rungs = skip_rungs or set()
    preds: dict[str, str | None] = {}
    tiers = pyramid.tiers
    for i, tier in enumerate(tiers):
        pred: Tier | None = None
        for cand in tiers[:i]:
            if not _divides(cand.bin, tier.bin):
                continue
            if pred is None or _approx_ms(cand.bin) > _approx_ms(pred.bin):
                pred = cand
        if pred is None and i > 0:
            raise ValueError(
                f"compile_plan: tier {tier.name!r} (bin {tier.bin!r}) has no "
                f"divisibility predecessor among finer tiers"
            )
        preds[tier.name] = pred.name if pred is not None else None

    expected = list_expected_shards(pyramid, time_range, filter=filter)
    outputs = [e for e in expected if (e.tier, e.shard_dur) not in skip_rungs]
    skipped = [e for e in expected if (e.tier, e.shard_dur) in skip_rungs]
    return BuildPlan(
        pyramid=pyramid,
        time_range=time_range,
        preds=preds,
        outputs=outputs,
        skipped_rungs=skipped,
    )

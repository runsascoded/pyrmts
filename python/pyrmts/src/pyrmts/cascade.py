"""`cascade_tiers`: given a Pyramid with its finest tier's smallest shard
populated, build every other (tier, shard_dur) rung by combining inputs via
the pyramid's monoid catalog.

Per-tier ladder model (post unified-shard-ladder refactor):

- For each tier T from `finest` upward, walk T's `shards` ladder from
  smallest to largest.
- Source for `(T, T.shards[i])`:
  - `i > 0`: same tier, smaller rung — `(T, T.shards[i-1])`
  - `i == 0` and T is finer than `finest`: caller materialized this rung;
    skip.
  - `i == 0` and T is coarser than `finest`: cross-tier promotion from
    `(T-1, T-1.shards[-1])` (finer tier's largest shard, re-binned to T's
    `bin`).

See `specs/done/unified-shard-ladder.md` (JS) and
`specs/done/python-unified-ladder.md` (Python catch-up)."""
from __future__ import annotations

import io
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq

from .axis import ShardPeriod, floor_to_span, parse_duration, shard_periods_covering
from .keys import substitute_key
from .monoids import Monoid, Row, get_monoid
from .types import Metric, Pyramid, Tier


@dataclass
class CascadeResult:
    written: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    errors: list[tuple[str, str]] = field(default_factory=list)

    def summary(self) -> str:
        return (
            f"cascade_tiers: wrote {len(self.written)}, skipped {len(self.skipped)}, "
            f"errors {len(self.errors)}"
        )


def cascade_tiers(
    pyramid: Pyramid,
    time_range: tuple[datetime, datetime],
    finest_tier: str | None = None,
    storage_write=None,
    overwrite: bool = False,
    concurrency: int = 1,
    filter: dict[str, str | int] | None = None,
) -> CascadeResult:
    """Build every (tier, shard_dur) rung at or above `finest_tier` by walking
    each tier's `shards` ladder.

    Caller must have already materialized `(finest_tier, finest_tier.shards[0])`
    for `time_range`.

    Args:
        pyramid: the Pyramid; tiers ordered finest-first.
        time_range: half-open `(from, to)` UTC interval.
        finest_tier: name of the already-materialized tier (defaults to
            `pyramid.tiers[0]`). Its smallest rung (`shards[0]`) is the assumed
            input; larger rungs and coarser tiers are built from it.
        storage_write: where to write outputs (defaults to `pyramid.storage`).
        overwrite: if False, skip outputs that already exist (cheap HEAD).
        concurrency: parallel shard-builds per rung (threaded).
        filter: extra `{...}` values to substitute into `pyramid.keyTemplate`
            (e.g. `{device_id: 17617}` for awair).
    """
    finest = finest_tier or pyramid.tiers[0].name
    finest_idx = pyramid.tier_index(finest)
    filter = filter or {}
    storage_write = storage_write or pyramid.storage

    result = CascadeResult()

    for tier_idx in range(finest_idx, len(pyramid.tiers)):
        tier = pyramid.tiers[tier_idx]
        for shard_idx in range(len(tier.shards)):
            if tier_idx == finest_idx and shard_idx == 0:
                continue  # caller materialized this rung
            src_tier, src_shard_dur = _pick_inputs(pyramid, tier_idx, shard_idx)
            _cascade_one_rung(
                pyramid=pyramid,
                tier=tier,
                shard_dur=tier.shards[shard_idx],
                src_tier=src_tier,
                src_shard_dur=src_shard_dur,
                time_range=time_range,
                storage_write=storage_write,
                overwrite=overwrite,
                concurrency=concurrency,
                filter=filter,
                result=result,
            )
    return result


def _pick_inputs(pyramid: Pyramid, tier_idx: int, shard_idx: int) -> tuple[Tier, str]:
    tier = pyramid.tiers[tier_idx]
    if shard_idx > 0:
        return tier, tier.shards[shard_idx - 1]
    # shard_idx == 0 and tier_idx > finest_idx: cross-tier promotion from
    # finer tier's largest shard.
    src_tier = pyramid.tiers[tier_idx - 1]
    return src_tier, src_tier.shards[-1]


def _cascade_one_rung(
    *,
    pyramid: Pyramid,
    tier: Tier,
    shard_dur: str,
    src_tier: Tier,
    src_shard_dur: str,
    time_range: tuple[datetime, datetime],
    storage_write,
    overwrite: bool,
    concurrency: int,
    filter: dict[str, str | int],
    result: CascadeResult,
) -> None:
    from_, to = time_range
    out_periods = shard_periods_covering(from_, to, shard_dur)

    def work(period: ShardPeriod) -> tuple[str, str]:
        out_key = substitute_key(
            pyramid.keyTemplate,
            {**filter, 'tier': tier.name, 'shard': shard_dur, 'period': period.label},
        )
        if not overwrite and storage_write.head(out_key) is not None:
            return out_key, 'skipped'
        try:
            src_periods = shard_periods_covering(period.start, period.end, src_shard_dur)
            src_tables: list[pa.Table] = []
            for sp in src_periods:
                src_key = substitute_key(
                    pyramid.keyTemplate,
                    {**filter, 'tier': src_tier.name, 'shard': src_shard_dur, 'period': sp.label},
                )
                blob = pyramid.storage.get(src_key)
                if blob is None:
                    continue
                src_tables.append(pq.read_table(io.BytesIO(blob)))
            if not src_tables:
                return out_key, 'skipped'

            out_rows = _combine_to_bin(
                src_tables,
                pyramid=pyramid,
                tier=tier,
                shard_start=period.start,
                shard_end=period.end,
            )
            if not out_rows:
                return out_key, 'skipped'

            out_table = _rows_to_table(out_rows, pyramid)
            buf = io.BytesIO()
            pq.write_table(out_table, buf, compression='snappy')
            storage_write.put(out_key, buf.getvalue())
            return out_key, 'written'
        except Exception as e:
            return out_key, f"error:{e!r}"

    if concurrency > 1:
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            for fut in as_completed(pool.submit(work, p) for p in out_periods):
                key, status = fut.result()
                _record(result, key, status)
    else:
        for p in out_periods:
            key, status = work(p)
            _record(result, key, status)


def _record(result: CascadeResult, key: str, status: str) -> None:
    if status == 'written':
        result.written.append(key)
    elif status == 'skipped':
        result.skipped.append(key)
    else:
        result.errors.append((key, status))


def _combine_to_bin(
    src_tables: list[pa.Table],
    *,
    pyramid: Pyramid,
    tier: Tier,
    shard_start: datetime,
    shard_end: datetime,
) -> list[Row]:
    """Group rows from `src_tables` by `(floor(bin), *dims)` for `tier.bin`,
    then apply each metric's monoid combine. Within a single tier (same-tier
    promotion across rungs), `tier.bin` is unchanged so the floor is a no-op
    on input bins — the same code path handles cross-tier re-binning."""
    bin_col = pyramid.binCol
    dim_names = [d.name for d in pyramid.dims]
    metric_specs: list[tuple[Metric, Monoid]] = [(m, get_monoid(m.monoid)) for m in pyramid.metrics]

    bin_span = parse_duration(tier.bin)
    shard_start_ms = int(shard_start.timestamp() * 1000)
    shard_end_ms = int(shard_end.timestamp() * 1000)

    groups: dict[tuple, Row] = {}

    for table in src_tables:
        bin_arr = table.column(bin_col).to_pylist()
        dim_arrs = [table.column(d).to_pylist() for d in dim_names]
        metric_arrs: dict[str, list] = {}
        for metric, monoid in metric_specs:
            for col in monoid.state_columns(metric.name):
                metric_arrs[col] = table.column(col).to_pylist()

        for i, ts_ms in enumerate(bin_arr):
            if ts_ms is None: continue
            ts_ms_int = int(ts_ms)
            if ts_ms_int < shard_start_ms or ts_ms_int >= shard_end_ms: continue
            ts = datetime.fromtimestamp(ts_ms_int / 1000, tz=shard_start.tzinfo)
            bin_start = floor_to_span(ts, bin_span)
            bin_start_ms = int(bin_start.timestamp() * 1000)

            dim_vals = tuple(arr[i] for arr in dim_arrs)
            key = (bin_start_ms,) + dim_vals

            src_row: Row = {col: arr[i] for col, arr in metric_arrs.items()}

            row = groups.get(key)
            if row is None:
                row = {bin_col: bin_start_ms}
                for d_name, d_val in zip(dim_names, dim_vals):
                    row[d_name] = d_val
                for metric, monoid in metric_specs:
                    for col in monoid.state_columns(metric.name):
                        row[col] = src_row.get(col)
                    monoid.init(row, metric.name)
                groups[key] = row
            else:
                for metric, monoid in metric_specs:
                    monoid.combine(row, src_row, metric.name)

    return list(groups.values())


def _rows_to_table(rows: list[Row], pyramid: Pyramid) -> pa.Table:
    """Build a sorted arrow Table from output rows. Sort by `(*dims, binCol)`
    for RG-pushdown-friendly layout."""
    bin_col = pyramid.binCol
    dim_names = [d.name for d in pyramid.dims]

    sort_key = lambda r: tuple(r.get(d) for d in dim_names) + (r.get(bin_col),)
    rows = sorted(rows, key=sort_key)

    columns: dict[str, list] = {bin_col: [r[bin_col] for r in rows]}
    for d in dim_names:
        columns[d] = [r.get(d) for r in rows]
    for m in pyramid.metrics:
        monoid = get_monoid(m.monoid)
        for col in monoid.state_columns(m.name):
            if m.monoid == 'histogram':
                columns[col] = [_dump_hist(r.get(col)) for r in rows]
            else:
                columns[col] = [r.get(col) for r in rows]

    return pa.table(columns)


def _dump_hist(v: object) -> str | None:
    if v is None: return None
    if isinstance(v, str): return v
    if isinstance(v, dict):
        return json.dumps(dict(sorted(v.items())))
    raise TypeError(f"histogram: cannot dump {type(v).__name__} ({v!r})")

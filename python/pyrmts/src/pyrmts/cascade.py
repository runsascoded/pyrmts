"""`cascade_tiers`: given a Pyramid with its finest tier populated, build each
coarser tier by combining input shards via the pyramid's monoid catalog.

Pure axis arithmetic + monoid application. No project-specific logic (h3,
GBFS, etc.) — consumers materialize the finest tier; this builds the rest.

See `specs/cascade-tiers-and-geo-materializer.md`."""
from __future__ import annotations

import io
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable

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
    derive_from: dict[str, str] | None = None,
    concurrency: int = 1,
    filter: dict[str, str | int] | None = None,
) -> CascadeResult:
    """For each tier coarser than `finest_tier`, build every shard covering
    `time_range` by combining the appropriate finer-tier shards via the
    monoid catalog.

    Args:
        pyramid: the Pyramid (finest tier must already be populated for `time_range`).
        time_range: half-open `(from, to)` UTC interval.
        finest_tier: name of the already-populated tier (defaults to
            `pyramid.tiers[0]`).
        storage_write: where to write outputs (defaults to `pyramid.storage`).
        overwrite: if False, skip outputs that already exist (cheap HEAD).
        derive_from: tier_name → source-tier name (defaults to the next-finer
            tier in `pyramid.tiers`).
        concurrency: parallel shard-builds per tier (threaded).
        filter: extra `{...}` values to substitute into `pyramid.keyTemplate`
            (e.g. `{device_id: 17617}` for awair).
    """
    finest = finest_tier or pyramid.tiers[0].name
    finest_idx = pyramid.tier_index(finest)
    derive_from = derive_from or {}
    filter = filter or {}

    result = CascadeResult()

    for tier_idx in range(finest_idx + 1, len(pyramid.tiers)):
        tier = pyramid.tiers[tier_idx]
        src_name = derive_from.get(tier.name) or pyramid.tiers[tier_idx - 1].name
        src_tier = pyramid.tier(src_name)
        _cascade_one_tier(
            pyramid=pyramid,
            tier=tier,
            src_tier=src_tier,
            time_range=time_range,
            storage_write=storage_write or pyramid.storage,
            overwrite=overwrite,
            concurrency=concurrency,
            filter=filter,
            result=result,
        )
    return result


def _cascade_one_tier(
    *,
    pyramid: Pyramid,
    tier: Tier,
    src_tier: Tier,
    time_range: tuple[datetime, datetime],
    storage_write,
    overwrite: bool,
    concurrency: int,
    filter: dict[str, str | int],
    result: CascadeResult,
) -> None:
    from_, to = time_range
    out_periods = shard_periods_covering(from_, to, tier.shard)

    def work(period: ShardPeriod) -> tuple[str, str]:
        out_key = substitute_key(
            pyramid.keyTemplate,
            {**filter, 'tier': tier.name, 'period': period.label},
        )
        if not overwrite and storage_write.head(out_key) is not None:
            return out_key, 'skipped'
        try:
            src_periods = shard_periods_covering(period.start, period.end, src_tier.shard)
            src_tables: list[pa.Table] = []
            for sp in src_periods:
                src_key = substitute_key(
                    pyramid.keyTemplate,
                    {**filter, 'tier': src_tier.name, 'period': sp.label},
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
    then apply each metric's monoid combine."""
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

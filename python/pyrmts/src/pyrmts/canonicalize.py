"""`recanonicalize_table` / `canonicalize_shards`: the id-map-keyed identity
rollup (`specs/pyrmts-identity-rollup.md`).

A pyramid whose ragged-vocab column mixes raw station leaves (`s:<raw_id>`)
with s2 rollup cells can carry a *canonical* identity level, materialized as a
rollup of the raw leaves per a `{raw_token: canonical_token}` id-map. It is a
purely additive, idempotent overlay on a built shard: it introduces
`c:<canonical>` rows summed from their constituent `s:` rows and leaves raw
leaves and s2 cells untouched. `cascade` never sees it — raw tiles stay
id-map-independent (cacheable), and a map change re-derives only the canonical
rows (a local shard rewrite: no source re-pull, no re-cascade)."""
from __future__ import annotations

import io
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq

from .axis import ShardPeriod, shard_periods_covering
from .cascade import _rows_to_table
from .keys import substitute_key
from .monoids import Monoid, Row, get_monoid
from .types import Metric, Pyramid, Tier


def _rollup_col(pyramid: Pyramid) -> str:
    if pyramid.identity_rollup is not None:
        return pyramid.identity_rollup.col
    if pyramid.geo is not None:
        return pyramid.geo.cellCol
    raise ValueError(
        "canonicalize: no rollup column — pass `col`, or declare `identityRollup`/`geo` on the pyramid"
    )


def recanonicalize_table(
    table: pa.Table,
    id_map: dict[str, str],
    *,
    pyramid: Pyramid,
    col: str | None = None,
    canonical_prefix: str = 'c:',
) -> pa.Table:
    """Return `table` with its canonical (`canonical_prefix`-namespace) rows
    replaced by a fresh rollup of the raw leaves per `id_map`.

    Idempotent: existing rows whose `col` value begins with `canonical_prefix`
    are dropped first, then rebuilt from the surviving rows whose `col` value
    is an `id_map` key — grouped by `(bin, id_map[value], *other-dims)` and
    monoid-combined. Raw `s:` leaves and s2 cells pass through untouched; a row
    whose token is absent from `id_map` (an unmerged station, or any s2 cell)
    keeps only its own leaf, so there is no canonical row and no duplication.

    No re-binning: a built shard's bins are already correct, so grouping is on
    the stored `bin` value directly (unlike `cascade`'s `floor_to_span`).

    When every metric's monoid is `additive` (sum/count — merge is elementwise
    addition of the state columns), this dispatches to a vectorized pyarrow
    group-by-sum (`_recanonicalize_additive`) that is O(1) Python per shard
    rather than O(rows); the generic per-row combine below still handles any
    non-additive monoid (e.g. histogram). Both paths produce the same rows."""
    col = col or _rollup_col(pyramid)
    bin_col = pyramid.binCol
    dim_names = [d.name for d in pyramid.dims]
    if col not in dim_names:
        raise ValueError(f"recanonicalize_table: rollup col {col!r} is not a pyramid dim ({dim_names})")
    other_dims = [d for d in dim_names if d != col]
    metric_specs: list[tuple[Metric, Monoid]] = [(m, get_monoid(m.monoid)) for m in pyramid.metrics]
    state_cols = [c for m, mon in metric_specs for c in mon.state_columns(m.name)]

    if metric_specs and all(mon.additive for _, mon in metric_specs):
        return _recanonicalize_additive(
            table, id_map,
            bin_col=bin_col, col=col, other_dims=other_dims,
            state_cols=state_cols, canonical_prefix=canonical_prefix,
        )

    n = table.num_rows
    col_arr = table.column(col).to_pylist()
    bin_arr = table.column(bin_col).to_pylist()
    other_arrs = {d: table.column(d).to_pylist() for d in other_dims}
    state_arrs = {c: table.column(c).to_pylist() for c in state_cols}

    kept_rows: list[Row] = []       # raw leaves + s2 cells, verbatim
    groups: dict[tuple, Row] = {}   # freshly-derived canonical rows

    for i in range(n):
        token = col_arr[i]
        if isinstance(token, str) and token.startswith(canonical_prefix):
            continue  # stale canonical row — drop; rebuilt below

        row: Row = {bin_col: bin_arr[i], col: token}
        for d in other_dims:
            row[d] = other_arrs[d][i]
        for c in state_cols:
            row[c] = state_arrs[c][i]
        kept_rows.append(row)

        canonical = id_map.get(token)
        if canonical is None:
            continue  # unmerged station / s2 cell — leaf only

        key = (bin_arr[i], canonical) + tuple(other_arrs[d][i] for d in other_dims)
        src_row: Row = {c: state_arrs[c][i] for c in state_cols}
        agg = groups.get(key)
        if agg is None:
            agg = {bin_col: bin_arr[i], col: canonical}
            for d in other_dims:
                agg[d] = other_arrs[d][i]
            for c in state_cols:
                agg[c] = src_row.get(c)
            for m, mon in metric_specs:
                mon.init(agg, m.name)
            groups[key] = agg
        else:
            for m, mon in metric_specs:
                mon.combine(agg, src_row, m.name)

    return _rows_to_table(kept_rows + list(groups.values()), pyramid)


def _recanonicalize_additive(
    table: pa.Table,
    id_map: dict[str, str],
    *,
    bin_col: str,
    col: str,
    other_dims: list[str],
    state_cols: list[str],
    canonical_prefix: str,
) -> pa.Table:
    """Vectorized `recanonicalize_table` for additive monoids (sum/count): the
    canonical rows are a pyarrow group-by-sum of the raw leaves the id-map folds
    together — semantically identical to the generic per-row combine, but the
    heavy work runs in pyarrow (C++) rather than a Python row loop, so it scales
    to shards with millions of rows. Raw leaves + s2 cells pass through unchanged
    and in their original order; only the derived `c:` rows are (re)built.

    `state_cols` are the concatenated `state_columns` of every metric; additive
    monoids merge each by addition, so a `group_by(bin, canonical, *dims).sum()`
    reproduces the combine exactly. The canonical rows are sorted deterministically
    so a re-run over already-canonicalized output is byte-identical (idempotent)."""
    import pyarrow.compute as pc

    # Drop any stale canonical rows; raw leaves + s2 cells survive verbatim.
    is_canon = pc.starts_with(table.column(col), canonical_prefix)
    raw = table.filter(pc.invert(is_canon))
    if not id_map or raw.num_rows == 0:
        return raw

    # Map each raw token → its canonical via a left join with the id-map, then
    # keep only the rows an id-map entry folds (unmapped tokens stay leaf-only).
    # The join key type must match `col`'s stored type exactly (pyarrow rejects
    # a `string` vs `large_string` key mismatch, which varies shard to shard).
    col_type = raw.schema.field(col).type
    map_tbl = pa.table({
        col: pa.array(list(id_map.keys()), type=col_type),
        '__canon': pa.array(list(id_map.values()), type=col_type),
    })
    joined = raw.join(map_tbl, keys=[col], join_type='left outer')
    to_roll = joined.filter(pc.is_valid(joined.column('__canon')))
    if to_roll.num_rows == 0:
        return raw

    grouped = to_roll.group_by([bin_col, '__canon', *other_dims]).aggregate(
        [(c, 'sum') for c in state_cols]
    )
    # `aggregate` names summed columns `<c>_sum`; restore the state names and
    # promote `__canon` into the rollup column, then match `raw`'s schema exactly
    # (names, order, and types) so the two tables concatenate.
    rename = {f'{c}_sum': c for c in state_cols}
    rename['__canon'] = col
    grouped = grouped.rename_columns([rename.get(n, n) for n in grouped.column_names])
    canon = grouped.select(raw.column_names).cast(raw.schema)
    canon = canon.sort_by([(col, 'ascending'), (bin_col, 'ascending')]
                          + [(d, 'ascending') for d in other_dims])
    return pa.concat_tables([raw, canon])


@dataclass
class CanonicalizeResult:
    written: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    errors: list[tuple[str, str]] = field(default_factory=list)

    def summary(self) -> str:
        return (
            f"canonicalize_shards: wrote {len(self.written)}, "
            f"skipped {len(self.skipped)}, errors {len(self.errors)}"
        )


def canonicalize_shards(
    pyramid: Pyramid,
    id_map: dict[str, str],
    time_range: tuple[datetime, datetime],
    *,
    storage_write=None,
    col: str | None = None,
    canonical_prefix: str = 'c:',
    concurrency: int = 1,
    filter: dict[str, str | int] | None = None,
) -> CanonicalizeResult:
    """Re-derive canonical rows in every built shard overlapping `time_range`,
    in place: read shard → `recanonicalize_table` → write. Missing shards are
    skipped. No source pull and no cascade — the raw leaves already in each
    shard are the rollup source, so this is the reactive fast path for an
    id-map change (append the affected span to the invalidation journal, then
    run this over it)."""
    storage_write = storage_write or pyramid.storage
    col = col or _rollup_col(pyramid)
    filter = filter or {}
    from_, to = time_range
    result = CanonicalizeResult()

    def work(tier: Tier, shard_dur: str, period: ShardPeriod) -> tuple[str, str]:
        key = substitute_key(
            pyramid.keyTemplate,
            {**filter, 'tier': tier.name, 'shard': shard_dur, 'period': period.label},
        )
        blob = pyramid.storage.get(key)
        if blob is None:
            return key, 'skipped'
        try:
            table = pq.read_table(io.BytesIO(blob))
            out = recanonicalize_table(
                table, id_map, pyramid=pyramid, col=col, canonical_prefix=canonical_prefix,
            )
            buf = io.BytesIO()
            pq.write_table(out, buf, compression='snappy')
            storage_write.put(key, buf.getvalue())
            return key, 'written'
        except Exception as e:
            return key, f"error:{e!r}"

    tasks = [
        (tier, shard_dur, period)
        for tier in pyramid.tiers
        for shard_dur in tier.shards
        for period in shard_periods_covering(from_, to, shard_dur)
    ]

    if concurrency > 1:
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            for fut in as_completed(pool.submit(work, *t) for t in tasks):
                _record(result, *fut.result())
    else:
        for t in tasks:
            _record(result, *work(*t))
    return result


def _record(result: CanonicalizeResult, key: str, status: str) -> None:
    if status == 'written':
        result.written.append(key)
    elif status == 'skipped':
        result.skipped.append(key)
    else:
        result.errors.append((key, status))

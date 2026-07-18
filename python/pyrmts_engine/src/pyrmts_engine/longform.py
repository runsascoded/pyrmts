"""Long-form ↔ wide conversions.

Long form is the canonical BUILD representation: one row per
`(*dims, bin, metric, state)` with a `count` value. Every cascade step is a
pure columnar `group_by(...).sum(count)` — no JSON anywhere inside the DAG.

Column layout:

- dim cols: one per `pyramid.dims` (Utf8 or Int64 per dim type)
- bin col (`pyramid.binCol`): Int64 epoch-ms
- `metric`: Utf8. For histogram metrics this is the metric name; for scalar
  monoids (sum/count) it is the *state column* name (`m_n`, `m_sum`,
  `m_sumsq` for sum; `m` for count) — so a sum metric contributes three
  long rows per (dims, bin) and the group_by-sum combine is uniform across
  monoid classes.
- `state`: Int32, null for scalar rows. (Spec said int16; int32 buys
  headroom for e.g. minute-valued histogram states at negligible cost.)
- `count`: Float64 — exact for integer values through 2^53; cast back to
  Int64 at wide-write for histogram counts, count-monoid cols, and `_n`.

Wide hist-JSON is materialized exactly ONCE per output shard via
`long_to_wide` (native polars expressions throughout — no `map_elements`;
ported from ctbk `pyramid_cascade/_hist.py`)."""
from __future__ import annotations

import polars as pl

from pyrmts import Metric, Pyramid

METRIC_COL = 'metric'
STATE_COL = 'state'
COUNT_COL = 'count'


def hist_metrics(pyramid: Pyramid) -> list[Metric]:
    return [m for m in pyramid.metrics if m.monoid == 'histogram']


def scalar_state_cols(pyramid: Pyramid) -> list[str]:
    """State-column names for non-histogram metrics, in metric order.
    These are the values the `metric` long-form column takes for scalar rows."""
    from pyrmts.monoids import get_monoid
    cols: list[str] = []
    for m in pyramid.metrics:
        if m.monoid == 'histogram':
            continue
        cols.extend(get_monoid(m.monoid).state_columns(m.name))
    return cols


def int_wide_cols(pyramid: Pyramid) -> set[str]:
    """Wide columns that must be Int64 on output: count-monoid cols and
    sum-monoid `_n` cols. (`_sum`/`_sumsq` stay Float64.)"""
    from pyrmts.monoids import get_monoid
    out: set[str] = set()
    for m in pyramid.metrics:
        if m.monoid == 'count':
            out.update(get_monoid(m.monoid).state_columns(m.name))
        elif m.monoid == 'sum':
            out.add(f"{m.name}_n")
    return out


def long_schema(pyramid: Pyramid) -> dict[str, pl.DataType]:
    schema: dict[str, pl.DataType] = {}
    for d in pyramid.dims:
        schema[d.name] = pl.Int64 if d.type == 'int' else pl.Utf8
    schema[pyramid.binCol] = pl.Int64
    schema[METRIC_COL] = pl.Utf8
    schema[STATE_COL] = pl.Int32
    schema[COUNT_COL] = pl.Float64
    return schema


def empty_long(pyramid: Pyramid) -> pl.DataFrame:
    return pl.DataFrame(schema=long_schema(pyramid))


def group_cols(pyramid: Pyramid) -> list[str]:
    return [d.name for d in pyramid.dims] + [pyramid.binCol, METRIC_COL, STATE_COL]


def combine_long(frames: list[pl.DataFrame], pyramid: Pyramid) -> pl.DataFrame:
    """Monoid-combine long frames: concat + group_by-sum. The single
    aggregation primitive every cascade edge and every flush reduces to."""
    if not frames:
        return empty_long(pyramid)
    df = frames[0] if len(frames) == 1 else pl.concat(frames)
    return (
        df.group_by(group_cols(pyramid), maintain_order=False)
        .agg(pl.col(COUNT_COL).sum())
    )


def rebin_long(df: pl.DataFrame, pyramid: Pyramid, bin_expr: pl.Expr) -> pl.DataFrame:
    """Re-bin a long frame to a coarser bin (`bin_expr` maps the bin col to
    its floored value) and combine."""
    bin_col = pyramid.binCol
    return (
        df.with_columns(bin_expr.alias(bin_col))
        .group_by(group_cols(pyramid), maintain_order=False)
        .agg(pl.col(COUNT_COL).sum())
    )


def wide_to_long(df: pl.DataFrame, pyramid: Pyramid) -> pl.DataFrame:
    """Parse a wide shard frame (hist-JSON strings + scalar state cols) into
    long form. This is the base-ingest path when the source is existing wide
    shards; app-specific ingesters may construct long frames directly.

    Hist parsing is native string ops (strip braces, split, explode) — valid
    because our hist JSON is canonical: `{"s":n,...}`, no spaces, no nesting.
    """
    dim_names = [d.name for d in pyramid.dims]
    bin_col = pyramid.binCol
    id_cols = dim_names + [bin_col]
    schema = long_schema(pyramid)
    parts: list[pl.DataFrame] = []

    for m in hist_metrics(pyramid):
        if m.name not in df.columns:
            continue
        part = (
            df.select(*id_cols, pl.col(m.name).alias('_hist'))
            .filter(pl.col('_hist').is_not_null() & (pl.col('_hist') != '{}'))
            .with_columns(pl.col('_hist').str.strip_chars('{}').str.split(',').alias('_kvs'))
            .explode('_kvs', empty_as_null=False)
            .with_columns(
                pl.col('_kvs').str.split(':').list.get(0).str.strip_chars('"')
                .cast(pl.Int32).alias(STATE_COL),
                pl.col('_kvs').str.split(':').list.get(1)
                .cast(pl.Float64).alias(COUNT_COL),
            )
            .with_columns(pl.lit(m.name).alias(METRIC_COL))
            .select(*id_cols, METRIC_COL, STATE_COL, COUNT_COL)
        )
        parts.append(part)

    scalar_cols = [c for c in scalar_state_cols(pyramid) if c in df.columns]
    if scalar_cols:
        part = (
            df.select(*id_cols, *scalar_cols)
            .unpivot(index=id_cols, on=scalar_cols, variable_name=METRIC_COL, value_name=COUNT_COL)
            .filter(pl.col(COUNT_COL).is_not_null())
            .with_columns(
                pl.lit(None, dtype=pl.Int32).alias(STATE_COL),
                pl.col(COUNT_COL).cast(pl.Float64),
            )
            .select(*id_cols, METRIC_COL, STATE_COL, COUNT_COL)
        )
        parts.append(part)

    if not parts:
        return empty_long(pyramid)
    out = pl.concat(parts)
    return out.cast({k: v for k, v in schema.items() if k in out.columns})


def long_to_wide(df: pl.DataFrame, pyramid: Pyramid) -> pl.DataFrame:
    """Materialize wide serving form from a (already-combined) long frame:
    hist-JSON strings (numerically state-sorted, byte-stable) + scalar cols.
    Called exactly once per output shard."""
    dim_names = [d.name for d in pyramid.dims]
    bin_col = pyramid.binCol
    id_cols = dim_names + [bin_col]
    hists = hist_metrics(pyramid)
    hist_names = [m.name for m in hists]
    scalar_cols = scalar_state_cols(pyramid)
    int_cols = int_wide_cols(pyramid)

    if df.height == 0:
        return _empty_wide(pyramid)

    wides: list[pl.DataFrame] = []

    if hist_names:
        hist_long = df.filter(pl.col(METRIC_COL).is_in(hist_names))
        if hist_long.height > 0:
            per_metric = (
                hist_long
                .with_columns(
                    pl.concat_str([
                        pl.lit('"'),
                        pl.col(STATE_COL).cast(pl.Utf8),
                        pl.lit('":'),
                        pl.col(COUNT_COL).cast(pl.Int64).cast(pl.Utf8),
                    ]).alias('_kv')
                )
                .sort(STATE_COL)
                .group_by(id_cols + [METRIC_COL])
                .agg(pl.col('_kv'))
                .with_columns(
                    (pl.lit('{') + pl.col('_kv').list.join(',') + pl.lit('}')).alias('_hist_json')
                )
                .drop('_kv')
            )
            wides.append(per_metric.pivot(on=METRIC_COL, index=id_cols, values='_hist_json'))

    if scalar_cols:
        scalar_long = df.filter(pl.col(METRIC_COL).is_in(scalar_cols))
        if scalar_long.height > 0:
            wides.append(scalar_long.pivot(on=METRIC_COL, index=id_cols, values=COUNT_COL))

    if not wides:
        return _empty_wide(pyramid)

    out = wides[0]
    for w in wides[1:]:
        out = out.join(w, on=id_cols, how='full', coalesce=True)

    # Ensure every output column exists (pivot drops all-absent metrics),
    # then fix dtypes for integer-typed wide cols.
    for m in hist_names:
        if m not in out.columns:
            out = out.with_columns(pl.lit(None, dtype=pl.Utf8).alias(m))
    for c in scalar_cols:
        if c not in out.columns:
            out = out.with_columns(pl.lit(None, dtype=pl.Float64).alias(c))
    out = out.with_columns([pl.col(c).cast(pl.Int64) for c in scalar_cols if c in int_cols])

    return out.select(*id_cols, *hist_names, *scalar_cols)


def _empty_wide(pyramid: Pyramid) -> pl.DataFrame:
    schema: dict[str, pl.DataType] = {}
    for d in pyramid.dims:
        schema[d.name] = pl.Int64 if d.type == 'int' else pl.Utf8
    schema[pyramid.binCol] = pl.Int64
    for m in hist_metrics(pyramid):
        schema[m.name] = pl.Utf8
    int_cols = int_wide_cols(pyramid)
    for c in scalar_state_cols(pyramid):
        schema[c] = pl.Int64 if c in int_cols else pl.Float64
    return pl.DataFrame(schema=schema)

"""DuckDB backend for fleet-scale multi-scan consolidation — the scale path for
`specs/multi-scan-consolidation.md`.

`pyrmts.consolidate_scans` (Python) is the portable reference + correctness
oracle: a one-pass in-memory fold, dependency-light, fine to mid scale. At fleet
scale (tens of scans × millions of keys) its Python-object working set is
multiple GB. This backend runs the *same* SCD-2 interval encoding as vectorized
DuckDB gaps-and-islands over `read_parquet` — columnar, out-of-core (spills to
disk), far less memory and faster — and produces a table **byte-identical** to
the Python path (tested against it as the oracle).

Optional: `pip install pyrmts-engine[duckdb]`. The Python path has no duckdb dep.

The encoding, as SQL: over the per-key scan stream (ordered by scan index), a new
value-run opens whenever the state changes *or* the scan index is discontinuous
(the key was absent in between) — a running sum of those boundaries is the run
id; group by `(key, run)` → `(state, scan_lo, scan_hi)`. Identical semantics to
`pyrmts.multiscan._fold_interval`.
"""
from __future__ import annotations

import io
from collections.abc import Sequence

import pyarrow as pa
import pyarrow.parquet as pq

from pyrmts import MultiScan, Pyramid, get_monoid, scan_digest
from pyrmts.multiscan import SCAN_HI, SCAN_LO

SCAN_COL = '__scan'  # the folded scan-index column in the `long` relation


def _cols(pyramid: Pyramid) -> tuple[list[str], list[str]]:
    """`(key_cols, state_cols)` — the shard's logical key (`binCol` + dims) and
    the concatenated monoid state columns (public-API twin of
    `pyrmts.multiscan._key_state_cols`)."""
    key_cols = [pyramid.binCol, *(d.name for d in pyramid.dims)]
    state_cols = [c for m in pyramid.metrics for c in get_monoid(m.monoid).state_columns(m.name)]
    return key_cols, state_cols


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _intervals_sql(union_sql: str, key_cols: list[str], state_cols: list[str], pyramid: Pyramid) -> str:
    """The gaps-and-islands query over a `long` relation `(__scan, *key_cols,
    *state_cols)` → interval rows `(*key_cols, *state_cols, __scan_lo,
    __scan_hi)`, in `pyrmts.multiscan._multiscan_table`'s exact column order and
    `(*dims, binCol, __scan_lo, __scan_hi)` sort."""
    key_by = ', '.join(_q(c) for c in key_cols)
    state_changed = ' OR '.join(f'{_q(c)} IS DISTINCT FROM lag({_q(c)}) OVER w' for c in state_cols)
    key_sel = ', '.join(_q(c) for c in key_cols)
    state_first = ', '.join(f'any_value({_q(c)}) AS {_q(c)}' for c in state_cols)
    order_by = ', '.join(_q(c) for c in (*(d.name for d in pyramid.dims), pyramid.binCol))
    return f"""
    WITH long AS ({union_sql}),
    marked AS (
        SELECT *,
            CASE WHEN row_number() OVER w = 1
                      OR {_q(SCAN_COL)} <> lag({_q(SCAN_COL)}) OVER w + 1
                      OR {state_changed}
                 THEN 1 ELSE 0 END AS __is_new
        FROM long
        WINDOW w AS (PARTITION BY {key_by} ORDER BY {_q(SCAN_COL)})
    ),
    grp AS (
        SELECT *,
            sum(__is_new) OVER (
                PARTITION BY {key_by} ORDER BY {_q(SCAN_COL)}
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS __grp
        FROM marked
    )
    SELECT {key_sel}, {state_first},
           min({_q(SCAN_COL)})::BIGINT AS {_q(SCAN_LO)},
           max({_q(SCAN_COL)})::BIGINT AS {_q(SCAN_HI)}
    FROM grp
    GROUP BY {key_by}, __grp
    ORDER BY {order_by}, {_q(SCAN_LO)}, {_q(SCAN_HI)}
    """


def _run(con, union_sql: str, labels: list[str], pyramid: Pyramid, digests: dict[str, str]) -> MultiScan:
    key_cols, state_cols = _cols(pyramid)
    table = con.execute(_intervals_sql(union_sql, key_cols, state_cols, pyramid)).to_arrow_table()
    return MultiScan(table=table, scans=list(labels), encoder='interval', digests=digests)


def _union_sql(sources: Sequence[str]) -> str:
    """`SELECT <j> AS __scan, * FROM <source>` UNION ALL over the ordered scan
    sources (a registered relation name or a `read_parquet(...)` expression)."""
    return '\nUNION ALL\n'.join(
        f'SELECT CAST({j} AS BIGINT) AS {_q(SCAN_COL)}, * FROM {src}'
        for j, src in enumerate(sources)
    )


def consolidate_arrow_duckdb(
    scan_tables: Sequence[tuple[str, pa.Table]],
    pyramid: Pyramid,
    *,
    con=None,
) -> MultiScan:
    """DuckDB consolidation of in-memory arrow scan tables (the oracle-parity
    entry). Registers each and folds via gaps-and-islands. For true out-of-core,
    prefer :func:`consolidate_parquet_duckdb`."""
    import duckdb

    if not scan_tables:
        raise ValueError("consolidate_arrow_duckdb: need at least one scan")
    labels = [label for label, _ in scan_tables]
    if len(set(labels)) != len(labels):
        raise ValueError(f"consolidate_arrow_duckdb: duplicate scan labels {labels}")
    con = con or duckdb.connect()
    names = []
    for j, (_, table) in enumerate(scan_tables):
        name = f'__scan_{j}'
        con.register(name, table)
        names.append(name)
    digests = {label: scan_digest(t, pyramid) for label, t in scan_tables}
    return _run(con, _union_sql(names), labels, pyramid, digests)


def consolidate_parquet_duckdb(
    scan_files: Sequence[tuple[str, str]],
    pyramid: Pyramid,
    *,
    con=None,
) -> MultiScan:
    """Out-of-core DuckDB consolidation reading each scan's shard directly with
    `read_parquet` — DuckDB streams + spills, so the working set is bounded far
    below the Python fold's. `scan_files` is ordered `(scan_label, parquet_path)`.
    Per-scan digests are computed one scan at a time (bounded memory)."""
    import duckdb

    if not scan_files:
        raise ValueError("consolidate_parquet_duckdb: need at least one scan")
    labels = [label for label, _ in scan_files]
    if len(set(labels)) != len(labels):
        raise ValueError(f"consolidate_parquet_duckdb: duplicate scan labels {labels}")
    con = con or duckdb.connect()
    sources = [f"read_parquet('{path}')" for _, path in scan_files]
    digests = {label: scan_digest(pq.read_table(path), pyramid) for label, path in scan_files}
    return _run(con, _union_sql(sources), labels, pyramid, digests)

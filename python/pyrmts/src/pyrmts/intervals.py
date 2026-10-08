"""Multi-scan intervals at fleet scale (`specs/multiscan-out-of-core.md`, Phase 1):
the SCD-2 gaps-and-islands kernel as a streaming, key-range-partitionable,
one-scan-appendable library API over DuckDB.

Core `pyrmts` (pyarrow) + `duckdb` (the `pyrmts[duckdb]` extra) — no `Pyramid`,
no polars, so a job image that ships only `pyrmts` can run it. The inputs are
generic relations; `pyrmts_engine.multiscan_duckdb` is the `Pyramid` adapter on
top, and `pyrmts.consolidate_scans` (the Python fold) stays the oracle.

Pieces, composable per range and per process:

- `islands_sql`: a `long` relation `(__scan, *key_cols, *state_cols)` → runs
  `(*key_cols, *state_cols, __scan_lo, __scan_hi)`. A run opens when a *change*
  column differs or the key's scan index skips (absent in between). *Carried*
  columns ride along with a `first` | `last` policy and never open a run.
- `stamped_sql`: scan indices → observation stamps `[vf, vt)`, `vt` = the
  stamp of the first scan after the run, or an `open_stamp` sentinel.
- `key_range_pieces` / `plan_ranges`: `k` contiguous, disjoint, roughly
  row-balanced key ranges (deterministic, JSON), each as conjunctive
  predicates that prune a key-sorted parquet by row-group statistics.
- `append_intervals`: previous stamped intervals + one new scan → the next
  intervals (equal to a rebuild through that scan) and the scan's delta.
- `write_query` / `write_exact_row_groups`: stream a query's batches into a
  parquet of exact `row_group_size`-row groups (bytes depend only on rows).
- `interval_digests` / `relation_digest`: order-insensitive md5 sums computed
  in DuckDB (no `pq.read_table`).
"""
from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any, Literal

import pyarrow as pa
import pyarrow.parquet as pq

SCAN = '__scan'
SCAN_LO = '__scan_lo'
SCAN_HI = '__scan_hi'
U64 = 1 << 64

Carry = Literal['first', 'last']


def ident(name: str) -> str:
    """A quoted DuckDB identifier."""
    return '"' + name.replace('"', '""') + '"'


def lit(v: Any) -> str:
    """A DuckDB literal for a key value (str / int / float / bool / None)."""
    if v is None:
        return 'NULL'
    if isinstance(v, bool):
        return 'TRUE' if v else 'FALSE'
    if isinstance(v, (int, float)):
        return repr(v)
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    raise TypeError(f"lit: unsupported key value {v!r}")


# ── Kernel ────────────────────────────────────────────────────────────────


def islands_sql(
    long_sql: str,
    key_cols: Sequence[str],
    state_cols: Sequence[str],
    *,
    carried: Mapping[str, Carry] | None = None,
    scan_col: str = SCAN,
    order: bool = False,
) -> str:
    """Gaps-and-islands over `long_sql` rows `(scan_col, *key_cols, *state_cols)`
    → `(*key_cols, *state_cols, __scan_lo, __scan_hi)`, one row per run.

    A run opens at a key's first scan, when its scan index skips (the key was
    absent in between), or when a **change** column differs from the previous
    scan's. Change columns = `state_cols` minus `carried`; they are equal
    within a run, so `any_value` is deterministic. A **carried** column never
    opens a run; its value is the run's first (`first`) or last (`last`)
    scan's. With no `carried` (the default) every state column is a change
    column — the original encoding, byte-identical to the Python fold.

    `order`: append the canonical `ORDER BY key_cols, __scan_lo` (opt-in — the
    sort is the expensive part, and callers that re-sort downstream skip it)."""
    carried = dict(carried or {})
    unknown = set(carried) - set(state_cols)
    if unknown:
        raise ValueError(f"islands_sql: carried columns {sorted(unknown)} are not state columns")
    bad = {c: p for c, p in carried.items() if p not in ('first', 'last')}
    if bad:
        raise ValueError(f"islands_sql: carried policy must be 'first' or 'last', got {bad}")
    change = [c for c in state_cols if c not in carried]
    if not change:
        raise ValueError("islands_sql: at least one state column must be a change column")
    s = ident(scan_col)
    key_by = ', '.join(ident(c) for c in key_cols)
    changed = ' OR '.join(f'{ident(c)} IS DISTINCT FROM lag({ident(c)}) OVER w' for c in change)

    def agg(c: str) -> str:
        policy = carried.get(c)
        if policy == 'first':
            return f'arg_min({ident(c)}, {s}) AS {ident(c)}'
        if policy == 'last':
            return f'arg_max({ident(c)}, {s}) AS {ident(c)}'
        return f'any_value({ident(c)}) AS {ident(c)}'

    vals = ', '.join(agg(c) for c in state_cols)
    sql = f"""
    WITH __marked AS (
        SELECT *,
            CASE WHEN row_number() OVER w = 1 OR {s} <> lag({s}) OVER w + 1 OR {changed}
                 THEN 1 ELSE 0 END AS __is_new
        FROM ({long_sql})
        WINDOW w AS (PARTITION BY {key_by} ORDER BY {s})
    ),
    __grp AS (
        SELECT *,
            sum(__is_new) OVER (
                PARTITION BY {key_by} ORDER BY {s}
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS __run
        FROM __marked
    )
    SELECT {key_by}, {vals},
           min({s})::BIGINT AS {ident(SCAN_LO)},
           max({s})::BIGINT AS {ident(SCAN_HI)}
    FROM __grp
    GROUP BY {key_by}, __run"""
    if order:
        sql += f"\n    ORDER BY {key_by}, {ident(SCAN_LO)}"
    return sql


def long_sql(sources: Sequence[str], *, scan_col: str = SCAN) -> str:
    """`SELECT <j> AS __scan, * FROM (<source j>)` UNION ALL over ordered scan
    sources (each a relation name, a `read_parquet(...)` call, or a query)."""
    if not sources:
        raise ValueError("long_sql: need at least one scan source")
    return '\nUNION ALL\n'.join(
        f'SELECT {j}::BIGINT AS {ident(scan_col)}, * FROM ({src})' for j, src in enumerate(sources)
    )


def stamped_sql(
    runs_sql: str,
    key_cols: Sequence[str],
    state_cols: Sequence[str],
    stamps: Sequence[int],
    open_stamp: int,
    *,
    vf: str = 'vf',
    vt: str = 'vt',
) -> str:
    """Map `islands_sql` runs' scan indices to observation stamps:
    `(*key_cols, vf, vt, *state_cols)` with `vf` = the run's first scan's
    stamp and `vt` = the stamp of the first scan *after* the run, or
    `open_stamp` when the run is still open at the last scan. Readers filter
    `vf <= D < vt`. `stamps` is per scan index, strictly increasing."""
    if any(b <= a for a, b in zip(stamps, stamps[1:])):
        raise ValueError("stamped_sql: stamps must be strictly increasing")
    if stamps and open_stamp <= stamps[-1]:
        raise ValueError("stamped_sql: open_stamp must exceed every scan stamp")
    arr = '[' + ', '.join(str(int(t)) for t in [*stamps, open_stamp]) + ']::BIGINT[]'
    keys = ', '.join(ident(c) for c in key_cols)
    vals = ', '.join(ident(c) for c in state_cols)
    return (
        f"SELECT {keys}, ({arr})[{ident(SCAN_LO)} + 1] AS {ident(vf)}, ({arr})[{ident(SCAN_HI)} + 2] AS {ident(vt)}, {vals}\n"
        f"        FROM ({runs_sql})"
    )


# ── Key ranges ────────────────────────────────────────────────────────────


def key_range_pieces(
    range_cols: Sequence[str],
    lo: Sequence[Any] | None,
    hi: Sequence[Any] | None,
) -> list[str]:
    """The lexicographic key range `[lo, hi)` over `range_cols` (`None` =
    unbounded) as a list of conjunctive SQL predicates whose union is exactly
    the range. Each piece is a plain conjunction of comparisons, so it prunes
    a parquet sorted by `range_cols` by row-group statistics — read each piece
    separately and `UNION ALL` them rather than OR-ing them into one filter."""
    cols = list(range_cols)
    lo_t = tuple(lo) if lo is not None else None
    hi_t = tuple(hi) if hi is not None else None
    for name, t in (('lo', lo_t), ('hi', hi_t)):
        if t is not None and len(t) != len(cols):
            raise ValueError(f"key_range_pieces: {name} {t!r} has {len(t)} values for {len(cols)} columns")
    if lo_t is not None and hi_t is not None and hi_t <= lo_t:
        raise ValueError(f"key_range_pieces: empty range {lo_t!r} → {hi_t!r}")

    def rec(cs: list[str], lo: tuple | None, hi: tuple | None) -> list[list[str]]:
        if not cs:
            # Zero columns left: every remaining key equals `lo` (inclusive)
            # and `hi` (exclusive), so a bounded-above range here is empty.
            return [] if hi is not None else [[]]
        c = ident(cs[0])
        if len(cs) == 1:
            # Last column: one comparison per bound (`>=` lo, `<` hi).
            last = []
            if lo is not None:
                last.append(f'{c} >= {lit(lo[0])}')
            if hi is not None:
                last.append(f'{c} < {lit(hi[0])}')
            return [last]
        if lo is not None and hi is not None and lo[0] == hi[0]:
            return [[f'{c} = {lit(lo[0])}', *p] for p in rec(cs[1:], lo[1:], hi[1:])]
        out: list[list[str]] = []
        if lo is not None:
            out += [[f'{c} = {lit(lo[0])}', *p] for p in rec(cs[1:], lo[1:], None)]
        middle = []
        if lo is not None:
            middle.append(f'{c} > {lit(lo[0])}')
        if hi is not None:
            middle.append(f'{c} < {lit(hi[0])}')
        out.append(middle)
        if hi is not None:
            out += [[f'{c} = {lit(hi[0])}', *p] for p in rec(cs[1:], None, hi[1:])]
        return out

    return [' AND '.join(p) if p else 'TRUE' for p in rec(cols, lo_t, hi_t)]


def plan_ranges(
    footers: Iterable[tuple[pq.FileMetaData, int]],
    range_cols: Sequence[str],
    k: int,
    *,
    floor: Sequence[Any] | None = None,
) -> dict:
    """`k` contiguous, disjoint key ranges over `range_cols` of about equal
    weighted rows, cut at row-group starts of key-sorted parquets.

    `footers` are `(metadata, weight)` pairs — typically each source format's
    newest scan, weighted by how many scans share its layout. A row group is a
    cut candidate only when every range column but the last is constant in it
    (its stats then give its exact first key). `floor` is the first range's
    `lo` (default `None`, unbounded); cuts at or below it are dropped.
    Deterministic: same footers → same ranges. Returns `{"k", "ranges": [{"i",
    "lo", "hi"}]}` (JSON-ready; the last range's `hi` is `None`)."""
    cols = list(range_cols)
    points: list[tuple[tuple, int]] = []
    for md, weight in footers:
        names = md.schema.names
        idx = [names.index(c) for c in cols]
        for g in range(md.num_row_groups):
            rg = md.row_group(g)
            stats = [rg.column(i).statistics for i in idx]
            if any(s is None or not s.has_min_max for s in stats):
                continue
            if any(s.min != s.max for s in stats[:-1]):
                continue
            points.append((tuple(s.min for s in stats), rg.num_rows * weight))
    points.sort()
    total = sum(w for _, w in points)
    cuts: list[tuple] = []
    acc, step = 0, total / k
    for key, w in points:
        if acc >= step * (len(cuts) + 1) and (not cuts or key > cuts[-1]):
            cuts.append(key)
        acc += w
    floor_t = tuple(floor) if floor is not None else None
    bounds: list[tuple | None] = [floor_t] + [c for c in cuts if floor_t is None or c > floor_t]
    ranges = []
    for i, lo in enumerate(bounds):
        hi = bounds[i + 1] if i + 1 < len(bounds) else None
        ranges.append({'i': i, 'lo': list(lo) if lo is not None else None, 'hi': list(hi) if hi is not None else None})
    return {'k': len(ranges), 'ranges': ranges}


# ── Append ────────────────────────────────────────────────────────────────


def append_intervals(
    con,
    prev: str,
    new: str,
    key_cols: Sequence[str],
    state_cols: Sequence[str],
    stamp: int,
    open_stamp: int,
    *,
    carried: Mapping[str, Carry] | None = None,
    vf: str = 'vf',
    vt: str = 'vt',
    out: str = '__ivs',
) -> tuple[int, int]:
    """Append one scan to stamped intervals. `prev` is a relation of previous
    intervals (`stamped_sql` shape, any column order); `new` is the scan's rows
    `(*key_cols, *state_cols)`, one per key. Creates table `out` holding the
    next intervals, in `prev`'s column order — equal, as a set of rows, to a
    rebuild through this scan with stamp `stamp`. Returns `(opened, closed)`.

    Closed runs are copied unchanged. An open run whose key is gone, or whose
    change columns differ, closes at `stamp`; a new or changed key opens a run
    at `stamp`. Carried `first` columns are untouched; carried `last` columns
    of an open run that continues are **rewritten** to the new scan's values
    (so `last` makes an append rewrite open rows, not only add rows).

    The delta (`delta_sql`) is the rows with `vf = stamp` (opened) plus the
    rows with `vt = stamp` (closed)."""
    carried = dict(carried or {})
    change = [c for c in state_cols if c not in carried]
    last_cols = [c for c, p in carried.items() if p == 'last']
    last_stamp = con.execute(f'SELECT max({ident(vf)}) FROM ({prev})').fetchone()[0]
    if last_stamp is not None and last_stamp >= stamp:
        raise ValueError(f"append_intervals: intervals already reach {last_stamp} ≥ the appended scan's stamp {stamp}")
    prev_cols = [r[0] for r in con.execute(f'DESCRIBE SELECT * FROM ({prev})').fetchall()]
    keys = ', '.join(ident(c) for c in key_cols)
    on = ' AND '.join(f'o.{ident(c)} = n.{ident(c)}' for c in key_cols)
    differ = ' OR '.join(f'o.{ident(c)} IS DISTINCT FROM n.{ident(c)}' for c in change)
    first_key = ident(key_cols[0])
    con.execute(f"""CREATE OR REPLACE TEMP TABLE __open AS SELECT * FROM ({prev}) WHERE {ident(vt)} = {open_stamp}""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE __new AS SELECT * FROM ({new})""")
    # Every open run vs the scan's rows, by key: (o, n) both present and equal
    # on change columns = continues; anything else closes and/or opens.
    con.execute(f"""CREATE OR REPLACE TEMP TABLE __j AS
        SELECT {', '.join(f'o.{ident(c)} AS {ident("o_" + c)}' for c in key_cols)}, o.{ident(vf)} AS __ovf,
               {', '.join(f'n.{ident(c)} AS {ident("n_" + c)}' for c in key_cols)},
               (o.{first_key} IS NULL) OR (n.{first_key} IS NULL) OR ({differ}) AS __changed
        FROM __open AS o FULL OUTER JOIN __new AS n ON {on}""")
    o_keys = ', '.join(f'{ident("o_" + c)} AS {ident(c)}' for c in key_cols)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE __closes AS
        SELECT {o_keys}, __ovf AS {ident(vf)} FROM __j WHERE {ident("o_" + key_cols[0])} IS NOT NULL AND __changed""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE __opens AS
        SELECT n.* FROM __new AS n SEMI JOIN (
            SELECT * FROM __j WHERE {ident("n_" + key_cols[0])} IS NOT NULL AND __changed
        ) AS c ON {' AND '.join(f'n.{ident(c)} = c.{ident("n_" + c)}' for c in key_cols)}""")
    c_on = ' AND '.join(f'o.{ident(c)} = c.{ident(c)}' for c in [*key_cols, vf])
    joins = f'LEFT JOIN __closes AS c ON {c_on}'
    if last_cols:
        joins += f' LEFT JOIN __new AS n ON {on} AND o.{ident(vt)} = {open_stamp} AND c.{first_key} IS NULL'

    def old_col(c: str) -> str:
        if c == vt:
            return f'CASE WHEN c.{first_key} IS NULL THEN o.{ident(vt)} ELSE {stamp} END::BIGINT AS {ident(vt)}'
        if c in last_cols:
            return f'CASE WHEN n.{first_key} IS NULL THEN o.{ident(c)} ELSE n.{ident(c)} END AS {ident(c)}'
        return f'o.{ident(c)}'

    def new_col(c: str) -> str:
        if c == vf:
            return f'{stamp}::BIGINT AS {ident(vf)}'
        if c == vt:
            return f'{open_stamp}::BIGINT AS {ident(vt)}'
        return ident(c)

    con.execute(f"""CREATE OR REPLACE TABLE {ident(out)} AS
        SELECT {', '.join(old_col(c) for c in prev_cols)} FROM ({prev}) AS o {joins}
        UNION ALL
        SELECT {', '.join(new_col(c) for c in prev_cols)} FROM __opens""")
    n_open, n_close = (con.execute(f'SELECT count(*) FROM {t}').fetchone()[0] for t in ('__opens', '__closes'))
    for t in ('__open', '__new', '__j', '__closes', '__opens'):
        con.execute(f'DROP TABLE {t}')
    return int(n_open), int(n_close)


def delta_sql(table: str, stamp: int, *, vf: str = 'vf', vt: str = 'vt', op: str = 'op') -> str:
    """One scan's delta from stamped intervals: rows opened at `stamp` (`op` =
    1) and rows closed at `stamp` (`op` = −1, carrying their new `vt`)."""
    return (
        f"SELECT *, 1::TINYINT AS {ident(op)} FROM {table} WHERE {ident(vf)} = {stamp}\n"
        f"        UNION ALL SELECT *, -1::TINYINT AS {ident(op)} FROM {table} WHERE {ident(vt)} = {stamp}"
    )


# ── Output ────────────────────────────────────────────────────────────────


def query_batches(con, sql: str, batch_rows: int = 1 << 17) -> Iterator[pa.RecordBatch]:
    """Stream a DuckDB query's result as arrow record batches."""
    yield from con.execute(sql).to_arrow_reader(batch_rows)


def write_exact_row_groups(
    batches: Iterable[pa.RecordBatch],
    out: str | Path,
    schema: pa.Schema,
    row_group_size: int,
    *,
    compression: str = 'zstd',
    dictionary: list[str] | bool = False,
    on_group: Callable[[pa.Table], None] | None = None,
) -> int:
    """Write already-ordered batches as exact `row_group_size`-row groups (the
    last may be short), so the file's bytes depend only on its rows and these
    settings — not on how the batches were chunked. `on_group(table)` sees
    each row group as written. Returns the row count."""
    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    pending: list[pa.RecordBatch] = []
    n_pending = 0
    total = 0
    with pq.ParquetWriter(
        out, schema, compression=compression, use_dictionary=dictionary, write_statistics=True,
        coerce_timestamps='ms', allow_truncated_timestamps=False,
    ) as w:
        def flush(final: bool) -> None:
            nonlocal pending, n_pending
            if not pending:
                return
            t = pa.Table.from_batches(pending, schema=schema).combine_chunks()
            off = 0
            while t.num_rows - off >= row_group_size or (final and off < t.num_rows):
                g = t.slice(off, min(row_group_size, t.num_rows - off))
                w.write_table(g, row_group_size=row_group_size)
                if on_group:
                    on_group(g)
                off += g.num_rows
            rest = t.slice(off)
            pending, n_pending = ([rest.combine_chunks().to_batches()[0]] if rest.num_rows else []), rest.num_rows

        for b in batches:
            if b.num_rows == 0:
                continue
            b = b.cast(schema) if b.schema != schema else b
            pending.append(b)
            n_pending += b.num_rows
            total += b.num_rows
            if n_pending >= row_group_size:
                flush(False)
        flush(True)
    return total


def write_query(
    con,
    sql: str,
    out: str | Path,
    schema: pa.Schema,
    *,
    row_group_size: int,
    sort: Sequence[str] | None = None,
    compression: str = 'zstd',
    dictionary: list[str] | bool = False,
    on_group: Callable[[pa.Table], None] | None = None,
) -> int:
    """Stream `sql` (optionally `ORDER BY sort`) into `out` with
    `write_exact_row_groups` — DuckDB spills, Python holds one row group."""
    if sort:
        sql = f"SELECT * FROM ({sql}) ORDER BY {', '.join(ident(c) for c in sort)}"
    return write_exact_row_groups(
        query_batches(con, sql), out, schema, row_group_size,
        compression=compression, dictionary=dictionary, on_group=on_group,
    )


# ── Digests ───────────────────────────────────────────────────────────────


def row_hash_sql(cols: Sequence[str]) -> str:
    """A row's md5 (upper 64 bits) over `col::VARCHAR` joined by `|`."""
    return f"md5_number_upper(concat_ws('|', {', '.join(f'{ident(c)}::VARCHAR' for c in cols)}))"


def relation_digest(con, sql: str, cols: Sequence[str]) -> tuple[int, int]:
    """`(rows, Σ row_hash mod 2⁶⁴)` of a relation — order-insensitive, computed
    in DuckDB. Two relations with equal digests hold the same rows (up to
    hash collision), whatever their order or file layout."""
    n, h = con.execute(f"SELECT count(*), (coalesce(sum({row_hash_sql(cols)}), 0) % {U64})::UBIGINT FROM ({sql})").fetchone()
    return int(n), int(h)


def interval_digests(
    con,
    table: str,
    open_cols: Sequence[str],
    close_cols: Sequence[str],
    open_stamp: int,
    *,
    vf: str = 'vf',
    vt: str = 'vt',
) -> dict[str, dict[str, list[int]]]:
    """Per stamp: runs opened (`[count, Σ hash(open_cols)]`, by `vf`) and
    closed (`[count, Σ hash(close_cols)]`, by `vt`, excluding `open_stamp`).
    Comparable across builds of any range split, and against another store
    hashing the same strings."""
    out: dict[str, dict[str, list[int]]] = {}
    for ts, n, h in con.execute(
        f"SELECT {ident(vf)}, count(*), (sum({row_hash_sql(open_cols)}) % {U64})::UBIGINT FROM {table} GROUP BY {ident(vf)}"
    ).fetchall():
        out.setdefault(str(ts), {})['opened'] = [int(n), int(h)]
    for ts, n, h in con.execute(
        f"SELECT {ident(vt)}, count(*), (sum({row_hash_sql(close_cols)}) % {U64})::UBIGINT FROM {table} "
        f"WHERE {ident(vt)} <> {open_stamp} GROUP BY {ident(vt)}"
    ).fetchall():
        out.setdefault(str(ts), {})['closed'] = [int(n), int(h)]
    return dict(sorted(out.items()))

"""Multi-scan consolidation: fold a re-observation (scan) axis into a shard
(`specs/multi-scan-consolidation.md`).

A repeated-scan dataset re-observes the whole keyspace on a cadence and stores
one full pyramid per scan — consecutive scans are near-identical, so the naive
layout is O(#scans) in storage. This module consolidates a contiguous set of
single-scan shard tables (all for one `(tier, period)` tile) into one
**multi-scan** table, folding the scan axis *inside* the shard. The scan axis
is a **stack, not a rollup**: values are stored per-scan (or per value-run),
never monoid-combined across scans (that would double-count an object observed
in successive scans).

Two encoders, benchmarked against each other (`multiscan-bench`):

- ``'densify'`` (a) — the full ``key × scan`` grid, sorted scan-innermost, with
  absent ``(key, scan)`` pairs filled with the monoid identity so a fixed key's
  states form a contiguous run parquet RLE/dictionary-encodes. O(#keys×#scans)
  rows; leans on the writer.
- ``'interval'`` (b) — one row per maximal run of consecutive scans in which a
  key is present with constant state (SCD-2 / temporal-table). A key stable
  across every scan is *one* row. O(#changes) rows; never materializes the
  constant cells.

Both round-trip losslessly: :func:`extract_table` reconstructs any member
scan's original rows, and :func:`scan_digest` gives a writer-independent
content hash so a caller can *verify* recovery before deleting the originals.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc

from .cascade import _rows_to_table
from .monoids import Monoid, Row, get_monoid
from .types import Pyramid

#: Reserved (double-underscore, per the in-repo internal-column convention)
#: names for the folded scan axis. `densify` adds `__scan`; `interval` adds
#: `__scan_lo`/`__scan_hi`. Both hold the *index* of a scan into the member
#: list (`MultiScan.scans`), not its label — compact and order-defining.
SCAN_COL = '__scan'
SCAN_LO = '__scan_lo'
SCAN_HI = '__scan_hi'

ENCODERS = ('densify', 'interval')


@dataclass
class MultiScan:
    """One consolidated `(tier, period)` tile plus the ordered member-scan
    labels its folded indices refer to. `table` carries the reserved scan
    column(s) for `encoder`; everything else is the original shard schema."""
    table: pa.Table
    scans: list[str]
    encoder: str


def _key_state_cols(pyramid: Pyramid) -> tuple[list[str], list[str], list[tuple]]:
    """`(key_cols, state_cols, metric_specs)` — the shard's logical key
    (`binCol` + dims) and the concatenated monoid state columns."""
    key_cols = [pyramid.binCol, *(d.name for d in pyramid.dims)]
    metric_specs: list[tuple] = [(m, get_monoid(m.monoid)) for m in pyramid.metrics]
    state_cols = [c for m, mon in metric_specs for c in mon.state_columns(m.name)]
    return key_cols, state_cols, metric_specs


def _identities(pyramid: Pyramid) -> dict[str, object]:
    """The monoid identity for each state column — the fill value for an
    absent `(key, scan)` cell (densify) and the "not present" sentinel
    (extract drops it). Additive monoids (sum/count) → 0; histogram → None.
    Safe because a real shard row exists only where there is data, so no
    genuine row equals the all-identity tuple."""
    ids: dict[str, object] = {}
    for m in pyramid.metrics:
        mon = get_monoid(m.monoid)
        for c in mon.state_columns(m.name):
            ids[c] = 0 if mon.additive else None
    return ids


def _scan_rows(table: pa.Table, key_cols: list[str], state_cols: list[str]) -> dict[tuple, tuple]:
    """A single scan's shard as `{key_tuple: state_tuple}`. State values are
    taken verbatim from the stored columns (numbers, or JSON strings for a
    histogram), so equality of two state tuples is exact value equality."""
    cols = {c: table.column(c).to_pylist() for c in (*key_cols, *state_cols)}
    out: dict[tuple, tuple] = {}
    for i in range(table.num_rows):
        key = tuple(cols[c][i] for c in key_cols)
        out[key] = tuple(cols[c][i] for c in state_cols)
    return out


def consolidate_tables(
    scan_tables: list[tuple[str, pa.Table]],
    pyramid: Pyramid,
    *,
    encoder: str = 'interval',
) -> MultiScan:
    """Consolidate the ordered `(scan_label, shard_table)` list for one
    `(tier, period)` tile into a :class:`MultiScan`. Deterministic (sorted
    output) so re-consolidating identical input is byte-identical."""
    if encoder not in ENCODERS:
        raise ValueError(f"consolidate_tables: unknown encoder {encoder!r}; want one of {ENCODERS}")
    if not scan_tables:
        raise ValueError("consolidate_tables: need at least one scan")
    scans = [label for label, _ in scan_tables]
    if len(set(scans)) != len(scans):
        raise ValueError(f"consolidate_tables: duplicate scan labels {scans}")

    key_cols, state_cols, _ = _key_state_cols(pyramid)
    per_scan = [_scan_rows(t, key_cols, state_cols) for _, t in scan_tables]
    all_keys = sorted({k for rows in per_scan for k in rows})

    if encoder == 'densify':
        table = _encode_densify(all_keys, per_scan, pyramid, key_cols, state_cols)
    else:
        table = _encode_interval(all_keys, per_scan, pyramid, key_cols, state_cols)
    return MultiScan(table=table, scans=scans, encoder=encoder)


def _encode_densify(all_keys, per_scan, pyramid, key_cols, state_cols) -> pa.Table:
    ids = _identities(pyramid)
    rows: list[Row] = []
    for key in all_keys:
        for j, scan in enumerate(per_scan):
            state = scan.get(key)
            row: Row = dict(zip(key_cols, key))
            if state is None:
                for c in state_cols:
                    row[c] = ids[c]
            else:
                row.update(dict(zip(state_cols, state)))
            row[SCAN_COL] = j
            rows.append(row)
    return _multiscan_table(rows, pyramid, key_cols, state_cols, [SCAN_COL])


def _encode_interval(all_keys, per_scan, pyramid, key_cols, state_cols) -> pa.Table:
    n = len(per_scan)
    rows: list[Row] = []
    for key in all_keys:
        run_start: int | None = None
        run_state: tuple | None = None
        for j in range(n):
            state = per_scan[j].get(key)
            if state == run_state and run_start is not None:
                continue  # extend the current run
            if run_start is not None:
                rows.append(_interval_row(key, run_state, run_start, j - 1, key_cols, state_cols))
            if state is None:
                run_start, run_state = None, None
            else:
                run_start, run_state = j, state
        if run_start is not None:
            rows.append(_interval_row(key, run_state, run_start, n - 1, key_cols, state_cols))
    return _multiscan_table(rows, pyramid, key_cols, state_cols, [SCAN_LO, SCAN_HI])


def _interval_row(key, state, lo, hi, key_cols, state_cols) -> Row:
    row: Row = dict(zip(key_cols, key))
    row.update(dict(zip(state_cols, state)))
    row[SCAN_LO] = lo
    row[SCAN_HI] = hi
    return row


def _multiscan_table(rows, pyramid, key_cols, state_cols, scan_cols) -> pa.Table:
    """Materialize consolidated rows, sorted `(*dims, binCol, *scan_cols)` so
    a fixed key's scan values are contiguous (densify → RLE-friendly) and the
    output is deterministic."""
    dim_names = [d.name for d in pyramid.dims]
    sort_key = lambda r: (
        tuple(r.get(d) for d in dim_names)
        + (r.get(pyramid.binCol),)
        + tuple(r[c] for c in scan_cols)
    )
    rows = sorted(rows, key=sort_key)
    columns: dict[str, list] = {}
    for c in (*key_cols, *state_cols, *scan_cols):
        columns[c] = [r.get(c) for r in rows]
    return pa.table(columns)


def extract_table(ms: MultiScan, scan: str, pyramid: Pyramid) -> pa.Table:
    """Reconstruct member `scan`'s original shard table (logical round-trip:
    same rows, canonically sorted `(*dims, binCol)` as pyrmts writes them).
    Raises if `scan` is not a member."""
    try:
        idx = ms.scans.index(scan)
    except ValueError:
        raise ValueError(f"extract_table: {scan!r} not a member scan ({ms.scans})") from None
    key_cols, state_cols, _ = _key_state_cols(pyramid)
    t = ms.table

    if ms.encoder == 'densify':
        sel = t.filter(pc.equal(t.column(SCAN_COL), idx)).drop_columns([SCAN_COL])
    elif ms.encoder == 'interval':
        covers = pc.and_(
            pc.less_equal(t.column(SCAN_LO), idx),
            pc.greater_equal(t.column(SCAN_HI), idx),
        )
        sel = t.filter(covers).drop_columns([SCAN_LO, SCAN_HI])
    else:
        raise ValueError(f"extract_table: unknown encoder {ms.encoder!r}")

    rows = _drop_absent(sel, key_cols, state_cols, pyramid)
    return _rows_to_table(rows, pyramid)


def _drop_absent(table: pa.Table, key_cols, state_cols, pyramid) -> list[Row]:
    """Drop identity-filled rows (a densify absence marker; interval never
    emits them) and return the survivors as `Row` dicts for `_rows_to_table`.
    Histogram state columns are JSON strings on the table; `_rows_to_table`
    passes strings through `_dump_hist` unchanged, so the round-trip is exact."""
    ids = _identities(pyramid)
    cols = {c: table.column(c).to_pylist() for c in (*key_cols, *state_cols)}
    rows: list[Row] = []
    for i in range(table.num_rows):
        if all(cols[c][i] == ids[c] for c in state_cols):
            continue  # absent (all-identity) — not a real row
        rows.append({c: cols[c][i] for c in (*key_cols, *state_cols)})
    return rows


def scan_digest(table: pa.Table, pyramid: Pyramid) -> str:
    """A writer-independent content hash of a shard: sha256 over the rows
    sorted `(*dims, binCol)`, each serialized as its `(key..., state...)`
    values. Independent of parquet byte layout (codec, row-group sizing, KV
    metadata), so it survives a foreign-writer original and proves a logical
    round-trip. Histogram states are compared as their stored JSON string."""
    key_cols, state_cols, _ = _key_state_cols(pyramid)
    dim_names = [d.name for d in pyramid.dims]
    cols = {c: table.column(c).to_pylist() for c in (*key_cols, *state_cols)}
    order = sorted(
        range(table.num_rows),
        key=lambda i: tuple(cols[d][i] for d in dim_names) + (cols[pyramid.binCol][i],),
    )
    h = hashlib.sha256()
    for i in order:
        rec = [cols[c][i] for c in (*key_cols, *state_cols)]
        h.update(json.dumps(rec, separators=(',', ':'), default=str).encode())
        h.update(b'\n')
    return h.hexdigest()

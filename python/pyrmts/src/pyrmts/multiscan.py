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
from collections.abc import Iterable
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

#: KV-metadata key under which a written multi-scan shard carries its own
#: encoder / member-scan list / per-scan digests, so it is self-describing and
#: digest-verifiable on extract (`to_arrow` / `from_arrow`).
META_KEY = b'pyrmts.multiscan'


@dataclass
class MultiScan:
    """One consolidated `(tier, period)` tile plus the ordered member-scan
    labels its folded indices refer to. `table` carries the reserved scan
    column(s) for `encoder`; everything else is the original shard schema.
    `digests` (when present) maps each member scan to its writer-independent
    content hash, so `extract` can prove recovery before the original is
    dropped."""
    table: pa.Table
    scans: list[str]
    encoder: str
    digests: dict[str, str] | None = None


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
    digests = {label: scan_digest(t, pyramid) for label, t in scan_tables}
    return MultiScan(table=table, scans=scans, encoder=encoder, digests=digests)


def consolidate_scans(
    scan_tables: Iterable[tuple[str, pa.Table]],
    pyramid: Pyramid,
    *,
    encoder: str = 'interval',
) -> MultiScan:
    """Streaming interval consolidation over an *iterable* of `(scan_label,
    shard_table)` yielded lazily — the fleet-scale Phase-2 path.

    Folds one scan at a time and discards it, so peak memory is O(#keys in the
    tile) (the open-run frontier plus the current scan), *not* the eager
    :func:`consolidate_tables`' O(#keys × #scans) — which materializes every
    scan's key-dict at once and OOMs at fleet scale (81 scans × ~6 M keys; see
    `specs/multi-scan-consolidation.md`, real-scan operating point). The result
    is byte-identical to `consolidate_tables(..., encoder='interval')` on the
    same scans. `densify` is not streamable (it needs the full grid up front,
    and loses the benchmark), so only `'interval'` is supported here."""
    if encoder != 'interval':
        raise ValueError(
            f"consolidate_scans: streaming supports only 'interval', not {encoder!r} "
            "(use consolidate_tables for densify)"
        )
    key_cols, state_cols, _ = _key_state_cols(pyramid)
    rows, scans, digests = _fold_interval(scan_tables, pyramid, key_cols, state_cols)
    table = _multiscan_table(rows, pyramid, key_cols, state_cols, [SCAN_LO, SCAN_HI])
    return MultiScan(table=table, scans=scans, encoder='interval', digests=digests)


def _fold_interval(scan_tables, pyramid, key_cols, state_cols):
    """One-pass fold: maintain `open_runs[key] = (run_start, state)` and emit a
    closed interval row whenever a key's state changes or it goes absent. Memory
    is the open-run frontier + one scan's rows; the emitted rows (O(#changes))
    are sorted once at materialization for determinism."""
    scans: list[str] = []
    digests: dict[str, str] = {}
    open_runs: dict[tuple, tuple[int, tuple]] = {}
    closed: list[Row] = []
    j = -1
    for j, (label, table) in enumerate(scan_tables):
        if label in digests:
            raise ValueError(f"consolidate_scans: duplicate scan label {label!r}")
        scans.append(label)
        digests[label] = scan_digest(table, pyramid)
        cur = _scan_rows(table, key_cols, state_cols)
        for key, state in cur.items():
            run = open_runs.get(key)
            if run is None:
                open_runs[key] = (j, state)
            elif run[1] != state:
                closed.append(_interval_row(key, run[1], run[0], j - 1, key_cols, state_cols))
                open_runs[key] = (j, state)
            # else: state unchanged → the open run continues (hi extends implicitly)
        for key in [k for k in open_runs if k not in cur]:  # absent this scan → close
            lo, st = open_runs.pop(key)
            closed.append(_interval_row(key, st, lo, j - 1, key_cols, state_cols))
    if j < 0:
        raise ValueError("consolidate_scans: need at least one scan")
    for key, (lo, st) in open_runs.items():
        closed.append(_interval_row(key, st, lo, j, key_cols, state_cols))
    return closed, scans, digests


def to_arrow(ms: MultiScan) -> pa.Table:
    """The consolidated table with its :class:`MultiScan` metadata (encoder,
    ordered member scans, per-scan digests) attached as parquet KV-metadata
    under :data:`META_KEY`. A shard written from this is self-describing:
    :func:`from_arrow` reconstructs the `MultiScan` and `extract` can verify a
    scan's digest before its original is deleted."""
    meta = {'encoder': ms.encoder, 'scans': ms.scans, 'digests': ms.digests or {}}
    existing = ms.table.schema.metadata or {}
    return ms.table.replace_schema_metadata({**existing, META_KEY: json.dumps(meta).encode()})


def from_arrow(table: pa.Table) -> MultiScan:
    """Inverse of :func:`to_arrow`: read the `pyrmts.multiscan` KV-metadata off
    a written multi-scan table and return the :class:`MultiScan`, with the
    metadata stripped from the carried table."""
    md = table.schema.metadata or {}
    raw = md.get(META_KEY)
    if raw is None:
        raise ValueError("from_arrow: table has no pyrmts.multiscan metadata")
    meta = json.loads(raw)
    rest = {k: v for k, v in md.items() if k != META_KEY}
    clean = table.replace_schema_metadata(rest or None)
    return MultiScan(
        table=clean, scans=meta['scans'], encoder=meta['encoder'],
        digests=meta.get('digests') or None,
    )


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


# ── Delta view: diff-indexing + over-time reads (`specs/…` §"observation axis")
# The interval store is also a *diff index*: a diff over `(a, b]` reads only the
# keys whose run boundary falls in the span (O(changes-in-span)), and a key's
# value stream across scans (the over-time plot) is its intervals expanded. The
# universal `diff_tables` works over *any* two scan states (consolidated or raw),
# so the diff API is uniform across the opt-in archive boundary.


def _changeset_table(rows: list[tuple], key_cols, state_cols, pyramid) -> pa.Table:
    """`rows` are `(key_tuple, state_a_tuple, state_b_tuple)` for keys whose
    state changed. Emit `key_cols` + `{c}__a`/`{c}__b` per state column (the
    dTM's before/after), sorted `(*dims, binCol)`."""
    dim_names = [d.name for d in pyramid.dims]
    ki = {c: i for i, c in enumerate(key_cols)}
    rows = sorted(rows, key=lambda r: tuple(r[0][ki[d]] for d in dim_names) + (r[0][ki[pyramid.binCol]],))
    cols: dict[str, list] = {c: [] for c in key_cols}
    for c in state_cols:
        cols[f'{c}__a'] = []
        cols[f'{c}__b'] = []
    for key, sa, sb in rows:
        for i, c in enumerate(key_cols):
            cols[c].append(key[i])
        for i, c in enumerate(state_cols):
            cols[f'{c}__a'].append(sa[i])
            cols[f'{c}__b'].append(sb[i])
    return pa.table(cols)


def diff_tables(table_a: pa.Table, table_b: pa.Table, pyramid: Pyramid) -> pa.Table:
    """Changeset between two scan states (any two shards — consolidated-and-
    extracted, or raw per-scan): one row per key whose state differs, with the
    before (`__a`) and after (`__b`) state columns. A birth is `identity → v`,
    a death `v → identity`; a subtree/fleet aggregate delta is the column-sum of
    `__b − __a`. This is the universal, all-scans diff primitive."""
    key_cols, state_cols, _ = _key_state_cols(pyramid)
    id_tuple = tuple(_identities(pyramid)[c] for c in state_cols)
    ra = _scan_rows(table_a, key_cols, state_cols)
    rb = _scan_rows(table_b, key_cols, state_cols)
    rows = [
        (key, ra.get(key, id_tuple), rb.get(key, id_tuple))
        for key in sorted(set(ra) | set(rb))
        if ra.get(key, id_tuple) != rb.get(key, id_tuple)
    ]
    return _changeset_table(rows, key_cols, state_cols, pyramid)


def _key_intervals(ms: MultiScan, pyramid: Pyramid) -> dict[tuple, list[tuple]]:
    """`{key: [(scan_lo, scan_hi, state_tuple), ...]}` from an interval-encoded
    MultiScan — the compressed per-key change history."""
    key_cols, state_cols, _ = _key_state_cols(pyramid)
    t = ms.table
    cols = {c: t.column(c).to_pylist() for c in (*key_cols, *state_cols, SCAN_LO, SCAN_HI)}
    out: dict[tuple, list[tuple]] = {}
    for i in range(t.num_rows):
        key = tuple(cols[c][i] for c in key_cols)
        state = tuple(cols[c][i] for c in state_cols)
        out.setdefault(key, []).append((cols[SCAN_LO][i], cols[SCAN_HI][i], state))
    return out


def _as_of(intervals: list[tuple], idx: int, id_tuple: tuple) -> tuple:
    for lo, hi, state in intervals:
        if lo <= idx <= hi:
            return state
    return id_tuple  # absent at this scan


def diff_scans(ms: MultiScan, scan_a: str, scan_b: str, pyramid: Pyramid) -> pa.Table:
    """The sparse diff of two member scans (same changeset shape as
    :func:`diff_tables`). For the interval encoder this reads only the keys with
    a run boundary inside the span — O(changes-in-span), not two full snapshots.
    Equivalent to `diff_tables(extract_table(a), extract_table(b))`."""
    for s in (scan_a, scan_b):
        if s not in ms.scans:
            raise ValueError(f"diff_scans: {s!r} not a member scan ({ms.scans})")
    if ms.encoder != 'interval':
        return diff_tables(
            extract_table(ms, scan_a, pyramid), extract_table(ms, scan_b, pyramid), pyramid,
        )
    key_cols, state_cols, _ = _key_state_cols(pyramid)
    id_tuple = tuple(_identities(pyramid)[c] for c in state_cols)
    ia, ib = ms.scans.index(scan_a), ms.scans.index(scan_b)
    lo, hi = min(ia, ib), max(ia, ib)
    intervals = _key_intervals(ms, pyramid)

    # Candidate keys: a run boundary in the span `(lo, hi]` — a run starting
    # after `lo` or ending before `hi`. A superset of the changed keys; the
    # `sa != sb` test below makes the result exact.
    t = ms.table
    los, his = t.column(SCAN_LO).to_pylist(), t.column(SCAN_HI).to_pylist()
    kcols = {c: t.column(c).to_pylist() for c in key_cols}
    cand = {
        tuple(kcols[c][i] for c in key_cols)
        for i in range(t.num_rows)
        if (lo < los[i] <= hi) or (lo <= his[i] < hi)
    }
    rows = []
    for key in sorted(cand):
        sa, sb = _as_of(intervals[key], ia, id_tuple), _as_of(intervals[key], ib, id_tuple)
        if sa != sb:
            rows.append((key, sa, sb))
    return _changeset_table(rows, key_cols, state_cols, pyramid)


def series_for(ms: MultiScan, key, pyramid: Pyramid) -> list[tuple]:
    """A key's value stream across every member scan — `[(scan_label, state), …]`,
    absent scans carrying the monoid identity. The over-time plot's per-key line
    (a state-view point read of the observation axis)."""
    key = tuple(key)
    key_cols, state_cols, _ = _key_state_cols(pyramid)
    id_tuple = tuple(_identities(pyramid)[c] for c in state_cols)
    if ms.encoder == 'interval':
        ivals = _key_intervals(ms, pyramid).get(key, [])
        return [(label, _as_of(ivals, j, id_tuple)) for j, label in enumerate(ms.scans)]
    t = ms.table
    cols = {c: t.column(c).to_pylist() for c in (*key_cols, *state_cols, SCAN_COL)}
    by_scan = {
        cols[SCAN_COL][i]: tuple(cols[c][i] for c in state_cols)
        for i in range(t.num_rows)
        if tuple(cols[c][i] for c in key_cols) == key
    }
    return [(label, by_scan.get(j, id_tuple)) for j, label in enumerate(ms.scans)]

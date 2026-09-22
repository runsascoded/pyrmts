"""Dyadic changeset-hierarchy: sublinear diffs between *any* two scans.

The multi-scan store (`multiscan.py`) compresses storage and makes *within-archive*
and *adjacent* diffs cheap, but a diff between two arbitrary scans in different
archives still costs O(fleet) (a junction between two archives has no stored
delta, so detecting which keys changed there needs a full-state comparison). This
module builds the **serve-side** index that makes an arbitrary-pair diff cheap and
on-by-default — the piece storage consolidation does *not* give you.

Model: a **changeset** is `{key: (state_a, state_b)}` over the keys that differ
across a span (a birth is `identity → v`, a death `v → identity`). Composition is
associative but **not invertible** (remove-then-re-add across the span nets to
zero yet is real churn), so a range must be covered by *disjoint* dyadic blocks,
never overlapping ones. :class:`SparseDiffIndex` stores composed changesets over
power-of-2 spans (binary lifting) so `diff(a, b)` composes O(log(b−a)) disjoint
blocks — O(log N + changes-in-span), vs. O(fleet) for a 2-snapshot diff or
O(#groups) for composing across capped-K archives.

Cost moves to ingest: appending scan N computes one adjacency changeset (from the
two newest snapshots) and updates O(log N) hierarchy nodes, so every later diff
query is cheap instead of every query paying O(fleet).
"""
from __future__ import annotations

from collections.abc import Sequence

import pyarrow as pa

from .multiscan import _changeset_table, _identities, _key_state_cols, _scan_rows
from .types import Pyramid

#: A changeset: `{key_tuple: (state_a_tuple, state_b_tuple)}` over changed keys.
Changeset = dict


def changeset_between(table_a: pa.Table, table_b: pa.Table, pyramid: Pyramid) -> Changeset:
    """The changeset from scan state `a` to scan state `b` as a dict — every key
    whose state differs, with before/after (absent → monoid identity). The dict
    twin of :func:`pyrmts.diff_tables`."""
    key_cols, state_cols, _ = _key_state_cols(pyramid)
    id_tuple = tuple(_identities(pyramid)[c] for c in state_cols)
    ra = _scan_rows(table_a, key_cols, state_cols)
    rb = _scan_rows(table_b, key_cols, state_cols)
    out: Changeset = {}
    for key in set(ra) | set(rb):
        sa = ra.get(key, id_tuple)
        sb = rb.get(key, id_tuple)
        if sa != sb:
            out[key] = (sa, sb)
    return out


def compose_changesets(left: Changeset, right: Changeset) -> Changeset:
    """Compose two adjacent changesets — `left` over `(a, m]`, `right` over
    `(m, b]` → the net changeset over `(a, b]`. Associative; a key changed in both
    chains `left.before → right.after` (dropped if they coincide — churn that
    cancels), a key in only one passes through. Not invertible, so callers must
    compose *disjoint* spans only."""
    out: Changeset = dict(left)
    for key, (_mid, sb) in right.items():
        if key in out:
            sa = out[key][0]
            if sa == sb:
                del out[key]  # net no-op across the composed span
            else:
                out[key] = (sa, sb)
        else:
            out[key] = (_mid, sb)  # unchanged in `left`, so value at a == mid
    return out


def changeset_to_table(changeset: Changeset, pyramid: Pyramid) -> pa.Table:
    """Materialize a changeset dict as the standard changeset table (`key_cols` +
    `{c}__a`/`{c}__b`), sorted `(*dims, binCol)` — same shape as `diff_scans`."""
    key_cols, state_cols, _ = _key_state_cols(pyramid)
    rows = [(key, sa, sb) for key, (sa, sb) in changeset.items()]
    return _changeset_table(rows, key_cols, state_cols, pyramid)


def changeset_from_table(table: pa.Table, pyramid: Pyramid) -> Changeset:
    """Inverse of :func:`changeset_to_table` — parse a persisted changeset node
    (`key_cols` + `{c}__a`/`{c}__b`) back into a dict."""
    key_cols, state_cols, _ = _key_state_cols(pyramid)
    cols = {c: table.column(c).to_pylist() for c in key_cols}
    a_cols = {c: table.column(f'{c}__a').to_pylist() for c in state_cols}
    b_cols = {c: table.column(f'{c}__b').to_pylist() for c in state_cols}
    out: Changeset = {}
    for i in range(table.num_rows):
        key = tuple(cols[c][i] for c in key_cols)
        out[key] = (
            tuple(a_cols[c][i] for c in state_cols),
            tuple(b_cols[c][i] for c in state_cols),
        )
    return out


def jumps(i: int, j: int) -> list[tuple[int, int]]:
    """The disjoint dyadic blocks composing `(i, j]`, as `[(level, start), ...]`
    — block `(level, start)` is the net change from scan `start` to scan
    `start + 2**level`. Binary lifting on `j − i`: popcount(j−i) = O(log) blocks.
    The reader fetches exactly these nodes; nothing else."""
    out: list[tuple[int, int]] = []
    pos, remaining, level = i, j - i, 0
    while remaining:
        if remaining & 1:
            out.append((level, pos))
            pos += 1 << level
        remaining >>= 1
        level += 1
    return out


class SparseDiffIndex:
    """Binary-lifting hierarchy over per-scan adjacency changesets, for O(log)
    diffs between any two scans.

    `deltas[k]` is the changeset from scan `k` to scan `k+1` (length `N−1` for `N`
    scans). `diff(i, j)` composes the disjoint dyadic blocks of `(i, j]` —
    O(log(j−i)) compositions. Build is O(N log N) stored changesets (each tiny on
    low-churn data); appending a scan is O(log N) (see :meth:`append`)."""

    def __init__(self, deltas: Sequence[Changeset] = ()) -> None:
        # table[level][i] = composed changeset over [i, i + 2**level) — the net
        # change from scan i to scan i + 2**level. Built by repeated `append`, so
        # a from-scratch build and an incremental one are identical.
        self.table: list[list[Changeset]] = [[]]
        self.n = 1  # scans; one scan has zero deltas
        for d in deltas:
            self.append(d)

    def append(self, delta: Changeset) -> list[tuple[int, int, Changeset]]:
        """Append the adjacency changeset from the current last scan to a new
        scan. **Append-only**: this creates exactly one new node per level
        `L` with `2**L ≤ #deltas` (each the composition of two existing nodes)
        and never touches an existing node — so persisted nodes are immutable.
        O(log N) compositions. Returns the new nodes as `[(level, i, changeset)]`,
        which is precisely what a store must persist."""
        self.table[0].append(delta)
        self.n += 1
        m = len(self.table[0])
        new: list[tuple[int, int, Changeset]] = [(0, m - 1, delta)]
        level = 1
        while (1 << level) <= m:
            i = m - (1 << level)
            prev = self.table[level - 1]
            node = compose_changesets(prev[i], prev[i + (1 << (level - 1))])
            if level == len(self.table):
                self.table.append([])
            self.table[level].append(node)
            new.append((level, i, node))
            level += 1
        return new

    def diff(self, i: int, j: int) -> Changeset:
        """Net changeset from scan `i` to scan `j` (`i ≤ j`), composing the O(log)
        disjoint blocks from :func:`jumps`. `diff(i, i)` is empty."""
        if not 0 <= i <= j < self.n:
            raise ValueError(f"SparseDiffIndex.diff: ({i}, {j}) out of range [0, {self.n})")
        result: Changeset = {}
        for level, pos in jumps(i, j):
            result = compose_changesets(result, self.table[level][pos])
        return result

    def blocks_composed(self, i: int, j: int) -> int:
        """The number of dyadic blocks a `diff(i, j)` composes — popcount(j−i),
        i.e. O(log). For asserting the log bound in tests."""
        return bin(j - i).count('1')

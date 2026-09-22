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


class SparseDiffIndex:
    """Binary-lifting hierarchy over per-scan adjacency changesets, for O(log)
    diffs between any two scans.

    `deltas[k]` is the changeset from scan `k` to scan `k+1` (length `N−1` for `N`
    scans). `diff(i, j)` composes the disjoint dyadic blocks of `(i, j]` —
    O(log(j−i)) compositions. Build is O(N log N) stored changesets (each tiny on
    low-churn data); appending a scan is O(log N) (see :meth:`append`)."""

    def __init__(self, deltas: list[Changeset]) -> None:
        self.n = len(deltas) + 1
        # table[level][i] = composed changeset over [i, i + 2**level) — i.e. the
        # net change from scan i to scan i + 2**level.
        self.table: list[list[Changeset]] = [list(deltas)] if deltas else [[]]
        width = 1
        while width * 2 <= len(deltas):
            prev = self.table[-1]
            nxt = [
                compose_changesets(prev[i], prev[i + width])
                for i in range(len(deltas) - width * 2 + 1)
            ]
            self.table.append(nxt)
            width *= 2

    def diff(self, i: int, j: int) -> Changeset:
        """Net changeset from scan `i` to scan `j` (`i ≤ j`), composing O(log)
        disjoint power-of-2 jumps. `diff(i, i)` is empty."""
        if not 0 <= i <= j < self.n:
            raise ValueError(f"SparseDiffIndex.diff: ({i}, {j}) out of range [0, {self.n})")
        result: Changeset = {}
        pos, remaining, level = i, j - i, 0
        while remaining:
            if remaining & 1:
                result = compose_changesets(result, self.table[level][pos])
                pos += 1 << level
            remaining >>= 1
            level += 1
        return result

    def blocks_composed(self, i: int, j: int) -> int:
        """The number of dyadic blocks a `diff(i, j)` composes — popcount(j−i),
        i.e. O(log). For asserting the log bound in tests."""
        return bin(j - i).count('1')

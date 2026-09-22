"""Flat-changeset diff-index: the changeset between *any* two scans, composed
from disjoint aligned dyadic blocks.

What this answers: "every key whose state differs between scan i and scan j",
as a flat `{key: (before, after)}` dict — the audit / changelog primitive, the
gross-churn input (Σ|Δ| per subtree, which net rollups cannot give), and the
build input for a materialized pairwise diff on a non-adjacent pair. It is
**not** the diff-treemap engine: a treemap needs O(rendered) work, which is a
best-first tandem walk over two random-access snapshots (see the spec); a flat
changeset over a long span is O(changes-in-span), far more than rendered cells.

Model: a **changeset** is `{key: (state_a, state_b)}` over the keys that differ
across a span (a birth is `identity → v`, a death `v → identity`). Composition is
associative but **not invertible** (remove-then-re-add across the span nets to
zero yet is real churn), so a span is covered by *disjoint* blocks only.

Layout: **level 0** is the events log — one adjacency changeset per scan pair
(k → k+1), O(total changes) storage, strictly smaller than an interval archive.
Optional higher levels (`levels` ≥ 1) hold **aligned** power-of-2 blocks: node
`(L, start)` with `start` a multiple of `2**L` is the net change from scan
`start` to `start + 2**L` (segment-tree / Fenwick layout, not a sparse table —
sliding windows would cost O(N) storage per level with changeset-valued nodes).
Storage per level is bounded by the level-0 total, so the whole index is at
most `(levels + 1) ×` the events log, and netting makes higher levels smaller
still. Ingest is append-only: scan m creates one level-0 node and one node at
each level L ≤ levels that divides m, ~2 nodes per scan amortized, never
touching an existing node. `diff(i, j)` composes the ≤ 2·log2(j−i) aligned
blocks of :func:`aligned_blocks` (with `levels=0`, the j−i adjacency nodes).
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


def aligned_blocks(i: int, j: int, levels: int = 0) -> list[tuple[int, int]]:
    """The disjoint aligned dyadic blocks covering `(i, j]`, as
    `[(level, start), ...]` — block `(level, start)` is the net change from scan
    `start` to scan `start + 2**level`, with `start` a multiple of `2**level`
    and `level ≤ levels`. Greedy: at each position take the largest aligned
    block that fits before `j`. At most `2·log2(j−i) + 1` blocks when the cap
    allows; with `levels=0` it is the `j−i` adjacency blocks. The reader fetches
    exactly these nodes; nothing else."""
    if levels < 0:
        raise ValueError(f"aligned_blocks: levels must be ≥ 0, got {levels}")
    out: list[tuple[int, int]] = []
    pos = i
    while pos < j:
        level = 0
        while level < levels and pos % (1 << (level + 1)) == 0 and pos + (1 << (level + 1)) <= j:
            level += 1
        out.append((level, pos))
        pos += 1 << level
    return out


class SparseDiffIndex:
    """Aligned dyadic hierarchy over per-scan adjacency changesets.

    `table[level][start]` is the net changeset from scan `start` to scan
    `start + 2**level`; level 0 holds every adjacency changeset (the events
    log), level `L ≥ 1` only starts that are multiples of `2**L`. `levels` caps
    the hierarchy (0 = events log only). `diff(i, j)` composes the aligned
    blocks of `(i, j]`; building by repeated :meth:`append` is canonical."""

    def __init__(self, deltas: Sequence[Changeset] = (), *, levels: int = 0) -> None:
        if levels < 0:
            raise ValueError(f"SparseDiffIndex: levels must be ≥ 0, got {levels}")
        self.levels = levels
        self.table: list[dict[int, Changeset]] = [{} for _ in range(levels + 1)]
        self.n = 1  # scans; one scan has zero deltas
        for d in deltas:
            self.append(d)

    def append(self, delta: Changeset) -> list[tuple[int, int, Changeset]]:
        """Append the adjacency changeset from the current last scan to a new
        scan. **Append-only**: writes the level-0 node at `m−1` (`m` = deltas
        after the append) plus, for each level `L ≤ levels` with `2**L | m`, the
        aligned node at `m − 2**L` composed from two level-`L−1` nodes; never
        touches an existing node. ~2 nodes per scan amortized. Returns the new
        nodes as `[(level, start, changeset)]` — what a store must persist."""
        m = len(self.table[0]) + 1
        self.table[0][m - 1] = delta
        self.n += 1
        new: list[tuple[int, int, Changeset]] = [(0, m - 1, delta)]
        for level in range(1, self.levels + 1):
            width = 1 << level
            if m % width:
                break
            start = m - width
            prev = self.table[level - 1]
            node = compose_changesets(prev[start], prev[start + (width >> 1)])
            self.table[level][start] = node
            new.append((level, start, node))
        return new

    def diff(self, i: int, j: int) -> Changeset:
        """Net changeset from scan `i` to scan `j` (`i ≤ j`), composing the
        disjoint blocks from :func:`aligned_blocks`. `diff(i, i)` is empty."""
        if not 0 <= i <= j < self.n:
            raise ValueError(f"SparseDiffIndex.diff: ({i}, {j}) out of range [0, {self.n})")
        result: Changeset = {}
        for level, start in aligned_blocks(i, j, self.levels):
            result = compose_changesets(result, self.table[level][start])
        return result

    def blocks_composed(self, i: int, j: int) -> int:
        """The number of blocks a `diff(i, j)` composes — ≤ 2·log2(j−i) + 1 with
        enough levels, `j − i` with `levels=0`. For asserting bounds in tests."""
        return len(aligned_blocks(i, j, self.levels))

    def entries(self) -> list[int]:
        """Total stored changeset entries per level — the storage cost. Each
        level is bounded by level 0 (netting only shrinks composed nodes)."""
        return [sum(len(node) for node in level.values()) for level in self.table]

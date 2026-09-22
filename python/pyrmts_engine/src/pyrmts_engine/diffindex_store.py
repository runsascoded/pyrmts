"""Persistent flat-changeset diff-index over a pyrmts `Storage`
(`specs/multi-scan-consolidation.md`, Phase 3): the changeset between *any*
two scans, composed from disjoint aligned dyadic nodes.

Layout under `prefix`:
- `{prefix}/index.json` — `{"dataset": …, "scans": [ordered labels], "levels": L}`:
  the scan-label → position map and the hierarchy cap.
- `{prefix}/L0/{i}.parquet` — the events log: the adjacency changeset from scan
  `i` to scan `i+1`, one per scan pair, in the standard changeset table shape
  (`key_cols` + `{c}__a` / `{c}__b`). O(total changes) storage.
- `{prefix}/L{level}/{start}.parquet` (`1 ≤ level ≤ levels`, `start` a multiple
  of `2**level`) — aligned composed nodes: the net change from scan `start` to
  `start + 2**level`. Each level's storage is bounded by L0's.

Nodes are **immutable and append-only**: `append_scan` computes one adjacency
changeset (new snapshot vs. the previous one — the single O(fleet) step, paid
once per scan at ingest) and writes it at L0 plus one aligned node per level
that divides the new delta count (~2 node writes per scan amortized). `diff(a, b)`
fetches only the nodes named by :func:`pyrmts.aligned_blocks` and composes them —
no snapshot is ever read at query time. With `levels=0` (the default) that is
the `j−i` adjacency nodes over the span; higher levels trade `(levels+1)×`
storage for ≤ `2·log2(j−i)+1` node reads.
"""
from __future__ import annotations

import io
import json
from collections.abc import Sequence

import pyarrow as pa
import pyarrow.parquet as pq

from pyrmts import (
    Pyramid,
    Storage,
    aligned_blocks,
    changeset_between,
    changeset_from_table,
    changeset_to_table,
    compose_changesets,
)
from pyrmts.diffindex import Changeset


class DiffIndexStore:
    def __init__(
        self,
        storage: Storage,
        prefix: str,
        pyramid: Pyramid,
        dataset: str,
        levels: int | None = None,
    ) -> None:
        """`levels`: the hierarchy cap for a *new* index (0 = events log only,
        the default). On an existing index the manifest's cap governs; passing
        a different explicit value is an error (the node set would be
        inconsistent), `None` adopts it."""
        if levels is not None and levels < 0:
            raise ValueError(f"DiffIndexStore: levels must be ≥ 0, got {levels}")
        self.storage = storage
        self.prefix = prefix.rstrip('/')
        self.pyramid = pyramid
        self.dataset = dataset
        self._levels = levels

    # ── layout

    @property
    def manifest_key(self) -> str:
        return f'{self.prefix}/index.json'

    def node_key(self, level: int, start: int) -> str:
        return f'{self.prefix}/L{level}/{start}.parquet'

    def _meta(self) -> dict | None:
        data = self.storage.get(self.manifest_key)
        if data is None:
            return None
        meta = json.loads(data)
        if meta.get('dataset') != self.dataset:
            raise ValueError(
                f"DiffIndexStore: manifest at {self.manifest_key!r} is for dataset "
                f"{meta.get('dataset')!r}, not {self.dataset!r}"
            )
        if self._levels is not None and meta['levels'] != self._levels:
            raise ValueError(
                f"DiffIndexStore: index at {self.prefix!r} has levels={meta['levels']}, "
                f"not {self._levels}"
            )
        return meta

    def scans(self) -> list[str]:
        """Ordered member-scan labels ([] before the first scan)."""
        meta = self._meta()
        return [] if meta is None else list(meta['scans'])

    def levels(self) -> int:
        """The index's hierarchy cap (the manifest's, else the constructor's, else 0)."""
        meta = self._meta()
        if meta is not None:
            return int(meta['levels'])
        return 0 if self._levels is None else self._levels

    def _write_manifest(self, scans: list[str], levels: int) -> None:
        body = {'dataset': self.dataset, 'scans': scans, 'levels': levels}
        self.storage.put(self.manifest_key, json.dumps(body).encode())

    def _load_node(self, level: int, start: int) -> Changeset:
        data = self.storage.get(self.node_key(level, start))
        if data is None:
            raise ValueError(f"DiffIndexStore: missing node L{level}/{start} at {self.node_key(level, start)!r}")
        return changeset_from_table(pq.read_table(io.BytesIO(data)), self.pyramid)

    def _write_node(self, level: int, start: int, node: Changeset) -> None:
        buf = io.BytesIO()
        pq.write_table(changeset_to_table(node, self.pyramid), buf, compression='snappy')
        self.storage.put(self.node_key(level, start), buf.getvalue())

    # ── ingest (append-only)

    def init_scan(self, label: str) -> None:
        """Register the first scan (no delta yet)."""
        if self.scans():
            raise ValueError("DiffIndexStore.init_scan: index already has scans; use append_scan")
        self._write_manifest([label], self.levels())

    def append_scan(self, label: str, table: pa.Table, prev_table: pa.Table) -> list[tuple[int, int]]:
        """Append `label` (snapshot `table`) after the current last scan (snapshot
        `prev_table`). Writes the level-0 adjacency node plus one aligned node
        per level that divides the new delta count; existing nodes are never
        touched. Returns the new `(level, start)` nodes."""
        meta = self._meta()
        if meta is None:
            raise ValueError("DiffIndexStore.append_scan: call init_scan for the first scan")
        scans = list(meta['scans'])
        levels = int(meta['levels'])
        if label in scans:
            raise ValueError(f"DiffIndexStore.append_scan: {label!r} already in the index")
        m = len(scans)  # = number of deltas after this append
        delta = changeset_between(prev_table, table, self.pyramid)
        self._write_node(0, m - 1, delta)
        new: list[tuple[int, int]] = [(0, m - 1)]
        for level in range(1, levels + 1):
            width = 1 << level
            if m % width:
                break
            start = m - width
            node = compose_changesets(
                self._load_node(level - 1, start),
                self._load_node(level - 1, start + (width >> 1)),
            )
            self._write_node(level, start, node)
            new.append((level, start))
        self._write_manifest([*scans, label], levels)
        return new

    def update(self, scan_tables: Sequence[tuple[str, pa.Table]]) -> list[str]:
        """Idempotent ingest stage: given the full ordered scan list, append every
        scan not yet in the index (in order). A cron fires this each cycle; it
        no-ops when nothing is new. Returns the labels appended."""
        have = self.scans()
        labels = [label for label, _ in scan_tables]
        if have and labels[:len(have)] != have:
            raise ValueError(
                f"DiffIndexStore.update: scan order diverges from the index "
                f"(index {have[:3]}…, given {labels[:3]}…)"
            )
        appended: list[str] = []
        for k in range(len(have), len(labels)):
            label, table = scan_tables[k]
            if k == 0:
                self.init_scan(label)
            else:
                self.append_scan(label, table, scan_tables[k - 1][1])
            appended.append(label)
        return appended

    # ── serve

    def diff(self, a: str, b: str) -> Changeset:
        """The changeset from scan `a` to scan `b` — composes only the aligned
        blocks from :func:`aligned_blocks`. If `a` is after `b`, the forward
        changeset is computed and each key's before/after swapped (a single
        changeset reverses; only *composition* is non-invertible)."""
        meta = self._meta()
        scans = [] if meta is None else list(meta['scans'])
        try:
            i, j = scans.index(a), scans.index(b)
        except ValueError as e:
            raise ValueError(f"DiffIndexStore.diff: {e.args[0].split(' ')[0]!r} not in the index ({scans})") from None
        reverse = i > j
        if reverse:
            i, j = j, i
        result: Changeset = {}
        for level, start in aligned_blocks(i, j, int(meta['levels'])):
            result = compose_changesets(result, self._load_node(level, start))
        if reverse:
            result = {key: (sb, sa) for key, (sa, sb) in result.items()}
        return result

    def diff_table(self, a: str, b: str) -> pa.Table:
        return changeset_to_table(self.diff(a, b), self.pyramid)

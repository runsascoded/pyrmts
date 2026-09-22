"""Persistent dyadic diff-index over a pyrmts `Storage` — the always-on,
serve-side index that makes a diff between *any* two scans O(log N + changes)
(`specs/multi-scan-consolidation.md`, Phase 3).

Layout under `prefix`:
- `{prefix}/index.json` — `{"dataset": …, "scans": [ordered labels]}`, the
  scan-label → position map.
- `{prefix}/L{level}/{i}.parquet` — one changeset node per `(level, i)`: the net
  change from scan `i` to scan `i + 2**level`, in the standard changeset table
  shape (`key_cols` + `{c}__a` / `{c}__b`).

Nodes are **immutable and append-only**: `append_scan` computes one adjacency
changeset (new snapshot vs. the previous one — the single O(fleet) step, paid
once per scan at ingest) and writes exactly one new node per level, composing
two existing nodes each (O(log N) node reads + writes). `diff(a, b)` fetches only
the popcount(j−i) nodes named by :func:`pyrmts.jumps` and composes them — no
snapshot is ever read at query time.
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
    changeset_between,
    changeset_from_table,
    changeset_to_table,
    compose_changesets,
    jumps,
)
from pyrmts.diffindex import Changeset


class DiffIndexStore:
    def __init__(self, storage: Storage, prefix: str, pyramid: Pyramid, dataset: str) -> None:
        self.storage = storage
        self.prefix = prefix.rstrip('/')
        self.pyramid = pyramid
        self.dataset = dataset

    # ── layout

    @property
    def manifest_key(self) -> str:
        return f'{self.prefix}/index.json'

    def node_key(self, level: int, i: int) -> str:
        return f'{self.prefix}/L{level}/{i}.parquet'

    def scans(self) -> list[str]:
        """Ordered member-scan labels ([] before the first scan)."""
        data = self.storage.get(self.manifest_key)
        if data is None:
            return []
        meta = json.loads(data)
        if meta.get('dataset') != self.dataset:
            raise ValueError(
                f"DiffIndexStore: manifest at {self.manifest_key!r} is for dataset "
                f"{meta.get('dataset')!r}, not {self.dataset!r}"
            )
        return list(meta['scans'])

    def _write_manifest(self, scans: list[str]) -> None:
        self.storage.put(self.manifest_key, json.dumps({'dataset': self.dataset, 'scans': scans}).encode())

    def _load_node(self, level: int, i: int) -> Changeset:
        data = self.storage.get(self.node_key(level, i))
        if data is None:
            raise ValueError(f"DiffIndexStore: missing node L{level}/{i} at {self.node_key(level, i)!r}")
        return changeset_from_table(pq.read_table(io.BytesIO(data)), self.pyramid)

    def _write_node(self, level: int, i: int, node: Changeset) -> None:
        buf = io.BytesIO()
        pq.write_table(changeset_to_table(node, self.pyramid), buf, compression='snappy')
        self.storage.put(self.node_key(level, i), buf.getvalue())

    # ── ingest (append-only)

    def init_scan(self, label: str) -> None:
        """Register the first scan (no delta yet)."""
        if self.scans():
            raise ValueError("DiffIndexStore.init_scan: index already has scans; use append_scan")
        self._write_manifest([label])

    def append_scan(self, label: str, table: pa.Table, prev_table: pa.Table) -> list[tuple[int, int]]:
        """Append `label` (snapshot `table`) after the current last scan (snapshot
        `prev_table`). Writes the level-0 adjacency node plus one composed node per
        higher level; existing nodes are never touched. Returns the new
        `(level, i)` nodes."""
        scans = self.scans()
        if not scans:
            raise ValueError("DiffIndexStore.append_scan: call init_scan for the first scan")
        if label in scans:
            raise ValueError(f"DiffIndexStore.append_scan: {label!r} already in the index")
        m = len(scans)  # = number of deltas after this append
        delta = changeset_between(prev_table, table, self.pyramid)
        self._write_node(0, m - 1, delta)
        new: list[tuple[int, int]] = [(0, m - 1)]
        level = 1
        while (1 << level) <= m:
            i = m - (1 << level)
            half = 1 << (level - 1)
            node = compose_changesets(self._load_node(level - 1, i), self._load_node(level - 1, i + half))
            self._write_node(level, i, node)
            new.append((level, i))
            level += 1
        self._write_manifest([*scans, label])
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
        """The changeset from scan `a` to scan `b` — composes only the
        popcount(|j−i|) dyadic nodes from :func:`jumps`. If `a` is after `b`, the
        forward changeset is computed and each key's before/after swapped (a
        single changeset reverses; only *composition* is non-invertible)."""
        scans = self.scans()
        try:
            i, j = scans.index(a), scans.index(b)
        except ValueError as e:
            raise ValueError(f"DiffIndexStore.diff: {e.args[0].split(' ')[0]!r} not in the index ({scans})") from None
        reverse = i > j
        if reverse:
            i, j = j, i
        result: Changeset = {}
        for level, pos in jumps(i, j):
            result = compose_changesets(result, self._load_node(level, pos))
        if reverse:
            result = {key: (sb, sa) for key, (sa, sb) in result.items()}
        return result

    def diff_table(self, a: str, b: str) -> pa.Table:
        return changeset_to_table(self.diff(a, b), self.pyramid)

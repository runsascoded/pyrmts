"""Spilled WIP shard buffers.

Holding every open shard's long-form rows in memory is the engine's
dominant footprint (ctbk measured ~40 GB on a 4-day avail smoke — see
`ctbk/specs/pyrmts-engine-validation.md` findings). Instead, each window's
contribution to each open output shard is written as its own **run file**
under the shard's spill namespace; at shard close the runs are
streaming-scanned, group_by-combined (partial bins from different windows
merge here), and deleted. Peak memory ≈ one shard's combined long form.

One file per append keeps workers lock-free on the write path (the
parallel executor appends to the same shard from many window tasks;
`specs/parallel-window-executor.md`), and is the seam for phase-2
sorted-run merge closes. Append order across workers can't reach output
bytes: close combines and the writer sorts.

Rows are routed per shard at append time, so rows that belong to no
expected shard (a tier's residual tail) are never written at all."""
from __future__ import annotations

import threading
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from pyrmts import Pyramid

from .longform import COUNT_COL, empty_long, group_cols


class SpillBuffer:
    """One scratch run file per (open output shard, append)."""

    def __init__(self, root: str | Path, pyramid: Pyramid) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.pyramid = pyramid
        self._runs: dict[str, list[Path]] = {}
        self._counts: dict[str, int] = {}
        self._schema: pa.Schema | None = None
        self._lock = threading.Lock()

    def append(self, shard_key: str, frame: pl.DataFrame) -> None:
        if frame.height == 0:
            return
        table = frame.to_arrow()
        with self._lock:
            if self._schema is None:
                self._schema = table.schema
            schema = self._schema
            seq = self._counts.get(shard_key, 0)
            self._counts[shard_key] = seq + 1
        path = self.root / f"{shard_key.replace('/', '_')}.{seq:04d}.run.parquet"
        pq.write_table(table.cast(schema), path, compression='snappy')
        with self._lock:
            self._runs.setdefault(shard_key, []).append(path)

    def close_shard(self, shard_key: str) -> pl.DataFrame:
        """Finalize a shard's spill: combine its runs (monoid
        group_by-sum, streaming) and delete them."""
        with self._lock:
            paths = sorted(self._runs.pop(shard_key, []))
            self._counts.pop(shard_key, None)
        if not paths:
            return empty_long(self.pyramid)
        combined = (
            pl.scan_parquet(paths)
            .group_by(group_cols(self.pyramid))
            .agg(pl.col(COUNT_COL).sum())
            .collect(engine='streaming')
        )
        for path in paths:
            path.unlink()
        return combined

    def close(self) -> None:
        """Abort: delete any remaining run files."""
        with self._lock:
            for paths in self._runs.values():
                for path in paths:
                    path.unlink(missing_ok=True)
            self._runs.clear()
            self._counts.clear()

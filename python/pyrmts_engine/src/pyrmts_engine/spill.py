"""Spilled WIP shard buffers.

Holding every open shard's long-form rows in memory is the engine's
dominant footprint (ctbk measured ~40 GB on a 4-day avail smoke — see
`ctbk/specs/pyrmts-engine-validation.md` findings). Instead, each window's
contribution to each open output shard is appended as a parquet row-group
to that shard's own scratch file; at shard close the file is
streaming-scanned, group_by-combined (partial bins from different windows
merge here), and deleted. Peak memory ≈ one shard's combined long form.

Rows are routed per shard at append time, so rows that belong to no
expected shard (a tier's residual tail) are never written at all."""
from __future__ import annotations

from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from pyrmts import Pyramid

from .longform import COUNT_COL, empty_long, group_cols


class SpillBuffer:
    """One scratch parquet file per open output shard."""

    def __init__(self, root: str | Path, pyramid: Pyramid) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.pyramid = pyramid
        self._writers: dict[str, tuple[pq.ParquetWriter, Path]] = {}
        self._schema: pa.Schema | None = None

    def _path(self, shard_key: str) -> Path:
        return self.root / (shard_key.replace('/', '_') + '.spill.parquet')

    def append(self, shard_key: str, frame: pl.DataFrame) -> None:
        if frame.height == 0:
            return
        table = frame.to_arrow()
        entry = self._writers.get(shard_key)
        if entry is None:
            if self._schema is None:
                self._schema = table.schema
            path = self._path(shard_key)
            entry = (pq.ParquetWriter(path, self._schema, compression='snappy'), path)
            self._writers[shard_key] = entry
        writer, _ = entry
        writer.write_table(table.cast(self._schema))

    def close_shard(self, shard_key: str) -> pl.DataFrame:
        """Finalize a shard's spill: combine its row-groups (monoid
        group_by-sum, streaming) and delete the scratch file."""
        entry = self._writers.pop(shard_key, None)
        if entry is None:
            return empty_long(self.pyramid)
        writer, path = entry
        writer.close()
        combined = (
            pl.scan_parquet(path)
            .group_by(group_cols(self.pyramid))
            .agg(pl.col(COUNT_COL).sum())
            .collect(engine='streaming')
        )
        path.unlink()
        return combined

    def close(self) -> None:
        """Abort: close + delete any remaining spill files."""
        for writer, path in self._writers.values():
            writer.close()
            path.unlink(missing_ok=True)
        self._writers.clear()

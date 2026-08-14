"""Streaming-tip producer helper (`specs/streaming-tip-writer.md`):
package the read-merge-write-invalidate cycle for growing a base-tier
tip shard in place — the streaming-push producer model (awair's Lambda,
a CFW cron, a long-running daemon), as opposed to the engine's bulk-pull
`TiledSource` path.

    with TipWriter(pyramid, tier='raw', at=now) as tip:
        tip.append(new_rows)  # pa.Table
    # on clean exit: read current tip shard → merge (dedupe on
    # (bin_col, *dims), keep-last) → sorted atomic write via
    # `pyramid.storage` → append an `Invalidation` covering the touched
    # bins so cascade re-derives downstream tiers on its next tick.

The writer always targets the tier's FINEST rung (`tier.shards[0]`) —
the tip; consolidation into coarser rungs is cascade's job
(`specs/calendar-rung-consolidation.md`). No concurrency control across
writers: `Storage.put` is atomic and concurrent writers last-write-win
on content (they can't corrupt each other's bytes); single-writer
semantics are the producer's deployment concern (Lambda
reserved-concurrency=1, CFW cron singleton, ...).
"""
from __future__ import annotations

import io
from datetime import datetime, timezone
from typing import Literal

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .axis import add_span, floor_to_span, format_period, parse_duration
from .invalidation import invalidate
from .keys import substitute_key
from .types import Pyramid
from .writer import write_tier_parquet

OnConflict = Literal['keep-last', 'keep-first', 'error']


def _to_dt(v: object) -> datetime:
    """Bin-col scalar → aware UTC datetime (epoch-ms int or timestamp)."""
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    if isinstance(v, int):
        return datetime.fromtimestamp(v / 1000, tz=timezone.utc)
    raise TypeError(f"TipWriter: unsupported bin-col value type {type(v).__name__}")


class TipWriter:
    """Context manager for one tip-shard update. `append` accumulates;
    the merge-write-invalidate happens once, on clean `__exit__` (or an
    explicit `flush()`). Appending nothing is a no-op — no write, no
    journal entry.

    Args:
        pyramid: Target pyramid (its `storage` receives the write).
        tier: Tier name; the write targets its finest rung.
        at: Instant selecting the tip shard — the one whose period
            contains `at`. Appended rows must lie inside that period
            (rows for another period would silently mis-file; raises).
        dims: Extra `keyTemplate` placeholder values (e.g.
            `{'device_id': 17617}`); merged into the `{tier}/{shard}/
            {period}` substitutions.
        on_conflict: Merge semantics when an appended row's
            `(bin_col, *dim_cols)` key collides with an existing (or
            earlier-appended) row: `keep-last` (default — streaming
            producers can correct earlier writes), `keep-first`, or
            `error` (raise on any collision).
        now: Injectable `requested_at` for the journal entry (tests).

    After `flush()`: `key`, `rows_written`, `bytes_written` describe the
    written shard (all `None` if nothing was appended).
    """

    def __init__(
        self,
        pyramid: Pyramid,
        *,
        tier: str,
        at: datetime,
        dims: dict[str, str | int] | None = None,
        on_conflict: OnConflict = 'keep-last',
        now: datetime | None = None,
    ) -> None:
        if on_conflict not in ('keep-last', 'keep-first', 'error'):
            raise ValueError(f"TipWriter: unknown on_conflict {on_conflict!r}")
        self.pyramid = pyramid
        self.tier = pyramid.tier(tier)
        self.at = at
        self.dims = dims or {}
        self.on_conflict: OnConflict = on_conflict
        self.now = now
        self._appends: list[pa.Table] = []
        self._flushed = False
        self.key: str | None = None
        self.rows_written: int | None = None
        self.bytes_written: int | None = None

    def append(self, rows: pa.Table) -> None:
        if not isinstance(rows, pa.Table):
            raise TypeError(
                f"TipWriter.append: expected pyarrow.Table, got {type(rows).__name__}"
            )
        if rows.num_rows:
            self._appends.append(rows)

    def __enter__(self) -> TipWriter:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            self.flush()

    def _dedupe(self, table: pa.Table) -> pa.Table:
        """Row-order-based dedupe on `(bin_col, *dim_cols)` — earlier
        rows are the existing shard + earlier appends, so 'last' is the
        most recent write."""
        key_cols = [self.pyramid.binCol] + [d.name for d in self.pyramid.dims]
        keys = zip(*(table.column(c).to_pylist() for c in key_cols))
        kept: dict[tuple, int] = {}
        for i, k in enumerate(keys):
            if k in kept:
                if self.on_conflict == 'error':
                    raise ValueError(
                        f"TipWriter: duplicate row key {dict(zip(key_cols, k))} "
                        f"(on_conflict='error')"
                    )
                if self.on_conflict == 'keep-first':
                    continue
            kept[k] = i
        if len(kept) == table.num_rows:
            return table
        return table.take(pa.array(sorted(kept.values())))

    def flush(self) -> None:
        if self._flushed:
            raise RuntimeError('TipWriter: already flushed')
        self._flushed = True
        if not self._appends:
            return
        pyramid = self.pyramid
        rung = self.tier.shards[0]
        span = parse_duration(str(rung))
        period_start = floor_to_span(self.at, span)
        period_end = add_span(period_start, span)
        key = substitute_key(pyramid.keyTemplate, {
            'tier': self.tier.name,
            'shard': str(rung),
            'period': format_period(period_start, span),
            **self.dims,
        })

        new = pa.concat_tables(self._appends)
        mm = pc.min_max(new.column(pyramid.binCol)).as_py()
        lo, hi = _to_dt(mm['min']), _to_dt(mm['max'])
        if lo < period_start or hi >= period_end:
            raise ValueError(
                f"TipWriter: appended rows span [{lo.isoformat()}, {hi.isoformat()}] "
                f"outside the tip shard period [{period_start.isoformat()}, "
                f"{period_end.isoformat()}) selected by at={self.at.isoformat()}"
            )

        blob = pyramid.storage.get(key)
        if blob is not None:
            existing = pq.read_table(io.BytesIO(blob))
            # Align writer-era schema drift (string vs large_string, column
            # order) so concat doesn't throw; column-set mismatches raise.
            new = new.select(existing.column_names).cast(existing.schema)
            combined = pa.concat_tables([existing, new])
        else:
            combined = new
        combined = self._dedupe(combined)

        buf = io.BytesIO()
        write_tier_parquet(combined, pyramid, out=buf)
        out = buf.getvalue()
        pyramid.storage.put(key, out)

        bin_span = parse_duration(self.tier.bin)
        invalidate(pyramid, (lo, add_span(hi, bin_span)), now=self.now)

        self.key = key
        self.rows_written = combined.num_rows
        self.bytes_written = len(out)

"""`materialize_resolutions`: pure h3 expansion. For each input row, emit one
row per materialized h3 resolution, with `geo.cellCol` set.

Pure h3 + row construction. No I/O, no aggregation. Aggregation happens
downstream via `pyrmts.cascade_tiers` or directly in the caller.

See `~/c/pyrmts/specs/cascade-tiers-and-geo-materializer.md` §2."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, Iterator

import h3

from pyrmts import GeoSpec


Row = dict[str, object]


@dataclass
class MaterializeStats:
    rows_in: int = 0
    rows_out: int = 0
    dropped_no_latlng: int = 0


def materialize_resolutions(
    rows: Iterable[Row],
    geo: GeoSpec,
    lat_lng: Callable[[Row], tuple[float, float] | None],
    stats: MaterializeStats | None = None,
) -> Iterator[Row]:
    """For each input row, emit one row per materialized h3 resolution with
    `geo.cellCol` set. Rows whose `lat_lng(row)` returns `None` are dropped.

    Yields rows in input order × resolution-finest-first (matching the
    order in `geo.resolutions`).

    Args:
        rows: input rows (any iterable of dicts).
        geo: GeoSpec — uses `cellCol` and `resolutions`.
        lat_lng: callable mapping a row to `(lat, lng)` or `None` if the row
            should be dropped (no resolvable location).
        stats: if provided, populated with rows_in / rows_out / dropped counts.
    """
    cell_col = geo.cellCol
    resolutions = geo.resolutions
    for row in rows:
        if stats is not None:
            stats.rows_in += 1
        ll = lat_lng(row)
        if ll is None:
            if stats is not None:
                stats.dropped_no_latlng += 1
            continue
        lat, lng = ll
        for res in resolutions:
            out = dict(row)
            out[cell_col] = h3.latlng_to_cell(lat, lng, res)
            if stats is not None:
                stats.rows_out += 1
            yield out

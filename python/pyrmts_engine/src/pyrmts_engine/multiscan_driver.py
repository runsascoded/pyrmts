"""Storage driver for multi-scan consolidation — Phase 2 of
`specs/multi-scan-consolidation.md`.

Reads a range of per-scan shards from storage, stream-consolidates them into
multi-scan (interval / SCD-2) shards, and extracts any member scan back —
digest-verified, so the originals are safe to drop.

Storage model, dependency-free of any consumer's layout: a *scan* is an opaque
`(label, Storage)` whose `keyTemplate`-resolved tiles hold that scan's shards.
Different scans are different `Storage`s (a prefix or bucket per scan); a tile
is read from each scan at the *same* key. The driver reads one scan's tile at a
time (via :func:`pyrmts.consolidate_scans`' streaming fold), so peak memory is
O(#keys in a tile), not O(#keys × #scans).
"""
from __future__ import annotations

import io
from collections.abc import Iterator, Sequence

import pyarrow as pa
import pyarrow.parquet as pq

from pyrmts import (
    FsStorage,
    MultiScan,
    Pyramid,
    Storage,
    consolidate_scans,
    extract_table,
    from_arrow,
    scan_digest,
    shard_periods_covering,
    substitute_key,
    to_arrow,
)

ENGINES = ('python', 'duckdb')


def _tile_key(pyramid: Pyramid, tier: str, shard: str, period: str) -> str:
    return substitute_key(pyramid.keyTemplate, {'tier': tier, 'shard': shard, 'period': period})


def _scan_tiles(
    scans: Sequence[tuple[str, Storage]],
    key: str,
) -> Iterator[tuple[str, pa.Table]]:
    """Lazily yield each scan's shard for `key`, one at a time (never holding
    more than one scan's table). A scan missing the tile is an error — a
    consolidation range is homogeneous (every member covers the same tiles); an
    absent *key within* a present tile is what represents "not observed"."""
    for label, storage in scans:
        data = storage.get(key)
        if data is None:
            raise ValueError(f"multiscan consolidate: scan {label!r} is missing tile {key!r}")
        yield label, pq.read_table(io.BytesIO(data))


def consolidate_tile(
    scans: Sequence[tuple[str, Storage]],
    pyramid: Pyramid,
    tier: str,
    shard: str,
    period: str,
) -> MultiScan:
    """Stream-consolidate one `(tier, shard, period)` tile across `scans` (in
    order). Interval encoder; carries per-scan digests."""
    key = _tile_key(pyramid, tier, shard, period)
    return consolidate_scans(_scan_tiles(scans, key), pyramid)


def _consolidate_tile_duckdb(
    scans: Sequence[tuple[str, Storage]],
    pyramid: Pyramid,
    key: str,
) -> MultiScan:
    """Out-of-core tile consolidation via the DuckDB backend, reading each scan's
    shard directly with `read_parquet`. Requires `FsStorage` scans (local paths);
    S3 via DuckDB httpfs is a follow-up."""
    from .multiscan_duckdb import consolidate_parquet_duckdb

    files: list[tuple[str, str]] = []
    for label, storage in scans:
        if not isinstance(storage, FsStorage):
            raise ValueError(
                "multiscan --engine duckdb currently supports FsStorage scans only "
                "(S3 via DuckDB httpfs is a follow-up)"
            )
        path = storage._path(key)
        if not path.exists():
            raise ValueError(f"multiscan consolidate: scan {label!r} is missing tile {key!r}")
        files.append((label, str(path)))
    return consolidate_parquet_duckdb(files, pyramid)


def consolidate_range(
    scans: Sequence[tuple[str, Storage]],
    pyramid: Pyramid,
    tier: str,
    shard: str,
    range_: tuple,
    out_storage: Storage,
    *,
    engine: str = 'python',
    compression: str = 'snappy',
) -> list[tuple[str, int, int]]:
    """Consolidate every `shard`-period tile of `tier` overlapping `range_` and
    write the self-describing multi-scan shard (with `pyrmts.multiscan`
    metadata) to `out_storage` at the same tile key. Returns
    `[(key, num_rows, num_scans), ...]`.

    `engine='python'` is the in-memory reference fold (`consolidate_scans`);
    `engine='duckdb'` is the out-of-core backend (`multiscan_duckdb`), for fleet
    scale — byte-identical output, far less memory."""
    if engine not in ENGINES:
        raise ValueError(f"consolidate_range: unknown engine {engine!r}; want one of {ENGINES}")
    frm, to = range_
    written: list[tuple[str, int, int]] = []
    for period in shard_periods_covering(frm, to, shard):
        key = _tile_key(pyramid, tier, shard, period.label)
        if engine == 'duckdb':
            ms = _consolidate_tile_duckdb(scans, pyramid, key)
        else:
            ms = consolidate_scans(_scan_tiles(scans, key), pyramid)
        buf = io.BytesIO()
        pq.write_table(to_arrow(ms), buf, compression=compression)
        out_storage.put(key, buf.getvalue())
        written.append((key, ms.table.num_rows, len(ms.scans)))
    return written


def extract_scan(
    ms_storage: Storage,
    key: str,
    scan: str,
    pyramid: Pyramid,
    *,
    verify: bool = True,
) -> pa.Table:
    """Reconstruct member `scan`'s original shard from the multi-scan shard at
    `key`. With `verify` (default), recompute the extracted scan's content
    digest and assert it matches the digest stored at consolidation time —
    proof the round-trip is lossless before the original is deleted."""
    data = ms_storage.get(key)
    if data is None:
        raise ValueError(f"multiscan extract: no multi-scan shard at {key!r}")
    ms = from_arrow(pq.read_table(io.BytesIO(data)))
    table = extract_table(ms, scan, pyramid)
    if verify:
        want = (ms.digests or {}).get(scan)
        if want is None:
            raise ValueError(f"multiscan extract: no stored digest for {scan!r} to verify against")
        got = scan_digest(table, pyramid)
        if got != want:
            raise ValueError(
                f"multiscan extract: digest mismatch for {scan!r}: extracted {got}, expected {want}"
            )
    return table

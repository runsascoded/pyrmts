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
import re
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
    ms_index=None,
    dataset: str | None = None,
    drop: bool = False,
) -> list[tuple[str, int, int]]:
    """Consolidate every `shard`-period tile of `tier` overlapping `range_` and
    write the self-describing multi-scan shard (with `pyrmts.multiscan`
    metadata) to `out_storage` at the same tile key. Returns
    `[(key, num_rows, num_scans), ...]`.

    `engine='python'` is the in-memory reference fold (`consolidate_scans`);
    `engine='duckdb'` is the out-of-core backend (`multiscan_duckdb`), for fleet
    scale — byte-identical output, far less memory.

    When `ms_index` (a `MultiScanIndex`) and `dataset` are given, a
    `MultiScanRecord` is recorded for each tile *after* its shard is written and
    digests are known — so reads route to the archive (see the safety contract in
    the spec; drop individuals only after this, via `drop_consolidated_scans`)."""
    if engine not in ENGINES:
        raise ValueError(f"consolidate_range: unknown engine {engine!r}; want one of {ENGINES}")
    if (ms_index is None) != (dataset is None):
        raise ValueError("consolidate_range: pass both ms_index and dataset, or neither")
    frm, to = range_
    written: list[tuple[str, int, int]] = []
    for period in shard_periods_covering(frm, to, shard):
        key = _tile_key(pyramid, tier, shard, period.label)
        written.append(_consolidate_one(
            scans, pyramid, tier, shard, period, key, out_storage,
            engine=engine, compression=compression, ms_index=ms_index, dataset=dataset, drop=drop,
        ))
    return written


def consolidate_groups(
    scans: Sequence[tuple[str, Storage]],
    pyramid: Pyramid,
    tier: str,
    shard: str,
    range_: tuple,
    out_storage: Storage,
    *,
    group_size: int,
    engine: str = 'python',
    compression: str = 'snappy',
    ms_index=None,
    dataset: str | None = None,
    drop: bool = False,
) -> list[tuple[str, int, int]]:
    """Capped-K consolidation: partition `scans` into consecutive groups of
    `group_size` and consolidate each group *separately* into its own sealed,
    immutable archive — the model that avoids the O(K²) rewrite of appending into
    a running MS. Each group's shard is keyed by the tile period plus the group's
    first-scan label (so groups sharing a `(tier, period)` don't collide), and
    gets its own manifest row. Returns `[(key, num_rows, num_scans), ...]`, one
    per (period, group)."""
    if group_size < 1:
        raise ValueError(f"consolidate_groups: group_size must be ≥ 1, got {group_size}")
    if engine not in ENGINES:
        raise ValueError(f"consolidate_groups: unknown engine {engine!r}; want one of {ENGINES}")
    if (ms_index is None) != (dataset is None):
        raise ValueError("consolidate_groups: pass both ms_index and dataset, or neither")
    groups = [scans[i:i + group_size] for i in range(0, len(scans), group_size)]
    frm, to = range_
    written: list[tuple[str, int, int]] = []
    for period in shard_periods_covering(frm, to, shard):
        in_key = _tile_key(pyramid, tier, shard, period.label)
        for group in groups:
            glabel = re.sub(r'[^A-Za-z0-9_.-]', '-', group[0][0])
            out_key = _tile_key(pyramid, tier, shard, f"{period.label}--{glabel}")
            written.append(_consolidate_one(
                group, pyramid, tier, shard, period, out_key, out_storage,
                engine=engine, compression=compression, ms_index=ms_index, dataset=dataset,
                drop=drop, in_key=in_key,
            ))
    return written


def _consolidate_one(
    scans, pyramid, tier, shard, period, out_key, out_storage,
    *, engine, compression, ms_index, dataset, drop, in_key=None,
) -> tuple[str, int, int]:
    """Consolidate one (period, scan-group) → write shard at `out_key`, record
    the manifest row, and (if `drop`) verify-then-delete the individuals. `in_key`
    is where the members' individual shards live (defaults to `out_key`)."""
    from .multiscan_index import MultiScanRecord, now_ms

    read_key = in_key if in_key is not None else out_key
    if engine == 'duckdb':
        ms = _consolidate_tile_duckdb(scans, pyramid, read_key)
    else:
        ms = consolidate_scans(_scan_tiles(scans, read_key), pyramid)
    buf = io.BytesIO()
    pq.write_table(to_arrow(ms), buf, compression=compression)
    out_storage.put(out_key, buf.getvalue())
    if ms_index is not None:
        ms_index.record_multiscan(MultiScanRecord(
            dataset=dataset,
            tier=tier,
            shard_dur=shard,
            period_start_ms=int(period.start.timestamp() * 1000),
            period_end_ms=int(period.end.timestamp() * 1000),
            key=out_key,
            scans=list(ms.scans),
            encoder=ms.encoder,
            written_at_ms=now_ms(),
            digests=ms.digests,
        ))
    if drop:
        drop_consolidated_scans(scans, out_storage, out_key, pyramid, read_key=read_key)
    return (out_key, ms.table.num_rows, len(ms.scans))


def drop_consolidated_scans(
    scans: Sequence[tuple[str, Storage]],
    ms_storage: Storage,
    key: str,
    pyramid: Pyramid,
    *,
    verify: bool = True,
    read_key: str | None = None,
) -> list[str]:
    """Delete each member scan's individual shard, once the multi-scan archive at
    `key` is proven to recover it. The safe tail of the consolidation flow: with
    `verify` (default), extract each scan from the archive and assert its content
    digest matches the one stored at consolidation — only then delete the
    original (at `read_key`, defaulting to `key` when the archive shares the tile
    key; distinct for capped-K groups). Returns the dropped scan labels. Never
    call before the archive is written and (if used) its manifest row recorded."""
    data = ms_storage.get(key)
    if data is None:
        raise ValueError(f"drop_consolidated_scans: no multi-scan shard at {key!r}")
    ms = from_arrow(pq.read_table(io.BytesIO(data)))
    individual_key = read_key if read_key is not None else key
    dropped: list[str] = []
    for label, storage in scans:
        if verify:
            want = (ms.digests or {}).get(label)
            if want is None:
                raise ValueError(f"drop_consolidated_scans: no stored digest for {label!r}; refusing to drop")
            got = scan_digest(extract_table(ms, label, pyramid), pyramid)
            if got != want:
                raise ValueError(
                    f"drop_consolidated_scans: digest mismatch for {label!r} "
                    f"(extracted {got}, expected {want}); refusing to drop"
                )
        storage.delete(individual_key)
        dropped.append(label)
    return dropped


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

"""Scan-location manifest for multi-scan consolidation — Phase 2c of
`specs/multi-scan-consolidation.md`.

Decoding a multi-scan shard is self-describing (its `pyrmts.multiscan`
KV-metadata), but *routing* — "scan `S`'s tile lives where: an individual shard,
or folded into which MS archive, at which fold index?" — is a separate problem
the reader must answer from a manifest, not by footer-reading every parquet.

The existing `ShardIndex` (`pyramid_shards`) has no scan axis, so this is an
additive overlay: one `MultiScanRecord` per consolidated `(dataset, tier, shard,
period)` tile, carrying the ordered member `scans` (a routing key: fold-index =
`scans.index(S)`) + the encoder. D1-ready row shape (`pyramid_multiscans`); the
first impl is JSONL-through-`Storage`, mirroring `StorageJsonlShardIndex`.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Protocol


@dataclass(frozen=True)
class MultiScanRecord:
    """One consolidated tile. `scans` is the ordered member-scan list — the
    routing key (a scan `S` is served from `key` at fold-index `scans.index(S)`).
    `digests` (optional) is each member's content hash, for verify-before-drop."""
    dataset: str
    tier: str
    shard_dur: str
    period_start_ms: int
    period_end_ms: int
    key: str
    scans: list[str]
    encoder: str
    written_at_ms: int
    digests: dict[str, str] | None = None

    def covers(self, scan: str) -> bool:
        return scan in self.scans

    def fold_index(self, scan: str) -> int:
        return self.scans.index(scan)


def _row(record: MultiScanRecord) -> dict:
    row = {
        'dataset': record.dataset,
        'tier': record.tier,
        'shard_dur': record.shard_dur,
        'period_start': record.period_start_ms,
        'period_end': record.period_end_ms,
        'key': record.key,
        'scans': record.scans,
        'encoder': record.encoder,
        'written_at': record.written_at_ms,
    }
    if record.digests is not None:
        row['digests'] = record.digests
    return row


def _from_row(row: dict) -> MultiScanRecord:
    return MultiScanRecord(
        dataset=row['dataset'],
        tier=row['tier'],
        shard_dur=row['shard_dur'],
        period_start_ms=row['period_start'],
        period_end_ms=row['period_end'],
        key=row['key'],
        scans=list(row['scans']),
        encoder=row['encoder'],
        written_at_ms=row['written_at'],
        digests=row.get('digests'),
    )


class MultiScanIndex(Protocol):
    def record_multiscan(self, record: MultiScanRecord) -> None: ...
    def list_multiscans(self, dataset: str) -> list[MultiScanRecord]: ...


@dataclass
class MemMultiScanIndex:
    records: list[MultiScanRecord] = field(default_factory=list)

    def record_multiscan(self, record: MultiScanRecord) -> None:
        self.records.append(record)

    def list_multiscans(self, dataset: str) -> list[MultiScanRecord]:
        return [r for r in self.records if r.dataset == dataset]


class StorageJsonlMultiScanIndex:
    """JSONL manifest written through a pyrmts `Storage` (S3/R2/fs/mem), one
    JSON object per line — mirrors `StorageJsonlShardIndex`. Object stores can't
    append, so each `record_multiscan` re-PUTs the whole manifest (a manifest PUT
    is noise next to the shard write it records). An existing manifest at `key`
    is loaded on init, so records survive resumed runs."""

    def __init__(self, storage, key: str) -> None:
        self.storage = storage
        self.key = key
        existing = storage.get(key)
        self._lines: list[str] = (
            existing.decode().rstrip('\n').split('\n') if existing else []
        )

    def record_multiscan(self, record: MultiScanRecord) -> None:
        self._lines.append(json.dumps(_row(record)))
        self.storage.put(self.key, ('\n'.join(self._lines) + '\n').encode())

    def list_multiscans(self, dataset: str) -> list[MultiScanRecord]:
        out = []
        for line in self._lines:
            if not line:
                continue
            rec = _from_row(json.loads(line))
            if rec.dataset == dataset:
                out.append(rec)
        return out


def resolve_scan(records: list[MultiScanRecord], scan: str) -> MultiScanRecord | None:
    """The routing decision: the multi-scan record covering `scan`, or None (the
    caller then falls back to the single-scan `ShardIndex`). Assumes at most one
    covering record per tile (the driver never double-consolidates a scan)."""
    for rec in records:
        if rec.covers(scan):
            return rec
    return None


def now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)

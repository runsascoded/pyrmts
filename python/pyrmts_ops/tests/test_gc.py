"""Registry-driven GC (`specs/pyrmts-ops-adoption.md` phase 3): every
decision branch of `gc_sweep` against an in-memory registry."""
from __future__ import annotations

from datetime import timedelta

from pyrmts_ops import GcReport, gc_sweep

from fixture_pyramid import FROM, TO, make_pyramid

NOW = TO
NOW_MS = int(NOW.timestamp() * 1000)
GRACE_MS = 15 * 60_000


class MemRegistry:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows
        self.deleted: list[str] = []

    def rows(self, pyramid_name: str) -> list[dict]:
        assert pyramid_name == 'test'
        return list(self._rows)

    def delete_keys(self, keys: list[str]) -> None:
        self.deleted.extend(keys)


def _row(tier: str, dur: str, start_iso: str, days: int, key: str, written_at_ms: int) -> dict:
    from datetime import datetime, timezone
    start = datetime.fromisoformat(start_iso).replace(tzinfo=timezone.utc)
    return {
        'tier': tier, 'shard_dur': dur,
        'period_start': int(start.timestamp() * 1000),
        'period_end': int((start + timedelta(days=days)).timestamp() * 1000),
        'key': key, 'written_at': written_at_ms,
    }


def test_gc_sweep_branches():
    pyramid = make_pyramid()
    old = NOW_MS - 10 * 86_400_000
    rows = [
        # In the expected cover → never eligible. (Also the covering
        # parent for the deletable h@1d child below.)
        _row('h', '4d', '2026-01-03', 4, 'pyr/h/4d/2026-01-03.parquet', old),
        # Raw prefix → never eligible.
        _row('q', '1d', '2026-01-03', 1, 'raw/2026-01-03.parquet', old),
        # Covered by the 4d parent (present on storage) + past grace → deleted.
        _row('h', '1d', '2026-01-03', 1, 'pyr/h/1d/2026-01-03.parquet', old),
        # In-cover q@1d parent registered below, but its object is gone → skipped.
        _row('q', '1d', '2026-01-04', 1, 'pyr/q/1d/2026-01-04.parquet', old),
        _row('q', '6h', '2026-01-04', 1, 'pyr/q/6h/2026-01-04T00.parquet', old),
        # No covering parent registered for Jan 5 → skipped.
        _row('q', '6h', '2026-01-05', 1, 'pyr/q/6h/2026-01-05T00.parquet', old),
        # Ends within grace of `now` → skipped.
        {**_row('h', '1d', '2026-01-07', 1, 'pyr/h/1d/2026-01-07b.parquet', old),
         'period_end': NOW_MS - 60_000},
        # Future period, recently written → skipped (in-flight writer).
        _row('h', '1d', '2026-01-08', 2, 'pyr/h/1d/2026-01-08.parquet', NOW_MS - 1000),
        # Future period, written long ago → mid-period registration, deleted
        # (no parent required).
        _row('h', '1d', '2026-01-09', 2, 'pyr/h/1d/2026-01-09.parquet', old),
    ]
    registry = MemRegistry(rows)
    storage = pyramid.storage
    storage.put('pyr/h/4d/2026-01-03.parquet', b'parent')
    storage.put('pyr/h/1d/2026-01-03.parquet', b'child')
    storage.put('pyr/h/1d/2026-01-09.parquet', b'future')

    report = gc_sweep(
        pyramid, genesis=FROM, registry=registry, pyramid_name='test',
        raw_prefixes=('raw/',), now=NOW,
    )
    assert report == GcReport(
        eligible=6, deleted=2,
        skipped={
            'parent-not-on-storage': 1,
            'no-covering-parent': 1,
            'within-grace': 1,
            'future-period-within-grace': 1,
        },
    )
    assert registry.deleted == [
        'pyr/h/1d/2026-01-03.parquet',
        'pyr/h/1d/2026-01-09.parquet',
    ]
    assert storage.get('pyr/h/1d/2026-01-03.parquet') is None
    assert storage.get('pyr/h/1d/2026-01-09.parquet') is None
    assert storage.get('pyr/h/4d/2026-01-03.parquet') == b'parent'


def test_gc_sweep_dry_run_and_budget():
    pyramid = make_pyramid()
    old = NOW_MS - 10 * 86_400_000
    rows = [
        _row('h', '1d', '2026-01-03', 1, 'pyr/h/1d/2026-01-03.parquet', old),
        _row('h', '1d', '2026-01-04', 1, 'pyr/h/1d/2026-01-04.parquet', old),
    ]
    pyramid.storage.put('pyr/h/4d/2026-01-03.parquet', b'parent')
    rows.append(_row('h', '4d', '2026-01-03', 4, 'pyr/h/4d/2026-01-03.parquet', old))
    for r in rows[:2]:
        pyramid.storage.put(r['key'], b'x')

    registry = MemRegistry(rows)
    report = gc_sweep(
        pyramid, genesis=FROM, registry=registry, pyramid_name='test',
        now=NOW, dry_run=True,
    )
    assert (report.eligible, report.deleted, registry.deleted) == (2, 2, [])
    assert pyramid.storage.get('pyr/h/1d/2026-01-03.parquet') == b'x'

    report = gc_sweep(
        pyramid, genesis=FROM, registry=registry, pyramid_name='test',
        now=NOW, max_deletes=1,
    )
    assert report == GcReport(eligible=2, deleted=1, skipped={'budget': 1})
    assert registry.deleted == ['pyr/h/1d/2026-01-03.parquet']

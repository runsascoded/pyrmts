"""Registry-driven pyramid GC (`specs/pyrmts-ops-adoption.md` phase 3 —
absorbed from ctbk `pyramid_cascade/gc.py`).

  eligible  = registered − expected-min-cover − raw prefixes
  deletable = eligible ∧ past grace ∧ same-tier covering parent
              (strictly larger shard_dur, fully contains period,
               HEAD-verified on storage)
            ∨ eligible ∧ period_end > now (mid-period registration —
              violates the registered ⇒ complete invariant; no parent
              can cover a future period, and the live ladder
              independently min-covers its real content)
  delete    = storage object first, then the registry row

Conservative by construction: anything uncertain is skipped and retried
next run. The `registry` seam is a two-method protocol; `D1GcRegistry`
implements it over `pyrmts.d1`.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import partial
from typing import Protocol

from pyrmts import Pyramid, list_expected_shards
from pyrmts_engine.plan import _approx_ms

err = partial(print, file=sys.stderr, flush=True)

GRACE_MS = 15 * 60_000


class GcRegistry(Protocol):
    def rows(self, pyramid_name: str) -> list[dict]:
        """Registered shard rows: dicts with `tier`, `shard_dur`,
        `period_start`, `period_end`, `key`, `written_at` (epoch ms)."""
        ...

    def delete_keys(self, keys: list[str]) -> None: ...


class D1GcRegistry:
    def __init__(self, database_id: str | None = None, table: str = 'pyramid_shards') -> None:
        self.database_id = database_id
        self.table = table

    def rows(self, pyramid_name: str) -> list[dict]:
        from pyrmts.d1 import d1_query
        return d1_query(
            f'SELECT tier, shard_dur, period_start, period_end, key, written_at '
            f'FROM {self.table} WHERE pyramid = ?',
            [pyramid_name],
            database_id=self.database_id,
        )

    def delete_keys(self, keys: list[str]) -> None:
        from pyrmts.d1 import d1_query
        # Chunked: one REST call per ~40 keys.
        for i in range(0, len(keys), 40):
            chunk = keys[i:i + 40]
            qs = ','.join('?' * len(chunk))
            d1_query(
                f'DELETE FROM {self.table} WHERE key IN ({qs})',
                chunk,
                database_id=self.database_id,
            )


@dataclass
class GcReport:
    eligible: int = 0
    deleted: int = 0
    skipped: dict[str, int] = field(default_factory=dict)

    def skip(self, reason: str) -> None:
        self.skipped[reason] = self.skipped.get(reason, 0) + 1


def covering_parent(rows_of_tier: list[dict], r: dict) -> dict | None:
    """A same-tier row with strictly larger shard_dur whose period fully
    contains `r`'s."""
    d = _approx_ms(r['shard_dur'])
    for cand in rows_of_tier:
        if (_approx_ms(cand['shard_dur']) > d
                and cand['period_start'] <= r['period_start']
                and cand['period_end'] >= r['period_end']):
            return cand
    return None


def gc_sweep(
    pyramid: Pyramid,
    *,
    genesis: datetime,
    registry: GcRegistry,
    pyramid_name: str,
    raw_prefixes: tuple[str, ...] = (),
    now: datetime | None = None,
    dry_run: bool = False,
    max_deletes: int | None = 5000,
    grace_ms: int = GRACE_MS,
) -> GcReport:
    """One sweep. `pyramid`'s ladder should be the extended view
    (`pyrmts.merge_lambda_shards`) — the expected cover MUST match the
    registry's pyramid, or the sweep deletes the other pyramid's rows as
    "not in cover". `raw_prefixes`: keys under these are never
    GC-eligible (the rebuild backstop)."""
    now = now or datetime.now(timezone.utc)
    now_ms = int(now.timestamp() * 1000)
    expected_keys = {e.key for e in list_expected_shards(pyramid, (genesis, now))}
    rows = registry.rows(pyramid_name)
    err(f'gc: {len(rows)} registered, {len(expected_keys)} expected in cover')

    by_tier: dict[str, list[dict]] = {}
    for r in rows:
        by_tier.setdefault(r['tier'], []).append(r)

    storage = pyramid.storage
    report = GcReport()
    to_delete_rows: list[str] = []
    for r in rows:
        key = r['key']
        if key in expected_keys or any(key.startswith(p) for p in raw_prefixes):
            continue
        report.eligible += 1
        if max_deletes is not None and report.deleted >= max_deletes:
            report.skip('budget')
            continue
        if r['period_end'] > now_ms:
            # Mid-period registration (module docstring). Grace on
            # written_at, not period_end (which never elapses while the
            # shard is doing damage), so an in-flight writer isn't raced.
            if r['written_at'] + grace_ms > now_ms:
                report.skip('future-period-within-grace')
                continue
        else:
            if r['period_end'] + grace_ms > now_ms:
                report.skip('within-grace')
                continue
            parent = covering_parent(by_tier[r['tier']], r)
            if parent is None:
                report.skip('no-covering-parent')
                continue
            if storage.head(parent['key']) is None:
                report.skip('parent-not-on-storage')
                continue
        if dry_run:
            report.deleted += 1
            continue
        storage.delete(key)
        to_delete_rows.append(key)
        report.deleted += 1

    if to_delete_rows:
        registry.delete_keys(to_delete_rows)
    err(f'gc: eligible={report.eligible} deleted={report.deleted}'
        f'{" (dry-run)" if dry_run else ""} skipped={report.skipped}')
    return report

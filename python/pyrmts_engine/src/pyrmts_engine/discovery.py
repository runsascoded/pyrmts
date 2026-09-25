"""Storage-state gap discovery (`specs/pyrmts-ops-adoption.md` phase 2 —
absorbed from ctbk `pyramid_cascade/fsck.py`'s discovery half).

Enumerates the ladder's expected shards (`list_expected_shards`), LISTs
what actually exists on the pyramid's storage, and diffs — the same
LIST/diff shape as `build_local(fill=True)`, exposed as composable
pieces for fsck-style drivers (report, per-gap materialization, fan-out).

Storage-driven, not index-driven: a shard on storage but missing from a
registry counts as present ("don't re-write what's there"); registry
reconciliation is a separate concern (`consolidate.run_extension_fill`'s
`reconcile`).
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from functools import partial

from pyrmts import ExpectedShard, Pyramid, format_period, list_expected_shards, parse_duration, slot_key, slot_of_any, template_has_hash

from .invalidation import Invalidation, slot_write_times, stale_keys_for
from .plan import _approx_ms

err = partial(print, file=sys.stderr, flush=True)


class KeySet:
    """What the fill path knows as "present": a set of **slot keys** with, for
    a hashed keyTemplate, each slot's current storage key. For a hashless
    template the two coincide, so this is a plain set of keys. Membership and
    iteration are by slot key (what `ExpectedShard.key` carries); `key(slot)`
    is what to `get`. `specs/content-addressed-shards.md`."""

    def __init__(self, keys=None, *, hashed: bool = False) -> None:
        self.hashed = hashed
        if isinstance(keys, KeySet):
            self.hashed = keys.hashed
            self._map = dict(keys._map)
        elif isinstance(keys, dict):
            self._map = dict(keys)
        else:
            self._map = {k: k for k in (keys or ())}

    def __contains__(self, slot: object) -> bool:
        return slot in self._map

    def __iter__(self):
        return iter(self._map)

    def __len__(self) -> int:
        return len(self._map)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, KeySet):
            return self._map == other._map
        if isinstance(other, (set, frozenset)):
            return set(self._map) == other
        return NotImplemented

    def __repr__(self) -> str:
        return f'KeySet({sorted(self._map)!r}, hashed={self.hashed})'

    def add(self, slot: str, key: str | None = None) -> None:
        self._map[slot] = key if key is not None else slot

    def discard(self, slot: str) -> None:
        self._map.pop(slot, None)

    def key(self, slot: str) -> str:
        """The storage key holding `slot`'s current bytes."""
        return self._map[slot]

    def __sub__(self, slots) -> 'KeySet':
        drop = set(slots)
        out = KeySet(hashed=self.hashed)
        out._map = {s: k for s, k in self._map.items() if s not in drop}
        return out

    def __isub__(self, slots):
        for s in set(slots):
            self._map.pop(s, None)
        return self


def slot_for_record(pyramid: Pyramid, record, filter: dict | None = None) -> str | None:
    """A registry row's slot key. From the row's own fields (tier, shard_dur,
    period_start) — so a legacy `…/{period}.parquet` row and a hashed one map
    alike, and a lazy migration never mistakes an unmigrated slot for a gap.
    Templates with extra placeholders take them from `filter`, else fall back
    to parsing the row's key (hashed or legacy shape, `slot_of_any`)."""
    template = pyramid.keyTemplate
    period = format_period(
        record.period_start_ms if isinstance(record.period_start_ms, datetime) else _dt(record.period_start_ms),
        parse_duration(record.shard_dur),
    )
    try:
        return slot_key(template, {**(filter or {}), 'tier': record.tier, 'shard': record.shard_dur, 'period': period})
    except KeyError:
        return slot_of_any(template, record.key)


def _dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, timezone.utc)


def registry_key_set(pyramid: Pyramid, records, filter: dict | None = None) -> KeySet:
    """The registry's current rows as a `KeySet` (hashed templates: the only
    truth for what is built; a LIST may hold orphans). Slots come from the
    rows' fields, so legacy-keyed rows count as present."""
    out = KeySet(hashed=True)
    for r in records:
        slot = slot_for_record(pyramid, r, filter)
        if slot is not None:
            out.add(slot, r.key)
    return out


def list_existing_keys(pyramid: Pyramid, prefix: str | None = None) -> set[str]:
    """Every key under `prefix` (default: the keyTemplate's static prefix
    — everything before the first `{…}` substitution). Pagination is the
    storage backend's concern (`Storage.list` iterates)."""
    if prefix is None:
        prefix = pyramid.keyTemplate.split('{')[0]
    return set(pyramid.storage.list(prefix))


def list_existing_with_mtime(pyramid: Pyramid) -> dict[str, datetime | None]:
    """Like `list_existing_keys` but `{key: LastModified}`, via the
    backend's `list_with_mtime` (all built-in storages have one); other
    backends fall back to `list()` with `None` mtimes (treated as fresh —
    backends that can't report mtimes shouldn't trigger rebuilds)."""
    storage = pyramid.storage
    prefix = pyramid.keyTemplate.split('{')[0]
    lister = getattr(storage, 'list_with_mtime', None)
    if lister is None:
        return {key: None for key in storage.list(prefix)}
    return dict(lister(prefix))


def split_stale(
    existing: dict[str, datetime | None],
    stale_before: datetime | None,
) -> tuple[set[str], set[str]]:
    """Partition `{key: mtime}` into `(fresh_keys, stale_keys)`. A key is
    stale iff `stale_before` is set and its mtime is known and earlier —
    the content-invalidation knob ("an upstream input changed; everything
    built before T is stale"). Unknown mtimes are fresh."""
    if stale_before is None:
        return set(existing), set()
    fresh: set[str] = set()
    stale: set[str] = set()
    for key, mtime in existing.items():
        if mtime is not None and mtime < stale_before:
            stale.add(key)
        else:
            fresh.add(key)
    return fresh, stale


def diff_with_existing(
    expected: list[ExpectedShard],
    existing_keys,
) -> list[ExpectedShard]:
    """`expected` entries whose slot isn't present (a set or `KeySet`)."""
    return [e for e in expected if e.key not in existing_keys]


def sort_by_dependency(
    pyramid: Pyramid,
    shards: list[ExpectedShard],
) -> list[ExpectedShard]:
    """Fill order: tier index (finest first — coarser tiers source from
    finer), then shard_dur ascending within tier (coarser rungs may
    consolidate the smaller-rung shards just written), then period_start
    for determinism."""
    tier_idx = {t.name: i for i, t in enumerate(pyramid.tiers)}
    return sorted(
        shards,
        key=lambda s: (tier_idx.get(s.tier, len(tier_idx)), _approx_ms(s.shard_dur), s.period_start),
    )


def group_by_tier_rung(
    shards: list[ExpectedShard],
) -> list[tuple[str, str, list[ExpectedShard]]]:
    """Consecutive-run grouping by (tier, shard_dur), preserving input
    order — the per-rung-batch report shape."""
    out: list[tuple[str, str, list[ExpectedShard]]] = []
    cur_key: tuple[str, str] | None = None
    cur_list: list[ExpectedShard] = []
    for s in shards:
        key = (s.tier, s.shard_dur)
        if key != cur_key:
            if cur_key is not None:
                out.append((*cur_key, cur_list))
            cur_key = key
            cur_list = []
        cur_list.append(s)
    if cur_key is not None:
        out.append((*cur_key, cur_list))
    return out


def discover_gaps(
    pyramid: Pyramid,
    time_range: tuple[datetime, datetime],
    filter: dict | None = None,
    stale_before: datetime | None = None,
    invalidations: list[Invalidation] | None = None,
    registry_records=None,
) -> tuple[list[ExpectedShard], KeySet, dict[str, list[ExpectedShard]]]:
    """End-to-end discovery: enumerate expected → LIST storage → diff →
    sort. Returns `(gaps_in_fill_order, existing_key_set,
    expected_by_tier)`:

    - `gaps_in_fill_order`: missing shards, dependency-sorted.
    - `existing_key_set`: the storage snapshot; fill loops extend it as
      shards are written so downstream source lookups see fresh inputs
      without new round trips. With `stale_before`, stale keys are
      excluded (treated as missing → rebuilt in place); `invalidations`
      is the interval-scoped version of the same mechanic (expected
      shards overlapping a journal entry newer than their build).
    - `expected_by_tier`: the full expected cover grouped by tier —
      threaded into `materialize.source_long_for_gap` so a gap's source
      picks come from the same outer cover the fill order materializes
      (a fresh per-gap cover can demand tiles that were never built).
    """
    err(f"fsck: discovering gaps in {pyramid.keyTemplate.split('{')[0]} "
        f"over [{time_range[0].date()}, {time_range[1].date()})...")
    expected = list_expected_shards(pyramid, time_range, filter=filter)
    err(f"  expected: {len(expected)} shards declared by the ladder")
    existing_mtimes = list_existing_with_mtime(pyramid)
    hashed = template_has_hash(pyramid.keyTemplate)
    if hashed:
        # Content-hashed keys: the registry says what is built; the LIST only
        # supplies mtimes for staleness (orphans it holds are `gc` / `adopt`'s
        # business, not a fill's).
        if registry_records is None:
            raise ValueError(
                "discover_gaps: a keyTemplate with {hash} needs `registry_records` — the registry, "
                "not a LIST, says which shards are built"
            )
        current = registry_key_set(pyramid, registry_records, filter)
        # Per-slot write time = the registry row's `written_at`, not the
        # current key's listing mtime: an identical-bytes rebuild reuses the
        # existing object, whose mtime never advances.
        written = slot_write_times(pyramid, registry_records, filter)
        existing_mtimes = {slot: written.get(slot) for slot in current}
    existing_set, stale = split_stale(existing_mtimes, stale_before)
    existing = current - stale if hashed else KeySet(existing_set)
    err(f"  existing: {len(existing_mtimes)} keys on storage"
        + (f" ({len(stale)} stale, modified before {stale_before.isoformat()})"
           if stale_before is not None else ""))
    if invalidations:
        inv_stale = stale_keys_for(expected, existing_mtimes, invalidations)
        if inv_stale:
            existing -= inv_stale
            err(f"  invalidated: {len(inv_stale)} built shards overlap journal "
                f"entries → rebuild in place")
    missing = diff_with_existing(expected, existing)
    err(f"  missing:  {len(missing)} shards to fill")
    expected_by_tier: dict[str, list[ExpectedShard]] = {}
    for e in expected:
        expected_by_tier.setdefault(e.tier, []).append(e)
    return sort_by_dependency(pyramid, missing), existing, expected_by_tier


def report_gaps(missing: list[ExpectedShard], limit_per_rung: int = 3) -> None:
    """Per-(tier, shard_dur) summary of missing shards on stdout."""
    if not missing:
        print("no gaps — pyramid is fully tiled per the ladder")
        return
    print(f"\n=== missing shards: {len(missing)} total ===")
    print(f"{'tier':<5} {'shard':<8} {'count':>6}  earliest..latest (sample)")
    for tier, shard_dur, periods in group_by_tier_rung(missing):
        sample = ", ".join(p.period_start.strftime('%Y-%m-%d') for p in periods[:limit_per_rung])
        more = f" +{len(periods) - limit_per_rung} more" if len(periods) > limit_per_rung else ""
        print(f"  {tier:<5} {shard_dur:<8} {len(periods):>6}  {sample}{more}")

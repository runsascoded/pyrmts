"""Generic Lambda handler dispatch (`specs/pyrmts-ops-adoption.md` phase
3 — absorbed from ctbk `gbfs/lambda/handler.py`). Consumer handlers
shrink to ~5 lines:

    def lambda_handler(event, context):
        return lambda_entry(event, load=my_app_loader, default_config='avail')

`load(config_name, event)` is the consumer seam: parse the bundled
config, build the (merged-ladder) pyramid + storage, wire the hole-fill
strategies / registry, refresh any per-process denorm caches when the
event carries `stale_before`. The dispatch itself — config-name
validation, single-gap vs discovery branch, time budget, GC cadence,
response shapes (the fan-out driver parses them) — lives here."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

from pyrmts import Pyramid
from pyrmts_engine import ShardIndex, decode_gap, run_extension_fill, run_single_gap
from pyrmts_engine.consolidate import CrossTierHoleFill, RawHoleFill

from .gc import GcRegistry, gc_sweep

# ~3 min headroom under the 15-min Lambda timeout: finish the in-flight
# shard + its registration, plus a safety margin.
TIME_BUDGET_S = 12 * 60


@dataclass
class LambdaApp:
    """Everything `lambda_entry` needs for one pyramid, built by the
    consumer's `load` callable."""
    pyramid: Pyramid            # extended-ladder view (`merge_lambda_shards`)
    pyramid_name: str
    genesis: datetime
    shard_index: ShardIndex | None = None
    raw_fill: RawHoleFill | None = None
    cross_tier_fill: CrossTierHoleFill | None = None
    sort: list[str] | None = None
    fill_all: bool = True       # own each tier's smallest rung too
    gc_registry: GcRegistry | None = None
    gc_raw_prefixes: tuple[str, ...] = ()
    gc_max_deletes: int | None = 5000


def lambda_entry(
    event: dict,
    *,
    load: Callable[[str, dict], LambdaApp],
    default_config: str,
    time_budget_s: float = TIME_BUDGET_S,
    gc_enabled: bool = False,
    now: datetime | None = None,
) -> dict:
    """Dispatch one invocation. Events with a `gap` key take the
    single-gap branch (fan-out rebuild driver; `register: false` marks a
    driver-planned scaffold — unregistered, so registry-gated reads and
    the registry-driven GC never see it). Otherwise: discovery fill over
    [genesis, now) with the time budget, then — when `gc_enabled` and
    the app has a registry — a GC sweep on the hour's first firing (a
    full scan per 5-min tick buys nothing; hourly cadences always
    sweep)."""
    config_name = event.get('config', default_config)
    if not config_name.replace('-', '').isalnum():
        raise ValueError(f'bad config name {config_name!r}')
    app = load(config_name, event)
    now = now or datetime.now(timezone.utc)
    sb = event.get('stale_before')
    stale_before = datetime.fromisoformat(sb) if sb else None

    if 'gap' in event:
        res = run_single_gap(
            app.pyramid,
            decode_gap(event['gap'], app.genesis),
            genesis=app.genesis,
            pyramid_name=app.pyramid_name,
            shard_index=app.shard_index if event.get('register', True) else None,
            stale_before=stale_before,
            sort=app.sort,
            raw_fill=app.raw_fill,
            cross_tier_fill=app.cross_tier_fill,
        )
        return {
            'status': res.status,
            'key': res.gap.key,
            'rows': res.rows,
            'bytes': res.bytes_written,
            'source': res.source_desc,
            'error': res.error,
        }

    results = run_extension_fill(
        app.pyramid,
        genesis=app.genesis,
        pyramid_name=app.pyramid_name,
        now=now,
        shard_index=app.shard_index,
        reconcile=True,
        time_budget_s=time_budget_s,
        fill_all=app.fill_all,
        stale_before=stale_before,
        sort=app.sort,
        raw_fill=app.raw_fill,
        cross_tier_fill=app.cross_tier_fill,
    )
    by_status: dict[str, int] = {}
    for r in results:
        by_status[r.status] = by_status.get(r.status, 0) + 1

    gc = None
    # At a 5-min fill_all cadence, sweep on the hour's first firing only.
    if gc_enabled and app.gc_registry is not None and (not app.fill_all or now.minute < 5):
        r = gc_sweep(
            app.pyramid,
            genesis=app.genesis,
            registry=app.gc_registry,
            pyramid_name=app.pyramid_name,
            raw_prefixes=app.gc_raw_prefixes,
            now=now,
            max_deletes=app.gc_max_deletes,
        )
        gc = {'eligible': r.eligible, 'deleted': r.deleted, 'skipped': r.skipped}

    return {'filled': by_status, 'total': len(results), 'gc': gc}

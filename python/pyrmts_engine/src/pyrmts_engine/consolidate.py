"""Same-tier consolidation + extension-rung fill
(`specs/pyrmts-ops-adoption.md` phase 2 — absorbed from ctbk
`pyramid_cascade/lambda_exec.py`'s generic ~70%).

Extension shards (`Tier.lambda_shards`, merged into the ladder via
`pyrmts.merge_lambda_shards`) — and any coarse rung whose sub-rungs
already exist — are **same-tier consolidations** with matching bins: read
the sub-rung cover tiles (disjoint, aligned), concat, sort, write. Peak
memory is one output table, not a long-frame explode — what makes these
buildable in a small Lambda.

Holes in the sub-rung cover (dust that never materialized) are filled by
injected strategies:
- `raw_fill(hole, now) -> wide pa.Table | None` — finest-tier holes, from
  the app's raw data. `None` = retry later (e.g. a raw minute inside the
  poller's finality window). App territory; no generic default.
- `cross_tier_fill(tier, hole, key_set) -> wide pa.Table | None` — holes
  on coarser tiers, re-binned from the finer source tier. `None` = the
  source tier can't cover the hole either. Defaults to the generic
  `cross_tier_rebin` (longform re-bin over an `overlap_cover`), which
  handles every pyrmts monoid; consumers with bespoke fast paths inject
  their own.

Tiling arithmetic requires fixed-width rungs (epoch-aligned slots);
calendar-variable rungs (`mo`/`y`) raise.
"""
from __future__ import annotations

import hashlib
import io
import sys
import time as _time
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import Callable

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from pyrmts import ExpectedShard, Pyramid, parse_duration, write_tier_parquet
from pyrmts.types import Tier

from .discovery import discover_gaps, list_existing_with_mtime, split_stale
from .longform import empty_long, long_to_wide, rebin_long, wide_to_long
from .materialize import MaterializeResult, shard_key, source_tier_for
from .plan import UNIT_MS, bin_floor_expr
from .shard_index import ShardIndex, ShardRecord, now_ms

err = partial(print, file=sys.stderr, flush=True)

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

RawHoleFill = Callable[[tuple[datetime, datetime], datetime], 'pa.Table | None']
CrossTierHoleFill = Callable[[Tier, tuple[datetime, datetime], set[str]], 'pa.Table | None']


def _fixed_delta(dur: str) -> timedelta:
    span = parse_duration(dur)
    if span.unit not in UNIT_MS:
        raise ValueError(
            f"consolidate: calendar-variable rung {dur!r} — same-tier tiling "
            f"needs fixed-width (epoch-aligned) rungs"
        )
    return timedelta(milliseconds=span.count * UNIT_MS[span.unit])


def tile_from_existing(
    pyramid: Pyramid,
    tier: Tier,
    gap: ExpectedShard,
    key_set: set[str],
    *,
    genesis: datetime,
) -> tuple[list[tuple[str, str]], list[tuple[datetime, datetime]]]:
    """Greedy largest-first tiling of the gap period from EXISTING
    same-tier shards (`key_set` — the discovery-time listing, kept fresh
    by the fill loop). The prescriptive expected cover is wrong here: it
    demands largest-fitting sub-rungs that no min-cover ever
    materialized; what's actually on storage is whatever mix of rungs
    history produced. Returns `([(rung, key)…] in period order,
    [uncovered holes])`; pre-genesis segments are dropped."""
    rungs = [r for r in tier.shards if _fixed_delta(r) < _fixed_delta(gap.shard_dur)]
    picks: list[tuple[str, str]] = []
    holes: list[tuple[datetime, datetime]] = []

    def tile(seg_start: datetime, seg_end: datetime, idx: int) -> None:
        if seg_end <= genesis:
            return
        if idx < 0:
            holes.append((seg_start, seg_end))
            return
        rung = rungs[idx]
        dur = _fixed_delta(rung)
        # Epoch-aligned slots of `rung` within [seg_start, seg_end).
        # Divisibility chaining ⇒ seg boundaries align to some rung ≤
        # the current one; misaligned leading/trailing parts descend.
        epoch_off = (seg_start - _EPOCH) % dur
        first_slot = seg_start + ((dur - epoch_off) % dur)
        cur = seg_start
        slot = first_slot
        while slot + dur <= seg_end:
            if cur < slot:
                tile(cur, slot, idx - 1)
            key = shard_key(pyramid, tier.name, rung, slot)
            if key in key_set:
                picks.append((rung, key))
            else:
                tile(slot, slot + dur, idx - 1)
            cur = slot + dur
            slot = cur
        if cur < seg_end:
            tile(cur, seg_end, idx - 1)

    tile(gap.period_start, gap.period_end, len(rungs) - 1)
    return picks, holes


def overlap_cover(
    pyramid: Pyramid,
    src: Tier,
    s: datetime,
    e: datetime,
    key_set: set[str],
) -> tuple[list[tuple[str, datetime, datetime]], list[tuple[datetime, datetime]]]:
    """Cover [s, e) with EXISTING source-tier tiles of any rung —
    including tiles that only PARTIALLY overlap the interval (the reader
    clips each tile to its assigned subinterval). Nested-slot tiling
    misses these: a source min-cover tile can cover the head of a hole
    while no aligned sub-tile ever exists. Greedy coarsest-first;
    assigned subintervals are disjoint by construction, so per-pick
    clipping can't double-count rows. Returns
    `([(key, clip_start, clip_end)…], [uncovered holes])`."""
    uncovered = [(s, e)]
    picks: list[tuple[str, datetime, datetime]] = []
    for rung in reversed(src.shards):
        dur = _fixed_delta(rung)
        remaining: list[tuple[datetime, datetime]] = []
        for a, b in uncovered:
            cur = a
            slot = a - ((a - _EPOCH) % dur)
            while slot < b:
                if shard_key(pyramid, src.name, rung, slot) in key_set:
                    if cur < slot:
                        remaining.append((cur, slot))
                    lo, hi = max(cur, slot), min(b, slot + dur)
                    if lo < hi:
                        picks.append((shard_key(pyramid, src.name, rung, slot), lo, hi))
                    cur = max(cur, hi)
                slot += dur
            if cur < b:
                remaining.append((cur, b))
        uncovered = remaining
        if not uncovered:
            break
    return picks, uncovered


def cross_tier_rebin(
    pyramid: Pyramid,
    tier: Tier,
    hole: tuple[datetime, datetime],
    key_set: set[str],
) -> pa.Table | None:
    """Generic cross-tier hole fill: cover the hole with existing tiles
    of the finer source tier (clipped per `overlap_cover`), parse to long
    form, re-bin to `tier`'s bin, and widen. Handles every pyrmts monoid
    via `longform`. Returns `None` when the source tier can't cover the
    hole either; a covered hole matching zero source rows returns an
    EMPTY table — "no data" is a valid, final answer (outage windows),
    not an unfillable state."""
    src = source_tier_for(pyramid, tier.name)
    if src is None:
        return None
    s, e = hole
    entries, uncovered = overlap_cover(pyramid, src, s, e, key_set)
    if uncovered:
        return None
    by_key: dict[str, list[tuple[int, int]]] = {}
    for k, cs, ce in entries:
        by_key.setdefault(k, []).append(
            (int(cs.timestamp() * 1000), int(ce.timestamp() * 1000)))

    bin_col = pyramid.binCol
    longs: list[pl.DataFrame] = []
    for k, ranges in by_key.items():
        blob = pyramid.storage.get(k)
        if blob is None:
            return None  # key_set raced a delete — treat as uncoverable
        # Stream row-group batches, arrow-filter to the clip ranges
        # BEFORE the wide→long explode, so work is proportional to the
        # clipped rows, not the source file.
        pf = pq.ParquetFile(io.BytesIO(blob))
        for batch in pf.iter_batches(batch_size=131_072, use_threads=False):
            dt_arr = batch.column(batch.schema.get_field_index(bin_col))
            mask = None
            for lo, hi in ranges:
                m = pc.and_(pc.greater_equal(dt_arr, lo), pc.less(dt_arr, hi))
                mask = m if mask is None else pc.or_(mask, m)
            if not pc.any(mask).as_py():
                continue
            clipped = pl.from_arrow(pa.Table.from_batches([batch.filter(mask)]))
            sub = wide_to_long(clipped, pyramid)
            if sub.height:
                longs.append(sub)
    if not longs:
        return long_to_wide(empty_long(pyramid), pyramid).to_arrow()
    rebinned = rebin_long(
        pl.concat(longs, how='vertical'), pyramid, bin_floor_expr(bin_col, tier.bin),
    )
    return long_to_wide(rebinned, pyramid).to_arrow()


def materialize_extension_shard(
    pyramid: Pyramid,
    gap: ExpectedShard,
    *,
    key_set: set[str],
    genesis: datetime,
    rg_size: int | None = None,
    sort: list[str] | None = None,
    head_check: bool = True,
    raw_fill: RawHoleFill | None = None,
    cross_tier_fill: CrossTierHoleFill | None = None,
) -> MaterializeResult:
    """Same-tier concat build of one shard. Idempotent (`key_set`/HEAD
    skip). Source = the tier's sub-rung tiling of the gap period; holes
    go to the injected strategies (module docstring).

    `head_check=False` for stale-content rebuilds: the key EXISTS on
    storage (with a pre-`stale_before` mtime, excluded from `key_set`)
    and must be overwritten in place — the HEAD probe would wrongly
    'exists'-skip it; `key_set` membership (fresh keys only) still
    short-circuits re-runs."""
    tag = f"/{gap.tier}@{gap.shard_dur} {gap.period_start.date()}"
    t0 = _time.time()
    if gap.key in key_set:
        return MaterializeResult(gap=gap, status='exists')
    if head_check and pyramid.storage.head(gap.key) is not None:
        key_set.add(gap.key)
        return MaterializeResult(gap=gap, status='exists')
    if gap.period_end <= genesis:
        return MaterializeResult(gap=gap, status='no_inputs', inputs_present=0,
                                 inputs_expected=0, source_desc='pre-genesis')

    tier = pyramid.tier(gap.tier)
    finest = pyramid.tiers[0]
    picks, holes = tile_from_existing(pyramid, tier, gap, key_set, genesis=genesis)
    hole_tables: list[pa.Table] = []
    for hole in holes:
        # Holes are scars (dust that never materialized) or — for a
        # tier's smallest rung, which has no sub-rungs — the whole
        # period. The finest tier fills from raw; others re-bin their
        # source tier.
        if tier.name == finest.name:
            if raw_fill is None:
                err(f"  ⟶ {tag} → no_inputs (finest-tier hole, no raw_fill strategy)")
                return MaterializeResult(
                    gap=gap, status='no_inputs',
                    inputs_present=len(picks), inputs_expected=len(picks) + len(holes),
                    source_desc='no-raw-ingester',
                )
            ht = raw_fill(hole, datetime.now(timezone.utc))
        elif cross_tier_fill is not None:
            ht = cross_tier_fill(tier, hole, key_set)
        else:
            ht = cross_tier_rebin(pyramid, tier, hole, key_set)
        if ht is None:
            err(f"  ⟶ {tag} → no_inputs (hole "
                f"[{hole[0].isoformat()}, {hole[1].isoformat()}) unfillable)")
            return MaterializeResult(
                gap=gap, status='no_inputs',
                inputs_present=len(picks), inputs_expected=len(picks) + len(holes),
                source_desc='same-tier tiling + hole-fill',
            )
        hole_tables.append(ht)
    inputs_expected = len(picks) + len(holes)
    if not picks and not hole_tables:
        return MaterializeResult(gap=gap, status='empty', source_desc='no data in period')

    err(f"  ⟶ {tag} → reading {len(picks)} existing tiles"
        + (f" + {len(holes)} hole fills" if holes else ""))
    tables: list[pa.Table] = list(hole_tables)
    for _rung, k in picks:
        blob = pyramid.storage.get(k)
        if blob is None:
            raise RuntimeError(
                f"consolidate: tile {k} was in key_set but storage returned "
                f"nothing (stale key_set or concurrent delete)"
            )
        tables.append(pq.read_table(io.BytesIO(blob)))
    # Tiles (and hole fills) can span writer eras whose schemas differ in
    # string vs large_string and in column order; normalize to
    # large_string (the engine writer's canonical — polars→arrow) + the
    # first table's column order so concat doesn't throw and re-written
    # shards match engine-built bytes.
    tables = [
        t.cast(pa.schema([
            (f.name, pa.large_string() if pa.types.is_string(f.type) else f.type)
            for f in t.schema
        ]))
        for t in tables
    ]
    tables = [t.select(tables[0].schema.names) for t in tables]
    combined = pa.concat_tables([t for t in tables if t.num_rows] or tables[:1])
    if combined.num_rows == 0:
        return MaterializeResult(gap=gap, status='empty',
                                 inputs_present=inputs_expected,
                                 inputs_expected=inputs_expected,
                                 source_desc='same-tier cover')
    # Cover tiles are disjoint and fit inside the gap period, so no bin
    # clipping is needed — write_tier_parquet handles the sort.
    buf = io.BytesIO()
    kwargs: dict = {'sort': sort} if sort is not None else {}
    rgs = rg_size if rg_size is not None else tier.rg_size
    if rgs is not None:
        kwargs['row_group_size'] = rgs
    write_tier_parquet(combined, pyramid, out=buf, **kwargs)
    blob = buf.getvalue()
    pyramid.storage.put(gap.key, blob)
    key_set.add(gap.key)
    err(f"  ⟵ {tag} → wrote ({combined.num_rows:,} rows, {len(blob)/1e6:.1f}MB, "
        f"{_time.time()-t0:.1f}s)")
    return MaterializeResult(
        gap=gap, status='wrote', bytes_written=len(blob), rows=combined.num_rows,
        md5=hashlib.md5(blob).hexdigest(),
        inputs_present=inputs_expected, inputs_expected=inputs_expected,
        source_desc=f'same-tier cover ×{inputs_expected}',
    )


def _register(
    shard_index: ShardIndex,
    pyramid_name: str,
    gap: ExpectedShard,
    res: MaterializeResult | None = None,
) -> None:
    shard_index.record_shard(ShardRecord(
        pyramid=pyramid_name,
        tier=gap.tier,
        shard_dur=gap.shard_dur,
        period_start_ms=int(gap.period_start.timestamp() * 1000),
        period_end_ms=int(gap.period_end.timestamp() * 1000),
        key=gap.key,
        written_at_ms=now_ms(),
        md5=res.md5 if res is not None else None,
        n_bytes=res.bytes_written if res is not None else None,
    ))


def reconcile_registrations(
    expected_by_tier: dict[str, list[ExpectedShard]],
    existing: set[str],
    shard_index: ShardIndex,
    pyramid_name: str,
) -> int:
    """Re-register expected shards that exist on storage but are absent
    from the registry (`shard_index.existing_keys()`). A write-then-die
    strands an unregistered object forever: storage-driven discovery
    sees the key and never re-fills, while registry-gated serving never
    sees the shard. Best-effort — a registry outage must not block the
    fill. Returns the stranded count (0 on registry-read failure)."""
    existing_keys = getattr(shard_index, 'existing_keys', None)
    if existing_keys is None:
        return 0
    try:
        registered = existing_keys()
    except Exception as e:
        err(f'  reconcile: registry read failed ({e}); skipping this tick')
        return 0
    stranded = [
        e for shards in expected_by_tier.values() for e in shards
        if e.key in existing and e.key not in registered
    ]
    for e in stranded:
        _register(shard_index, pyramid_name, e)
    if stranded:
        err(f'  reconcile: re-registered {len(stranded)} present-but-unregistered shards')
    return len(stranded)


def run_extension_fill(
    pyramid: Pyramid,
    *,
    genesis: datetime,
    pyramid_name: str,
    now: datetime | None = None,
    shard_index: ShardIndex | None = None,
    reconcile: bool = False,
    fill_limit: int | None = None,
    time_budget_s: float | None = None,
    dry_run: bool = False,
    fill_all: bool = False,
    stale_before: datetime | None = None,
    sort: list[str] | None = None,
    raw_fill: RawHoleFill | None = None,
    cross_tier_fill: CrossTierHoleFill | None = None,
) -> list[MaterializeResult]:
    """Discover + fill missing shards over [genesis, `now`) — the cron
    tick / catch-up driver. `pyramid`'s ladder should already be the
    extended view (`pyrmts.merge_lambda_shards`). By default only gaps
    with same-tier sub-rungs to build from are handled (each tier's
    smallest rung needs raw/cross-tier ingest — `fill_all` opts those
    in). Each 'wrote' is registered via `shard_index` immediately after
    its put, so a mid-run abort can't strand unregistered keys beyond
    the one in flight; `reconcile` closes the historical write-then-die
    window first.

    `stale_before`: shards last-modified before this timestamp are
    treated as missing and rebuilt in place (content invalidation)."""
    now = now or datetime.now(timezone.utc)
    gaps, existing, expected_by_tier = discover_gaps(
        pyramid, (genesis, now), stale_before=stale_before)
    if reconcile and shard_index is not None and not dry_run:
        reconcile_registrations(expected_by_tier, existing, shard_index, pyramid_name)
    smallest = {t.name: t.shards[0] for t in pyramid.tiers}
    ext_gaps = [
        g for g in gaps
        if (fill_all or g.shard_dur != smallest[g.tier])
        # Trailing max-shards whose notional period ends pre-genesis can
        # never exist — permanent no-ops, excluded from the census.
        and g.period_end > genesis
    ]
    err(f"fillable gaps: {len(ext_gaps)} of {len(gaps)} total missing")
    if dry_run:
        for g in ext_gaps[:40]:
            err(f"  would fill /{g.tier}@{g.shard_dur} {g.period_start.date()}")
        return []

    t0 = _time.time()
    results: list[MaterializeResult] = []
    for g in ext_gaps:
        if fill_limit is not None and len(results) >= fill_limit:
            err(f"  hit fill limit {fill_limit}; stopping")
            break
        if time_budget_s is not None and _time.time() - t0 > time_budget_s:
            err(f"  hit time budget {time_budget_s:.0f}s; stopping")
            break
        res = materialize_extension_shard(
            pyramid, g,
            key_set=existing, genesis=genesis, sort=sort,
            head_check=stale_before is None,
            raw_fill=raw_fill, cross_tier_fill=cross_tier_fill,
        )
        results.append(res)
        if res.status == 'wrote' and shard_index is not None:
            _register(shard_index, pyramid_name, g, res)
    by_status: dict[str, int] = {}
    for r in results:
        by_status[r.status] = by_status.get(r.status, 0) + 1
    err(f"extension fill: {by_status or 'nothing to do'}")
    return results


def run_single_gap(
    pyramid: Pyramid,
    gap: ExpectedShard,
    *,
    genesis: datetime,
    pyramid_name: str,
    shard_index: ShardIndex | None = None,
    stale_before: datetime | None = None,
    sort: list[str] | None = None,
    raw_fill: RawHoleFill | None = None,
    cross_tier_fill: CrossTierHoleFill | None = None,
) -> MaterializeResult:
    """Materialize ONE shard (no discovery loop) — the handler branch a
    fan-out driver invokes concurrently. Lists existing keys so tiling /
    covers see current storage state; with `stale_before`, pre-cutoff
    keys are excluded from that view (stale sub-tiles are never concat'd
    into a rebuilt shard) and the target key is overwritten in place."""
    existing_mtimes = list_existing_with_mtime(pyramid)
    fresh, stale = split_stale(existing_mtimes, stale_before)
    err(f"single-gap /{gap.tier}@{gap.shard_dur} {gap.period_start.date()}: "
        f"{len(fresh)} fresh keys" + (f", {len(stale)} stale" if stale else ""))
    res = materialize_extension_shard(
        pyramid, gap,
        key_set=fresh, genesis=genesis, sort=sort,
        head_check=stale_before is None,
        raw_fill=raw_fill, cross_tier_fill=cross_tier_fill,
    )
    if res.status == 'wrote' and shard_index is not None:
        _register(shard_index, pyramid_name, gap, res)
    return res


def encode_gap(gap: ExpectedShard) -> dict:
    """`ExpectedShard` → JSON-serializable event payload (fan-out wire
    format; fixed-width durations only, so the driver-side key/period
    computation is authoritative)."""
    return {
        'tier': gap.tier,
        'shard_dur': gap.shard_dur,
        'period_start': gap.period_start.isoformat(),
        'period_end': gap.period_end.isoformat(),
        'key': gap.key,
    }


def decode_gap(d: dict, genesis: datetime) -> ExpectedShard:
    """Event payload → `ExpectedShard` (inverse of `encode_gap`). The
    wire format omits the effective bounds — reconstructed as the
    genesis-clipped period (trailing-edge clipping is handled by the
    fill paths' own head checks, not by `effective_end`)."""
    period_start = datetime.fromisoformat(d['period_start'])
    period_end = datetime.fromisoformat(d['period_end'])
    return ExpectedShard(
        tier=d['tier'],
        shard_dur=d['shard_dur'],
        period_start=period_start,
        period_end=period_end,
        effective_start=max(period_start, genesis),
        effective_end=period_end,
        key=d['key'],
    )

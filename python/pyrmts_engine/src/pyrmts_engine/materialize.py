"""Strict-cascade per-shard materialization (`specs/pyrmts-ops-adoption.md`
phase 2 — absorbed from ctbk `pyramid_cascade/materialize.py`, generalized
onto the engine's long-form primitives).

A gap at tier N sources ONLY from tier N-1's cover intersecting the gap's
period — `source_tier_for` picks the largest finer tier whose bin divides
N's bin, so the per-source pre-aggregate to the target bin is exact. Tier
N-1 must be complete-in-range before tier N is materialized (enforced by
`discovery.sort_by_dependency`'s finest-first ordering); any uncovered
segment raises rather than silently under-populating.

Consumer seams:
- **Raw ingest** (`raw_ingest(start, end) -> long DataFrame`): fills
  base-tier gaps from the app's raw data (WAL, monthly dumps, …); when
  provided, it also substitutes for reading base-tier shards when the
  *source* tier is the base (ctbk measured raw ~5× cheaper than decoding
  base parquets back to long form). Without it, base-tier gaps are
  `no_inputs` and base-sourced gaps read the base rung's shards.
- Dim/metric shapes come off `pyramid.dims`/`pyramid.metrics` via
  `longform` (nothing consumer-specific is hardwired).
- `genesis` is always explicit: notional periods of coarse trailing
  shards can extend arbitrarily far pre-genesis; sourcing clips to it.
"""
from __future__ import annotations

import hashlib
import io
import sys
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from functools import partial
from time import time
from typing import Callable

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from pyrmts import ExpectedShard, Pyramid, write_tier_parquet
from pyrmts.axis import format_period, parse_duration
from pyrmts.keys import substitute_key

from .longform import combine_long, empty_long, long_to_wide, rebin_long, wide_to_long
from .plan import _approx_ms, bin_floor_expr

err = partial(print, file=sys.stderr, flush=True)

# App-provided base-tier ingest: `(start, end) -> long-form DataFrame`
# (see `longform.long_schema`; bins floored to the base tier's bin).
RawIngest = Callable[[datetime, datetime], pl.DataFrame]


@dataclass
class MaterializeResult:
    gap: ExpectedShard
    status: str   # 'wrote' / 'exists' / 'no_inputs' / 'empty' / 'error'
    bytes_written: int = 0
    rows: int = 0
    md5: str | None = None  # content hash of the written blob ('wrote' only)
    inputs_present: int = 0
    inputs_expected: int = 0
    source_desc: str = ''  # e.g. '/1h@30d×2', 'raw'
    error: str | None = None


def source_tier_for(pyramid: Pyramid, tier_name: str):
    """Strict-cascade source tier: the largest tier T' with
    `bin(T') < bin(tier)` AND `bin(tier) % bin(T') == 0` (divisibility
    keeps floor-then-sum exact). `None` for the base tier (raw-ingest
    territory); raises for any other tier without a divisor (a malformed
    ladder — the base bin must divide everything)."""
    tiers = pyramid.tiers
    if tier_name == tiers[0].name:
        return None
    target_ms = _approx_ms(pyramid.tier(tier_name).bin)
    tier_idx = next(i for i, t in enumerate(tiers) if t.name == tier_name)
    best = None
    best_ms = 0
    for cand in tiers[:tier_idx]:
        cand_ms = _approx_ms(cand.bin)
        if cand_ms >= target_ms or target_ms % cand_ms != 0:
            continue
        if cand_ms > best_ms:
            best = cand
            best_ms = cand_ms
    if best is None:
        raise AssertionError(f"no source tier for /{tier_name} — pyramid ladder is malformed")
    return best


@dataclass
class SourcePick:
    """One source shard contributing to a gap's cover. In strict cascade
    all picks share the gap's source tier; `shard_dur` varies with the
    cover's rung mix."""
    tier: str
    shard_dur: str
    period_start: datetime
    period_end: datetime  # exclusive
    key: str


def plan_source_cover_single_tier(
    gap: ExpectedShard,
    source_expected: list[ExpectedShard],
    key_set: set[str],
) -> tuple[list[SourcePick], list[tuple[datetime, datetime]]]:
    """Filter `source_expected` (the outer discovery's expected cover for
    the source tier) to shards intersecting the gap's period; return
    `(present picks, uncovered segments)`.

    Uses the *outer* expected cover rather than a fresh cover of just the
    gap period: min-cover tiles that overshoot the gap's bounds are what
    actually got built, and overshoot is safe (the build filters rows to
    the gap period). Uncovered segments — untouched spans plus
    expected-but-missing shards — are a strict-cascade invariant
    violation for the caller to raise on."""
    intersecting = sorted(
        (e for e in source_expected
         if e.period_start < gap.period_end and e.period_end > gap.period_start),
        key=lambda e: e.period_start,
    )
    picks = [
        SourcePick(
            tier=e.tier, shard_dur=e.shard_dur,
            period_start=e.period_start, period_end=e.period_end, key=e.key,
        )
        for e in intersecting if e.key in key_set
    ]

    # Uncovered = spans of the gap not covered by any *present* pick —
    # which includes the spans of expected-but-missing source shards
    # (min-cover shards within a tier are disjoint, so a missing shard's
    # span can't be covered by a present sibling).
    uncovered: list[tuple[datetime, datetime]] = []
    cur = gap.period_start
    for e in (e for e in intersecting if e.key in key_set):
        seg_start = max(e.period_start, gap.period_start)
        if seg_start > cur:
            uncovered.append((cur, seg_start))
        cur = max(cur, min(e.period_end, gap.period_end))
    if cur < gap.period_end:
        uncovered.append((cur, gap.period_end))
    return picks, uncovered


def _summarize_picks(picks: list[SourcePick]) -> str:
    from collections import Counter
    counts = Counter((p.tier, p.shard_dur) for p in picks)
    parts = [f"/{t}@{s}×{n}" for (t, s), n in counts.most_common()]
    return "+".join(parts) if parts else "no-source"


def parse_wide_blob(blob: bytes, pyramid: Pyramid) -> pl.DataFrame:
    """Wide shard parquet bytes → long form, streamed per record batch
    (whole-shard wide→long transients are the thing that busts small
    containers — same rationale as `WideShardSource._load`)."""
    pf = pq.ParquetFile(io.BytesIO(blob))
    parts = [
        wide_to_long(pl.from_arrow(pa.Table.from_batches([batch])), pyramid)
        for batch in pf.iter_batches(batch_size=131_072, use_threads=False)
    ]
    parts = [p for p in parts if p.height]
    if not parts:
        return empty_long(pyramid)
    return pl.concat(parts, rechunk=False)


def shard_key(pyramid: Pyramid, tier: str, shard_dur: str, period_start: datetime) -> str:
    """Substitute the pyramid's keyTemplate for one shard."""
    label = format_period(period_start, parse_duration(shard_dur))
    return substitute_key(
        pyramid.keyTemplate,
        {'tier': tier, 'shard': shard_dur, 'period': label},
    )


def source_long_for_gap(
    pyramid: Pyramid,
    gap: ExpectedShard,
    *,
    genesis: datetime,
    key_set: set[str] | None = None,
    expected_by_tier: dict[str, list[ExpectedShard]] | None = None,
    raw_ingest: RawIngest | None = None,
) -> tuple[pl.DataFrame, int, int, str]:
    """`(long_df, inputs_present, inputs_expected, source_desc)` for the
    gap's source reads. Base-tier gaps (and, when `raw_ingest` is given,
    gaps whose source tier IS the base) ingest raw, chunked by UTC day so
    peak memory stays O(1 day) regardless of shard_dur; other gaps read
    the source tier's cover shards, each pre-aggregated to the target bin
    before concatenation."""
    tiers = pyramid.tiers
    if gap.tier == tiers[0].name:
        if raw_ingest is None:
            return empty_long(pyramid), 0, 1, 'no-raw-ingester'
        long = raw_ingest(max(gap.period_start, genesis), gap.period_end)
        return long, (0 if long.is_empty() else 1), 1, 'raw'

    target_bin = pyramid.tier(gap.tier).bin
    floor_expr = bin_floor_expr(pyramid.binCol, target_bin)
    ks: set[str] = key_set if key_set is not None else set()
    eff_gap = replace(gap, period_start=genesis) if gap.period_start < genesis else gap
    src_tag = f"/{gap.tier}@{gap.shard_dur} {gap.period_start.date()}"
    src_tier = source_tier_for(pyramid, gap.tier)

    if src_tier.name == tiers[0].name and raw_ingest is not None:
        # Base-tier substitution: re-ingest raw instead of decoding the
        # base rung's shards back to long form (ctbk measured ~5×
        # cheaper for the same window).
        t_reads = time()
        err(f"    {src_tag} sourcing raw (base-tier substitution), 1d chunks...")
        longs: list[pl.DataFrame] = []
        chunk_from = eff_gap.period_start
        while chunk_from < eff_gap.period_end:
            next_midnight = (chunk_from + timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0,
            )
            chunk_to = min(next_midnight, eff_gap.period_end)
            sub = raw_ingest(chunk_from, chunk_to)
            if not sub.is_empty():
                longs.append(rebin_long(sub, pyramid, floor_expr))
            chunk_from = chunk_to
        err(f"    {src_tag} raw reads done ({time()-t_reads:.1f}s)")
        if not longs:
            return empty_long(pyramid), 0, 1, 'raw'
        return pl.concat(longs, how='vertical'), 1, 1, 'raw'

    if expected_by_tier is None:
        # Fall-back for single-shard runs outside a full discovery: cover
        # just the effective gap period.
        from pyrmts.gap_discovery import _cover_for_tier
        source_expected = _cover_for_tier(
            pyramid, src_tier, eff_gap.period_start, eff_gap.period_end, filter={},
        )
    else:
        source_expected = expected_by_tier.get(src_tier.name, [])

    picks, uncovered = plan_source_cover_single_tier(eff_gap, source_expected, ks)
    err(f"    {src_tag} source_plan: {len(picks)} picks from /{src_tier.name}, "
        f"{len(uncovered)} uncovered")
    if uncovered:
        sample = ', '.join(f"[{s.isoformat()}, {e.isoformat()})" for s, e in uncovered[:3])
        more = f" +{len(uncovered) - 3} more" if len(uncovered) > 3 else ""
        raise RuntimeError(
            f"strict-cascade invariant violation for {src_tag}: "
            f"source tier /{src_tier.name} has {len(uncovered)} uncovered segment(s): "
            f"{sample}{more}. Ensure all /{src_tier.name} shards in the range are "
            f"materialized before /{gap.tier} shards are scheduled."
        )

    longs = []
    inputs_present = 0
    t_reads = time()
    for i, pick in enumerate(picks):
        t_pick = time()
        blob = pyramid.storage.get(pick.key)
        if blob is None:
            raise RuntimeError(
                f"strict-cascade read failure for {src_tag}: source pick "
                f"{pick.key} was in key_set but storage returned nothing. Either "
                f"the key_set is stale or the source shard was deleted concurrently."
            )
        sub = parse_wide_blob(blob, pyramid)
        inputs_present += 1
        if sub.is_empty():
            err(f"      [{i+1}/{len(picks)}] {pick.key} → empty ({time()-t_pick:.1f}s)")
            continue
        pre_rows = sub.height
        agg = rebin_long(sub, pyramid, floor_expr)
        longs.append(agg)
        err(f"      [{i+1}/{len(picks)}] {pick.key} → {pre_rows:,}→{agg.height:,} rows ({time()-t_pick:.1f}s)")
    err(f"    {src_tag} reads done: {inputs_present}/{len(picks)} present ({time()-t_reads:.1f}s)")
    source_desc = _summarize_picks(picks)
    if not longs:
        return empty_long(pyramid), inputs_present, len(picks), source_desc
    return combine_long(longs, pyramid), inputs_present, len(picks), source_desc


def materialize_shard(
    pyramid: Pyramid,
    gap: ExpectedShard,
    *,
    genesis: datetime,
    rg_size: int | None = None,
    sort: list[str] | None = None,
    skip_existing: bool = True,
    key_set: set[str] | None = None,
    expected_by_tier: dict[str, list[ExpectedShard]] | None = None,
    raw_ingest: RawIngest | None = None,
) -> MaterializeResult:
    """Build + write one gap's shard via the strict cascade. Idempotent:
    skips when the key already exists (in `key_set` if provided, else via
    HEAD) unless `skip_existing=False`. `rg_size` defaults to the gap
    tier's `Tier.rg_size` (then the writer heuristic); `sort` defaults to
    the writer's pyramid default."""
    tag = f"/{gap.tier}@{gap.shard_dur} {gap.period_start.date()}"
    t0 = time()
    if skip_existing:
        if key_set is not None:
            if gap.key in key_set:
                return MaterializeResult(gap=gap, status='exists')
        elif pyramid.storage.head(gap.key) is not None:
            return MaterializeResult(gap=gap, status='exists')

    if gap.period_end <= genesis:
        return MaterializeResult(
            gap=gap, status='no_inputs',
            inputs_present=0, inputs_expected=0, source_desc='pre-genesis',
        )

    err(f"  ⟶ {tag} → START")
    try:
        long, inputs_present, inputs_expected, source_desc = source_long_for_gap(
            pyramid, gap,
            genesis=genesis, key_set=key_set,
            expected_by_tier=expected_by_tier, raw_ingest=raw_ingest,
        )
    except Exception as e:
        err(f"  ⟵ {tag} → ERROR source: {e!r} ({time()-t0:.1f}s)")
        return MaterializeResult(gap=gap, status='error', error=f"source: {e!r}")

    if inputs_present == 0:
        err(f"  ⟵ {tag} → no_inputs ({source_desc}, {time()-t0:.1f}s)")
        return MaterializeResult(
            gap=gap, status='no_inputs',
            inputs_present=0, inputs_expected=inputs_expected, source_desc=source_desc,
        )

    bin_col = pyramid.binCol
    s_ms = int(max(gap.period_start, genesis).timestamp() * 1000)
    e_ms = int(gap.period_end.timestamp() * 1000)
    long = long.filter((pl.col(bin_col) >= s_ms) & (pl.col(bin_col) < e_ms))
    if long.is_empty():
        err(f"  ⟵ {tag} → empty ({source_desc}, {time()-t0:.1f}s)")
        return MaterializeResult(
            gap=gap, status='empty',
            inputs_present=inputs_present, inputs_expected=inputs_expected,
            source_desc=source_desc,
        )

    wide = long_to_wide(long, pyramid)
    buf = io.BytesIO()
    kwargs: dict = {'sort': sort} if sort is not None else {}
    rgs = rg_size if rg_size is not None else pyramid.tier(gap.tier).rg_size
    if rgs is not None:
        kwargs['row_group_size'] = rgs
    write_tier_parquet(wide.to_arrow(), pyramid, out=buf, **kwargs)
    blob = buf.getvalue()
    pyramid.storage.put(gap.key, blob)
    err(f"  ⟵ {tag} → wrote ({wide.height:,} rows, {len(blob)/1e6:.1f}MB, "
        f"total {time()-t0:.1f}s)")
    return MaterializeResult(
        gap=gap, status='wrote',
        bytes_written=len(blob), rows=wide.height,
        md5=hashlib.md5(blob).hexdigest(),
        inputs_present=inputs_present, inputs_expected=inputs_expected,
        source_desc=source_desc,
    )


def emit_d1_insert_sql(
    pyramid_name: str,
    results: list[MaterializeResult],
    sql_path: str,
    *,
    shards_table: str = 'pyramid_shards',
    watermarks_table: str = 'pyramid_watermarks',
) -> int:
    """Emit a wrangler-runnable SQL file with idempotent INSERTs for each
    written (or already-present) shard — the offline-registration path
    for interactive backfills; headless runners use `pyrmts.d1` /
    `D1ShardIndex` directly. Schema matches
    `pyrmts-cfw/src/shard-index.ts:schemaSql`. Returns rows planned.

    `'exists'` results are emitted too: a shard already on storage but
    absent from the registry needs a row for serving to see it."""
    eligible = [r for r in results if r.status in ('wrote', 'exists')]
    if not eligible:
        return 0
    lines: list[str] = []
    for r in eligible:
        ps = int(r.gap.period_start.timestamp() * 1000)
        pe = int(r.gap.period_end.timestamp() * 1000)
        for s in (pyramid_name, r.gap.tier, r.gap.shard_dur, r.gap.key):
            assert "'" not in s, f"single-quote in {s!r} — SQL injection guard"
        lines.append(
            f"INSERT INTO {shards_table} "
            f"(pyramid, tier, shard_dur, period_start, period_end, key, written_at) "
            f"VALUES ('{pyramid_name}', '{r.gap.tier}', '{r.gap.shard_dur}', "
            f"{ps}, {pe}, '{r.gap.key}', unixepoch()*1000) "
            f"ON CONFLICT (pyramid, tier, shard_dur, period_start) DO UPDATE SET "
            f"period_end=excluded.period_end, key=excluded.key, written_at=excluded.written_at;"
        )
        lines.append(
            f"INSERT INTO {watermarks_table} "
            f"(pyramid, tier, shard_dur, latest_period_end, updated_at) "
            f"VALUES ('{pyramid_name}', '{r.gap.tier}', '{r.gap.shard_dur}', "
            f"{pe}, unixepoch()*1000) "
            f"ON CONFLICT (pyramid, tier, shard_dur) DO UPDATE SET "
            f"latest_period_end=MAX(excluded.latest_period_end, {watermarks_table}.latest_period_end), "
            f"updated_at=excluded.updated_at;"
        )
    with open(sql_path, 'w') as f:
        f.write('\n'.join(lines) + '\n')
    err(f"emitted {len(eligible)} shard INSERTs ({len(lines)} statements) → {sql_path}")
    return len(eligible)

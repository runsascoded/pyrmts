"""Fan-out bulk rebuilds via single-gap Lambda invocations
(`specs/pyrmts-ops-adoption.md` phase 3 — absorbed from ctbk
`pyramid_cascade/rebuild.py`).

Discover gaps locally (~seconds), then invoke a schedule-less single-gap
function once per gap — concurrency bounded only by the driver's thread
pool. Invocations are SYNCHRONOUS (`RequestResponse` from a thread pool):
the driver learns each shard's exact status from the invoke response —
no registry/storage completion polling, and no async retries that could
double-invoke.

Layering: gaps are grouped by `(tier, rung)` in dependency order (finest
tier first, smallest rung first) with a barrier between layers, so
coarser rungs concat just-rebuilt sub-rung tiles instead of re-reading
raw per rung. `expand_scaffolds` inserts fill-safe-rung scaffold layers
so no single invocation's whole-period fill exceeds the source-bin
budget; scaffolds are invoked with `register=False` (a registry-driven
GC can't sweep unregistered keys mid-rebuild, and registry-gated reads
never see them) and deleted after a clean run.

Consumer knobs live in `FanoutConfig` (function names, cost-model
constants, progress key prefix, env-bump var); `genesis` is explicit.
"""
from __future__ import annotations

import json
import math
import sys
import time as _time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import Callable

from pyrmts import ExpectedShard, Pyramid, Storage
from pyrmts.types import Tier
from pyrmts_engine import discover_gaps, encode_gap, group_by_tier_rung, shard_key, source_tier_for
from pyrmts_engine.plan import _approx_ms

err = partial(print, file=sys.stderr, flush=True)


@dataclass(frozen=True)
class FanoutConfig:
    """Per-consumer fan-out knobs. Cost-model defaults are ctbk's
    measured 2026-07 numbers — override per app."""
    function_name: str                    # single-gap Lambda
    tick_function: str | None = None      # steady-state tick fn (env-bump target)
    progress_prefix: str = 'build-progress/'
    env_bump_var: str = 'DENORM_REV'
    # Max whole-period fill size per invocation, in SOURCE bins (base-bin
    # rows for the finest tier; source-tier bins elsewhere).
    source_bin_budget: int = 720
    # Per-class cost model for `--plan` estimates.
    raw_s_per_bin: float = 0.36
    xtier_s_per_srcbin: float = 0.45
    concat_s: float = 25.0
    lambda_usd_per_s: float = 10 * 0.0000166667  # 10 GB memory


Layer = tuple[str, str, list[ExpectedShard], bool]  # (tier, rung, batch, is_scaffold)


def _src_bin_ms(pyramid: Pyramid, tier: Tier) -> int:
    """The bin the tier's whole-period fills consume: its source tier's
    bin, or — for the finest tier (raw territory) — its own base bin."""
    src = source_tier_for(pyramid, tier.name)
    return _approx_ms((src or tier).bin)


def fill_safe_rung(pyramid: Pyramid, tier: Tier, *, source_bin_budget: int) -> str:
    """Largest rung of `tier` whose whole-period fill fits the budget —
    the scaffold rung for bigger siblings."""
    src_bin = _src_bin_ms(pyramid, tier)
    fits = [r for r in tier.shards if _approx_ms(r) // src_bin <= source_bin_budget]
    if not fits:
        raise ValueError(f"tier {tier.name}: no rung fits {source_bin_budget} source bins")
    return max(fits, key=_approx_ms)


def _estimate_layer(
    pyramid: Pyramid,
    cfg: FanoutConfig,
    tier_name: str,
    rung: str,
    is_scaffold: bool,
) -> tuple[float, str]:
    """(est seconds per shard, class label) for one `(tier, rung)` layer.
    Scaffolds and each tier's smallest rung are whole-period fills (raw
    at the finest tier, cross-tier rebin elsewhere); larger rungs concat
    just-built same-tier tiles."""
    tier = pyramid.tier(tier_name)
    if not is_scaffold and rung != tier.shards[0]:
        return cfg.concat_s, 'concat'
    if tier.name == pyramid.tiers[0].name:
        bins = _approx_ms(rung) // _approx_ms(tier.bin)
        return bins * cfg.raw_s_per_bin, 'raw-fill'
    src = source_tier_for(pyramid, tier.name)
    src_bins = _approx_ms(rung) // _approx_ms(src.bin)
    return src_bins * cfg.xtier_s_per_srcbin, 'xtier-fill'


def print_plan(
    pyramid: Pyramid,
    cfg: FanoutConfig,
    layers: list[Layer],
    concurrency: int,
    dot_path: str | None = None,
) -> None:
    """Per-layer plan with wall/cost estimates (layers are sequential
    barriers, so total wall = Σ per-layer walls at the given
    concurrency), plus an optional Graphviz DAG of the semantic
    dependencies: concat layers depend on their tier's scaffold layer;
    fill layers depend on the source tier's last layer (raw for the
    finest tier)."""
    total_wall = total_compute = 0.0
    rows = []
    last_layer_of_tier: dict[str, str] = {}
    nodes: list[tuple[str, str, float, int]] = []
    edges: list[tuple[str, str]] = []
    finest = pyramid.tiers[0].name
    for tier, rung, batch, is_scaffold in layers:
        per, cls = _estimate_layer(pyramid, cfg, tier, rung, is_scaffold)
        wall = math.ceil(len(batch) / concurrency) * per
        compute = len(batch) * per
        total_wall += wall
        total_compute += compute
        node = f'{tier}@{rung}' + (' [scaffold]' if is_scaffold else '')
        rows.append((node, len(batch), cls, per, wall))
        nodes.append((node, cls, compute, len(batch)))
        if cls == 'concat':
            dep = last_layer_of_tier.get(tier)
        elif tier == finest:
            dep = 'raw'
        else:
            src = source_tier_for(pyramid, tier)
            dep = last_layer_of_tier.get(src.name, 'raw')
        if dep:
            edges.append((dep, node))
        last_layer_of_tier[tier] = node
    err(f"{'layer':<22} {'n':>5} {'class':<11} {'s/shard':>8} {'est wall':>9}")
    for node, n, cls, per, wall in rows:
        err(f'{node:<22} {n:>5} {cls:<11} {per:>7.0f}s {wall:>8.0f}s')
    err(f'plan: {sum(r[1] for r in rows)} invocations | est wall ≈ '
        f'{total_wall / 60:.0f} min at -c {concurrency} | est compute ≈ '
        f'{total_compute / 3600:.1f} Lambda-hrs ≈ ${total_compute * cfg.lambda_usd_per_s:.2f}')
    if dot_path:
        with open(dot_path, 'w') as f:
            f.write('digraph build {\n  rankdir=LR;\n  node [shape=box, fontsize=10];\n')
            f.write('  "raw" [shape=cylinder];\n')
            for node, cls, compute, n in nodes:
                f.write(f'  "{node}" [label="{node}\\n{n}× {cls}, ~{compute / 60:.0f} min"];\n')
            for a, b in edges:
                f.write(f'  "{a}" -> "{b}";\n')
            f.write('}\n')
        err(f'DAG → {dot_path}')


def expand_scaffolds(
    pyramid: Pyramid,
    layers: list[tuple[str, str, list[ExpectedShard]]],
    *,
    genesis: datetime,
    source_bin_budget: int,
) -> list[Layer]:
    """Insert scaffold layers so no invocation's fill exceeds the budget.

    For each tier, rungs above its fill-safe rung F get a preceding layer
    of F-sized shards tiling their gap periods (epoch-aligned; gap
    periods are F-aligned by the divisibility chain). The big rungs then
    concat fresh F-tiles instead of whole-period-filling. Scaffolds are
    deduped across the tier's big rungs; entirely-pre-genesis slots are
    dropped. Returns `(tier, rung, batch, is_scaffold)` layers in fill
    order."""
    tiers = {t.name: t for t in pyramid.tiers}
    out: list[Layer] = []
    scaffolded: dict[str, set[str]] = {}
    for tier_name, rung, batch in layers:
        tier = tiers[tier_name]
        safe = fill_safe_rung(pyramid, tier, source_bin_budget=source_bin_budget)
        if _approx_ms(rung) > _approx_ms(safe):
            dur = timedelta(milliseconds=_approx_ms(safe))
            seen = scaffolded.setdefault(tier_name, set())
            slots: list[ExpectedShard] = []
            for gap in batch:
                cur = gap.period_start
                while cur < gap.period_end:
                    slot_end = cur + dur
                    key = shard_key(pyramid, tier_name, safe, cur)
                    if slot_end > genesis and key not in seen:
                        seen.add(key)
                        slots.append(ExpectedShard(
                            tier=tier_name, shard_dur=safe,
                            period_start=cur, period_end=slot_end,
                            effective_start=max(cur, genesis),
                            effective_end=slot_end,
                            key=key,
                        ))
                    cur = slot_end
            if slots:
                out.append((tier_name, safe, slots, True))
        out.append((tier_name, rung, batch, False))
    return out


def touch_tick_function(function_name: str, env_var: str = 'DENORM_REV') -> datetime:
    """Recycle the steady-state tick function's execution environments by
    bumping a no-op env var. Warm containers cache per-process denorm
    state; after a re-key they'd keep writing tail shards with the OLD
    state — with fresh mtimes, invisible to any `stale_before`. Returns
    the update's completion time: the earliest instant from which all
    tick writes are known to use the new state (the correct effective
    `stale_before`)."""
    import boto3
    lam = boto3.client('lambda')
    cfg = lam.get_function_configuration(FunctionName=function_name)
    env = cfg['Environment']['Variables']
    env[env_var] = datetime.now(timezone.utc).isoformat()
    lam.update_function_configuration(
        FunctionName=function_name, Environment={'Variables': env})
    lam.get_waiter('function_updated').wait(FunctionName=function_name)
    ts = datetime.now(timezone.utc)
    err(f"touched {function_name} ({env_var}={env[env_var]}) — "
        f"effective stale_before {ts.isoformat()}")
    return ts


class BuildProgress:
    """Best-effort build-progress JSON on storage
    (`<progress_prefix><name>.json`) — feeds a /health builds card (the
    shared contract with `pyrmts-cfw`'s `BuildProgress` reader, phase 4).
    Written at layer boundaries + intra-layer checkpoints; a PUT failure
    warns once and disables itself (progress reporting must never fail a
    rebuild)."""

    def __init__(self, storage: Storage, key: str, name: str, layers: list[Layer]) -> None:
        self.storage = storage
        self.key = key
        self.enabled = True
        self.doc: dict = {
            'pyramid': name,
            'driver': 'lambda-fanout',
            'startedAt': datetime.now(timezone.utc).isoformat(),
            'status': 'running',
            'plan': {
                'layers': len(layers),
                'invocations': sum(len(b) for _, _, b, _ in layers),
                'scaffolds': sum(len(b) for _, _, b, s in layers if s),
            },
            'byStatus': {},
            'layers': [],       # completed layers: {tier, rung, scaffold, n, wallS, status}
            'currentLayer': None,
        }

    def write(self, **updates) -> None:
        if not self.enabled:
            return
        self.doc.update(updates)
        self.doc['updatedAt'] = datetime.now(timezone.utc).isoformat()
        try:
            self.storage.put(self.key, json.dumps(self.doc).encode())
        except Exception as e:
            self.enabled = False
            err(f'  (build-progress writes disabled: {e})')


def lambda_invoker(function_name: str, concurrency: int) -> Callable[[dict], dict]:
    """The production transport: synchronous boto3 invoke, no
    transport-level retries (a read timeout must not re-invoke a shard
    build — idempotent, but doubles the work)."""
    import boto3
    from botocore.config import Config
    lam = boto3.client('lambda', config=Config(
        connect_timeout=10,
        read_timeout=920,  # ≥ the function's 900 s timeout
        max_pool_connections=concurrency + 4,
        retries={'mode': 'standard', 'max_attempts': 1},
    ))

    def invoke(payload: dict) -> dict:
        resp = lam.invoke(FunctionName=function_name,
                          Payload=json.dumps(payload).encode())
        body = json.loads(resp['Payload'].read() or b'null')
        if resp.get('FunctionError'):
            return {'status': 'error', 'error': json.dumps(body)[:300]}
        return body or {'status': 'error', 'error': 'empty invoke response'}

    return invoke


def run_rebuild(
    pyramid: Pyramid,
    *,
    genesis: datetime,
    pyramid_name: str,
    cfg: FanoutConfig,
    stale_before: datetime | None = None,
    touch_tick: bool = False,
    concurrency: int = 16,
    dry_run: bool = False,
    limit: int | None = None,
    keep_scaffolds: bool = False,
    dot_path: str | None = None,
    progress_storage: Storage | None = None,
    invoke: Callable[[dict], dict] | None = None,
    now: datetime | None = None,
) -> dict[str, int]:
    """Discover → layer (+ scaffolds) → fan out. Returns `{status: count}`.

    Idempotent + resumable: a re-run's discovery sees fresh mtimes and
    'exists'-skips completed shards; a killed driver loses nothing
    (per-shard registration happens inside each invocation), and leftover
    scaffolds get reused then cleaned by the re-run. `pyramid`'s ladder
    should be the extended view (`pyrmts.merge_lambda_shards`); `invoke`
    is injectable for tests (default: `lambda_invoker`)."""
    if touch_tick:
        if dry_run:
            err('(dry-run: skipping tick touch; planning with stale_before=now)')
            ts = datetime.now(timezone.utc)
        else:
            if cfg.tick_function is None:
                raise ValueError('run_rebuild: touch_tick needs FanoutConfig.tick_function')
            ts = touch_tick_function(cfg.tick_function, cfg.env_bump_var)
        stale_before = ts if stale_before is None else max(stale_before, ts)

    now = now or datetime.now(timezone.utc)
    gaps, _existing, expected_by_tier = discover_gaps(
        pyramid, (genesis, now), stale_before=stale_before)
    # Trailing max-shards whose notional period ends pre-genesis can
    # never exist (same exclusion as `run_extension_fill`).
    gaps = [g for g in gaps if g.period_end > genesis]
    layers = expand_scaffolds(
        pyramid, group_by_tier_rung(gaps),
        genesis=genesis, source_bin_budget=cfg.source_bin_budget,
    )
    n_scaffold = sum(len(b) for _, _, b, s in layers if s)
    err(f"rebuild: {len(gaps)} shards + {n_scaffold} scaffolds across "
        f"{len(layers)} (tier, rung) layers")
    if dry_run:
        print_plan(pyramid, cfg, layers, concurrency, dot_path)
        return {}

    if invoke is None:
        invoke = lambda_invoker(cfg.function_name, concurrency)
    sb_iso = stale_before.isoformat() if stale_before else None

    def payload_for(gap: ExpectedShard, register: bool) -> dict:
        payload: dict = {'gap': encode_gap(gap), 'register': register, 'config': pyramid_name}
        if sb_iso:
            payload['stale_before'] = sb_iso
        return payload

    by_status: dict[str, int] = {}
    scaffold_keys: set[str] = set()
    done = 0
    t0 = _time.time()
    progress = BuildProgress(
        progress_storage if progress_storage is not None else pyramid.storage,
        f'{cfg.progress_prefix}{pyramid_name}.json',
        pyramid_name, layers,
    )
    progress.write()
    for tier, rung, batch, is_scaffold in layers:
        if limit is not None:
            if done >= limit:
                err(f"  hit limit {limit}; stopping")
                break
            batch = batch[:limit - done]
        if is_scaffold:
            scaffold_keys.update(g.key for g in batch)
        lt0 = _time.time()
        layer_status: dict[str, int] = {}
        cur = {'tier': tier, 'rung': rung, 'scaffold': is_scaffold, 'n': len(batch), 'done': 0}
        progress.write(currentLayer=cur)
        with ThreadPoolExecutor(max_workers=min(concurrency, len(batch))) as pool:
            futs = {pool.submit(invoke, payload_for(g, not is_scaffold)): g for g in batch}
            for fut in as_completed(futs):
                g = futs[fut]
                try:
                    r = fut.result()
                except Exception as e:
                    r = {'status': 'error', 'error': str(e)}
                st = r.get('status') or 'error'
                layer_status[st] = layer_status.get(st, 0) + 1
                by_status[st] = by_status.get(st, 0) + 1
                done += 1
                if st in ('error', 'no_inputs'):
                    detail = f": {r['error']}" if r.get('error') else ""
                    err(f"  ! /{g.tier}@{g.shard_dur} {g.period_start.date()} → {st}{detail}")
                n_done = sum(layer_status.values())
                if len(batch) > concurrency and n_done % 25 == 0:
                    err(f"  … /{tier}@{rung}: {n_done}/{len(batch)} ({_time.time() - lt0:.0f}s)")
                    cur['done'] = n_done
                    progress.write(byStatus=dict(by_status))
        err(f"  /{tier}@{rung}{' [scaffold]' if is_scaffold else ''}: {len(batch)} → "
            + ", ".join(f"{k}={v}" for k, v in sorted(layer_status.items()))
            + f" ({_time.time() - lt0:.0f}s)")
        progress.doc['layers'].append({
            'tier': tier, 'rung': rung, 'scaffold': is_scaffold, 'n': len(batch),
            'wallS': round(_time.time() - lt0, 1), 'status': dict(layer_status),
        })
        progress.write(currentLayer=None, byStatus=dict(by_status))
    err(f"rebuild: {done} shards in {(_time.time() - t0) / 60:.1f} min: "
        + (", ".join(f"{k}={v}" for k, v in sorted(by_status.items())) or "nothing to do"))

    bounced = (bool(by_status.get('no_inputs') or by_status.get('error'))
               or (limit is not None and done >= limit))
    progress.write(status='bounced' if bounced else 'done', byStatus=dict(by_status))
    if bounced:
        err("some shards bounced — re-run the same command to retry "
            "(discovery skips completed shards; scaffolds kept for reuse)")
    # Expected-cover keys are never scaffolds to clean: a scaffold slot
    # coinciding with an expected shard was rebuilt (and registered) in
    # its own earlier layer and must stay.
    expected_keys = {e.key for batch in expected_by_tier.values() for e in batch}
    to_clean = sorted(scaffold_keys - expected_keys)
    if to_clean and not bounced and not keep_scaffolds:
        for key in to_clean:
            pyramid.storage.delete(key)
        err(f"cleaned {len(to_clean)} scaffold keys")
    elif to_clean:
        err(f"kept {len(to_clean)} scaffold keys (unregistered; reused by a re-run)")
    return by_status

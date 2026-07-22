"""`pyrmts-engine` CLI: compile/inspect build plans and run local builds
whose base rung is already materialized as wide shards (`WideShardSource`).
App-specific ingest (raw → long form) is library territory — see
`build_local`."""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from functools import partial
from pathlib import Path

from click import Choice, argument, group, option

from pyrmts import FsStorage, S3Storage, parse_pyramid_yaml, pyramid_from_config
from .engine import EmptySourceError, SourceCoverageError, build_local
from .plan import compile_plan
from .shard_index import JsonlShardIndex, NoopShardIndex, StorageJsonlShardIndex
from .source import WideShardSource

err = partial(print, file=sys.stderr)


def _parse_range(s: str) -> tuple[datetime, datetime]:
    try:
        from_s, to_s = s.split('/')
        from_ = datetime.fromisoformat(from_s).replace(tzinfo=timezone.utc)
        to = datetime.fromisoformat(to_s).replace(tzinfo=timezone.utc)
    except ValueError as e:
        raise SystemExit(f"invalid range {s!r} (want <from-iso>/<to-iso>): {e}")
    return from_, to


def _read_config(config_path: str) -> str:
    """Local path, or `s3://bucket/key` (R2 via the usual `R2_*`/`AWS_*`
    endpoint env — Batch containers have no bind mounts)."""
    if config_path.startswith('s3://'):
        bucket, _, key = config_path[len('s3://'):].partition('/')
        blob = S3Storage(bucket=bucket).get(key)
        if blob is None:
            raise SystemExit(f"config not found: {config_path}")
        return blob.decode()
    return Path(config_path).read_text()


def _load_pyramid(config_path: str, fs_root: str | None):
    cfg = parse_pyramid_yaml(_read_config(config_path))
    if fs_root is not None:
        storage = FsStorage(fs_root)
    else:
        stype = cfg.storage.get('type')
        if stype != 's3':
            raise SystemExit(
                f"storage.type {stype!r} unsupported by the CLI; pass -R/--fs-root "
                f"or wire storage via the library"
            )
        storage = S3Storage(
            bucket=cfg.storage['bucket'],
            prefix=cfg.storage.get('prefix', ''),
        )
    return pyramid_from_config(cfg, storage)


def _parse_bytes(s: str) -> int:
    """`24g`/`512m`/`0` → bytes (binary units)."""
    import re
    m = re.fullmatch(r'(\d+(?:\.\d+)?)\s*([kmgt]?)i?b?', s.strip().lower())
    if m is None:
        raise SystemExit(f"invalid size {s!r} (want e.g. 24g, 512m, 0)")
    return int(float(m.group(1)) * 1024 ** ('bkmgt'.index(m.group(2) or 'b')))


def _parse_filters(filters: tuple[str, ...]) -> dict[str, str]:
    out: dict[str, str] = {}
    for f in filters:
        if '=' not in f:
            raise SystemExit(f"invalid -F filter {f!r} (want key=value)")
        k, v = f.split('=', 1)
        out[k] = v
    return out


@group()
def cli() -> None:
    """Fused long-form pyramid build engine."""


@cli.command()
@option('-F', '--filter', 'filters', multiple=True, help="Extra keyTemplate substitution, key=value (repeatable)")
@option('-o', '--dot-out', help="Write graphviz DOT of the plan DAG to this path")
@option('-R', '--fs-root', help="Use filesystem storage rooted here (instead of the config's storage block)")
@option('-r', '--range', 'range_', required=True, help="Half-open build range, <from-iso>/<to-iso> (UTC)")
@argument('config')
def plan(filters: tuple[str, ...], dot_out: str | None, fs_root: str | None, range_: str, config: str) -> None:
    """Compile the build plan for CONFIG (pyramid YAML) and print a summary."""
    pyramid = _load_pyramid(config, fs_root or '.')
    p = compile_plan(pyramid, _parse_range(range_), filter=_parse_filters(filters))
    per_tier: dict[str, int] = {}
    for e in p.outputs:
        per_tier[e.tier] = per_tier.get(e.tier, 0) + 1
    for t in pyramid.tiers:
        pred = p.preds[t.name] or '<source>'
        print(f"{t.name:8s} bin={t.bin:5s} ← {pred:8s} shards={per_tier.get(t.name, 0)}")
    print(f"total expected shards: {len(p.outputs)}")
    if dot_out is not None:
        Path(dot_out).write_text(p.to_dot() + '\n')
        err(f"wrote {dot_out}")


@cli.command()
@option('-b', '--mem-budget', help="Byte budget for window admission, e.g. 24g (default: 70% of the detected memory limit; 0 disables)")
@option('-C', '--close-workers', type=int, help="Concurrent close computations (default 2): more overlaps closes with the walk (wall) at the cost of stacked close transients (peak RSS)")
@option('-c', '--close-chunk', help="Target combined-long bytes per close chunk, e.g. 1g (default 1g); smaller bounds each close's transient tighter")
@option('-d', '--source-shard', help="Source rung shard Duration for the default WideShardSource (default: the base tier's smallest; ctbk-style durable rungs are usually the LARGEST)")
@option('-e', '--allow-empty', is_flag=True, help="Permit a 0-source-row (all-EMPTY) build; without it that exits nonzero (~always a mis-specified source rung)")
@option('-F', '--filter', 'filters', multiple=True, help="Extra keyTemplate substitution, key=value (repeatable)")
@option('-g', '--rg-size', type=int, help="Output-shard parquet row-group size (all tiers; per-tier via the library)")
@option('-j', '--workers', type=int, help="Window-worker threads (default: cpu count)")
@option('-K', '--max-inflight', type=int, help="Max windows in flight past the watermark (memory bound; default 2×workers)")
@option('-M', '--max-missing', type=float, default=0.0, help="Tolerated fraction of absent source shards (default 0.0 = strict; absent ≠ present-but-EMPTY)")
@option('-m', '--manifest', help="Record written shards to this JSONL manifest")
@option('-n', '--pyramid-name', required=True, help="Pyramid name for shard registration")
@option('-R', '--fs-root', help="Use filesystem storage rooted here (instead of the config's storage block)")
@option('-r', '--range', 'range_', required=True, help="Half-open build range, <from-iso>/<to-iso> (UTC)")
@option('-S', '--spill-dir', help="Scratch dir for WIP spill files (default: fresh temp dir)")
@option('-s', '--sort', 'sort_csv', help="Override shard sort columns (comma-separated)")
@option('-t', '--source-tier', help="Source rung tier name for the default WideShardSource (default: base tier)")
@option('-u', '--resume', is_flag=True, help="Skip shards already in the manifest (-m required) and the windows that only feed them")
@option('-v', '--verbose', is_flag=True, help="Per-flush progress on stderr")
@option('-w', '--window', default='1d', help="Streaming window Duration (default 1d)")
@option('-x', '--source', 'source_spec', help="Source factory `module:attr`, called as factory(pyramid, filter) → Source (default: WideShardSource on the base rung)")
@argument('config')
def build(
    mem_budget: str | None,
    close_workers: int | None,
    close_chunk: str | None,
    source_shard: str | None,
    allow_empty: bool,
    filters: tuple[str, ...],
    rg_size: int | None,
    workers: int | None,
    max_inflight: int | None,
    max_missing: float,
    manifest: str | None,
    pyramid_name: str,
    fs_root: str | None,
    range_: str,
    spill_dir: str | None,
    sort_csv: str | None,
    source_tier: str | None,
    resume: bool,
    verbose: bool,
    window: str,
    source_spec: str | None,
    config: str,
) -> None:
    """Build CONFIG's pyramid from its materialized base rung (or a
    `--source`-provided raw-ingest Source)."""
    pyramid = _load_pyramid(config, fs_root)
    filter_ = _parse_filters(filters)
    if source_spec is not None:
        if source_tier is not None or source_shard is not None:
            raise SystemExit("-t/--source-tier and -d/--source-shard only apply to the default WideShardSource (not -x/--source)")
        from importlib import import_module
        mod_name, _, attr = source_spec.partition(':')
        if not attr:
            raise SystemExit(f"invalid --source {source_spec!r} (want module:attr)")
        source = getattr(import_module(mod_name), attr)(pyramid, filter_)
    else:
        source = WideShardSource(pyramid, tier_name=source_tier, shard_dur=source_shard, filter=filter_)
    if manifest is None:
        if resume:
            raise SystemExit("-u/--resume needs -m/--manifest (prior records are what get skipped)")
        shard_index = NoopShardIndex()
    elif manifest.startswith('s3://'):
        bucket, _, key = manifest[len('s3://'):].partition('/')
        shard_index = StorageJsonlShardIndex(S3Storage(bucket=bucket), key)
    else:
        shard_index = JsonlShardIndex(manifest)
    try:
        result = build_local(
            pyramid,
            _parse_range(range_),
            source,
            pyramid_name=pyramid_name,
            shard_index=shard_index,
            window=window,
            filter=filter_,
            sort=sort_csv.split(',') if sort_csv else None,
            row_group_size=rg_size,
            spill_dir=spill_dir,
            workers=workers,
            max_inflight=max_inflight,
            mem_budget=_parse_bytes(mem_budget) if mem_budget is not None else None,
            close_workers=close_workers,
            close_chunk_bytes=_parse_bytes(close_chunk) if close_chunk is not None else None,
            resume=resume,
            allow_empty=allow_empty,
            max_missing_source=max_missing,
            verbose=verbose,
        )
    except SourceCoverageError as e:
        err(str(e))
        raise SystemExit(4)
    except EmptySourceError as e:
        err(str(e))
        raise SystemExit(3)
    print(result.summary())


@cli.group()
def batch() -> None:
    """AWS Batch (Fargate Spot) packaging — see specs/engine-batch-packaging.md."""


@batch.command()
@option('-a', '--arch', default='X86_64', type=Choice(['X86_64', 'ARM64']), help="Fargate CPU architecture (default X86_64; ARM64 = Graviton, matches an arm64 image build)")
@option('-e', '--env', 'envs', multiple=True, help="Job-definition env var, NAME=VALUE (repeatable; e.g. R2 creds)")
@option('-g', '--ephemeral', default=100, help="Ephemeral storage GiB (spill scratch; default 100)")
@option('-i', '--image', required=True, help="Container image ref (ECR); repo is created if missing")
@option('-M', '--max-vcpus', default=16, help="Compute-environment max vCPUs (default 16)")
@option('-m', '--memory', default=32768, help="Job-definition memory MiB (default 32768)")
@option('-o', '--on-demand', is_flag=True, help="Also create an on-demand (non-Spot) CE + queue `pyrmts-engine-od` (submit -O targets it)")
@option('-v', '--vcpus', default=8, help="Job-definition vCPUs (default 8)")
def bootstrap(arch: str, envs: tuple[str, ...], ephemeral: int, image: str, max_vcpus: int, memory: int, on_demand: bool, vcpus: int) -> None:
    """Idempotently create the role, log group, ECR repo, Fargate-Spot
    compute environment, queue, and job definition."""
    from .batch import bootstrap as _bootstrap
    _bootstrap(
        image=image,
        arch=arch,
        max_vcpus=max_vcpus,
        vcpus=vcpus,
        memory_mib=memory,
        ephemeral_gib=ephemeral,
        on_demand=on_demand,
        environment=_parse_filters(envs),
    )


@batch.command('push')
@option('-B', '--no-build', is_flag=True, help="Skip docker build (image tag already exists locally)")
@option('-c', '--context', default='.', help="Docker build context (default: cwd; base image needs the pyrmts repo root)")
@option('-f', '--dockerfile', help="Dockerfile path (default: <context>/Dockerfile)")
@option('-p', '--platform', default='linux/amd64', help="Target platform (default linux/amd64; use linux/arm64 with `bootstrap --arch ARM64`)")
@argument('image')
def batch_push(no_build: bool, context: str, dockerfile: str | None, platform: str, image: str) -> None:
    """ECR-login docker, build (unless -B), and push IMAGE (a full ECR ref,
    e.g. <acct>.dkr.ecr.<region>.amazonaws.com/pyrmts-engine:<rev>).
    Creates the ECR repo if missing, so this can run before `bootstrap`."""
    from .batch import push_image
    push_image(
        image,
        dockerfile=dockerfile,
        context=context,
        platform=platform,
        build=not no_build,
    )


@batch.command('submit')
@option('-b', '--mem-budget', help="Byte budget for window admission, e.g. 24g (default: 70% of the container's cgroup limit; 0 disables)")
@option('-C', '--close-workers', type=int, help="build -C: concurrent close computations")
@option('-c', '--close-chunk', help="build -c: target combined-long bytes per close chunk, e.g. 1g")
@option('-d', '--source-shard', help="Source rung shard Duration (WideShardSource; durable rungs are usually the LARGEST)")
@option('-e', '--env', 'envs', multiple=True, help="Extra container env var, NAME=VALUE (repeatable)")
@option('-E', '--allow-empty', is_flag=True, help="Permit a 0-source-row (all-EMPTY) build")
@option('-F', '--filter', 'filters', multiple=True, help="Extra keyTemplate substitution, key=value (repeatable)")
@option('-g', '--rg-size', type=int, help="Output-shard parquet row-group size")
@option('-j', '--job-name', help="Batch job name (default: derived from pyramid name)")
@option('-K', '--max-inflight', type=int, help="build -K: max windows in flight past the watermark")
@option('-m', '--manifest', help="Manifest destination (use s3:// — container disk is ephemeral)")
@option('-M', '--memory', type=int, help="Override job memory MiB")
@option('--max-missing', type=float, help="Tolerated fraction of absent source shards (build -M)")
@option('-n', '--pyramid-name', required=True, help="Pyramid name for shard registration")
@option('-O', '--on-demand', is_flag=True, help="Submit to the on-demand queue (needs `bootstrap -o`); no Spot reclaims")
@option('-r', '--range', 'range_', required=True, help="Half-open build range, <from-iso>/<to-iso> (UTC)")
@option('-s', '--sort', 'sort_csv', help="Override shard sort columns (comma-separated)")
@option('-t', '--source-tier', help="Source rung tier name (WideShardSource)")
@option('-u', '--resume', is_flag=True, help="Skip shards already in the manifest (-m required)")
@option('-V', '--vcpus', type=int, help="Override job vCPUs")
@option('-w', '--window', help="Streaming window Duration")
@option('-W', '--watch', is_flag=True, help="Tail the job's log stream; exit with its status")
@option('--workers', type=int, help="build -j: window-worker threads (default: the job's vCPUs; -j here is --job-name)")
@option('-x', '--source', 'source_spec', help="Source factory module:attr (needs an app-derived image)")
@argument('config')
def batch_submit(
    mem_budget: str | None,
    close_workers: int | None,
    close_chunk: str | None,
    source_shard: str | None,
    envs: tuple[str, ...],
    allow_empty: bool,
    filters: tuple[str, ...],
    rg_size: int | None,
    job_name: str | None,
    max_inflight: int | None,
    manifest: str | None,
    memory: int | None,
    max_missing: float | None,
    pyramid_name: str,
    on_demand: bool,
    range_: str,
    sort_csv: str | None,
    source_tier: str | None,
    resume: bool,
    vcpus: int | None,
    window: str | None,
    watch: bool,
    workers: int | None,
    source_spec: str | None,
    config: str,
) -> None:
    """Submit a build of CONFIG (an s3:// URL — the container has no local
    files) to the bootstrapped queue."""
    from .batch import PREFIX, build_command, submit as _submit
    code = _submit(
        command=build_command(
            config,
            pyramid_name=pyramid_name,
            range_=range_,
            window=window,
            rg_size=rg_size,
            sort=sort_csv,
            source=source_spec,
            source_tier=source_tier,
            source_shard=source_shard,
            manifest=manifest,
            resume=resume,
            allow_empty=allow_empty,
            max_missing=max_missing,
            filters=filters,
            workers=workers,
            max_inflight=max_inflight,
            mem_budget=mem_budget,
            close_workers=close_workers,
            close_chunk=close_chunk,
        ),
        job_name=job_name or f'{pyramid_name}-build',
        queue=f'{PREFIX}-od' if on_demand else PREFIX,
        vcpus=vcpus,
        memory_mib=memory,
        environment=_parse_filters(envs),
        watch=watch,
    )
    raise SystemExit(code)


if __name__ == '__main__':
    cli()

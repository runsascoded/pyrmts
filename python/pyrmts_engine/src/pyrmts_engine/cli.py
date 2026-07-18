"""`pyrmts-engine` CLI: compile/inspect build plans and run local builds
whose base rung is already materialized as wide shards (`WideShardSource`).
App-specific ingest (raw → long form) is library territory — see
`build_local`."""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from functools import partial
from pathlib import Path

from click import argument, group, option

from pyrmts import FsStorage, S3Storage, parse_pyramid_yaml, pyramid_from_config
from .engine import build_local
from .plan import compile_plan
from .shard_index import JsonlShardIndex, NoopShardIndex
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


def _load_pyramid(config_path: str, fs_root: str | None):
    cfg = parse_pyramid_yaml(Path(config_path).read_text())
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
@option('-F', '--filter', 'filters', multiple=True, help="Extra keyTemplate substitution, key=value (repeatable)")
@option('-m', '--manifest', help="Record written shards to this JSONL manifest")
@option('-n', '--pyramid-name', required=True, help="Pyramid name for shard registration")
@option('-P', '--prefetch', default=2, help="Source windows to read ahead")
@option('-R', '--fs-root', help="Use filesystem storage rooted here (instead of the config's storage block)")
@option('-r', '--range', 'range_', required=True, help="Half-open build range, <from-iso>/<to-iso> (UTC)")
@option('-s', '--sort', 'sort_csv', help="Override shard sort columns (comma-separated)")
@option('-v', '--verbose', is_flag=True, help="Per-flush progress on stderr")
@option('-w', '--window', default='1d', help="Streaming window Duration (default 1d)")
@argument('config')
def build(
    filters: tuple[str, ...],
    manifest: str | None,
    pyramid_name: str,
    prefetch: int,
    fs_root: str | None,
    range_: str,
    sort_csv: str | None,
    verbose: bool,
    window: str,
    config: str,
) -> None:
    """Build CONFIG's pyramid from its materialized base rung."""
    pyramid = _load_pyramid(config, fs_root)
    filter_ = _parse_filters(filters)
    source = WideShardSource(pyramid, filter=filter_)
    shard_index = JsonlShardIndex(manifest) if manifest else NoopShardIndex()
    result = build_local(
        pyramid,
        _parse_range(range_),
        source,
        pyramid_name=pyramid_name,
        shard_index=shard_index,
        window=window,
        filter=filter_,
        sort=sort_csv.split(',') if sort_csv else None,
        prefetch=prefetch,
        verbose=verbose,
    )
    print(result.summary())


if __name__ == '__main__':
    cli()

"""`pyrmts-engine` CLI: compile/inspect build plans and run local builds
whose base rung is already materialized as wide shards (`WideShardSource`).
App-specific ingest (raw → long form) is library territory — see
`build_local`."""
from __future__ import annotations

import sys
import time
from datetime import datetime, timezone
from functools import partial
from pathlib import Path

from click import Choice, argument, group, option

from pyrmts import FsStorage, S3Storage, parse_pyramid_yaml, pyramid_from_config
# Safe at module scope: `batch` defers its boto3 import to `_clients()`, so
# this costs nothing for consumers without the `[batch]` extra.
from .batch import PREFIX
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


def _load_id_map(pyramid, map_override: str | None) -> dict[str, str]:
    """`{raw_token: canonical_token}` for the identity rollup
    (`specs/pyrmts-identity-rollup.md`). From `--map` (local JSON path), else
    the declared `identityRollup.map` — an `s3://` URL or a storage key
    relative to the pyramid's storage."""
    import json
    ir = pyramid.identity_rollup
    if map_override is not None:
        blob: bytes | None = Path(map_override).read_bytes()
        loc = map_override
    elif ir is not None:
        loc = ir.map
        if loc.startswith('s3://'):
            bucket, _, key = loc[len('s3://'):].partition('/')
            blob = S3Storage(bucket=bucket).get(key)
        else:
            blob = pyramid.storage.get(loc)
    else:
        raise SystemExit("canonicalize: no `identityRollup` block and no --map; nothing to load")
    if blob is None:
        raise SystemExit(f"canonicalize: id-map not found at {loc!r}")
    m = json.loads(blob)
    if not isinstance(m, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in m.items()):
        raise SystemExit("canonicalize: id-map must be a JSON object of {raw_token: canonical_token} strings")
    return m


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
@option('-R', '--fs-root', help="Use filesystem storage rooted here (instead of the config's storage block)")
@option('-r', '--range', 'range_', required=True, help="Half-open interval to invalidate, <from-iso>/<to-iso> (UTC)")
@argument('config')
def invalidate(fs_root: str | None, range_: str, config: str) -> None:
    """Mark built shards overlapping the interval stale (append to the
    pyramid's invalidation journal); the next extension-fill tick rebuilds
    them in place. See `specs/shard-invalidation.md`."""
    from .invalidation import invalidate as _invalidate, journal_key
    pyramid = _load_pyramid(config, fs_root)
    interval = _parse_range(range_)
    n = _invalidate(pyramid, interval)
    err(f"appended [{interval[0].isoformat()}, {interval[1].isoformat()}) to "
        f"{journal_key(pyramid)}: {n} entries pending")


@cli.command()
@option('-F', '--filter', 'filters', multiple=True, help="Extra keyTemplate substitution, key=value (repeatable)")
@option('-g', '--rg-size', type=int, help="Override the rewrite's row-group size (default: the shard's stamped layout, else its first row group's size)")
@option('-i', '--index', 'manifest', help="Shard registry: a JSONL manifest path or s3://bucket/key (required for a {hash} keyTemplate: current keys are looked up there, rewritten shards registered there)")
@option('-j', '--concurrency', type=int, default=1, help="Parallel shard rewrites (default 1)")
@option('-m', '--map', 'map_override', help="Local id-map JSON path (overrides identityRollup.map)")
@option('-n', '--pyramid-name', help="Pyramid name for shard registration (with -i)")
@option('-R', '--fs-root', help="Use filesystem storage rooted here (instead of the config's storage block)")
@option('-r', '--range', 'range_', required=True, help="Half-open range to (re)derive canonical rows over, <from-iso>/<to-iso> (UTC)")
@option('-s', '--sort', 'sort_csv', help="Override the rewrite's sort columns, comma-separated (default: the shard's stamped layout, else the pyramid's default sort)")
@argument('config')
def canonicalize(
    filters: tuple[str, ...],
    rg_size: int | None,
    manifest: str | None,
    concurrency: int,
    map_override: str | None,
    pyramid_name: str | None,
    fs_root: str | None,
    range_: str,
    sort_csv: str | None,
    config: str,
) -> None:
    """Re-derive canonical identity rows in every built shard overlapping the
    range, in place (`specs/pyrmts-identity-rollup.md`).

    Reads each shard's raw `s:` leaves + the declared id-map and writes summed
    canonical rows alongside — no source re-pull, no cascade. Run after a build
    to materialize the canonical level, or after an id-map change to refresh it
    (the stale canonical rows are dropped and rebuilt, so it is idempotent for
    a fixed map). The rewrite keeps each shard's build layout (sort + row-group
    size, from its footer stamp) unless `-s` / `-g` override it."""
    from pyrmts import canonicalize_shards
    pyramid = _load_pyramid(config, fs_root)
    ir = pyramid.identity_rollup
    if ir is not None:
        col, canonical_prefix = ir.col, ir.canonicalPrefix
    elif pyramid.geo is not None:
        col, canonical_prefix = pyramid.geo.cellCol, 'c:'
    else:
        raise SystemExit("canonicalize: need an `identityRollup` or `geo` block to resolve the vocab column")
    id_map = _load_id_map(pyramid, map_override)
    registry = _open_registry(manifest)
    from .shard_index import RegistryResolver
    result = canonicalize_shards(
        pyramid, id_map, _parse_range(range_),
        col=col,
        canonical_prefix=canonical_prefix,
        concurrency=concurrency,
        filter=_parse_filters(filters),
        sort=sort_csv.split(',') if sort_csv else None,
        row_group_size=rg_size,
        resolver=RegistryResolver(registry) if registry is not None else None,
        registry=registry,
        pyramid_name=pyramid_name,
    )
    for key, status in result.errors:
        err(f"  error {key}: {status}")
    print(result.summary())
    if result.errors:
        raise SystemExit(1)


def _synth_scans(keys: int, scans: int, churn: float, births: float, seed: int):
    """Synthetic single-scan (b, o) tiles for the multi-scan benchmark: `keys`
    keys keyed `(dt=0, path)`, evolved over `scans` observations. Each scan a
    `churn` fraction of live keys get a new value, and a `births` fraction are
    (re)born or die — so intervals split and gaps appear, exercising both
    encoders on a tunable churn regime."""
    import random

    import pyarrow as pa
    rng = random.Random(seed)
    live = {i: (rng.randint(1, 1_000_000), rng.randint(1, 100)) for i in range(keys)}
    out = []
    for j in range(scans):
        if j:
            for i in list(live):
                if rng.random() < churn:
                    live[i] = (live[i][0] + rng.randint(1, 1000), live[i][1] + 1)
            for i in range(keys):
                if rng.random() < births:
                    if i in live:
                        del live[i]
                    else:
                        live[i] = (rng.randint(1, 1_000_000), rng.randint(1, 100))
        paths = sorted(live)
        tbl = pa.table({
            'dt': [0] * len(paths),
            'path': [f'p{i}' for i in paths],
            'b': [live[i][0] for i in paths],
            'o': [live[i][1] for i in paths],
        })
        out.append((f's{j}', tbl))
    return out


@cli.command('multiscan-bench')
@option('-B', '--births', type=float, default=0.0, help="Per-scan birth/death fraction (default 0)")
@option('-c', '--churn', default='0,0.01,0.05,0.2', help="Comma-separated per-scan value-churn fractions to sweep (default 0,0.01,0.05,0.2)")
@option('-k', '--keys', type=int, default=100_000, help="Key universe size (default 100000)")
@option('-n', '--scans', type=int, default=20, help="Number of scans to consolidate (default 20)")
@option('-s', '--seed', type=int, default=0, help="RNG seed (default 0)")
def multiscan_bench(births: float, churn: str, keys: int, scans: int, seed: int) -> None:
    """Benchmark the two multi-scan encoders on synthetic churn
    (`specs/multi-scan-consolidation.md`): for each churn fraction, report the
    O(#scans) per-scan baseline vs. densify (a) vs. interval (b), in bytes and
    rows. Decides the (a)-vs-(b) crossover; the real operating point comes from
    running `consolidate_tables` on a project's actual scans."""
    import io

    import pyarrow.parquet as pq

    from pyrmts import Dim, MemStorage, Metric, Pyramid, Tier, consolidate_tables

    pyramid = Pyramid(
        storage=MemStorage(),
        keyTemplate='p/{tier}/{shard}/{period}.parquet',
        binCol='dt',
        dims=[Dim(name='path', type='string')],
        metrics=[Metric(name='b', monoid='count'), Metric(name='o', monoid='count')],
        tiers=[Tier(name='base', bin='1d', shards=('1mo',))],
    )

    def nbytes(t) -> int:
        buf = io.BytesIO()
        pq.write_table(t, buf, compression='snappy')
        return len(buf.getvalue())

    err(f"multiscan-bench: keys={keys} scans={scans} births={births} seed={seed}")
    print('churn    baseline_B  densify_B  interval_B  densify_rows  interval_rows  win  ratio')
    for c in [float(x) for x in churn.split(',')]:
        scan_tables = _synth_scans(keys, scans, c, births, seed)
        baseline = sum(nbytes(t) for _, t in scan_tables)
        dns = consolidate_tables(scan_tables, pyramid, encoder='densify')
        ivl = consolidate_tables(scan_tables, pyramid, encoder='interval')
        dns_b, ivl_b = nbytes(dns.table), nbytes(ivl.table)
        win = 'interval' if ivl_b <= dns_b else 'densify'
        ratio = baseline / min(dns_b, ivl_b)
        print(
            f'{c:<8.3g} {baseline:>10} {dns_b:>10} {ivl_b:>11} {dns.table.num_rows:>13} '
            f'{ivl.table.num_rows:>14}  {win:<8} {ratio:>5.1f}x'
        )


@cli.group()
def multiscan() -> None:
    """Multi-scan consolidation storage driver — Phase 2 of
    `specs/multi-scan-consolidation.md`. Fold a contiguous range of per-scan
    shards into interval (SCD-2) multi-scan shards and extract any scan back,
    digest-verified. Reference driver over the config's storage; each member
    scan is a subdir `<root>/<label>/` in the config's keyTemplate layout."""


@multiscan.command('consolidate')
@option('-D', '--dataset', help="Scan-family scope for the routing manifest (required with --index)")
@option('-e', '--engine', type=Choice(['python', 'duckdb']), default='python', help="Consolidation backend: in-memory reference (python) or out-of-core (duckdb, needs the [duckdb] extra)")
@option('-g', '--group-size', type=int, default=0, help="Seal every K consecutive scans into a separate immutable archive (capped-K), one manifest row each; 0 = one archive over all scans")
@option('-i', '--index', help="Manifest JSONL key (under --out) recording each consolidated tile for routing; needs --dataset")
@option('-o', '--out', required=True, help="Output root for the consolidated multi-scan shards")
@option('-r', '--range', 'range_', required=True, help="Half-open scan range <from-iso>/<to-iso> (UTC) selecting shard periods")
@option('-R', '--root', required=True, help="Scans root; each member scan is a subdir <root>/<label>/")
@option('-s', '--scan', 'scan_labels', multiple=True, required=True, help="Member scan subdir label, in observation order (repeatable)")
@option('-S', '--shard', required=True, help="Shard duration to consolidate (e.g. 1mo)")
@option('-t', '--tier', required=True, help="Tier name to consolidate")
@option('-x', '--drop', is_flag=True, help="After recording the manifest, digest-verify then delete the individual per-scan shards (needs --index)")
@argument('config')
def multiscan_consolidate(
    dataset: str | None,
    engine: str,
    group_size: int,
    index: str | None,
    out: str,
    range_: str,
    root: str,
    scan_labels: tuple[str, ...],
    shard: str,
    tier: str,
    drop: bool,
    config: str,
) -> None:
    """Consolidate the member scans' `tier`@`shard` tiles over the range into
    multi-scan shards at `--out`. The `python` engine folds in memory (peak
    O(#keys per tile)); `duckdb` reads the shards out-of-core (byte-identical
    output) for fleet scale. With `--group-size K`, seals every K scans into a
    separate immutable archive (capped-K). With `--index`/`--dataset`, records
    each tile in a routing manifest; add `--drop` to safely delete individuals."""
    from .multiscan_driver import consolidate_groups, consolidate_range
    from .multiscan_index import StorageJsonlMultiScanIndex

    if index and not dataset:
        raise SystemExit("multiscan consolidate: --index needs --dataset")
    if drop and not index:
        raise SystemExit("multiscan consolidate: --drop needs --index (reads must route to the archive first)")

    pyramid = _load_pyramid(config, None)
    scans = [(label, FsStorage(Path(root) / label)) for label in scan_labels]
    out_storage = FsStorage(out)
    ms_index = StorageJsonlMultiScanIndex(out_storage, index) if index else None
    common = dict(engine=engine, ms_index=ms_index, dataset=dataset, drop=drop)
    if group_size:
        written = consolidate_groups(
            scans, pyramid, tier, shard, _parse_range(range_), out_storage,
            group_size=group_size, **common,
        )
    else:
        written = consolidate_range(
            scans, pyramid, tier, shard, _parse_range(range_), out_storage, **common,
        )
    for key, rows, n in written:
        print(f"{key}\t{rows} rows\t{n} scans")
    grouping = f", groups of {group_size}" if group_size else ""
    err(
        f"multiscan consolidate: {len(written)} archives, {len(scan_labels)} scans "
        f"({engine}{grouping}{', indexed' if index else ''}{', dropped' if drop else ''})"
    )


@multiscan.command('extract')
@option('-N', '--no-verify', is_flag=True, help="Skip per-scan digest verification")
@option('-o', '--out', help="Write the extracted scan's parquet here (default: report row count only)")
@option('-p', '--period', required=True, help="Shard period label (e.g. 2026-01)")
@option('-R', '--root', required=True, help="Multi-scan shards root (a `consolidate --out`)")
@option('-s', '--scan', required=True, help="Member scan label to extract")
@option('-S', '--shard', required=True, help="Shard duration (e.g. 1mo)")
@option('-t', '--tier', required=True, help="Tier name")
@argument('config')
def multiscan_extract(
    no_verify: bool,
    out: str | None,
    period: str,
    root: str,
    scan: str,
    shard: str,
    tier: str,
    config: str,
) -> None:
    """Reconstruct member `scan`'s original shard from the multi-scan shard,
    digest-verified (unless `-N`), and optionally write it to `--out`."""
    import pyarrow.parquet as pq

    from .multiscan_driver import _tile_key, extract_scan

    pyramid = _load_pyramid(config, None)
    key = _tile_key(pyramid, tier, shard, period)
    table = extract_scan(FsStorage(root), key, scan, pyramid, verify=not no_verify)
    verified = '' if no_verify else ', digest verified'
    print(f"{scan}: {table.num_rows} rows{verified}")
    if out:
        pq.write_table(table, out)


@multiscan.command('seal')
@option('-e', '--engine', type=Choice(['python', 'duckdb']), default='python', help="Consolidation backend (python | duckdb)")
@option('-i', '--index', required=True, help="Routing-manifest JSONL key (under --out); read to skip already-sealed scans, appended for new groups")
@option('-o', '--out', required=True, help="Output root for sealed archives + the manifest")
@option('-r', '--range', 'range_', required=True, help="Half-open scan range <from-iso>/<to-iso> (UTC) selecting shard periods")
@option('-R', '--root', required=True, help="Scans root; each scan is a subdir <root>/<label>/")
@option('-s', '--scan', 'scan_labels', multiple=True, help="Ordered scan labels (default: <root>'s subdirs, sorted)")
@argument('config')
def multiscan_seal(
    engine: str,
    index: str,
    out: str,
    range_: str,
    root: str,
    scan_labels: tuple[str, ...],
    config: str,
) -> None:
    """Incremental seal driven by the config's `multiScan` policy. `scheme:
    fixed` seals each complete group of `groupSize` not-yet-sealed scans;
    `scheme: exponential` reconciles archives to the dyadic decomposition (old
    scans coalesce into `base`-power blocks, O(log N) archives). Idempotent — a
    consumer's cron fires it each cycle and it no-ops when nothing changed."""
    from .multiscan_driver import seal
    from .multiscan_index import StorageJsonlMultiScanIndex

    pyramid = _load_pyramid(config, None)
    p = pyramid.multi_scan
    if p is None:
        raise SystemExit("multiscan seal: config has no `multiScan` policy block")
    labels = list(scan_labels) or sorted(pp.name for pp in Path(root).iterdir() if pp.is_dir())
    scans = [(label, FsStorage(Path(root) / label)) for label in labels]
    out_storage = FsStorage(out)
    ms_index = StorageJsonlMultiScanIndex(out_storage, index)
    written = seal(scans, pyramid, _parse_range(range_), out_storage, ms_index, engine=engine)
    for key, rows, n in written:
        print(f"{key}\t{rows} rows\t{n} scans")
    detail = f"groups of {p.group_size}" if p.scheme == 'fixed' else f"base-{p.base} dyadic"
    err(f"multiscan seal: wrote {len(written)} archive(s), {p.scheme} ({detail}), dataset {p.dataset}, {engine}")


def _open_registry(manifest: str | None):
    """A shard registry from a JSONL manifest path or `s3://bucket/key`; None when not given."""
    if manifest is None:
        return None
    if manifest.startswith('s3://'):
        bucket, _, key = manifest[len('s3://'):].partition('/')
        return StorageJsonlShardIndex(S3Storage(bucket=bucket), key)
    return JsonlShardIndex(manifest)


@cli.command()
@option('-a', '--apply', is_flag=True, help="Delete (default: dry-run, list what would be deleted)")
@option('-G', '--grace', type=float, default=24.0, help="Hours an orphan must be older than to be deleted (default 24: in-flight reads + edge cache TTL)")
@option('-i', '--index', 'manifest', required=True, help="Shard registry: JSONL manifest path or s3://bucket/key")
@option('-R', '--fs-root', help="Use filesystem storage rooted here (instead of the config's storage block)")
@argument('config')
def gc(apply: bool, grace: float, manifest: str, fs_root: str | None, config: str) -> None:
    """Delete blobs under the pyramid's prefix that no registry row references
    (content-hashed keys leave the previous version behind on every rewrite)
    once older than the grace period. `specs/content-addressed-shards.md`."""
    from datetime import timedelta

    from .gc import gc_orphans

    pyramid = _load_pyramid(config, fs_root)
    registry = _open_registry(manifest)
    result = gc_orphans(pyramid, registry.existing_keys(), grace=timedelta(hours=grace), apply=apply)
    for key in result.deleted:
        print(key)
    err(result.summary())


@cli.command()
@option('-i', '--index', 'manifest', required=True, help="Shard registry: JSONL manifest path or s3://bucket/key")
@option('-n', '--pyramid-name', required=True, help="Pyramid name for shard registration")
@option('-R', '--fs-root', help="Use filesystem storage rooted here (instead of the config's storage block)")
@option('-r', '--range', 'range_', required=True, help="Half-open range of expected shards to check, <from-iso>/<to-iso> (UTC)")
@argument('config')
def adopt(manifest: str, pyramid_name: str, fs_root: str | None, range_: str, config: str) -> None:
    """Register present-but-unregistered shards (a write that died before
    registering): for each expected slot without a registry row, the newest
    listed blob whose bytes hash to its key. Content-hashed keyTemplates only."""
    from .gc import adopt_unregistered

    pyramid = _load_pyramid(config, fs_root)
    registry = _open_registry(manifest)
    adopted = adopt_unregistered(pyramid, registry, pyramid_name, _parse_range(range_))
    for rec in adopted:
        print(rec.key)
    err(f"adopt: registered {len(adopted)} shard(s)")


@cli.group()
def bench() -> None:
    """Bake-offs on real scans (`bench_diff.py`)."""


@bench.command('diff')
@option('-a', '--scan-a', required=True, help="Path-index parquet of scan A ((depth, path)-sorted)")
@option('-b', '--scan-b', required=True, help="Path-index parquet of scan B")
@option('-B', '--budget', type=int, default=10_000, help="Walk expansion budget (backstop; the render floor is the real bound)")
@option('-c', '--cell-px', type=float, default=4.0, help="Smallest drawable cell side (px) — sets the render floor with the canvas")
@option('-C', '--cols', default='path,depth,b,o', help="Column mapping path,depth,size,count (disk-tree: path,depth,size,n_desc)")
@option('-e', '--engine', type=Choice(['walk', 'materialize', 'both']), default='both', help="Which side(s) of the bake-off to run")
@option('-F', '--no-footer-cache', is_flag=True, help="Walk: parse the footer on every open (default: cache decoded metadata)")
@option('-G', '--no-rg-cache', is_flag=True, help="Walk: decode a row group on every expansion that touches it (default: cache per request)")
@option('-H', '--height', type=int, default=340, help="Canvas height (px)")
@option('-j', '--json', 'json_out', is_flag=True, help="Print the results as JSON on stdout (human table always on stderr)")
@option('-l', '--listing', type=Choice(['filter', 'bisect']), default='filter', help="Walk: search a decoded row group by a vectorized filter (wins at small RGs) or by bisection over its sorted keys (wins at 64K+-row RGs)")
@option('-n', '--repeat', type=int, default=2, help="Walk runs (first = cold footer, rest = warm)")
@option('-O', '--order', type=Choice(['level', 'bestfirst']), default='level', help="Walk order: level-synchronous (one dependent round per tree level) or best-first (largest |Δ| first, sequential)")
@option('-p', '--parallel', type=int, default=8, help="In-flight requests for the modelled wall time")
@option('-r', '--root', default='', help="View root path ('' = the scan root)")
@option('-t', '--rtt', default='0,30', help="Modelled per-request latencies (ms), comma-separated")
@option('-W', '--width', type=int, default=1400, help="Canvas width (px)")
def bench_diff(
    scan_a: str, scan_b: str, budget: int, cell_px: float, cols: str, engine: str,
    no_footer_cache: bool, no_rg_cache: bool, height: int, json_out: bool, listing: str, repeat: int,
    order: str, parallel: int, root: str, rtt: str, width: int,
) -> None:
    """Index-free diff walk vs. materialized pairwise diff, on two real scans:
    per-stage CPU, requests, bytes, and modelled wall time at each RTT."""
    import json

    from .bench_diff import (
        Cols, SnapshotReader, Stats, materialize_diff, render_floor, slice_view, walk_diff,
    )

    names = cols.split(',')
    if len(names) != 4:
        raise SystemExit("bench diff: --cols wants 4 names: path,depth,size,count")
    c = Cols(*names)
    rtts = [float(x) for x in rtt.split(',') if x]
    results: dict = {'root': root, 'canvas': [width, height], 'cell_px': cell_px}

    probe = SnapshotReader(scan_b, c, rg_cache=False)
    root_node = probe.node(root) if root else None
    if root and root_node is None:
        raise SystemExit(f"bench diff: root {root!r} not found in scan B")
    if root_node is None:
        # Scan root = the sum of depth-1 nodes (the tree may have several).
        root_size = sum(s for s, _ in probe.children('', 0).values())
    else:
        root_size = root_node[0]
    floor = render_floor(root_size, width, height, cell_px)
    results['root_size'] = root_size
    results['floor'] = floor
    err(f"root {root!r}: {root_size:,} bytes; render floor at {width}x{height} / {cell_px}px cells = {floor:,} bytes")

    if engine in ('walk', 'both'):
        footer_cache: dict | None = None if no_footer_cache else {}
        runs = []
        for k in range(repeat):
            stats = Stats()
            t0 = time.perf_counter()
            ra = SnapshotReader(scan_a, c, stats=stats, footer_cache=footer_cache, rg_cache=not no_rg_cache, listing=listing)
            rb = SnapshotReader(scan_b, c, stats=stats, footer_cache=footer_cache, rg_cache=not no_rg_cache, listing=listing)
            res = walk_diff(ra, rb, root, floor=floor, budget=budget, order=order)
            wall = (time.perf_counter() - t0) * 1000
            run = {
                'run': k, 'rows': len(res.rows), 'expansions': res.expansions, 'truncated': res.truncated,
                'wall_local_ms': round(wall, 1), **stats.as_dict(),
                'wall_model_ms': {str(r): round(stats.wall_model(r, parallel), 1) for r in rtts},
            }
            runs.append(run)
            err(
                f"walk run {k}: {len(res.rows)} rows, {res.expansions} expansions, {stats.listings} listings, "
                f"{stats.requests} RG reads in {stats.gets} GETs / {stats.bytes/1e6:.1f} MB, rg decodes {stats.rg_decodes} (cache hits {stats.rg_cache_hits}), "
                f"footer parses {stats.footer_parses}; {stats.round_trips} dependent rounds; cpu {stats.cpu_ms:.0f} ms "
                f"(footer {stats.ms['footer']:.0f}, locate {stats.ms['locate']:.0f}, read {stats.ms['read']:.0f}, post {stats.ms['post']:.0f}); "
                f"local wall {wall:.0f} ms; modelled @rtt " + ', '.join(f"{r:g}ms→{stats.wall_model(r, parallel):.0f}ms" for r in rtts)
                + (" [budget-cut]" if res.truncated else "")
            )
        results['walk'] = runs

    if engine in ('materialize', 'both'):
        diff, timings = materialize_diff(scan_a, scan_b, c)
        view, slice_ms = slice_view(diff, root, floor=floor)
        results['materialize'] = {
            'load_ms': round(timings['load'], 1), 'join_ms': round(timings['join'], 1), 'diff_rows': timings['rows'],
            'slice_ms': round(slice_ms, 1), 'view_rows': view.num_rows,
        }
        err(
            f"materialize: load {timings['load']:.0f} ms + join {timings['join']:.0f} ms → {timings['rows']:,} diff rows; "
            f"slice for this view {slice_ms:.0f} ms → {view.num_rows} rows"
        )
        if engine == 'both':
            # Same-floor comparison: the walk's drawable rows vs the materialized
            # view. Rows only the materialized side has are the walk's blind
            # spot (change under a dir whose size AND count are unchanged) or
            # change below a floor-pruned dir; rows only the walk has are
            # sub-floor children of expanded dirs (harmless, the client filters).
            walk_rows = {r.path for r in res.rows if max(r.size_a, r.size_b) >= floor or abs(r.delta) >= floor}
            view_rows = set(view['path'].to_pylist()) - {root or '.'}   # the walk emits children, not the root row
            hidden = sorted(view_rows - walk_rows)
            results['compare'] = {
                'walk_drawable_rows': len(walk_rows), 'view_rows': len(view_rows),
                'hidden_from_walk': len(hidden), 'hidden_examples': hidden[:5],
                'walk_only': len(walk_rows - view_rows),
            }
            err(
                f"compare: walk drawable rows {len(walk_rows)}, materialized view rows {len(view_rows)}, "
                f"hidden from walk {len(hidden)} (e.g. {hidden[:3]}), walk-only {len(walk_rows - view_rows)}"
            )
    if json_out:
        print(json.dumps(results, indent=2))


@cli.group()
def diffindex() -> None:
    """Flat-changeset diff-index (`specs/multi-scan-consolidation.md`, Phase 3):
    the changeset between ANY two scans from disjoint aligned dyadic nodes.
    `update` is the idempotent per-scan ingest stage (one adjacency changeset at
    L0 + ~1 aligned node amortized per new scan); `diff` composes only the
    nodes covering the span at query time — no snapshot read."""


def _scan_tables(root: str, tile_key: str, labels: list[str]):
    """Each scan's snapshot at `tile_key` from `<root>/<label>/`, in order."""
    import io

    import pyarrow.parquet as pq

    out = []
    for label in labels:
        data = FsStorage(Path(root) / label).get(tile_key)
        if data is None:
            raise SystemExit(f"diffindex: scan {label!r} has no shard at {tile_key!r}")
        out.append((label, pq.read_table(io.BytesIO(data))))
    return out


@diffindex.command('update')
@option('-D', '--dataset', required=True, help="Dataset scope (the index lives at <out>/diffidx/<dataset>/)")
@option('-k', '--key', 'tile_key', required=True, help="Tile key of the snapshot within each scan (e.g. p/base/1mo/2026-01.parquet)")
@option('-L', '--levels', type=int, default=None, help="Hierarchy cap for a NEW index: 0 = events log only (default); L adds aligned 2^1..2^L nodes (≤(L+1)× storage, ≤2·log2(span)+1 reads). On an existing index, must match its cap")
@option('-o', '--out', required=True, help="Index root")
@option('-R', '--root', required=True, help="Scans root; each scan is a subdir <root>/<label>/")
@option('-s', '--scan', 'scan_labels', multiple=True, help="Ordered scan labels (default: <root>'s subdirs, sorted)")
@argument('config')
def diffindex_update(dataset: str, tile_key: str, levels: int | None, out: str, root: str, scan_labels: tuple[str, ...], config: str) -> None:
    """Idempotent ingest: append every scan not yet in the index, in order. A
    cron fires this each cycle; it no-ops when nothing is new."""
    from .diffindex_store import DiffIndexStore

    pyramid = _load_pyramid(config, None)
    labels = list(scan_labels) or sorted(p.name for p in Path(root).iterdir() if p.is_dir())
    # `--levels` sizes a NEW index; an existing index keeps its own cap (an
    # explicit different value is rejected by the store).
    store = DiffIndexStore(FsStorage(out), f'diffidx/{dataset}', pyramid, dataset, levels=levels)
    appended = store.update(_scan_tables(root, tile_key, labels))
    for label in appended:
        print(label)
    err(f"diffindex update: appended {len(appended)} scan(s); index now {len(store.scans())} scans (dataset {dataset})")


@diffindex.command('diff')
@option('-a', '--from', 'scan_a', required=True, help="From-scan label")
@option('-b', '--to', 'scan_b', required=True, help="To-scan label")
@option('-D', '--dataset', required=True, help="Dataset scope")
@option('-o', '--out', required=True, help="Index root (a `diffindex update --out`)")
@option('-w', '--write', 'write_path', help="Write the changeset parquet here (default: report row count only)")
@argument('config')
def diffindex_diff(scan_a: str, scan_b: str, dataset: str, out: str, write_path: str | None, config: str) -> None:
    """The changeset between any two scans, composed from the aligned nodes
    covering the span — no snapshot read."""
    import pyarrow.parquet as pq

    from .diffindex_store import DiffIndexStore

    pyramid = _load_pyramid(config, None)
    store = DiffIndexStore(FsStorage(out), f'diffidx/{dataset}', pyramid, dataset)
    table = store.diff_table(scan_a, scan_b)
    print(f"{scan_a} -> {scan_b}: {table.num_rows} changed keys")
    if write_path:
        pq.write_table(table, write_path)


@cli.command()
@option('-b', '--mem-budget', help="Byte budget for window admission, e.g. 24g (default: 70% of the detected memory limit; 0 disables)")
@option('-C', '--close-workers', type=int, help="Concurrent close computations (default 2): more overlaps closes with the walk (wall) at the cost of stacked close transients (peak RSS)")
@option('-c', '--close-chunk', help="Target combined-long bytes per close chunk, e.g. 1g (default 1g); smaller bounds each close's transient tighter")
@option('-d', '--source-shard', help="Pin the WideShardSource to one rung shard Duration (default: min-cover — read the tier as stored, largest present tile wins at each instant)")
@option('-e', '--allow-empty', is_flag=True, help="Permit a 0-source-row (all-EMPTY) build; without it that exits nonzero (~always a mis-specified source rung)")
@option('-F', '--filter', 'filters', multiple=True, help="Extra keyTemplate substitution, key=value (repeatable)")
@option('-f', '--fill', is_flag=True, help="Gap-fill: LIST the target prefix, build exactly the expected-but-missing shards (walking only their windows); missing shards the source can't cover are reported + skipped. Tiled sources are checked before the walk: shards over an absent open-period tile are deferred (exit 0), shards over an absent closed-period tile fail fast past -M with nothing written (exit 4)")
@option('-g', '--rg-size', type=int, help="Output-shard parquet row-group size (all tiers; per-tier via the library)")
@option('-j', '--workers', type=int, help="Window-worker threads (default: cpu count)")
@option('-K', '--max-inflight', type=int, help="Max windows in flight past the watermark (memory bound; default 2×workers)")
@option('-M', '--max-missing', type=float, default=0.0, help="Tolerated fraction of absent source shards (default 0.0 = strict; absent ≠ present-but-EMPTY)")
@option('-m', '--manifest', help="Record written shards to this JSONL manifest")
@option('-n', '--pyramid-name', required=True, help="Pyramid name for shard registration")
@option('-o', '--strict-open-periods', is_flag=True, help="Count absent open-period sources toward -M/--max-missing (default: an absent source whose period extends past the range's `to` is expected-absent, excluded from the ratio)")
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
    fill: bool,
    rg_size: int | None,
    workers: int | None,
    max_inflight: int | None,
    max_missing: float,
    manifest: str | None,
    pyramid_name: str,
    strict_open_periods: bool,
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
            fill=fill,
            resume=resume,
            allow_empty=allow_empty,
            max_missing_source=max_missing,
            strict_open_periods=strict_open_periods,
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
@option('-M', '--max-vcpus', default=16, help="Compute-environment max vCPUs (default 16 = one full job at a time; raise to run concurrent builds)")
@option('-m', '--memory', default=32768, help="Job-definition memory MiB (default 32768 — fits the mem-tight profile's 24 GB peak; use 49152+ for the par-max profile)")
@option('-o', '--on-demand', is_flag=True, help="Also create an on-demand (non-Spot) CE + queue `<prefix>-od` (submit -O targets it)")
@option('-p', '--prefix', default=PREFIX, help=f"Name prefix for every resource created (default {PREFIX!r}); Batch job definitions, queues, CEs, IAM roles, and log groups are account-global, so a second consumer sharing an AWS account needs its own prefix")
@option('-v', '--vcpus', default=16, help="Job-definition vCPUs (default 16 — the size every converged profile was measured at; see `specs/done/engine-batch-packaging.md` §3)")
def bootstrap(arch: str, envs: tuple[str, ...], ephemeral: int, image: str, max_vcpus: int, memory: int, on_demand: bool, prefix: str, vcpus: int) -> None:
    """Idempotently create the role, log group, ECR repo, Fargate-Spot
    compute environment, queue, and job definition."""
    from .batch import bootstrap as _bootstrap
    _bootstrap(
        image=image,
        arch=arch,
        prefix=prefix,
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
@option('-b', '--mem-budget', help="Byte budget for window admission, e.g. 24g (default: 70% of the job's memory — `-M`, else the job definition's; 0 disables)")
@option('-C', '--close-workers', type=int, help="build -C: concurrent close computations")
@option('-c', '--close-chunk', help="build -c: target combined-long bytes per close chunk, e.g. 1g")
@option('-d', '--source-shard', help="Pin the WideShardSource to one rung shard Duration (default: min-cover across the tier's rungs)")
@option('-e', '--env', 'envs', multiple=True, help="Extra container env var, NAME=VALUE (repeatable)")
@option('-E', '--allow-empty', is_flag=True, help="Permit a 0-source-row (all-EMPTY) build")
@option('-F', '--filter', 'filters', multiple=True, help="Extra keyTemplate substitution, key=value (repeatable)")
@option('-f', '--fill', is_flag=True, help="build -f: gap-fill exactly the expected-but-missing shards")
@option('-g', '--rg-size', type=int, help="Output-shard parquet row-group size")
@option('-j', '--job-name', help="Batch job name (default: derived from pyramid name)")
@option('-K', '--max-inflight', type=int, help="build -K: max windows in flight past the watermark")
@option('-m', '--manifest', help="Manifest destination (use s3:// — container disk is ephemeral)")
@option('-M', '--memory', type=int, help="Override job memory MiB")
@option('--max-missing', type=float, help="Tolerated fraction of absent source shards (build -M)")
@option('--strict-open-periods', is_flag=True, help="build -o: count absent open-period sources toward --max-missing")
@option('-n', '--pyramid-name', required=True, help="Pyramid name for shard registration")
@option('-O', '--on-demand', is_flag=True, help="Submit to the on-demand queue (needs `bootstrap -o`); no Spot reclaims")
@option('-p', '--prefix', default=PREFIX, help=f"Name prefix of the bootstrapped resources to submit to (default {PREFIX!r}); must match `bootstrap -p`")
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
    fill: bool,
    rg_size: int | None,
    job_name: str | None,
    max_inflight: int | None,
    manifest: str | None,
    memory: int | None,
    max_missing: float | None,
    strict_open_periods: bool,
    pyramid_name: str,
    on_demand: bool,
    prefix: str,
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
    from .batch import build_command, submit as _submit
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
            fill=fill,
            resume=resume,
            allow_empty=allow_empty,
            max_missing=max_missing,
            strict_open_periods=strict_open_periods,
            filters=filters,
            workers=workers,
            max_inflight=max_inflight,
            mem_budget=mem_budget,
            close_workers=close_workers,
            close_chunk=close_chunk,
        ),
        job_name=job_name or f'{pyramid_name}-build',
        prefix=prefix,
        on_demand=on_demand,
        vcpus=vcpus,
        memory_mib=memory,
        environment=_parse_filters(envs),
        watch=watch,
    )
    raise SystemExit(code)


if __name__ == '__main__':
    cli()

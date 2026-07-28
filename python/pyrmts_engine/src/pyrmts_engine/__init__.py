"""pyrmts-engine — fused long-form pyramid build engine.

See `specs/pyramid-build-engine.md` (repo root)."""
from __future__ import annotations

from .consolidate import (
    cross_tier_rebin,
    decode_gap,
    encode_gap,
    materialize_extension_shard,
    overlap_cover,
    reconcile_registrations,
    run_extension_fill,
    run_single_gap,
    tile_from_existing,
)
from .discovery import (
    diff_with_existing,
    discover_gaps,
    group_by_tier_rung,
    list_existing_keys,
    list_existing_with_mtime,
    report_gaps,
    sort_by_dependency,
    split_stale,
)
from .engine import (
    BuildResult,
    EmptySourceError,
    SourceCoverageError,
    WrittenShard,
    build_local,
)
from .materialize import (
    MaterializeResult,
    SourcePick,
    emit_d1_insert_sql,
    materialize_shard,
    plan_source_cover_single_tier,
    shard_key,
    source_long_for_gap,
    source_tier_for,
)
from .longform import (
    COUNT_COL,
    METRIC_COL,
    STATE_COL,
    combine_long,
    empty_long,
    long_schema,
    long_to_wide,
    rebin_long,
    wide_to_long,
)
from .plan import BuildPlan, bin_floor_expr, compile_plan
from .shard_index import (
    D1ShardIndex,
    JsonlShardIndex,
    MemShardIndex,
    NoopShardIndex,
    ShardIndex,
    ShardRecord,
    StorageJsonlShardIndex,
)
from .source import Source, WideShardSource
from .validate import (
    aligned_range,
    canonical_long,
    compare_manifest,
    compare_streaming,
    covering_shard,
)

__version__ = "0.0.0"

__all__ = [
    'BuildPlan', 'BuildResult', 'EmptySourceError', 'SourceCoverageError', 'WrittenShard',
    'build_local', 'compile_plan', 'bin_floor_expr',
    'COUNT_COL', 'METRIC_COL', 'STATE_COL',
    'combine_long', 'empty_long', 'long_schema', 'long_to_wide',
    'rebin_long', 'wide_to_long',
    'D1ShardIndex', 'JsonlShardIndex', 'MemShardIndex', 'NoopShardIndex',
    'ShardIndex', 'ShardRecord', 'StorageJsonlShardIndex',
    'Source', 'WideShardSource',
    'diff_with_existing', 'discover_gaps', 'group_by_tier_rung',
    'list_existing_keys', 'list_existing_with_mtime', 'report_gaps',
    'sort_by_dependency', 'split_stale',
    'MaterializeResult', 'SourcePick', 'emit_d1_insert_sql',
    'materialize_shard', 'plan_source_cover_single_tier', 'shard_key',
    'source_long_for_gap', 'source_tier_for',
    'cross_tier_rebin', 'decode_gap', 'encode_gap',
    'materialize_extension_shard', 'overlap_cover',
    'reconcile_registrations', 'run_extension_fill', 'run_single_gap',
    'tile_from_existing',
    'aligned_range', 'canonical_long', 'compare_manifest',
    'compare_streaming', 'covering_shard',
]

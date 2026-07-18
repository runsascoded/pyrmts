"""pyrmts-engine — fused long-form pyramid build engine.

See `specs/pyramid-build-engine.md` (repo root)."""
from __future__ import annotations

from .engine import BuildResult, WrittenShard, build_local
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
)
from .source import Source, WideShardSource

__version__ = "0.0.0"

__all__ = [
    'BuildPlan', 'BuildResult', 'WrittenShard',
    'build_local', 'compile_plan', 'bin_floor_expr',
    'COUNT_COL', 'METRIC_COL', 'STATE_COL',
    'combine_long', 'empty_long', 'long_schema', 'long_to_wide',
    'rebin_long', 'wide_to_long',
    'D1ShardIndex', 'JsonlShardIndex', 'MemShardIndex', 'NoopShardIndex',
    'ShardIndex', 'ShardRecord',
    'Source', 'WideShardSource',
]

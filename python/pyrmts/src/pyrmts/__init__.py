"""pyrmts — multi-scale timeseries pyramids."""
from __future__ import annotations

from .axis import (
    ParsedTimeSpan,
    ShardPeriod,
    add_span,
    bins_in_range,
    ceil_to_span,
    floor_to_span,
    format_period,
    nominal_delta_ms,
    parse_duration,
    shard_periods_covering,
)
from .canonicalize import CanonicalizeResult, canonicalize_shards, recanonicalize_table
from .cascade import CascadeResult, cascade_tiers
from .diffindex import (
    SparseDiffIndex,
    changeset_between,
    changeset_from_table,
    changeset_to_table,
    compose_changesets,
    aligned_blocks,
)
from .gap_discovery import ExpectedShard, list_expected_shards
from .keys import substitute_key
from .monoids import Monoid, Row, get_monoid, state_columns
from .multiscan import (
    MultiScan,
    consolidate_scans,
    consolidate_tables,
    diff_scans,
    diff_tables,
    extract_table,
    from_arrow,
    scan_digest,
    series_for,
    to_arrow,
)
from .storage import EtagConflict, FsStorage, MemStorage, S3Storage, storage_from_cfg
from .types import (
    Axis,
    Dim,
    DimType,
    GeoSpec,
    IdentityRollup,
    Metric,
    MonoidName,
    MultiScanPolicy,
    Pyramid,
    Storage,
    Tier,
)
from .writer import write_tier_parquet
from .yaml import (
    PyramidConfig,
    merge_lambda_shards,
    parse_pyramid_yaml,
    pyramid_from_config,
)

__version__ = "0.0.0"

__all__ = [
    'Axis', 'Dim', 'DimType', 'GeoSpec', 'IdentityRollup', 'Metric', 'MonoidName',
    'MultiScanPolicy', 'Pyramid', 'Storage', 'Tier',
    'ParsedTimeSpan', 'ShardPeriod',
    'add_span', 'bins_in_range', 'ceil_to_span', 'floor_to_span', 'format_period',
    'nominal_delta_ms', 'parse_duration', 'shard_periods_covering',
    'CanonicalizeResult', 'canonicalize_shards', 'recanonicalize_table',
    'CascadeResult', 'cascade_tiers',
    'SparseDiffIndex', 'changeset_between', 'changeset_from_table', 'changeset_to_table',
    'compose_changesets', 'aligned_blocks',
    'ExpectedShard', 'list_expected_shards',
    'substitute_key',
    'Monoid', 'Row', 'get_monoid', 'state_columns',
    'MultiScan', 'consolidate_scans', 'consolidate_tables', 'diff_scans',
    'diff_tables', 'extract_table', 'from_arrow', 'scan_digest', 'series_for',
    'to_arrow',
    'EtagConflict', 'FsStorage', 'MemStorage', 'S3Storage', 'storage_from_cfg',
    'write_tier_parquet',
    'PyramidConfig', 'merge_lambda_shards', 'parse_pyramid_yaml',
    'pyramid_from_config',
]

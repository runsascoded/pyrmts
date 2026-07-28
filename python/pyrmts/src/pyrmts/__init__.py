"""pyrmts — multi-scale timeseries pyramids."""
from __future__ import annotations

from .axis import (
    ParsedTimeSpan,
    ShardPeriod,
    add_span,
    bins_in_range,
    floor_to_span,
    format_period,
    parse_duration,
    shard_periods_covering,
)
from .cascade import CascadeResult, cascade_tiers
from .gap_discovery import ExpectedShard, list_expected_shards
from .keys import substitute_key
from .monoids import Monoid, Row, get_monoid, state_columns
from .storage import FsStorage, MemStorage, S3Storage, storage_from_cfg
from .types import (
    Axis,
    Dim,
    DimType,
    GeoSpec,
    Metric,
    MonoidName,
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
    'Axis', 'Dim', 'DimType', 'GeoSpec', 'Metric', 'MonoidName', 'Pyramid',
    'Storage', 'Tier',
    'ParsedTimeSpan', 'ShardPeriod',
    'add_span', 'bins_in_range', 'floor_to_span', 'format_period',
    'parse_duration', 'shard_periods_covering',
    'CascadeResult', 'cascade_tiers',
    'ExpectedShard', 'list_expected_shards',
    'substitute_key',
    'Monoid', 'Row', 'get_monoid', 'state_columns',
    'FsStorage', 'MemStorage', 'S3Storage', 'storage_from_cfg',
    'write_tier_parquet',
    'PyramidConfig', 'merge_lambda_shards', 'parse_pyramid_yaml',
    'pyramid_from_config',
]

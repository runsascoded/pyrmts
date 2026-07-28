"""Generic handler dispatch (`specs/pyrmts-ops-adoption.md` phase 3):
single-gap vs discovery branches, registration/scaffold semantics, GC
cadence, response contracts (the fan-out driver parses these)."""
from __future__ import annotations

import pytest

from pyrmts import list_expected_shards
from pyrmts_engine import MemShardIndex, encode_gap
from pyrmts_ops import LambdaApp, lambda_entry

from fixture_pyramid import FROM, H4D_KEY, TO, build_base_ladder


class MemRegistry:
    def __init__(self) -> None:
        self.deleted: list[str] = []

    def rows(self, pyramid_name: str) -> list[dict]:
        return []

    def delete_keys(self, keys: list[str]) -> None:
        self.deleted.extend(keys)


def _app(pyramid, index, registry=None) -> LambdaApp:
    return LambdaApp(
        pyramid=pyramid, pyramid_name='test', genesis=FROM,
        shard_index=index, gc_registry=registry,
    )


def test_single_gap_branch():
    pyramid = build_base_ladder()
    index = MemShardIndex()
    gap = next(e for e in list_expected_shards(pyramid, (FROM, TO)) if e.key == H4D_KEY)

    resp = lambda_entry(
        {'gap': encode_gap(gap), 'config': 'test'},
        load=lambda name, event: _app(pyramid, index),
        default_config='test',
    )
    blob = pyramid.storage.get(H4D_KEY)
    assert blob is not None
    assert resp == {
        'status': 'wrote',
        'key': H4D_KEY,
        'rows': 192,
        'bytes': len(blob),
        'source': 'same-tier cover ×4',
        'error': None,
    }
    assert [r.key for r in index.records] == [H4D_KEY]


def test_single_gap_scaffold_is_unregistered():
    pyramid = build_base_ladder()
    index = MemShardIndex()
    gap = next(e for e in list_expected_shards(pyramid, (FROM, TO)) if e.key == H4D_KEY)
    resp = lambda_entry(
        {'gap': encode_gap(gap), 'config': 'test', 'register': False},
        load=lambda name, event: _app(pyramid, index),
        default_config='test',
    )
    assert resp['status'] == 'wrote'
    assert index.records == []


def test_tick_branch_fills_reconciles_and_gcs():
    pyramid = build_base_ladder()
    index = MemShardIndex()
    registry = MemRegistry()

    resp = lambda_entry(
        {},
        load=lambda name, event: _app(pyramid, index, registry),
        default_config='test',
        gc_enabled=True,
        now=TO,  # minute 0 → the hourly GC window
    )
    assert resp == {
        'filled': {'wrote': 2},
        'total': 2,
        'gc': {'eligible': 0, 'deleted': 0, 'skipped': {}},
    }
    # Reconcile registered the 9 present cover shards; the fill added 2.
    assert sorted(r.key for r in index.records) == sorted(
        e.key for e in list_expected_shards(pyramid, (FROM, TO))
    )


def test_bad_config_name_rejected():
    with pytest.raises(ValueError) as exc:
        lambda_entry(
            {'config': '../etc'},
            load=lambda name, event: None,
            default_config='test',
        )
    assert str(exc.value) == "bad config name '../etc'"

"""Fan-out rebuild driver (`specs/pyrmts-ops-adoption.md` phase 3):
fill-safe rungs, scaffold expansion, cost model, and the full driver
loop with an injected invoke transport."""
from __future__ import annotations

import json

from pyrmts import MemStorage
from pyrmts_engine import MemShardIndex, decode_gap, encode_gap, run_single_gap
from pyrmts_ops import FanoutConfig, expand_scaffolds, fill_safe_rung, run_rebuild
from pyrmts_ops.rebuild import _estimate_layer

from fixture_pyramid import FROM, TO, build_base_ladder, make_pyramid

CFG = FanoutConfig(function_name='fn', source_bin_budget=100)


def test_fill_safe_rung():
    pyramid = make_pyramid()
    q, h, _ = pyramid.tiers
    # q's fills consume its own base bin (15min): 6h = 24 bins, 1d = 96.
    assert fill_safe_rung(pyramid, q, source_bin_budget=24) == '6h'
    assert fill_safe_rung(pyramid, q, source_bin_budget=96) == '1d'
    # h's fills consume q bins (15min): 1d = 96 ≤ 100 < 4d = 384.
    assert fill_safe_rung(pyramid, h, source_bin_budget=100) == '1d'


def test_estimate_layer_classes():
    pyramid = make_pyramid()
    # Non-scaffold coarse rung → same-tier concat.
    assert _estimate_layer(pyramid, CFG, 'h', '4d', False) == (25.0, 'concat')
    # Scaffold at h → cross-tier fill from q: 96 bins × 0.45.
    assert _estimate_layer(pyramid, CFG, 'h', '1d', True) == (96 * 0.45, 'xtier-fill')
    # Finest tier whole-period fill → raw: 24 base bins × 0.36.
    assert _estimate_layer(pyramid, CFG, 'q', '6h', True) == (24 * 0.36, 'raw-fill')


def test_expand_scaffolds_budget_and_genesis_clip():
    pyramid = build_base_ladder()
    from pyrmts import list_expected_shards
    gaps = [e for e in list_expected_shards(pyramid, (FROM, TO)) if e.shard_dur == '4d' and e.tier == 'h']
    layers = expand_scaffolds(
        pyramid, [('h', '4d', gaps)], genesis=FROM, source_bin_budget=100,
    )
    assert [(t, r, [g.key for g in b], s) for t, r, b, s in layers] == [
        # Pre-genesis slots of the Dec-30 tile are dropped; Jan-2 survives.
        ('h', '1d', [
            'pyr/h/1d/2026-01-02.parquet',
            'pyr/h/1d/2026-01-03.parquet',
            'pyr/h/1d/2026-01-04.parquet',
            'pyr/h/1d/2026-01-05.parquet',
            'pyr/h/1d/2026-01-06.parquet',
        ], True),
        ('h', '4d', [
            'pyr/h/4d/2025-12-30.parquet',
            'pyr/h/4d/2026-01-03.parquet',
        ], False),
    ]


def test_run_rebuild_driver():
    """Full driver loop with a local transport standing in for the
    Lambda: scaffolds invoked unregistered ('exists' — their tiles are
    already present), 4d layers written + registered, progress doc
    complete, scaffold keys cleaned after the clean run."""
    pyramid = build_base_ladder()
    index = MemShardIndex()
    prog = MemStorage()
    payloads: list[dict] = []

    def fake_invoke(payload: dict) -> dict:
        payloads.append(payload)
        gap = decode_gap(payload['gap'], FROM)
        res = run_single_gap(
            pyramid, gap, genesis=FROM, pyramid_name='test',
            shard_index=index if payload['register'] else None,
        )
        return {'status': res.status, 'key': res.gap.key, 'rows': res.rows,
                'bytes': res.bytes_written, 'source': res.source_desc, 'error': res.error}

    by_status = run_rebuild(
        pyramid,
        genesis=FROM, pyramid_name='test', cfg=CFG,
        concurrency=4, progress_storage=prog, invoke=fake_invoke, now=TO,
    )
    assert by_status == {'exists': 5, 'wrote': 2}
    assert sorted(r.key for r in index.records) == [
        'pyr/h/4d/2025-12-30.parquet',
        'pyr/h/4d/2026-01-03.parquet',
    ]
    # Scaffold payloads carry register=False; real layers register.
    by_key = {p['gap']['key']: p for p in payloads}
    assert len(payloads) == 7
    assert by_key['pyr/h/1d/2026-01-03.parquet'] == {
        'gap': encode_gap(next(
            decode_gap(p['gap'], FROM) for p in payloads
            if p['gap']['key'] == 'pyr/h/1d/2026-01-03.parquet'
        )),
        'register': False,
        'config': 'test',
    }
    assert by_key['pyr/h/4d/2026-01-03.parquet']['register'] is True
    # Clean run → scaffold keys (not part of the expected cover) deleted.
    for day in (2, 3, 4, 5, 6):
        assert pyramid.storage.get(f'pyr/h/1d/2026-01-0{day}.parquet') is None
    assert pyramid.storage.get('pyr/h/1d/2026-01-07.parquet') is not None  # in cover

    doc = json.loads(prog.get('build-progress/test.json'))
    for layer in doc['layers']:
        layer['wallS'] = 0.0
    doc.pop('startedAt')
    doc.pop('updatedAt')
    assert doc == {
        'pyramid': 'test',
        'driver': 'lambda-fanout',
        'status': 'done',
        'plan': {'layers': 2, 'invocations': 7, 'scaffolds': 5},
        'byStatus': {'exists': 5, 'wrote': 2},
        'layers': [
            {'tier': 'h', 'rung': '1d', 'scaffold': True, 'n': 5, 'wallS': 0.0,
             'status': {'exists': 5}},
            {'tier': 'h', 'rung': '4d', 'scaffold': False, 'n': 2, 'wallS': 0.0,
             'status': {'wrote': 2}},
        ],
        'currentLayer': None,
    }


def test_run_rebuild_dry_run_plans_only():
    pyramid = build_base_ladder()
    payloads: list[dict] = []
    result = run_rebuild(
        pyramid,
        genesis=FROM, pyramid_name='test', cfg=CFG,
        dry_run=True, invoke=lambda p: payloads.append(p) or {}, now=TO,
    )
    assert (result, payloads) == ({}, [])

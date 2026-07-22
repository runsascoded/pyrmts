"""Local-executor conformance:

- output key set == `list_expected_shards` minus the source-provided rung
- content equality vs the `cascade_tiers` row-at-a-time reference
- an independent hand-derived content anchor (no shared code path)
- window-size invariance, byte-for-byte (RGIP)
- registration records
- EMPTY-shard semantics for zero-row periods
"""
from __future__ import annotations

import io
import json
from datetime import datetime, timezone

import pyarrow.parquet as pq
import pytest

from pyrmts import MemStorage, cascade_tiers, list_expected_shards
from pyrmts_engine import (
    EmptySourceError,
    MemShardIndex,
    SourceCoverageError,
    WideShardSource,
    build_local,
)

from conftest import (
    CELLS,
    FROM,
    Q_MS,
    TO,
    bikes_hist,
    make_pyramid,
    rides_count,
    write_base_shards,
)

DAY_MS = 86_400_000


def _parse_shard(blob: bytes) -> list[dict]:
    """Parquet bytes → normalized, (cell, dt)-sorted row dicts. Hist JSON
    strings parse to `{int: int}` so key-order conventions don't leak into
    comparisons."""
    rows = pq.read_table(io.BytesIO(blob)).to_pylist()
    for r in rows:
        if r.get('bikes') is not None:
            r['bikes'] = {int(k): v for k, v in json.loads(r['bikes']).items()}
    return sorted(rows, key=lambda r: (r['cell'], r['dt']))


def _run_engine(window: str = '1d', workers: int | None = None) -> tuple:
    pyramid = make_pyramid()
    write_base_shards(pyramid)
    index = MemShardIndex()
    result = build_local(
        pyramid,
        (FROM, TO),
        WideShardSource(pyramid),
        pyramid_name='test',
        shard_index=index,
        window=window,
        workers=workers,
    )
    return pyramid, result, index


EXPECTED_KEYS = [
    'pyr/d/4d/2025-12-30.parquet',
    'pyr/d/4d/2026-01-03.parquet',
    'pyr/h/1d/2026-01-07.parquet',
    'pyr/h/4d/2025-12-30.parquet',
    'pyr/h/4d/2026-01-03.parquet',
    'pyr/q/1d/2026-01-02.parquet',
    'pyr/q/1d/2026-01-03.parquet',
    'pyr/q/1d/2026-01-04.parquet',
    'pyr/q/1d/2026-01-05.parquet',
    'pyr/q/1d/2026-01-06.parquet',
    'pyr/q/1d/2026-01-07.parquet',
]


def test_engine_writes_exactly_the_expected_cover():
    pyramid, result, _ = _run_engine()
    assert sorted(w.key for w in result.written) == EXPECTED_KEYS
    # And the spec'd contract: outputs ≡ list_expected_shards minus the
    # source-provided (q, 6h) rung.
    expected = [
        e.key for e in list_expected_shards(pyramid, (FROM, TO))
        if (e.tier, e.shard_dur) != ('q', '6h')
    ]
    assert sorted(w.key for w in result.written) == sorted(expected)


def test_engine_matches_cascade_tiers_reference():
    engine_pyramid, result, _ = _run_engine()

    ref_pyramid = make_pyramid()
    write_base_shards(ref_pyramid)
    ref_result = cascade_tiers(ref_pyramid, (FROM, TO))
    assert ref_result.errors == []

    for key in (w.key for w in result.written):
        engine_blob = engine_pyramid.storage.get(key)
        ref_blob = ref_pyramid.storage.get(key)
        assert ref_blob is not None, f"reference did not write {key}"
        assert _parse_shard(engine_blob) == _parse_shard(ref_blob), key


def test_day_tier_content_anchor():
    """Independent re-derivation (plain python loop over the synthetic
    formula) of `d/4d/2025-12-30` — whose only in-range day is 2026-01-02."""
    pyramid, _, _ = _run_engine()
    rows = _parse_shard(pyramid.storage.get('pyr/d/4d/2025-12-30.parquet'))

    day_ms = int(datetime(2026, 1, 2, tzinfo=timezone.utc).timestamp() * 1000)
    bin_indices = [(day_ms + k * Q_MS) // Q_MS for k in range(DAY_MS // Q_MS)]
    expected = []
    for cell_idx, cell in enumerate(CELLS):
        hist: dict[int, int] = {}
        for i in bin_indices:
            for state, n in bikes_hist(i).items():
                hist[state] = hist.get(state, 0) + n
        expected.append({
            'cell': cell,
            'dt': day_ms,
            'bikes': hist,
            'rides': sum(rides_count(i, cell_idx) for i in bin_indices),
            'temp_n': 2 * len(bin_indices),
            'temp_sum': float(sum(bin_indices)),
            'temp_sumsq': float(sum(i * i for i in bin_indices)),
        })
    assert rows == sorted(expected, key=lambda r: (r['cell'], r['dt']))


def test_window_size_invariance_byte_identical():
    p1, r1, _ = _run_engine(window='6h')
    p2, r2, _ = _run_engine(window='2d')
    assert sorted(w.key for w in r1.written) == sorted(w.key for w in r2.written)
    for key in (w.key for w in r1.written):
        assert p1.storage.get(key) == p2.storage.get(key), key


def test_parallel_workers_byte_identical():
    """The acceptance gate of `specs/parallel-window-executor.md`: an
    N-worker build is byte-identical to the sequential (workers=1)
    reference — out-of-order window completion and spill-append order
    can't reach output bytes. 6h windows → 24 windows to interleave."""
    p1, r1, _ = _run_engine(window='6h', workers=1)
    p4, r4, _ = _run_engine(window='6h', workers=4)
    assert sorted(w.key for w in r1.written) == sorted(w.key for w in r4.written)
    for key in (w.key for w in r1.written):
        assert p1.storage.get(key) == p4.storage.get(key), key


def test_registration_records():
    t0_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    pyramid, result, index = _run_engine()
    t1_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

    got = sorted(
        (r.pyramid, r.tier, r.shard_dur, r.period_start_ms, r.period_end_ms, r.key)
        for r in index.records
    )
    expected = sorted(
        ('test', e.tier, e.shard_dur,
         int(e.period_start.timestamp() * 1000), int(e.period_end.timestamp() * 1000),
         e.key)
        for e in list_expected_shards(pyramid, (FROM, TO))
        if (e.tier, e.shard_dur) != ('q', '6h')
    )
    assert got == expected
    assert all(t0_ms <= r.written_at_ms <= t1_ms for r in index.records)


def test_parallel_close_registration_order_deterministic():
    """Close compute runs on a pool, but registration order must equal
    the sequential run's (the ordered-`seq` barrier): identical record
    sequences, not just identical sets."""
    _, _, idx1 = _run_engine(window='6h', workers=1)
    _, _, idx4 = _run_engine(window='6h', workers=4)
    strip = lambda r: (r.pyramid, r.tier, r.shard_dur, r.period_start_ms, r.period_end_ms, r.key)
    assert [strip(r) for r in idx4.records] == [strip(r) for r in idx1.records]


def test_resume_from_manifest():
    """Spot-reclaim recovery: shards recorded in the manifest are skipped,
    windows that only feed them are never processed, and the resumed run's
    outputs are byte-identical to an uninterrupted build."""
    full_pyramid, full_result, full_index = _run_engine()

    # Simulate a run reclaimed after the shards closing by Jan 5 landed.
    cutoff_ms = int(datetime(2026, 1, 5, tzinfo=timezone.utc).timestamp() * 1000)
    done = [r for r in full_index.records if r.period_end_ms <= cutoff_ms]
    assert sorted(r.key for r in done) == [
        'pyr/d/4d/2025-12-30.parquet',
        'pyr/h/4d/2025-12-30.parquet',
        'pyr/q/1d/2026-01-02.parquet',
        'pyr/q/1d/2026-01-03.parquet',
        'pyr/q/1d/2026-01-04.parquet',
    ]
    done_keys = {r.key for r in done}

    pyramid = make_pyramid()
    write_base_shards(pyramid)
    index = MemShardIndex(records=list(done))
    result = build_local(
        pyramid, (FROM, TO), WideShardSource(pyramid),
        pyramid_name='test', shard_index=index, resume=True,
    )
    assert result.resumed_shards == 5
    # The earliest unfinished shard (h/4d/2026-01-03) starts Jan 3, so the
    # Jan-02 window is skipped outright: 5 of 6 windows processed.
    assert result.windows == 5
    assert sorted(w.key for w in result.written) == [
        k for k in EXPECTED_KEYS if k not in done_keys
    ]
    for w in result.written:
        assert pyramid.storage.get(w.key) == full_pyramid.storage.get(w.key), w.key
    # Manifest ends complete: prior records + the resumed run's.
    assert sorted(r.key for r in index.records) == EXPECTED_KEYS


def test_zero_source_rows_raises_unless_allowed():
    """A source that yields nothing over the whole range (the mis-specified
    source-rung shape) fails loudly — after writing/registering its
    zero-row outputs — unless `allow_empty` opts in."""
    pyramid = make_pyramid()  # no base shards seeded
    index = MemShardIndex()
    with pytest.raises(EmptySourceError) as exc:
        build_local(
            pyramid, (FROM, TO), WideShardSource(pyramid),
            pyramid_name='test', shard_index=index, max_missing_source=1.0,
        )
    assert str(exc.value) == (
        'build_local: 0 source rows across 6 windows — almost always a '
        'mis-specified source rung (the 11 zero-row shards WERE '
        'written/registered); pass allow_empty=True / --allow-empty if intentional'
    )
    assert sorted(r.key for r in index.records) == EXPECTED_KEYS

    pyramid2 = make_pyramid()
    result = build_local(
        pyramid2, (FROM, TO), WideShardSource(pyramid2),
        pyramid_name='test', allow_empty=True, max_missing_source=1.0,
    )
    assert sorted(w.key for w in result.written) == EXPECTED_KEYS


def test_source_coverage_strict_by_default():
    """Partial source holes — the quietly-wrong-numbers shape (GC'd keys,
    filter typo) — fail loudly under the default strict policy, listing
    the absent keys."""
    pyramid = make_pyramid()
    write_base_shards(pyramid)
    missing = sorted(pyramid.storage.list('pyr/q/6h/2026-01-04'))
    assert len(missing) == 4
    for k in missing:
        pyramid.storage._data.pop(k)

    with pytest.raises(SourceCoverageError) as exc:
        build_local(
            pyramid, (FROM, TO), WideShardSource(pyramid), pyramid_name='test',
        )
    assert str(exc.value) == (
        f"build_local: 4/24 source shards absent (> max_missing_source=0.0): "
        f"{', '.join(missing)} — a real hole (GC'd rung, filter typo, wrong "
        f"rung), not an outage (outage shards are present-but-EMPTY); raise "
        f"max_missing_source / --max-missing if such holes are expected here "
        f"(outputs WERE written/registered)"
    )


def test_zero_row_period_writes_empty_shard():
    """A day with no source shards still yields its (q, 1d) cover tile —
    EMPTY (zero rows), written and registered. (`max_missing_source=1.0`
    opts into absent-as-outage; the strict default raises — see
    `test_source_coverage_strict_by_default`.)"""
    pyramid = make_pyramid()
    write_base_shards(pyramid)
    # Remove all four 6h base shards of 2026-01-04.
    missing = [k for k in pyramid.storage.list('pyr/q/6h/2026-01-04')]
    assert len(missing) == 4
    for k in missing:
        pyramid.storage._data.pop(k)

    index = MemShardIndex()
    result = build_local(
        pyramid, (FROM, TO), WideShardSource(pyramid),
        pyramid_name='test', shard_index=index, max_missing_source=1.0,
    )
    assert sorted(w.key for w in result.written) == EXPECTED_KEYS

    empty_day = pyramid.storage.get('pyr/q/1d/2026-01-04.parquet')
    assert _parse_shard(empty_day) == []
    assert 'pyr/q/1d/2026-01-04.parquet' in [r.key for r in index.records]

    # Neighboring day unaffected.
    jan5 = _parse_shard(pyramid.storage.get('pyr/q/1d/2026-01-05.parquet'))
    assert len(jan5) == 2 * (DAY_MS // Q_MS)

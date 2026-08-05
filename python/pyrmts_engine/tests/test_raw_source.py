"""Raw-ingest via `TiledSource` (`specs/engine-raw-ingest.md` acceptance #2):
a synthetic daily-event archive (no ctbk shapes) proving

- base-tier emission: `provides=None` → the engine writes every rung,
  including the base tier, byte-identical to the wide-shard reference path
- the dedupe-then-max-ts parse contract, exercised via the `parse` hook
- two-level coverage: a missing day tile is a hard error
  (`max_missing_source`); an empty window *inside* a present tile is a
  legitimate empty bin, not a miss
"""
from __future__ import annotations

import io
import json
from datetime import datetime, timedelta

import polars as pl
import pyarrow.parquet as pq
import pytest

from pyrmts import MemStorage, Pyramid, shard_periods_covering
from pyrmts_engine import (
    MemShardIndex,
    SourceCoverageError,
    Tile,
    TiledSource,
    WideShardSource,
    build_local,
)
from pyrmts_engine.longform import empty_long, long_schema

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


class DailyEventSource(TiledSource):
    """Toy raw-archive source: one JSON blob of event records per day
    under `raw/<YYYY-MM-DD>.json`; each record is
    `{ts, cell, bikes: {state: n}, rides, temp: [n, sum, sumsq]}`.
    `parse` implements the spec's contract: dedupe exact `(ts, cell)`
    duplicates (identical content — either copy is fine), then keep the
    max-`ts` record per (bin, cell) — "state as of end of bin"."""

    def tile_at(self, at: datetime) -> Tile:
        period = shard_periods_covering(at, at + timedelta(milliseconds=1), '1d')[0]
        return Tile(key=f'raw/{period.label}.json', period=period)

    def parse(self, blob: bytes, tile: Tile) -> pl.DataFrame:
        best: dict[tuple[int, str], dict] = {}
        for rec in json.loads(blob):
            k = (rec['ts'] // Q_MS, rec['cell'])
            cur = best.get(k)
            if cur is None or rec['ts'] > cur['ts']:
                best[k] = rec
        rows = []
        for (i, cell), rec in best.items():
            dt = i * Q_MS
            for state, n in rec['bikes'].items():
                rows.append({
                    'cell': cell, 'dt': dt, 'metric': 'bikes',
                    'state': int(state), 'count': float(n),
                })
            rows.append({'cell': cell, 'dt': dt, 'metric': 'rides', 'state': None, 'count': float(rec['rides'])})
            for col, v in zip(('temp_n', 'temp_sum', 'temp_sumsq'), rec['temp']):
                rows.append({'cell': cell, 'dt': dt, 'metric': col, 'state': None, 'count': float(v)})
        if not rows:
            return empty_long(self.pyramid)
        return pl.DataFrame(rows, schema=long_schema(self.pyramid))


def _auth_event(ms: int, cell_idx: int) -> dict:
    """The authoritative (max-ts, :14 into the bin) record for a bin."""
    i = ms // Q_MS
    return {
        'ts': ms + 840_000,
        'cell': CELLS[cell_idx],
        'bikes': {str(k): v for k, v in bikes_hist(i).items()},
        'rides': rides_count(i, cell_idx),
        'temp': [2, float(i), float(i * i)],
    }


def _events_blob(start_ms: int, end_ms: int) -> bytes:
    """A day's records for bins in `[start_ms, end_ms)`. Every bin carries
    a decoy (earlier-ts, corrupted values) and an exact duplicate of the
    authoritative record, ordered so that naive keep-first fails on even
    bins and naive keep-last fails on odd bins — only dedupe-then-max-ts
    reproduces the reference content."""
    events = []
    for ms in range(start_ms, end_ms, Q_MS):
        i = ms // Q_MS
        for cell_idx in range(len(CELLS)):
            auth = _auth_event(ms, cell_idx)
            decoy = {
                'ts': ms + 60_000,
                'cell': CELLS[cell_idx],
                'bikes': {'9': 99},
                'rides': auth['rides'] + 3,
                'temp': [1, -1.0, -1.0],
            }
            if i % 2:
                events += [auth, dict(auth), decoy]
            else:
                events += [decoy, auth, dict(auth)]
    return json.dumps(events).encode()


def _write_raw_days(
    storage: MemStorage,
    start: datetime = FROM,
    to: datetime = TO,
    skip_label: str | None = None,
) -> list[str]:
    keys = []
    for period in shard_periods_covering(start, to, '1d'):
        if period.label == skip_label:
            continue
        key = f'raw/{period.label}.json'
        storage.put(key, _events_blob(
            int(period.start.timestamp() * 1000),
            int(period.end.timestamp() * 1000),
        ))
        keys.append(key)
    return keys


def _raw_pyramid(**kw) -> tuple[Pyramid, DailyEventSource]:
    pyramid = make_pyramid()
    _write_raw_days(pyramid.storage, **kw)
    return pyramid, DailyEventSource(pyramid)


def test_read_window_parse_contract():
    """`read_window` over two bins returns exactly the long rows of each
    bin's authoritative record — decoys superseded by max-ts, duplicates
    collapsed."""
    pyramid, src = _raw_pyramid()
    out = src.read_window(FROM, FROM + timedelta(minutes=30))
    start_ms = int(FROM.timestamp() * 1000)
    expected = []
    for ms in (start_ms, start_ms + Q_MS):
        i = ms // Q_MS
        for cell_idx, cell in enumerate(CELLS):
            expected += [
                (cell, ms, 'bikes', s, float(n)) for s, n in sorted(bikes_hist(i).items())
            ]
            expected += [
                (cell, ms, 'rides', None, float(rides_count(i, cell_idx))),
                (cell, ms, 'temp_n', None, 2.0),
                (cell, ms, 'temp_sum', None, float(i)),
                (cell, ms, 'temp_sumsq', None, float(i * i)),
            ]
    assert sorted(out.rows()) == sorted(expected)


def test_raw_ingest_builds_every_rung_byte_identical():
    """`provides=None` → the engine writes every expected rung *including
    the base tier* (its min-cover rung, q@1d), each shard byte-identical
    to the reference path (base rung materialized as wide q@6h shards +
    `WideShardSource` build, which writes the same q@1d/h/d outputs)."""
    pyramid, src = _raw_pyramid()
    index = MemShardIndex()
    build_local(
        pyramid, (FROM, TO), src,
        pyramid_name='test', shard_index=index,
    )
    assert src.coverage() == (6, [])

    ref = make_pyramid()
    write_base_shards(ref)
    build_local(
        ref, (FROM, TO), WideShardSource(ref, shard_dur='6h'),
        pyramid_name='test',
    )

    # The ref's q@6h keys are its *source material*, not build outputs;
    # everything else on both storages is engine output and must agree.
    keys = sorted(pyramid.storage.list('pyr/'))
    assert keys == [
        k for k in sorted(ref.storage.list('pyr/')) if not k.startswith('pyr/q/6h/')
    ]
    assert [k for k in keys if pyramid.storage.get(k) != ref.storage.get(k)] == []
    # Every written shard (base rung included) is registered.
    assert sorted(r.key for r in index.records) == keys


def test_missing_day_tile_is_hard_error():
    """Two-level coverage, level 1: an absent day tile post-genesis is a
    real hole — strict `max_missing_source=0.0` raises."""
    pyramid, src = _raw_pyramid(skip_label='2026-01-04')
    with pytest.raises(SourceCoverageError) as exc:
        build_local(pyramid, (FROM, TO), src, pyramid_name='test')
    assert src.coverage() == (6, ['raw/2026-01-04.json'])
    assert str(exc.value) == (
        "build_local: 1/6 source shards absent (> max_missing_source=0.0): "
        "raw/2026-01-04.json — a real hole (GC'd rung, filter typo, wrong rung), "
        "not an outage (outage shards are present-but-EMPTY); raise "
        "max_missing_source / --max-missing if such holes are expected here "
        "(outputs WERE written/registered)"
    )


def test_empty_window_within_tile_is_not_a_miss():
    """Two-level coverage, level 2: a present-but-eventless tile yields
    affirmatively-EMPTY shards — zero coverage misses, no threshold, no
    error. (Day 1's blob is full; day 2's exists with no records.)"""
    end = FROM + timedelta(days=2)
    pyramid = make_pyramid()
    start_ms = int(FROM.timestamp() * 1000)
    pyramid.storage.put(
        'raw/2026-01-02.json', _events_blob(start_ms, start_ms + DAY_MS),
    )
    pyramid.storage.put('raw/2026-01-03.json', json.dumps([]).encode())
    src = DailyEventSource(pyramid)
    build_local(pyramid, (FROM, end), src, pyramid_name='test')
    assert src.coverage() == (2, [])

    rows_per_1d = 24 * 4 * len(CELLS)
    assert [
        (k, pq.read_table(io.BytesIO(pyramid.storage.get(k))).num_rows)
        for k in sorted(pyramid.storage.list('pyr/q/1d/'))
    ] == [
        ('pyr/q/1d/2026-01-02.parquet', rows_per_1d),
        ('pyr/q/1d/2026-01-03.parquet', 0),
    ]

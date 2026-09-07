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

from pyrmts import MemStorage, Pyramid, list_expected_shards, shard_periods_covering
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


# ---- fill mode + open-period tiles ----------------------------------------
#
# ctbk 2026-09-07: an uncapped `-f` fill on a tiled source (daily parquet)
# reached `now`; the day-in-progress tile didn't exist yet, and the engine
# built the trailing rungs over it as 0-row shards that every later fill
# then read as "built" (28 on smg-v1, 3 inside avail-v6's live tip). The
# open-period classification only forgave the absence after the fact. In
# fill mode a missing shard overlapping an ABSENT OPEN tile is deferred —
# not built — and a later fill builds it once the tile exists.

# 12h into the (absent) [TO, TO+1d) day tile: two whole q@6h shards sit
# inside the open day and are expected; the h@1d / d@4d shards over it
# extend past `to` and aren't.
TO_OPEN_DAY = TO + timedelta(hours=12)
OPEN_DAY_LINE_PREFIX = (
    'fill: {n} deferred (open-period source absent: raw/2026-01-08.json '
    '[2026-01-08T00:00:00+00:00, 2026-01-09T00:00:00+00:00)): '
)


def _split_by_open_day(pyramid) -> tuple[list[str], list[str]]:
    """(closed, deferred) expected keys for `(FROM, TO_OPEN_DAY)`: a shard
    is deferred iff its effective period overlaps the absent open day."""
    closed, deferred = [], []
    for e in list_expected_shards(pyramid, (FROM, TO_OPEN_DAY)):
        (deferred if e.effective_end > TO else closed).append(e.key)
    return sorted(closed), sorted(deferred)


def test_fill_defers_shards_over_absent_open_tile(capsys):
    pyramid, src = _raw_pyramid()  # raw days [FROM, TO) present; 2026-01-08 never written
    closed, deferred = _split_by_open_day(pyramid)
    assert deferred == ['pyr/q/6h/2026-01-08T00.parquet', 'pyr/q/6h/2026-01-08T06.parquet']
    assert len(closed) == 11
    index = MemShardIndex()
    result = build_local(
        pyramid, (FROM, TO_OPEN_DAY), src,
        pyramid_name='test', shard_index=index, fill=True, window='3h',
    )
    assert sorted(w.key for w in result.written) == closed
    assert sorted(r.key for r in index.records) == closed
    assert (result.deferred, result.unfillable, result.missing_source, result.expected_absent) == (2, 0, 0, 0)
    assert sorted(pyramid.storage.list('pyr/')) == closed
    lines = capsys.readouterr().err.splitlines()
    assert [l for l in lines if l.startswith('fill:')] == [
        'fill: 13 expected shards, 0 present, 13 missing, 11 fillable',
        OPEN_DAY_LINE_PREFIX.format(n=2) + ', '.join(deferred),
    ]

    # The day closes and its tile lands: the next fill builds exactly the
    # deferred shards (everything else is present) and defers nothing.
    _write_raw_days(pyramid.storage, start=TO, to=TO + timedelta(days=1))
    result2 = build_local(
        pyramid, (FROM, TO_OPEN_DAY), src,
        pyramid_name='test', shard_index=index, fill=True, window='3h',
    )
    assert sorted(w.key for w in result2.written) == deferred
    assert (result2.deferred, result2.present_shards) == (0, 11)
    assert sorted(r.key for r in index.records) == closed + deferred


def test_fill_strict_open_periods_builds_over_absent_open_tile():
    """`strict_open_periods=True` keeps the old behaviour: the trailing
    shards build (empty over the absent day) and the coverage guard fires."""
    pyramid, src = _raw_pyramid()
    with pytest.raises(SourceCoverageError):
        build_local(
            pyramid, (FROM, TO_OPEN_DAY), src,
            pyramid_name='test', fill=True, window='3h', strict_open_periods=True,
        )
    closed, deferred = _split_by_open_day(pyramid)
    assert sorted(pyramid.storage.list('pyr/')) == closed + deferred

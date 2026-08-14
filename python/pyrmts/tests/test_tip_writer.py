"""`TipWriter` (`specs/streaming-tip-writer.md` optional follow-on): the
streaming-tip read-merge-write-invalidate cycle. Core-only imports —
same packaging constraint as the reference producer (awair's Lambda)."""
from __future__ import annotations

import io
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyrmts import Dim, MemStorage, Metric, Pyramid, Tier, write_tier_parquet
from pyrmts.invalidation import Invalidation, load_invalidations
from pyrmts.tip_writer import TipWriter


def utc(*a: int) -> datetime:
    return datetime(*a, tzinfo=timezone.utc)


T0 = utc(2026, 8, 14, 10)
TIP_KEY = 'pyr/raw/1d/2026-08-14.parquet'


def make_pyramid() -> Pyramid:
    return Pyramid(
        storage=MemStorage(),
        keyTemplate='pyr/{tier}/{shard}/{period}.parquet',
        binCol='dt',
        dims=[Dim(name='device', type='string')],
        metrics=[Metric(name='rides', monoid='count')],
        tiers=[Tier(name='raw', bin='1min', shards=('1d',))],
    )


def ms(t: datetime) -> int:
    return int(t.timestamp() * 1000)


def rows(*entries: tuple[datetime, str, int]) -> pa.Table:
    return pa.Table.from_pylist([
        {'dt': ms(t), 'device': device, 'rides': rides}
        for t, device, rides in entries
    ])


def test_sixty_tip_appends_round_trip():
    """Acceptance #7: 60 one-minute ticks growing the same tip shard.
    Final tip content is byte-identical to a one-shot write of all rows
    (concat + dedupe + sort), and the journal carries all 60 entries
    with the correct `[bin_start, bin_end)` intervals."""
    pyramid = make_pyramid()
    ticks = [T0 + timedelta(minutes=i) for i in range(60)]
    for i, t in enumerate(ticks):
        with TipWriter(pyramid, tier='raw', at=t, now=t) as tip:
            tip.append(rows((t, 'a', i)))
        assert (tip.key, tip.rows_written) == (TIP_KEY, i + 1)

    one_shot = io.BytesIO()
    write_tier_parquet(
        rows(*((t, 'a', i) for i, t in enumerate(ticks))), pyramid, out=one_shot,
    )
    assert pyramid.storage.get(TIP_KEY) == one_shot.getvalue()

    invs, _ = load_invalidations(pyramid)
    assert invs == [
        Invalidation(start=t, end=t + timedelta(minutes=1), requested_at=t)
        for t in ticks
    ]


def test_multi_append_single_flush():
    """Appends within one context accumulate into one write and ONE
    journal entry spanning the touched bins."""
    pyramid = make_pyramid()
    with TipWriter(pyramid, tier='raw', at=T0, now=T0) as tip:
        tip.append(rows((T0, 'a', 1), (T0, 'b', 2)))
        tip.append(rows((utc(2026, 8, 14, 10, 5), 'a', 3)))
    assert tip.rows_written == 3
    assert pq.read_table(io.BytesIO(pyramid.storage.get(TIP_KEY))).to_pylist() == [
        {'dt': ms(T0), 'device': 'a', 'rides': 1},
        {'dt': ms(T0), 'device': 'b', 'rides': 2},
        {'dt': ms(utc(2026, 8, 14, 10, 5)), 'device': 'a', 'rides': 3},
    ]
    invs, _ = load_invalidations(pyramid)
    assert invs == [
        Invalidation(start=T0, end=utc(2026, 8, 14, 10, 6), requested_at=T0),
    ]


def test_keep_last_corrects_earlier_write():
    """Default merge semantics: a later tick re-writing an existing
    `(bin, dims)` key replaces the earlier row."""
    pyramid = make_pyramid()
    with TipWriter(pyramid, tier='raw', at=T0, now=T0) as tip:
        tip.append(rows((T0, 'a', 1)))
    t1 = utc(2026, 8, 14, 10, 1)
    with TipWriter(pyramid, tier='raw', at=t1, now=t1) as tip:
        tip.append(rows((T0, 'a', 99)))
    assert tip.rows_written == 1
    assert pq.read_table(io.BytesIO(pyramid.storage.get(TIP_KEY))).to_pylist() == [
        {'dt': ms(T0), 'device': 'a', 'rides': 99},
    ]


def test_keep_first_and_error_conflict_modes():
    pyramid = make_pyramid()
    with TipWriter(pyramid, tier='raw', at=T0, now=T0) as tip:
        tip.append(rows((T0, 'a', 1)))
    with TipWriter(pyramid, tier='raw', at=T0, now=T0, on_conflict='keep-first') as tip:
        tip.append(rows((T0, 'a', 99)))
    assert pq.read_table(io.BytesIO(pyramid.storage.get(TIP_KEY))).to_pylist() == [
        {'dt': ms(T0), 'device': 'a', 'rides': 1},
    ]
    with pytest.raises(ValueError) as exc:
        with TipWriter(pyramid, tier='raw', at=T0, now=T0, on_conflict='error') as tip:
            tip.append(rows((T0, 'a', 99)))
    assert str(exc.value) == (
        "TipWriter: duplicate row key {'dt': 1786701600000, 'device': 'a'} "
        "(on_conflict='error')"
    )


def test_out_of_period_append_raises():
    """Rows outside the `at`-selected shard period must not silently
    mis-file into the wrong tip."""
    pyramid = make_pyramid()
    with pytest.raises(ValueError) as exc:
        with TipWriter(pyramid, tier='raw', at=T0, now=T0) as tip:
            tip.append(rows((utc(2026, 8, 15), 'a', 1)))
    assert str(exc.value) == (
        'TipWriter: appended rows span [2026-08-15T00:00:00+00:00, '
        '2026-08-15T00:00:00+00:00] outside the tip shard period '
        '[2026-08-14T00:00:00+00:00, 2026-08-15T00:00:00+00:00) selected by '
        'at=2026-08-14T10:00:00+00:00'
    )
    assert pyramid.storage.get(TIP_KEY) is None
    assert load_invalidations(pyramid) == ([], None)


def test_empty_append_is_noop():
    pyramid = make_pyramid()
    with TipWriter(pyramid, tier='raw', at=T0, now=T0) as tip:
        pass
    assert (tip.key, tip.rows_written, tip.bytes_written) == (None, None, None)
    assert pyramid.storage.get(TIP_KEY) is None
    assert load_invalidations(pyramid) == ([], None)


def test_exception_in_body_skips_flush():
    pyramid = make_pyramid()
    with pytest.raises(RuntimeError):
        with TipWriter(pyramid, tier='raw', at=T0, now=T0) as tip:
            tip.append(rows((T0, 'a', 1)))
            raise RuntimeError('producer blew up')
    assert pyramid.storage.get(TIP_KEY) is None
    assert load_invalidations(pyramid) == ([], None)


def test_dims_fill_extra_key_template_placeholders():
    pyramid = Pyramid(
        storage=MemStorage(),
        keyTemplate='awair-{device_id}/{tier}/{shard}/{period}.parquet',
        binCol='dt',
        dims=[Dim(name='device', type='string')],
        metrics=[Metric(name='rides', monoid='count')],
        tiers=[Tier(name='raw', bin='1min', shards=('1d',))],
    )
    with TipWriter(pyramid, tier='raw', at=T0, now=T0, dims={'device_id': 17617}) as tip:
        tip.append(rows((T0, 'a', 1)))
    assert tip.key == 'awair-17617/raw/1d/2026-08-14.parquet'
    assert pyramid.storage.get(tip.key) is not None

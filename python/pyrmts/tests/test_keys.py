import pytest

from pyrmts import substitute_key


def test_basic():
    assert substitute_key('avail/{tier}/{period}.parquet', {'tier': 'h1', 'period': '2026-05'}) == 'avail/h1/2026-05.parquet'


def test_int_value():
    assert substitute_key('awair-{device_id}/{tier}.parquet', {'device_id': 17617, 'tier': 'd1'}) == 'awair-17617/d1.parquet'


def test_missing_value_raises():
    with pytest.raises(KeyError, match=r'\{period\}'):
        substitute_key('{tier}/{period}', {'tier': 'h1'})


def test_no_placeholders():
    assert substitute_key('static/path.parquet', {}) == 'static/path.parquet'

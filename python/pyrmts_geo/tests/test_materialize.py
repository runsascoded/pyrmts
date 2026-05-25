import h3
import pytest

from pyrmts import GeoSpec
from pyrmts_geo import MaterializeStats, materialize_resolutions


GEO = GeoSpec(cellCol='h3_cell', resolutions=(9, 7, 5))


# A few known Manhattan / NYC stations.
STATIONS = {
    's1': (40.7128, -74.0060),   # downtown
    's2': (40.7580, -73.9855),   # Times Square
    's3': (40.6892, -74.0445),   # Statue of Liberty
}


def _lookup(row: dict) -> tuple[float, float] | None:
    return STATIONS.get(row['station_id'])


def test_one_row_emits_per_resolution():
    rows = [{'station_id': 's1', 'dt': 1000, 'bikes': 7}]
    out = list(materialize_resolutions(rows, GEO, _lookup))
    assert len(out) == 3
    lat, lng = STATIONS['s1']
    expected_cells = [h3.latlng_to_cell(lat, lng, r) for r in GEO.resolutions]
    assert [o['h3_cell'] for o in out] == expected_cells
    for o in out:
        assert o['station_id'] == 's1'
        assert o['dt'] == 1000
        assert o['bikes'] == 7


def test_emits_in_input_order_then_finest_first():
    rows = [
        {'station_id': 's1', 'i': 0},
        {'station_id': 's2', 'i': 1},
    ]
    out = list(materialize_resolutions(rows, GEO, _lookup))
    # Each input row produces len(resolutions) outputs in finest-first order
    assert [o['i'] for o in out] == [0, 0, 0, 1, 1, 1]


def test_drops_rows_with_no_latlng():
    rows = [
        {'station_id': 's1'},
        {'station_id': 'unknown'},
        {'station_id': 's2'},
    ]
    stats = MaterializeStats()
    out = list(materialize_resolutions(rows, GEO, _lookup, stats=stats))
    assert len(out) == 6  # 2 valid rows × 3 resolutions
    assert stats.rows_in == 3
    assert stats.rows_out == 6
    assert stats.dropped_no_latlng == 1


def test_does_not_mutate_input():
    rows = [{'station_id': 's1', 'foo': 'bar'}]
    list(materialize_resolutions(rows, GEO, _lookup))
    assert rows == [{'station_id': 's1', 'foo': 'bar'}]
    assert 'h3_cell' not in rows[0]


def test_custom_cell_col():
    geo = GeoSpec(cellCol='cell', resolutions=(9,))
    rows = [{'station_id': 's1'}]
    out = list(materialize_resolutions(rows, geo, _lookup))
    assert len(out) == 1
    assert 'cell' in out[0]
    assert 'h3_cell' not in out[0]


def test_single_resolution():
    geo = GeoSpec(cellCol='h3_cell', resolutions=(9,))
    rows = [{'station_id': 's1'}, {'station_id': 's2'}]
    out = list(materialize_resolutions(rows, geo, _lookup))
    assert len(out) == 2
    s1_lat, s1_lng = STATIONS['s1']
    s2_lat, s2_lng = STATIONS['s2']
    assert out[0]['h3_cell'] == h3.latlng_to_cell(s1_lat, s1_lng, 9)
    assert out[1]['h3_cell'] == h3.latlng_to_cell(s2_lat, s2_lng, 9)


def test_stats_none_works():
    rows = [{'station_id': 's1'}, {'station_id': 'unknown'}]
    out = list(materialize_resolutions(rows, GEO, _lookup))
    assert len(out) == 3

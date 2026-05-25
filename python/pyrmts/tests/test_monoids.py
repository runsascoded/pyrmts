import json

from pyrmts import get_monoid, state_columns


def test_sum_combine():
    m = get_monoid('sum')
    assert m.state_columns('x') == ('x_n', 'x_sum', 'x_sumsq')
    tgt = {'x_n': 1, 'x_sum': 5.0, 'x_sumsq': 25.0}
    src = {'x_n': 2, 'x_sum': 7.0, 'x_sumsq': 41.0}
    m.combine(tgt, src, 'x')
    assert tgt == {'x_n': 3, 'x_sum': 12.0, 'x_sumsq': 66.0}


def test_sum_combine_missing():
    m = get_monoid('sum')
    tgt = {'x_n': 1, 'x_sum': 5.0, 'x_sumsq': 25.0}
    src = {}
    m.combine(tgt, src, 'x')
    assert tgt == {'x_n': 1, 'x_sum': 5.0, 'x_sumsq': 25.0}


def test_count_combine():
    m = get_monoid('count')
    assert m.state_columns('n') == ('n',)
    tgt = {'n': 3}
    src = {'n': 5}
    m.combine(tgt, src, 'n')
    assert tgt == {'n': 8}


def test_histogram_combine_dict():
    m = get_monoid('histogram')
    assert m.state_columns('h') == ('h',)
    tgt = {'h': {'10': 3}}
    m.init(tgt, 'h')
    src = {'h': {'10': 2, '20': 1}}
    m.combine(tgt, src, 'h')
    assert tgt == {'h': {'10': 5, '20': 1}}


def test_histogram_combine_json_string():
    m = get_monoid('histogram')
    tgt = {'h': json.dumps({'5': 2})}
    m.init(tgt, 'h')
    src = {'h': json.dumps({'5': 1, '7': 3})}
    m.combine(tgt, src, 'h')
    assert tgt == {'h': {'5': 3, '7': 3}}


def test_state_columns_helper():
    assert state_columns('sum', 'foo') == ('foo_n', 'foo_sum', 'foo_sumsq')
    assert state_columns('count', 'n') == ('n',)
    assert state_columns('histogram', 'bikes') == ('bikes',)

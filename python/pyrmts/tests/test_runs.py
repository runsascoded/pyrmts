"""`pyrmts.runs` (`specs/multiscan-out-of-core.md` Phase 2): the streaming
k-way merge of sorted runs with a per-identity reduce, against brute force,
and against `pyrmts.intervals` rebuilds: base ⊕ per-scan deltas = rebuild
(byte for byte), and tiered delta merges are associative."""
from __future__ import annotations

import hashlib
import random
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

duckdb = pytest.importorskip('duckdb')

from pyrmts.intervals import (  # noqa: E402
    append_intervals,
    delta_sql,
    islands_sql,
    long_sql,
    stamped_sql,
    write_exact_row_groups,
    write_query,
)
from pyrmts.runs import merge_parquets, merge_sorted, row_group_spans  # noqa: E402

KEYS = ['depth', 'path']
STATE = ['size', 'n']
ID = [*KEYS, 'vf']
OPEN = 10_000
STAMPS = [100, 200, 300, 400, 500, 600, 700]
IVS = pa.schema([
    pa.field('depth', pa.int64()), pa.field('path', pa.string()),
    pa.field('vf', pa.int64()), pa.field('vt', pa.int64()),
    pa.field('size', pa.int64()), pa.field('n', pa.int64()),
])
DELTA = IVS.append(pa.field('op', pa.int8()))
RG = 4


def _md5(path: Path) -> str:
    return hashlib.md5(path.read_bytes()).hexdigest()


def _write_scans(root: Path, seed: int = 11) -> list[str]:
    """Per scan, sorted `(depth, path, size, n)`: keys drop out and return,
    sizes drift."""
    rng = random.Random(seed)
    universe = sorted({(p.count('/') + 1, p) for p in (
        f'{a}/{b}' if b else a for a in 'abcdéfgh' for b in ['', 'x', 'Y', 'zz', 'q/r', 'ü']
    )})
    sizes = {k: rng.choice([10, 20]) for k in universe}
    paths = []
    for j in range(len(STAMPS)):
        rows = []
        for k in universe:
            if rng.random() < 0.15:
                continue
            if rng.random() < 0.2:
                sizes[k] = rng.choice([10, 20, 30])
            rows.append((*k, sizes[k], 1 + (sizes[k] > 20)))
        cols = list(zip(*rows))
        p = root / f'scan{j}.parquet'
        pq.write_table(pa.table({
            'depth': pa.array(cols[0], pa.int64()), 'path': pa.array(cols[1]),
            'size': pa.array(cols[2], pa.int64()), 'n': pa.array(cols[3], pa.int64()),
        }), p, row_group_size=RG)
        paths.append(str(p))
    return paths


def _rebuild(con, paths: list[str], n: int, out: Path) -> Path:
    """Stamped intervals through scan `n - 1`, sorted by identity."""
    runs = islands_sql(long_sql([f"SELECT * FROM read_parquet('{p}')" for p in paths[:n]]), KEYS, STATE)
    write_query(con, stamped_sql(runs, KEYS, STATE, STAMPS[:n], OPEN), out, IVS, row_group_size=RG, sort=ID)
    return out


def _deltas(con, paths: list[str], base_n: int, root: Path) -> list[Path]:
    """One delta run per scan after the base's `base_n` scans, from chained
    `append_intervals`, sorted `(*ID, op)` like disky's `cdelta`."""
    prev = _rebuild(con, paths, base_n, root / 'base.parquet')
    out = []
    for j in range(base_n, len(STAMPS)):
        append_intervals(con, f"SELECT * FROM read_parquet('{prev}')", f"SELECT * FROM read_parquet('{paths[j]}')",
                         KEYS, STATE, STAMPS[j], OPEN)
        d = root / f'delta{j}.parquet'
        write_query(con, delta_sql('__ivs', STAMPS[j]), d, DELTA, row_group_size=RG, sort=[*ID, 'op'])
        prev = root / f'ivs{j}.parquet'
        write_query(con, 'SELECT * FROM __ivs', prev, IVS, row_group_size=RG, sort=ID)
        out.append(d)
    return out


def _merge_to(paths, out: Path, schema: pa.Schema, key, **kw) -> Path:
    write_exact_row_groups(merge_parquets(paths, key, **kw), out, schema, RG)
    return out


DELTA_MERGE = dict(identity=ID, reduce={'vt': 'min', 'op': 'max'})


def test_base_plus_deltas_equals_rebuild_byte_for_byte(tmp_path):
    """Compaction: the base's intervals ⊕ every later scan's delta (ops
    dropped, combined on identity with the smallest `vt`) is the rebuild."""
    con = duckdb.connect()
    paths = _write_scans(tmp_path)
    deltas = _deltas(con, paths, 2, tmp_path)
    rebuilt = _rebuild(con, paths, len(STAMPS), tmp_path / 'rebuilt.parquet')
    merged = _merge_to([tmp_path / 'base.parquet', *deltas], tmp_path / 'merged.parquet', IVS, ID,
                       reduce={'vt': 'min'}, columns=IVS.names)
    assert pq.read_table(merged) == pq.read_table(rebuilt)
    assert _md5(merged) == _md5(rebuilt)


def test_tiered_delta_merges_are_associative_and_match_rebuild(tmp_path):
    """Merging per-scan deltas (min `vt`, max `op`) in any grouping gives the
    same bytes, equal to the span's delta read off a rebuild: versions opened
    in the span (`op` 1, final `vt`) plus older versions it closed (`op` −1)."""
    con = duckdb.connect()
    paths = _write_scans(tmp_path)
    d = _deltas(con, paths, 2, tmp_path)
    assert len(d) == 5
    flat = _merge_to(d, tmp_path / 'flat.parquet', DELTA, [*ID, 'op'], **DELTA_MERGE)
    a = _merge_to(d[:2], tmp_path / 'a.parquet', DELTA, [*ID, 'op'], **DELTA_MERGE)
    b = _merge_to(d[2:4], tmp_path / 'b.parquet', DELTA, [*ID, 'op'], **DELTA_MERGE)
    ab = _merge_to([a, b], tmp_path / 'ab.parquet', DELTA, [*ID, 'op'], **DELTA_MERGE)
    counter = _merge_to([ab, d[4]], tmp_path / 'counter.parquet', DELTA, [*ID, 'op'], **DELTA_MERGE)
    right = _merge_to([d[0], _merge_to(d[1:], tmp_path / 'r.parquet', DELTA, [*ID, 'op'], **DELTA_MERGE)],
                      tmp_path / 'right.parquet', DELTA, [*ID, 'op'], **DELTA_MERGE)
    assert _md5(counter) == _md5(flat) == _md5(right)

    rebuilt = _rebuild(con, paths, len(STAMPS), tmp_path / 'rebuilt.parquet')
    lo, hi = STAMPS[2], STAMPS[-1]
    want = tmp_path / 'want.parquet'
    write_query(con, f"""
        SELECT *, 1::TINYINT AS op FROM read_parquet('{rebuilt}') WHERE vf >= {lo}
        UNION ALL
        SELECT *, -1::TINYINT AS op FROM read_parquet('{rebuilt}') WHERE vf < {lo} AND vt BETWEEN {lo} AND {hi}
    """, want, DELTA, row_group_size=RG, sort=[*ID, 'op'])
    assert pq.read_table(flat).num_rows > 0
    assert _md5(flat) == _md5(want)


def _brute(tables: list[pa.Table], key: list[str], ident: list[str], reduce) -> list[dict]:
    rows = [(j, r) for j, t in enumerate(tables) for r in t.to_pylist()]
    rest = key[len(ident):]
    if reduce == 'newest':
        rows.sort(key=lambda x: (tuple(x[1][c] for c in ident), x[0], tuple(x[1][c] for c in rest)))
    else:
        rows.sort(key=lambda x: (tuple(x[1][c] for c in key), x[0]))
    if reduce is None:
        return [r for _, r in rows]
    out: list[dict] = []
    for _, r in rows:
        if out and all(out[-1][c] == r[c] for c in ident):
            if reduce == 'newest':
                out[-1] = r
            else:
                for c, p in reduce.items():
                    out[-1][c] = (min if p == 'min' else max)(out[-1][c], r[c])
        else:
            out.append(dict(r))
    return out


@pytest.mark.parametrize('key', [['s', 'i'], ['s', 'i', 'w']])
@pytest.mark.parametrize('reduce', [None, 'newest', {'v': 'min', 'w': 'max'}])
def test_merge_matches_brute_force(reduce, key):
    """Random sorted inputs (dictionary keys, non-ASCII strings, duplicate
    identities within and across inputs, random batch sizes, keys longer than
    the identity) vs brute force."""
    rng = random.Random(5)
    words = ['a', 'b', 'ab', 'B', 'é', 'z', 'ü', 'zz', '']
    ident = ['s', 'i']
    schema = pa.schema([('s', pa.string()), ('i', pa.int64()), ('x', pa.int64()), ('v', pa.int64()), ('w', pa.int64())])
    for trial in range(60):
        tables = []
        for j in range(rng.randrange(1, 5)):
            ids = sorted({(rng.choice(words), rng.randrange(3)) for _ in range(rng.randrange(0, 25))})
            if reduce is not None:
                ids += rng.sample(ids, len(ids) // 3)
            # `x` is a function of the identity, so rows sharing one agree on
            # everything but the reduced columns.
            rows = [{'s': s, 'i': i, 'x': len(s) * 7 + i, 'v': rng.randrange(9), 'w': rng.randrange(9)} for s, i in ids]
            rows.sort(key=lambda r: tuple(r[c] for c in key))
            t = pa.Table.from_pylist(rows, schema=schema)
            tables.append(t.set_column(0, 's', t.column('s').dictionary_encode()))
        batches = [t.to_batches(max_chunksize=rng.randrange(1, 6)) for t in tables]
        got = [r for b in merge_sorted(batches, key, identity=ident, reduce=reduce) for r in b.to_pylist()]
        assert got == _brute(tables, key, ident, reduce), (trial, reduce, key)


def test_merge_errors():
    t = pa.table({'k': [1, 2], 'x': [1, 1]})
    with pytest.raises(ValueError, match=r"identity \['x'\] must be a prefix of key \['k'\]"):
        list(merge_sorted([t.to_batches()], ['k'], identity=['x']))
    with pytest.raises(ValueError, match='not sorted'):
        list(merge_sorted([[pa.record_batch({'k': [3]}), pa.record_batch({'k': [1]})]], ['k']))
    with pytest.raises(ValueError, match=r"rows with identity \(1,\) disagree on 'x' \(1 vs 2\)"):
        list(merge_sorted([t.to_batches(), pa.table({'k': [1], 'x': [2]}).to_batches()], ['k'], reduce={}))
    with pytest.raises(ValueError, match='nulls in key'):
        list(merge_sorted([pa.table({'k': [None, 1]}).to_batches()], ['k']))
    with pytest.raises(ValueError, match=r"reduce columns \['k'\] are identity columns"):
        list(merge_sorted([t.to_batches()], ['k'], reduce={'k': 'min'}))


def test_row_group_spans_tile_the_data_pages(tmp_path):
    out = tmp_path / 'x.parquet'
    write_exact_row_groups(iter(pa.table({'a': list(range(10)), 'b': [str(i) for i in range(10)]}).to_batches()),
                           out, pa.schema([('a', pa.int64()), ('b', pa.string())]), 4)
    spans = row_group_spans(out)
    assert [(s['rg'], s['rows']) for s in spans] == [(0, 4), (1, 4), (2, 2)]
    assert spans[0]['offset'] == 4
    assert [s['offset'] for s in spans[1:]] == [s['offset'] + s['length'] for s in spans[:-1]]

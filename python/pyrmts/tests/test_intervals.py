"""`pyrmts.intervals` (`specs/multiscan-out-of-core.md` Phase 1): the DuckDB
interval kernel vs a sequential-ingest oracle, change vs carried columns,
key-range pieces vs brute force, range-union = whole, append = rebuild (byte
for byte), exact row groups, and order-insensitive digests."""
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
    interval_digests,
    islands_sql,
    key_range_pieces,
    long_sql,
    plan_ranges,
    relation_digest,
    stamped_sql,
    write_exact_row_groups,
    write_query,
)

KEYS = ['depth', 'path']
STATE = ['size', 'n', 'seen']        # `seen` churns every scan: a carried column
OPEN = 10_000
STAMPS = [100, 200, 300, 400, 500, 600]
SCHEMA = pa.schema([
    pa.field('depth', pa.int64()), pa.field('path', pa.string()),
    pa.field('vf', pa.int64()), pa.field('vt', pa.int64()),
    pa.field('size', pa.int64()), pa.field('n', pa.int64()), pa.field('seen', pa.int64()),
])


def _scans(seed: int = 7, n_scans: int = len(STAMPS)) -> list[list[tuple]]:
    """Per scan, sorted `(depth, path, size, n, seen)` rows: keys drop out
    (absence gaps), sizes drift rarely, `seen` changes every scan."""
    rng = random.Random(seed)
    universe = sorted({(p.count('/') + 1, p) for p in (
        f'{a}/{b}' if b else a for a in 'abcdefgh' for b in ['', 'x', 'y', 'zz', 'q/r']
    )})
    sizes = {k: rng.choice([10, 20]) for k in universe}
    out = []
    for j in range(n_scans):
        rows = []
        for k in universe:
            if rng.random() < 0.15:
                continue
            if rng.random() < 0.2:
                sizes[k] = rng.choice([10, 20, 30])
            rows.append((*k, sizes[k], 1, j * 7 + rng.randrange(3)))
        out.append(rows)
    return out


def _write_scans(scans: list[list[tuple]], root: Path, rg: int = 4) -> list[str]:
    paths = []
    for j, rows in enumerate(scans):
        p = root / f'scan{j}.parquet'
        cols = list(zip(*rows))
        pq.write_table(pa.table({
            'depth': pa.array(cols[0], pa.int64()), 'path': pa.array(cols[1], pa.string()),
            'size': pa.array(cols[2], pa.int64()), 'n': pa.array(cols[3], pa.int64()), 'seen': pa.array(cols[4], pa.int64()),
        }), p, row_group_size=rg)
        paths.append(str(p))
    return paths


def _oracle(scans: list[list[tuple]], stamps: list[int], carried: dict[str, str] | None = None) -> list[tuple]:
    """Sequential ingest: per scan, close open runs whose key is gone or whose
    change columns differ; open new / changed keys; carried `last` columns of
    continuing runs take the scan's value."""
    carried = carried or {}
    change_idx = [i for i, c in enumerate(STATE) if c not in carried]
    open_: dict[tuple, list] = {}   # key → [vf, vals]
    done = []
    for ts, rows in zip(stamps, scans):
        cur = {(d, p): list(v) for d, p, *v in rows}
        for k in list(open_):
            vf, vals = open_[k]
            if k not in cur or any(cur[k][i] != vals[i] for i in change_idx):
                done.append((*k, vf, ts, *vals))
                del open_[k]
            else:
                for i, c in enumerate(STATE):
                    if carried.get(c) == 'last':
                        vals[i] = cur[k][i]
        for k, vals in cur.items():
            if k not in open_:
                open_[k] = [ts, vals]
    done += [(*k, vf, OPEN, *vals) for k, (vf, vals) in open_.items()]
    return sorted(done)


def _stamped(paths: list[str], stamps: list[int], carried=None, where: str | None = None) -> str:
    srcs = [f"SELECT * FROM read_parquet('{p}')" + (f' WHERE {where}' if where else '') for p in paths]
    runs = islands_sql(long_sql(srcs), KEYS, STATE, carried=carried)
    return stamped_sql(runs, KEYS, STATE, stamps, OPEN)


def _rows(con, sql: str) -> list[tuple]:
    return sorted(con.execute(sql).fetchall())


def _md5(path: Path) -> str:
    return hashlib.md5(path.read_bytes()).hexdigest()


@pytest.mark.parametrize('carried', [None, {'seen': 'first'}, {'seen': 'last'}])
def test_kernel_equals_sequential_ingest(tmp_path, carried):
    scans = _scans()
    paths = _write_scans(scans, tmp_path)
    con = duckdb.connect()
    assert _rows(con, _stamped(paths, STAMPS, carried)) == _oracle(scans, STAMPS, carried)


def test_carried_columns_never_open_a_run(tmp_path):
    """One key, size constant, `seen` changing every scan: three runs when
    `seen` is a change column, one when it is carried (first / last value)."""
    scans = [[(1, 'a', 10, 1, seen)] for seen in (1, 2, 3)]
    paths = _write_scans(scans, tmp_path)
    con = duckdb.connect()
    assert _rows(con, _stamped(paths, STAMPS[:3])) == [
        (1, 'a', 100, 200, 10, 1, 1), (1, 'a', 200, 300, 10, 1, 2), (1, 'a', 300, OPEN, 10, 1, 3),
    ]
    assert _rows(con, _stamped(paths, STAMPS[:3], {'seen': 'first'})) == [(1, 'a', 100, OPEN, 10, 1, 1)]
    assert _rows(con, _stamped(paths, STAMPS[:3], {'seen': 'last'})) == [(1, 'a', 100, OPEN, 10, 1, 3)]


def test_kernel_argument_checks():
    with pytest.raises(ValueError, match='not state columns'):
        islands_sql('SELECT 1', KEYS, STATE, carried={'nope': 'first'})
    with pytest.raises(ValueError, match="'first' or 'last'"):
        islands_sql('SELECT 1', KEYS, STATE, carried={'seen': 'max'})
    with pytest.raises(ValueError, match='at least one state column must be a change column'):
        islands_sql('SELECT 1', KEYS, ['seen'], carried={'seen': 'last'})
    with pytest.raises(ValueError, match='strictly increasing'):
        stamped_sql('SELECT 1', KEYS, STATE, [2, 2], OPEN)
    with pytest.raises(ValueError, match='open_stamp must exceed'):
        stamped_sql('SELECT 1', KEYS, STATE, [1, 2], 2)


def test_key_range_pieces_match_brute_force():
    """For random `[lo, hi)` over `(int, str)` keys the pieces' union is exactly
    the range, and the pieces are disjoint."""
    rng = random.Random(3)
    keys = sorted({(rng.randrange(4), ''.join(rng.choice('ab') for _ in range(rng.randrange(3)))) for _ in range(80)})
    con = duckdb.connect()
    con.execute('CREATE TABLE k (depth BIGINT, path VARCHAR)')
    con.executemany('INSERT INTO k VALUES (?, ?)', keys)
    bounds = [None, *keys, (5, '')]
    for _ in range(200):
        lo, hi = rng.choice(bounds), rng.choice(bounds + [None])
        if lo is not None and hi is not None and hi <= lo:
            continue
        pieces = key_range_pieces(KEYS, lo, hi)
        got = [r for p in pieces for r in con.execute(f'SELECT * FROM k WHERE {p}').fetchall()]
        want = [k for k in keys if (lo is None or k >= lo) and (hi is None or k < hi)]
        assert sorted(got) == want, (lo, hi, pieces)
    assert key_range_pieces(KEYS, (1, 'b'), (3, 'a')) == [
        '"depth" = 1 AND "path" >= \'b\'',
        '"depth" > 1 AND "depth" < 3',
        '"depth" = 3 AND "path" < \'a\'',
    ]
    assert key_range_pieces(KEYS, None, None) == ['TRUE']
    with pytest.raises(ValueError, match='empty range'):
        key_range_pieces(KEYS, (2, 'a'), (2, 'a'))


def test_plan_ranges_partition_and_union_equals_whole(tmp_path):
    """Ranges planned from the newest scan's footer are contiguous and
    deterministic, and building each range separately and concatenating (in
    range order) is byte-identical to one whole build."""
    scans = _scans()
    paths = _write_scans(scans, tmp_path, rg=3)
    md = pq.ParquetFile(paths[-1]).metadata
    ranges = plan_ranges([(md, len(paths))], KEYS, 4, floor=(0, ''))
    assert ranges == plan_ranges([(md, len(paths))], KEYS, 4, floor=(0, ''))
    rs = ranges['ranges']
    assert (ranges['k'], rs[0]['lo'], rs[-1]['hi']) == (len(rs), [0, ''], None)
    assert len(rs) > 1 and all(a['hi'] == b['lo'] for a, b in zip(rs, rs[1:]))

    con = duckdb.connect()
    whole = tmp_path / 'whole.parquet'
    write_query(con, _stamped(paths, STAMPS), whole, SCHEMA, row_group_size=5, sort=[*KEYS, 'vf'])
    parts = []
    for r in rs:
        srcs = [
            ' UNION ALL '.join(f"SELECT * FROM read_parquet('{p}') WHERE {piece}" for piece in key_range_pieces(KEYS, r['lo'], r['hi']))
            for p in paths
        ]
        runs = islands_sql(long_sql(srcs), KEYS, STATE)
        parts.append(con.execute(f"SELECT * FROM ({stamped_sql(runs, KEYS, STATE, STAMPS, OPEN)}) ORDER BY depth, path, vf").to_arrow_table())
    joined = tmp_path / 'joined.parquet'
    write_exact_row_groups(pa.concat_tables(parts).to_batches(max_chunksize=3), joined, SCHEMA, 5)
    assert _md5(joined) == _md5(whole)


@pytest.mark.parametrize('carried', [None, {'seen': 'last'}, {'seen': 'first'}])
def test_append_equals_rebuild_byte_for_byte(tmp_path, carried):
    scans = _scans()
    paths = _write_scans(scans, tmp_path)
    con = duckdb.connect()
    prev = tmp_path / 'prev.parquet'
    write_query(con, _stamped(paths[:-1], STAMPS[:-1], carried), prev, SCHEMA, row_group_size=5, sort=[*KEYS, 'vf'])
    rebuilt = tmp_path / 'rebuilt.parquet'
    write_query(con, _stamped(paths, STAMPS, carried), rebuilt, SCHEMA, row_group_size=5, sort=[*KEYS, 'vf'])

    opened, closed = append_intervals(
        con, f"SELECT * FROM read_parquet('{prev}')", f"SELECT * FROM read_parquet('{paths[-1]}')",
        KEYS, STATE, STAMPS[-1], OPEN, carried=carried,
    )
    appended = tmp_path / 'appended.parquet'
    write_query(con, 'SELECT * FROM __ivs', appended, SCHEMA, row_group_size=5, sort=[*KEYS, 'vf'])
    assert _md5(appended) == _md5(rebuilt)

    oracle = _oracle(scans, STAMPS, carried)
    D = STAMPS[-1]
    assert (opened, closed) == (sum(r[2] == D for r in oracle), sum(r[3] == D for r in oracle))
    delta = sorted(con.execute(delta_sql('__ivs', D)).fetchall())
    assert delta == sorted([(*r, 1) for r in oracle if r[2] == D] + [(*r, -1) for r in oracle if r[3] == D])
    with pytest.raises(ValueError, match='already reach'):
        append_intervals(con, 'SELECT * FROM __ivs', f"SELECT * FROM read_parquet('{paths[-1]}')", KEYS, STATE, D, OPEN)


def test_exact_row_groups_are_independent_of_batch_chunking(tmp_path):
    t = pa.table({'x': list(range(23)), 's': [str(i % 5) for i in range(23)]})
    files = []
    for chunk in (1, 4, 23):
        f = tmp_path / f'c{chunk}.parquet'
        assert write_exact_row_groups(t.to_batches(max_chunksize=chunk), f, t.schema, 10, dictionary=['s']) == 23
        files.append(f)
    assert len({_md5(f) for f in files}) == 1
    md = pq.ParquetFile(files[0]).metadata
    assert [md.row_group(i).num_rows for i in range(md.num_row_groups)] == [10, 10, 3]


def test_digests_are_order_insensitive_and_split_invariant(tmp_path):
    scans = _scans()
    paths = _write_scans(scans, tmp_path)
    con = duckdb.connect()
    con.execute(f'CREATE TABLE iv AS {_stamped(paths, STAMPS)}')
    a = relation_digest(con, 'SELECT * FROM iv ORDER BY path', [*KEYS, 'vf', 'size'])
    b = relation_digest(con, 'SELECT * FROM iv ORDER BY size DESC, depth', [*KEYS, 'vf', 'size'])
    assert a == b and a[0] == len(_oracle(scans, STAMPS))
    whole = interval_digests(con, 'iv', [*KEYS, 'vf', 'size', 'n'], [*KEYS, 'vf', 'vt'], OPEN)
    assert sorted(whole) == [str(t) for t in STAMPS[:]] and 'closed' not in whole[str(STAMPS[0])]
    # Splitting by depth parity and summing (mod 2⁶⁴) reproduces the whole.
    con.execute('CREATE TABLE even AS SELECT * FROM iv WHERE depth % 2 = 0')
    con.execute('CREATE TABLE odd AS SELECT * FROM iv WHERE depth % 2 = 1')
    halves = [interval_digests(con, t, [*KEYS, 'vf', 'size', 'n'], [*KEYS, 'vf', 'vt'], OPEN) for t in ('even', 'odd')]
    summed: dict = {}
    for h in halves:
        for ts, kinds in h.items():
            for kind, (n, x) in kinds.items():
                cur = summed.setdefault(ts, {}).setdefault(kind, [0, 0])
                cur[0] += n
                cur[1] = (cur[1] + x) % (1 << 64)
    assert dict(sorted(summed.items())) == whole

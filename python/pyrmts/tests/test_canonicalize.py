"""Identity-rollup transform (`specs/pyrmts-identity-rollup.md`).

`recanonicalize_table` is a purely additive, idempotent overlay on a built
shard: it derives `c:<canonical>` rows by monoid-summing the raw `s:<raw>`
leaves an id-map folds together, leaving raw leaves and s2 cells untouched.
Assertions parse the output back into sorted `(token, bin, *state)` tuples and
compare by exact equality — a stray/dropped row fails loudly."""
from __future__ import annotations

import io

import pyarrow as pa
import pytest
import pyarrow.parquet as pq

from pyrmts import (
    Dim,
    GeoSpec,
    IdentityRollup,
    MemStorage,
    Metric,
    Pyramid,
    Tier,
    canonicalize_shards,
    recanonicalize_table,
)


def _sum_pyramid(storage: MemStorage | None = None, *, extra_dim: bool = False) -> Pyramid:
    dims = [Dim(name='cell', type='s2')]
    if extra_dim:
        dims.append(Dim(name='dir', type='string'))
    return Pyramid(
        storage=storage or MemStorage(),
        keyTemplate='p/{tier}/{shard}/{period}.parquet',
        binCol='dt',
        dims=dims,
        metrics=[Metric(name='rides', monoid='sum')],
        tiers=[Tier(name='base', bin='1d', shards=('1mo',))],
        geo=GeoSpec(cellCol='cell', resolutions=(20, 10)),
        identity_rollup=IdentityRollup(col='cell', map='id-map.json'),
    )


def _sum_table(rows: list[tuple], *, extra_dim: bool = False) -> pa.Table:
    """rows: (dt, cell, [dir,] n, sum, sumsq)."""
    cols: dict[str, list] = {'dt': [], 'cell': []}
    if extra_dim:
        cols['dir'] = []
    cols |= {'rides_n': [], 'rides_sum': [], 'rides_sumsq': []}
    for r in rows:
        it = iter(r)
        cols['dt'].append(next(it))
        cols['cell'].append(next(it))
        if extra_dim:
            cols['dir'].append(next(it))
        cols['rides_n'].append(next(it))
        cols['rides_sum'].append(next(it))
        cols['rides_sumsq'].append(next(it))
    return pa.table(cols)


def _parse_sum(table: pa.Table, *, extra_dim: bool = False) -> list[tuple]:
    d = table.to_pydict()
    n = table.num_rows
    out = []
    for i in range(n):
        key = (d['cell'][i], d['dt'][i])
        if extra_dim:
            key = (d['cell'][i], d['dir'][i], d['dt'][i])
        out.append(key + (d['rides_n'][i], d['rides_sum'][i], d['rides_sumsq'][i]))
    return sorted(out)


def test_merged_cluster_sums_and_raw_s2_rows_are_preserved():
    p = _sum_pyramid()
    # s:A + s:B → c:X (merge); s:C unmerged; s2cell is an s2 rollup row.
    table = _sum_table([
        (0, 's:A', 3, 30, 300),
        (0, 's:B', 2, 20, 200),
        (0, 's:C', 1, 10, 100),
        (0, 's2cell', 6, 60, 600),
    ])
    out = recanonicalize_table(table, {'s:A': 'c:X', 's:B': 'c:X'}, pyramid=p)
    assert _parse_sum(out) == [
        ('c:X', 0, 5, 50, 500),      # s:A + s:B
        ('s2cell', 0, 6, 60, 600),   # untouched
        ('s:A', 0, 3, 30, 300),      # raw leaf kept
        ('s:B', 0, 2, 20, 200),      # raw leaf kept
        ('s:C', 0, 1, 10, 100),      # unmerged → no c: row, leaf kept
    ]


def test_canonical_rows_group_by_bin_and_other_dims():
    p = _sum_pyramid(extra_dim=True)
    table = _sum_table([
        (0, 's:A', 'in',  1, 10, 100),
        (0, 's:B', 'in',  1, 20, 400),
        (0, 's:A', 'out', 1, 30, 900),   # different `dir` → separate canonical group
        (1, 's:A', 'in',  1, 40, 1600),  # different bin → separate canonical group
    ], extra_dim=True)
    out = recanonicalize_table(table, {'s:A': 'c:X', 's:B': 'c:X'}, pyramid=p)
    canonical = [r for r in _parse_sum(out, extra_dim=True) if r[0] == 'c:X']
    assert canonical == [
        ('c:X', 'in',  0, 2, 30, 500),   # s:A+s:B, bin 0, dir in
        ('c:X', 'in',  1, 1, 40, 1600),  # s:A, bin 1, dir in
        ('c:X', 'out', 0, 1, 30, 900),   # s:A, bin 0, dir out
    ]


def test_recanonicalize_is_idempotent_byte_for_byte():
    p = _sum_pyramid()
    table = _sum_table([
        (0, 's:A', 3, 30, 300),
        (0, 's:B', 2, 20, 200),
        (0, 's2cell', 6, 60, 600),
    ])
    id_map = {'s:A': 'c:X', 's:B': 'c:X'}
    once = recanonicalize_table(table, id_map, pyramid=p)
    twice = recanonicalize_table(once, id_map, pyramid=p)

    def _bytes(t: pa.Table) -> bytes:
        buf = io.BytesIO()
        pq.write_table(t, buf, compression='snappy')
        return buf.getvalue()

    assert _bytes(once) == _bytes(twice)


def test_a_changed_map_replaces_canonical_rows_rather_than_accumulating():
    p = _sum_pyramid()
    table = _sum_table([
        (0, 's:A', 3, 30, 300),
        (0, 's:B', 2, 20, 200),
    ])
    first = recanonicalize_table(table, {'s:A': 'c:X', 's:B': 'c:X'}, pyramid=p)
    # Re-map both to c:Y — the stale c:X row must be dropped, not kept.
    second = recanonicalize_table(first, {'s:A': 'c:Y', 's:B': 'c:Y'}, pyramid=p)
    assert _parse_sum(second) == [
        ('c:Y', 0, 5, 50, 500),
        ('s:A', 0, 3, 30, 300),
        ('s:B', 0, 2, 20, 200),
    ]


def test_histogram_monoid_merges_constituent_maps():
    import json
    p = Pyramid(
        storage=MemStorage(),
        keyTemplate='p/{tier}/{shard}/{period}.parquet',
        binCol='dt',
        dims=[Dim(name='cell', type='s2')],
        metrics=[Metric(name='bikes', monoid='histogram')],
        tiers=[Tier(name='base', bin='1d', shards=('1mo',))],
        identity_rollup=IdentityRollup(col='cell', map='id-map.json'),
    )
    table = pa.table({
        'dt': [0, 0, 0],
        'cell': ['s:A', 's:B', 's:C'],
        'bikes': [
            json.dumps({'0': 2, '1': 3}),
            json.dumps({'1': 1, '2': 4}),
            json.dumps({'0': 9}),
        ],
    })
    out = recanonicalize_table(table, {'s:A': 'c:X', 's:B': 'c:X'}, pyramid=p, col='cell')
    d = out.to_pydict()
    by_cell = {d['cell'][i]: json.loads(d['bikes'][i]) for i in range(out.num_rows)}
    assert by_cell == {
        'c:X': {'0': 2, '1': 4, '2': 4},  # merged {0:2,1:3} + {1:1,2:4}
        's:A': {'0': 2, '1': 3},
        's:B': {'1': 1, '2': 4},
        's:C': {'0': 9},
    }


def test_canonicalize_shards_rewrites_present_shards_and_skips_missing():
    from datetime import datetime, timezone

    from pyrmts import shard_periods_covering, substitute_key

    storage = MemStorage()
    p = _sum_pyramid(storage)
    tr = (datetime(2026, 1, 1, tzinfo=timezone.utc), datetime(2026, 3, 1, tzinfo=timezone.utc))
    # Derive shard keys from the same helpers the implementation uses (no
    # hardcoded period-label format). Jan + Feb over a 1mo ladder.
    keys = [
        substitute_key(p.keyTemplate, {'tier': 'base', 'shard': '1mo', 'period': sp.label})
        for sp in shard_periods_covering(*tr, '1mo')
    ]
    jan_key, feb_key = keys

    buf = io.BytesIO()
    pq.write_table(_sum_table([(0, 's:A', 3, 30, 300), (0, 's:B', 2, 20, 200)]), buf, compression='snappy')
    storage.put(jan_key, buf.getvalue())  # Feb left absent

    result = canonicalize_shards(p, {'s:A': 'c:X', 's:B': 'c:X'}, tr)
    assert (result.written, result.skipped, result.errors) == ([jan_key], [feb_key], [])

    written = pq.read_table(io.BytesIO(storage.get(jan_key)))
    assert _parse_sum(written) == [
        ('c:X', 0, 5, 50, 500),
        ('s:A', 0, 3, 30, 300),
        ('s:B', 0, 2, 20, 200),
    ]


def test_additive_fast_path_matches_generic_combine():
    # The vectorized additive fast path (pyarrow group-by-sum) must reproduce
    # the generic per-row combine exactly. Mix a merged cluster, an unmapped
    # leaf, and an s2 cell across two bins and two `dir` values; compare fast
    # (default) against the generic loop (additive temporarily disabled).
    from pyrmts.monoids import _Sum
    p = _sum_pyramid(extra_dim=True)
    table = _sum_table([
        (0, 's:A', 'in',  3, 30, 300),
        (0, 's:B', 'in',  2, 20, 200),
        (0, 's:A', 'out', 1, 30, 900),
        (1, 's:A', 'in',  4, 40, 1600),
        (0, 's:C', 'in',  1, 10, 100),   # unmapped → leaf only
        (0, 's2', 'in',   5, 50, 500),   # s2 cell → untouched
    ], extra_dim=True)
    id_map = {'s:A': 'c:X', 's:B': 'c:X'}

    fast = _parse_sum(recanonicalize_table(table, id_map, pyramid=p), extra_dim=True)
    _Sum.additive = False
    try:
        slow = _parse_sum(recanonicalize_table(table, id_map, pyramid=p), extra_dim=True)
    finally:
        _Sum.additive = True
    assert fast == slow


def test_additive_fast_path_handles_large_string_col():
    # Built shards may store the rollup column as `large_string` (pyarrow picks
    # it by size), so the fast path's id-map join key must match that type —
    # pyarrow rejects a `string` vs `large_string` key mismatch. Cast the column
    # to `large_string` and confirm the rollup still works.
    p = _sum_pyramid()
    table = _sum_table([(0, 's:A', 3, 30, 300), (0, 's:B', 2, 20, 200), (0, 's2cell', 6, 60, 600)])
    i = table.schema.get_field_index('cell')
    table = table.set_column(i, 'cell', table.column('cell').cast(pa.large_string()))
    out = recanonicalize_table(table, {'s:A': 'c:X', 's:B': 'c:X'}, pyramid=p)
    assert _parse_sum(out) == [
        ('c:X', 0, 5, 50, 500),
        ('s2cell', 0, 6, 60, 600),
        ('s:A', 0, 3, 30, 300),
        ('s:B', 0, 2, 20, 200),
    ]


def _layout_fixture(storage: MemStorage):
    """A 2-dim shard written the way the engine writes it (`write_tier_parquet`
    with an explicit non-default sort and 4-row groups → several RGs), plus the
    id-map and time range that canonicalize the January shard."""
    from datetime import datetime, timezone

    from pyrmts import shard_periods_covering, substitute_key, write_tier_parquet

    p = _sum_pyramid(storage, extra_dim=True)
    tr = (datetime(2026, 1, 1, tzinfo=timezone.utc), datetime(2026, 2, 1, tzinfo=timezone.utc))
    (period,) = shard_periods_covering(*tr, '1mo')
    key = substitute_key(p.keyTemplate, {'tier': 'base', 'shard': '1mo', 'period': period.label})
    rows = [
        (dt, cell, d, n, n * 10, n * 100)
        for d in ('a', 'b')
        for dt in (0, 1)
        for cell, n in (('s:A', 3), ('s:B', 2), ('s:C', 5))
    ]
    table = _sum_table(rows, extra_dim=True)
    layout = {'row_group_size': 4, 'sort': ['dir', 'dt', 'cell']}
    buf = io.BytesIO()
    write_tier_parquet(table, p, out=buf, **layout)
    storage.put(key, buf.getvalue())
    return p, tr, key, table, layout


def test_canonicalize_preserves_the_build_layout():
    """`specs/canonicalize-preserve-layout.md`: the canonicalized shard has the
    row-group layout and global sort `write_tier_parquet` would give the same
    logical table — `c:` rows interleaved at their sorted position, not one
    unsorted row group with them appended. Byte-equal, since both go through
    the one writer with the layout the shard's footer records."""
    from pyrmts import write_tier_parquet

    storage = MemStorage()
    p, tr, key, table, layout = _layout_fixture(storage)
    id_map = {'s:A': 'c:X', 's:B': 'c:X'}

    result = canonicalize_shards(p, id_map, tr)
    assert (result.written, result.errors) == ([key], [])

    expected_buf = io.BytesIO()
    write_tier_parquet(recanonicalize_table(table, id_map, pyramid=p), p, out=expected_buf, **layout)
    got = storage.get(key)
    got_md = pq.ParquetFile(io.BytesIO(got)).metadata
    exp_md = pq.ParquetFile(io.BytesIO(expected_buf.getvalue())).metadata
    assert [got_md.row_group(i).num_rows for i in range(got_md.num_row_groups)] == \
           [exp_md.row_group(i).num_rows for i in range(exp_md.num_row_groups)] == [4, 4, 4, 4]
    got_rows = pq.read_table(io.BytesIO(got)).to_pylist()
    assert [(r['dir'], r['dt'], r['cell']) for r in got_rows] == sorted((r['dir'], r['dt'], r['cell']) for r in got_rows)
    assert 'c:X' in [r['cell'] for r in got_rows[:4]]          # interleaved, not appended
    assert got == expected_buf.getvalue()


def test_canonicalize_infers_layout_for_legacy_shards_and_honours_overrides():
    """A shard without the layout stamp (written by a bare `pq.write_table`)
    keeps its first row group's size and gets the pyramid's default sort; an
    explicit `sort` / `row_group_size` wins over both."""
    from pyrmts import write_tier_parquet

    storage = MemStorage()
    p, tr, key, table, _ = _layout_fixture(storage)
    buf = io.BytesIO()
    pq.write_table(table, buf, row_group_size=3, compression='snappy')   # legacy: no stamp, 3-row RGs
    storage.put(key, buf.getvalue())
    id_map = {'s:A': 'c:X', 's:B': 'c:X'}

    canonicalize_shards(p, id_map, tr)
    got = storage.get(key)
    exp = io.BytesIO()
    write_tier_parquet(recanonicalize_table(table, id_map, pyramid=p), p, out=exp, row_group_size=3)
    assert got == exp.getvalue()

    storage.put(key, buf.getvalue())
    canonicalize_shards(p, id_map, tr, sort=['dir', 'cell', 'dt'], row_group_size=5)
    got = storage.get(key)
    exp = io.BytesIO()
    write_tier_parquet(recanonicalize_table(table, id_map, pyramid=p), p, out=exp, row_group_size=5, sort=['dir', 'cell', 'dt'])
    assert got == exp.getvalue()


def test_canonicalize_hashed_template_writes_a_new_key_and_swaps_the_registry_row():
    """`specs/content-addressed-shards.md`: with `{hash:N}` in the template the
    current shard is found via the registry, the rewrite lands at a new
    content-hashed key (the old blob is untouched, left for GC), and the
    registry row swaps to it; without a registry the call refuses."""
    from pyrmts import parse_key, put_shard, write_tier_parquet
    from pyrmts_engine.shard_index import MemShardIndex, RegistryResolver, ShardRecord

    p, tr, key, table, layout = _layout_fixture(MemStorage())
    storage = MemStorage()                                                  # fresh: only hashed keys live here
    p.storage = storage
    p.keyTemplate = p.keyTemplate.replace('.parquet', '.{hash:10}.parquet')
    values = {'tier': 'base', 'shard': '1mo', 'period': key.split('/')[-1].removesuffix('.parquet')}
    buf = io.BytesIO()
    write_tier_parquet(table, p, out=buf, **layout)
    first = put_shard(storage, p.keyTemplate, values, buf.getvalue())     # the "build"
    index = MemShardIndex()
    from datetime import datetime, timezone
    index.record_shard(ShardRecord(
        pyramid='t', tier='base', shard_dur='1mo',
        period_start_ms=int(tr[0].timestamp() * 1000), period_end_ms=int(tr[1].timestamp() * 1000),
        key=first.key, written_at_ms=1, md5=first.md5, n_bytes=first.n_bytes,
    ))
    id_map = {'s:A': 'c:X', 's:B': 'c:X'}

    with pytest.raises(ValueError, match='needs `registry`'):
        canonicalize_shards(p, id_map, tr)
    result = canonicalize_shards(p, id_map, tr, resolver=RegistryResolver(index), registry=index, pyramid_name='t')
    (new_key,) = result.written
    assert new_key != first.key and parse_key(p.keyTemplate, new_key)['hash'] == hashlib_md5(storage.get(new_key))[:10]
    assert storage.get(first.key) == buf.getvalue()                          # old version untouched (orphan)
    assert index.lookup('base', '1mo', int(tr[0].timestamp() * 1000)).key == new_key
    assert _parse_sum(pq.read_table(io.BytesIO(storage.get(new_key))), extra_dim=True)[:2] == [
        ('c:X', 'a', 0, 5, 50, 500), ('c:X', 'a', 1, 5, 50, 500),
    ]
    # Idempotent: a second pass produces identical bytes → the same key, no new object, no re-registration.
    again = canonicalize_shards(p, id_map, tr, resolver=RegistryResolver(index), registry=index, pyramid_name='t')
    assert (again.written, again.unchanged) == ([], [new_key])
    assert sorted(storage.list('p/')) == sorted([first.key, new_key])


def hashlib_md5(b: bytes) -> str:
    import hashlib
    return hashlib.md5(b).hexdigest()


def test_canonicalize_hashless_template_warns_about_in_place_rewrite():
    storage = MemStorage()
    p, tr, key, table, layout = _layout_fixture(storage)
    with pytest.warns(UserWarning, match='rewritten IN PLACE'):
        canonicalize_shards(p, {'s:A': 'c:X'}, tr)

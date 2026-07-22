"""Regression tests for the ctbk validation findings
(`ctbk/specs/pyrmts-engine-validation.md` §Findings):

1. WIP spill — engine memory is bounded by per-shard scratch files, and
   the scratch dir is empty (removed) after a clean run.
2. `WideShardSource` parse cache — each source blob is fetched once even
   when `window < shard_dur`.
3. `row_group_size` plumbs through to the output shards (per-tier).
8. `batch submit` passes `--workers`/`-K`/`-b` through to `build`.
10. Byte-aware admission: a starvation-level `mem_budget` degrades to
    serial claims but still completes, byte-identically.
"""
from __future__ import annotations

import io
from pathlib import Path

import pyarrow.parquet as pq

from pyrmts import MemStorage
from pyrmts_engine import WideShardSource, build_local

from conftest import FROM, TO, make_pyramid, write_base_shards

DAY_ROWS = 2 * 96  # 2 cells × 96 15-min bins


class CountingStorage(MemStorage):
    def __init__(self) -> None:
        super().__init__()
        self.get_counts: dict[str, int] = {}

    def get(self, key: str) -> bytes | None:
        self.get_counts[key] = self.get_counts.get(key, 0) + 1
        return super().get(key)


def test_source_cache_fetches_each_blob_once():
    """window=1h over 6h source shards: without the cache each blob would
    be fetched 6×."""
    storage = CountingStorage()
    pyramid = make_pyramid(storage=storage)
    base_keys = write_base_shards(pyramid)

    build_local(
        pyramid, (FROM, TO), WideShardSource(pyramid),
        pyramid_name='test', window='1h', workers=1,
    )
    assert {k: n for k, n in storage.get_counts.items() if k in set(base_keys)} == {
        k: 1 for k in base_keys
    }


def test_spill_dir_used_and_emptied(tmp_path: Path):
    spill_dir = tmp_path / 'spill'
    pyramid = make_pyramid()
    write_base_shards(pyramid)

    seen_spill_files: set[str] = set()
    orig_put = pyramid.storage.put

    def spying_put(key: str, data: bytes) -> None:
        seen_spill_files.update(p.name for p in spill_dir.glob('*.run.parquet'))
        orig_put(key, data)

    pyramid.storage.put = spying_put  # type: ignore[method-assign]

    result = build_local(
        pyramid, (FROM, TO), WideShardSource(pyramid),
        pyramid_name='test', spill_dir=spill_dir,
    )
    assert len(result.written) == 11
    # Spill run files existed during the run (max-rung shards accumulate
    # on disk, not in memory): h/4d/2026-01-03 covers [Jan 3, Jan 7) → 4
    # runs (one per contributing window, whatever order they completed).
    assert sorted(f for f in seen_spill_files if f.startswith('pyr_h_4d_2026-01-03')) == [
        f'pyr_h_4d_2026-01-03.parquet.{i:04d}.run.parquet' for i in range(4)
    ]
    # ...and a clean run leaves the scratch dir empty.
    assert sorted(spill_dir.iterdir()) == []


def test_mem_budget_throttles_but_completes_byte_identical():
    """Finding 10: a 1-byte budget forces serial admission (the ≥1-window
    progress guarantee) — the build still completes and outputs are
    byte-identical to an unthrottled parallel run."""
    p_ref = make_pyramid()
    write_base_shards(p_ref)
    r_ref = build_local(
        p_ref, (FROM, TO), WideShardSource(p_ref),
        pyramid_name='test', window='6h', workers=4, mem_budget=0,
    )
    p_tight = make_pyramid()
    write_base_shards(p_tight)
    r_tight = build_local(
        p_tight, (FROM, TO), WideShardSource(p_tight),
        pyramid_name='test', window='6h', workers=4, mem_budget=1,
    )
    assert sorted(w.key for w in r_tight.written) == sorted(w.key for w in r_ref.written)
    for w in r_ref.written:
        assert p_tight.storage.get(w.key) == p_ref.storage.get(w.key), w.key


def test_source_cache_bytes_tracks_residency():
    """`cache_bytes` (admission's cache term) grows with parsed shards and
    returns to 0 once the watermark evicts them."""
    pyramid = make_pyramid()
    write_base_shards(pyramid)
    src = WideShardSource(pyramid)
    assert src.cache_bytes() == 0
    src.read_window(FROM, TO)
    assert src.cache_bytes() == sum(
        e.frame.estimated_size() for e in src._cache.values()
    ) > 0
    src.evict_before(int(TO.timestamp() * 1000))
    assert src.cache_bytes() == 0


def test_batch_submit_tuning_passthrough():
    """Finding 8: `batch submit` can forward `-j`(workers)/`-K`/`-b` to the
    container's `build` (`-j` itself collides with `--job-name` there)."""
    from pyrmts_engine.batch import build_command
    cmd = build_command(
        's3://b/config.yaml', pyramid_name='p', range_='2026-01-01T00:00/2026-01-02T00:00',
        workers=16, max_inflight=8, mem_budget='24g',
    )
    assert cmd == [
        'build', '-n', 'p', '-r', '2026-01-01T00:00/2026-01-02T00:00',
        '-j', '16', '-K', '8', '-b', '24g', '-v', 's3://b/config.yaml',
    ]


def test_row_group_size_int_and_per_tier():
    pyramid = make_pyramid()
    write_base_shards(pyramid)
    build_local(
        pyramid, (FROM, TO), WideShardSource(pyramid),
        pyramid_name='test', row_group_size={'q': 64, 'h': 48},
    )

    def rg_sizes(key: str) -> list[int]:
        f = pq.ParquetFile(io.BytesIO(pyramid.storage.get(key)))
        return [f.metadata.row_group(i).num_rows for i in range(f.metadata.num_row_groups)]

    assert rg_sizes('pyr/q/1d/2026-01-02.parquet') == [64, 64, 64]  # 192 rows
    assert rg_sizes('pyr/h/4d/2026-01-03.parquet') == [48, 48, 48, 48]  # 192 rows
    # d tier: no override → writer default (single RG for 8 rows).
    assert rg_sizes('pyr/d/4d/2026-01-03.parquet') == [8]

"""`_detect_mem_limit`: the engine's default `mem_budget` source.

ctbk 2026-09-07: on Fargate the container's root `memory.max` reads `max`,
so the old root-only detection fell through to the host's MemTotal and
budgeted 46 GB inside a 32 GiB task (exit 137). The task limit is visible
on an ancestor cgroup and/or in ECS task metadata; both are consulted, the
tightest wins, and the source is reported for the build banner."""
from __future__ import annotations

from pathlib import Path

from pyrmts_engine.engine import _detect_mem_limit

GIB = 1 << 30


def _fs(tmp_path: Path, files: dict[str, str]) -> tuple[str, str]:
    for rel, text in files.items():
        p = tmp_path / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)
    return str(tmp_path / 'sys'), str(tmp_path / 'proc')


def test_ancestor_cgroup_limit_wins_over_unlimited_root(tmp_path: Path):
    sysfs, proc = _fs(tmp_path, {
        'sys/memory.max': 'max\n',
        'sys/ecs/task1/memory.max': f'{32 * GIB}\n',
        'sys/ecs/task1/container1/memory.max': 'max\n',
        'proc/self/cgroup': '0::/ecs/task1/container1\n',
        'proc/meminfo': 'MemTotal:       67108864 kB\n',
    })
    assert _detect_mem_limit(sysfs=sysfs, proc=proc) == (32 * GIB, 'cgroup:/ecs/task1')


def test_ecs_metadata_when_no_cgroup_exposes_the_limit(tmp_path: Path):
    sysfs, proc = _fs(tmp_path, {
        'sys/memory.max': 'max\n',
        'proc/self/cgroup': '0::/\n',
        'proc/meminfo': 'MemTotal:       67108864 kB\n',
    })
    assert _detect_mem_limit(sysfs=sysfs, proc=proc, ecs_task={'Limits': {'CPU': 8, 'Memory': 32768}}) == (32 * GIB, 'ecs-metadata')


def test_tightest_candidate_wins(tmp_path: Path):
    sysfs, proc = _fs(tmp_path, {
        'sys/memory.max': f'{48 * GIB}\n',
        'proc/self/cgroup': '0::/\n',
    })
    assert _detect_mem_limit(sysfs=sysfs, proc=proc, ecs_task={'Limits': {'Memory': 32768}}) == (32 * GIB, 'ecs-metadata')
    assert _detect_mem_limit(sysfs=sysfs, proc=proc, ecs_task={'Limits': {'Memory': 65536}}) == (48 * GIB, 'cgroup:/')


def test_cgroup_v1_root_limit(tmp_path: Path):
    sysfs, proc = _fs(tmp_path, {
        'sys/memory/memory.limit_in_bytes': f'{16 * GIB}\n',
        'proc/self/cgroup': '5:memory:/docker/abc\n',
    })
    assert _detect_mem_limit(sysfs=sysfs, proc=proc) == (16 * GIB, 'cgroup-v1:/')


def test_meminfo_fallback_is_tagged(tmp_path: Path):
    sysfs, proc = _fs(tmp_path, {
        'sys/memory.max': 'max\n',
        'sys/memory/memory.limit_in_bytes': '9223372036854771712\n',
        'proc/self/cgroup': '0::/\n',
        'proc/meminfo': 'MemTotal:       67108864 kB\nMemFree:        1 kB\n',
    })
    assert _detect_mem_limit(sysfs=sysfs, proc=proc) == (64 * GIB, 'meminfo')


def test_nothing_readable(tmp_path: Path):
    sysfs, proc = _fs(tmp_path, {})
    assert _detect_mem_limit(sysfs=sysfs, proc=proc) is None

# Core `pyrmts`: a registry-agnostic orphan GC, and a pyarrow range instead of a pin

Source: crashes session, 2026-09-28 (`hccs/crashes` `specs/cells-immutable-keys.md`, branch `cells-immutable-keys`). Written while adopting content-hashed shard keys for crashes' S2 cells pyramid.

## Context

crashes now publishes its cells pyramid with pyrmts's key grammar and write protocol: `keyTemplate` `s2_pyramid/s2_l{level}/{shard}.{hash:12}.parquet`, keys via `pyrmts.keys.substitute_key`, writes via `put_shard` over `pyrmts.storage.S3Storage` (R2). Its "registry" isn't a `ShardIndex`: it's a set of immutable per-build manifests (`manifests/<data_version>.json`) plus the active one (`manifest.json`); a blob is live if any *retained* manifest (active + newest N + anything within the grace period) references it.

Two things kept crashes from reusing more of pyrmts:

### 1. The core package pins `pyarrow==22.0.0`

crashes pins `pyarrow==21.0.0`: its DVX-tracked parquets' md5s depend on the writer version (a bump is a full-DAG re-baseline). Installing `pyrmts` (core) therefore needs a uv override in crashes:

```toml
[tool.uv]
override-dependencies = ["pyarrow==21.0.0"]
```

It works (`pyrmts.keys` / `pyrmts.storage` never touch pyarrow; the rest of the core imports fine on 21), but an override silently applies to *every* pyarrow requirement, and it's the kind of thing that outlives its reason.

**Ask**: in `python/pyrmts/pyproject.toml`, replace the exact pin with a range the core actually supports (e.g. `pyarrow>=21,<23`), and keep the exact pin where byte-reproducibility is load-bearing: the engine / Batch image (`pyrmts-engine`, or an `image` extra / constraints file). The comment on the pin already says the goal is "one pyarrow across pyrmts + a consumer"; an exact pin achieves that only for consumers on the same version. If there's a real 22-only dependency in the core, say which, and crashes will keep the override.

### 2. `gc_orphans` lives in `pyrmts_engine` and wants a `Pyramid` + `ShardIndex`

`pyrmts_engine.gc.list_orphans` / `gc_orphans` are exactly the semantics crashes needs — hashed-shape keys only (`slot_of`), unreferenced, older than a grace, never an mtime-less blob, refuse an empty registry, re-read the registry right before deleting (`kept_repointed`) — but:

- `pyrmts_engine/__init__` imports the polars engine (`polars==1.44.1`), a heavy dep for a listing + set difference;
- they take a `Pyramid` (for `.keyTemplate` / `.storage`) and a registry with `current_records(name)`, and `_require_hashed` checks for time-slot placeholders (`{tier}`/`{shard}`/`{period}`; crashes' `{level}`/`{shard}` passes only because of `{shard}`).

crashes reimplemented the ~40 lines (`njdot/cells_publish.py` `gc`), mirroring those semantics, with a test for the re-read race.

**Ask**: lift the core into `pyrmts.gc` (core package, stdlib + `pyrmts.keys`):

```python
def list_orphans(storage, template: str, referenced: set[str], prefix: str | None = None) -> list[Orphan]
def gc_orphans(
    storage,
    template: str,
    referenced: Callable[[], set[str]],   # called twice: plan, then re-read before deleting
    *,
    grace: timedelta = DEFAULT_GRACE,
    now: datetime | None = None,
    apply: bool = False,
    prefix: str | None = None,
) -> GcResult
```

- `_require_hashed(template)` checks "has a hash token and at least one non-hash placeholder" (the real condition: a pure-CA template can be shared across pyramids), not specific placeholder names.
- The engine's `gc_orphans(pyramid, shard_index, name, …)` becomes a thin wrapper: `referenced=lambda: _registry_keys(shard_index, name)`, and keeps its empty-registry refusal.
- crashes then calls `pyrmts.gc.gc_orphans(storage, PYRAMID_KEY_TEMPLATE, referenced=lambda: <union of retained manifests' keys>)` and keeps only its manifest-retention logic.

### 3. (Optional) server-side copy in `put_shard`

crashes' pyramid shards are already in R2 as DVX remote blobs (`.dvc/files/md5/ab/cdef…`, same bucket), so its push wraps the storage so `put` becomes an S3 `CopyObject` from that blob when it exists (bytes never leave R2; a promotion uploads 0 of 602 MB). If other consumers have a CA store alongside (ctbk's DVX remote?), a `put_shard(..., copy_from: Callable[[str], str | None] = None)` hook (md5 → source key or None) plus `S3Storage.copy(src_key, dst_key)` would make that first-class. Low priority; the wrapper is 20 lines.

## Acceptance

- `pyrmts` core installs next to `pyarrow==21.0.0` without an override.
- `from pyrmts.gc import gc_orphans` works without polars installed; the engine's tests pass unchanged through the wrapper.
- crashes swaps `cells_publish.gc`'s orphan loop for `pyrmts.gc.gc_orphans` and drops `[tool.uv] override-dependencies` (tracked on the crashes side in `specs/cells-immutable-keys.md`).

## Landed

### 1. pyarrow range

- Core `pyrmts` now requires `pyarrow>=21,<23`. No 22-only dependency exists in the core: its whole suite passes on pyarrow 21.0.0 in an env with no engine and no polars.
- The exact `pyarrow==22.0.0` pin moved to `pyrmts-engine`, next to its `polars==1.44.1` pin. The Batch image installs `./pyrmts_engine[batch]`, so the image stays byte-reproducible. `uv.lock` still resolves 22.0.0.
- CI gains a `core-pyarrow-floor` job that installs `pyarrow==21.0.0` + `./pyrmts[test]` alone and runs the core suite.
- Three core tests were cross-package (they import `pyrmts_engine.shard_index` for a registry). They now `pytest.importorskip('pyrmts_engine.shard_index')`, so a core-only env skips them; the workspace job still runs them.

### 2. `pyrmts.gc`

- `pyrmts/src/pyrmts/gc.py`: `require_hashed(template)`, `list_orphans(storage, template, referenced, prefix=None)`, `gc_orphans(storage, template, referenced: Callable[[], set[str]], *, grace, now, apply, prefix)`, `Orphan`, `GcResult`, `DEFAULT_GRACE`. Stdlib + `pyrmts.keys`; importable without polars.
- `require_hashed` now checks the real condition, as asked: a hash token and at least one non-hash placeholder (so `s2_l{level}/{shard}.{hash:12}` passes on its own merits, and `cas/{hash}` is refused).
- Semantics as before: hashed-shape keys only, older than `grace`, never an mtime-less blob, an empty referenced set refused, and `referenced()` re-read before deleting (`kept_repointed`). One addition: an empty set on the **re-read** also raises, rather than deleting every candidate.
- `pyrmts_engine.gc.gc_orphans` / `list_orphans` are thin wrappers with unchanged signatures; the engine keeps its "registry has no rows for this pyramid" refusal (raised from inside its `referenced` callable, so the read count is unchanged). `adopt_unregistered` uses the core `require_hashed`. Engine tests pass unchanged.
- Tests: `pyrmts/tests/test_orphan_gc.py` (6; named to avoid colliding with `pyrmts_ops/tests/test_gc.py` under rootdir imports), on the crashes-shaped template.

### 3. Server-side copy in `put_shard`

Not done. It stays optional and low priority, as the spec says; crashes' 20-line storage wrapper covers it. Worth revisiting if ctbk wants to promote shards out of its DVX remote.

### For crashes

Pin pyrmts at this commit, replace the orphan loop in `cells_publish.gc` with `pyrmts.gc.gc_orphans(storage, PYRAMID_KEY_TEMPLATE, referenced=lambda: <union of retained manifests' keys>)`, and drop `[tool.uv] override-dependencies`.

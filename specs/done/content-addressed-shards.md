# Content-hashed shard keys + registry-as-truth

Status: **done** (proposed 2026-09-24 from ctbk's rides re-key; phases 1 and 2 landed the same day, see "Landed" at the end; the two consumer-side items are listed under "Consumer follow-ups"). Companion: `done/canonicalize-preserve-layout.md`.

## Problem: shard blobs are mutable

Readers already go through an indirection. The CFW serve path reads `key` from the registry row (`pyramid_shards`, PK `(pyramid, tier, shard_dur, period_start)`) and fetches that object. But writers derive keys purely from `keyTemplate` (`rides/start/{tier}/{shard}/{period}.parquet`), so every rewrite of a period (a rebuild, a `canonicalize` pass, a cascade re-fill) **overwrites the blob readers point at**. ctbk has now hit every flavor of that bug surface:

- **RG manifest staleness (silent corruption).** `rg_manifest` stores row-group byte ranges keyed by `(key, written_at)`. An in-place rewrite without a registry `written_at` bump leaves the manifest describing the old bytes, and nothing detects it. ctbk now carries a manual "every canonicalize needs `reconcile -f`" rule.
- **Torn reads mid-rewrite.** A query spanning shards sees a mix of old and new periods while a rebuild or canonicalize runs.
- **`written_at` as a version proxy.** Correctness depends on every writer remembering to bump it.
- **Edge caching.** Responses for closed periods are served `Cache-Control: immutable`, but the data under them isn't. After a data fix, the edge serves stale answers for up to a day.
- **Discovery by LIST.** Fill (`-f`), fsck and reconcile diff the expected cover against a storage **listing**. So "what exists" has two truths, storage and the registry, and the reconcile machinery exists to heal the gap between them.

## Proposal

Put a content hash in the shard key. **Keys never get rewritten.** A rewrite writes a new blob and swaps the registry row to it (`INSERT OR REPLACE` on the existing PK is already atomic per row).

### Key shape: one template token, per-project choice

Add a `{hash}` token to `keyTemplate`, with an optional inline length: `{hash}` is the payload's full md5 (32 hex), and `{hash:N}` is its first N hex chars. There's no separate config key and no implicit default length. This follows the hashed-asset-filename convention (webpack `[contenthash:8]`, Rollup/Vite `[hash:8]`, where `:N` truncates). Note it is *not* Python `str.format` semantics, where `:8` is a minimum width and truncation is `:.8`. `keyTemplate` is expanded by pyrmts's own `substitute_key` (Python + TS), so we define `:N` = truncate, identically in both.

This abstracts over the shapes a project might want:

| Shape | Template | Notes |
|---|---|---|
| Human-readable (today) | `…/{period}.parquet` | No `{hash}` means legacy mutable keys, behavior unchanged. |
| **Human-readable + hash suffix (recommended for new pyramids)** | `…/{period}.{hash:12}.parquet` | Browsable R2 layout and greppable keys, plus immutability. |
| Pure content-addressed | `blobs/{hash}.parquet` | Dedupes identical shards across periods/pyramids. Keys are opaque, and GC needs the registry. |

In the suffix form the hash only has to be unique among versions of **one** `(tier, shard, period)` slot, not globally, so a short prefix suffices; the project picks N explicitly. Pure CA uses the bare (full) `{hash}`. The engine already computes `md5` per shard at flush (`ShardRecord.md5`), so the key is free.

Key parsing (listings for GC / adopt / fsck) must accept the hash group, `[0-9a-f]{N}` (or `{32}` for bare `{hash}`). It's fixed-width, so `substitute_key`'s inverse stays unambiguous. `N` outside `1..32` is a config error.

### Write protocol

1. Serialize the shard, compute its md5, and derive the key.
2. `put` the key. If it already exists, the content is identical by construction, so skip it (makes rebuilds idempotent for free).
3. Register: `INSERT OR REPLACE` the slot's row with the new `key`, `md5`, `n_bytes` and `written_at`. The previous key becomes an **orphan**.
4. Never delete the old blob inline; GC does it (below).

Every in-place rewriter adopts this: engine build/fill, `canonicalize_shards`, the cascade Lambda/Worker writers, and `consolidate`.

### Registry is the truth for "what's built"

- **Fill / fsck / reconcile** diff the expected cover against **registry rows**, not a LIST. `reconcile` shrinks to "adopt blobs that were written but whose registration was lost" (a crash between put and register): LIST, filter keys not referenced by any row, check the hash matches the content, register the newest per slot.
- **Readers are unchanged.** They already take `key` from the row.

### RG manifest

The manifest is keyed by `(pyramid, key)`. With hashed keys a key's bytes never change, so manifest rows can never go stale. `shard_written_at` drops out of the validity check (keep it as information only), and `manifest status`'s "stale" category disappears.

### GC

A periodic `gc` pass (the existing `gc.py` listing skeleton, extended) deletes objects under a pyramid prefix that:
- no registry row references, **and**
- are older than a grace period (default 24 h), which covers in-flight reads of the previous version and the edge cache TTL.

It runs a dry-run by default, with explicit `--apply`.

### Edge cache

Immutable keys make **shard fetches** cacheable forever. **Query responses** still depend on which keys the registry currently points at. Options:
- (a) derive a response `ETag` from the covered rows' keys (or a registry generation counter), and serve `max-age` short + revalidate;
- (b) keep `immutable`, but put a registry generation in the URL the FE requests (FE fetches `/api/registry/gen` once).

Pick one in the CFW package (`pyrmts-cfw`). Out of scope for the Python engine.

## Migration (lazy, per pyramid)

- Adding `{hash}` to a pyramid's `keyTemplate` affects **new writes only**. Existing rows keep their legacy keys and stay readable, and each slot flips to a hashed key the next time it's rewritten. No bulk rebuild.
- The template change and the registry-diff discovery must land together (fill must stop trusting LIST once keys can drift from the template).
- A pyramid without `{hash}` keeps today's semantics exactly, including the need to bump `written_at` on in-place rewrites. Warn when `canonicalize_shards` / rebuild runs against a hashless template.

## ctbk adoption

- Add `{hash:N}` to `configs/pyramids/rides-{start,end}.yaml` (e.g. `…/{period}.{hash:12}.parquet`) **before the P4 cutover**. After P4, `rides/` is prod-served, and in-place rewrites there would be prod corruption risks.
- Ideally combine with the `canonicalize-preserve-layout.md` re-run: the re-canonicalize pass writes hashed keys, and the manual `reconcile -f` rule goes away.
- Avail / smg pyramids adopt on their own schedule (their Lambda cascade writer needs the same write protocol).

## Open questions

- Should hashing cover the parquet bytes (depends on writer version / compression) or a canonical content digest? Bytes are simpler and are what the manifest actually depends on. Recommend bytes.
- GC grace: tie it to the edge cache TTL, and to the maximum query duration.

## Landed

### Phase 1 — key grammar, write protocol, registry lookup; engine + canonicalize adopt

- **Key grammar** (`pyrmts.keys` / `keys.ts`, twins with identical tests): `{hash}` = full md5, `{hash:N}` = first N (1..32); `validate_key_template` (config-time: `:N` only on `hash`, N in range — wired into `parse_pyramid_yaml` / `pyramid_from_config` and the TS `parsePyramidYaml`); `template_has_hash`; `substitute_key(values with hash)`; **`slot_key`** (everything but `{hash}` substituted — a slot's stable identity, equal to the storage key for a hashless template); `key_pattern` / `parse_key` / `slot_of` (the inverse for listings: fixed-width `[0-9a-f]{N}`, repeated placeholders back-referenced).
- **Write protocol**: `put_shard(storage, template, values, payload) → ShardWrite(key, md5, n_bytes, put)`: hashed template → key from the bytes, `put` only if absent, never overwrite; hashless → in place. `content_hash` = md5 hex.
- **Resolver**: `KeyResolver` protocol; `TemplateResolver` (refuses a hashed template with a pointer to the registry); `pyrmts_engine.shard_index.RegistryResolver` over any index with `lookup`.
- **Registry**: `ShardIndex` impls (Mem / Jsonl / StorageJsonl / D1) gain `lookup(tier, shard_dur, period_start_ms)`; `existing_keys()` now returns the *current* key per slot (last row wins = `INSERT OR REPLACE`).
- **Engine** (`build_local`): `ExpectedShard.key` is the slot key; `_write_shard` goes through `put_shard` and registers the returned key/md5/bytes; with a hashed template `fill` and `resume` take "built" from the **registry** (a LIST may hold several versions of a slot), and refuse a `shard_index` that cannot list; the source-coverage check compares slots. `WideShardSource` resolves a hashed slot to its one listed key and refuses a slot with several versions (GC the orphans, or read through the registry). Tests: keys derive from bytes and register; a byte-identical rebuild uploads nothing and re-points the registry; a fill trusts registry rows, not orphans.
- **`canonicalize_shards`**: finds the current shard through `resolver` (a hashed template requires `RegistryResolver` + `registry` + `pyramid_name`), writes through `put_shard` (new key, old blob untouched), registers the new row (atomic swap), and is idempotent (a second pass yields the same key, no new object). A hashless template warns that the rewrite is in place.
- **TS**: `planQuery` throws on a hashed template ("plan from the registry with `planQueryFromInventory`"); `shardKey` (gap discovery) yields slot keys.

### Phase 2 — every writer, registry-driven discovery, adopt + gc, CLI, ETag

- **Fill path** (`materialize.py` / `consolidate.py` / `discovery.py`): `KeySet` — what the fill knows as "present": a set of *slot keys* with, under a hashed template, each slot's current storage key (`key(slot)` is what to `get`). `discover_gaps(registry_keys=)`: with a hashed template the registry says what is built (a LIST only supplies mtimes for staleness; orphans it holds are `gc` / `adopt`'s business) and it refuses to run without one; `run_extension_fill` / `run_single_gap` pass the registry's current keys, skip the HEAD "exists" probe (a slot key is not an object), and register the *written* key (`MaterializeResult.key`). Cover-tile reads go through `KeySet.key`. `emit_d1_insert_sql` emits the written key.
- **`cascade_tiers`** and **`TipWriter`**: `resolver` / `registry` / `pyramid_name` like `canonicalize_shards`; sources found through the resolver, outputs through `put_shard`, registered on write; a hashed template refuses to run without a registry. A tip append writes a new key and swaps the row; the previous tip is an orphan.
- **`pyrmts_engine.gc`**: `list_orphans` (listed keys the template can produce that no registry row references), `gc_orphans(grace=24h, apply=False)` (dry-run by default; never deletes a blob without an mtime), `adopt_unregistered(pyramid, index, name, range)` (for each expected slot with no row, register the newest listed version whose bytes hash to its key). `run_extension_fill(reconcile=True)` under a hashed template runs `adopt_unregistered` instead of `reconcile_registrations`.
- **`WideShardSource`** and the other template-derived readers (`validate.py`, `multiscan_driver.py` still use `slot_key` semantics; `listed_slots(storage, template)` is the LIST-based resolver they can use, refusing a slot with several versions).
- **CLI**: `canonicalize -i/--index MANIFEST -n/--pyramid-name` (JSONL path or `s3://bucket/key`; required for a hashed template); `pyrmts-engine gc -i MANIFEST [-G hours] [-a/--apply] CONFIG`; `pyrmts-engine adopt -i MANIFEST -n NAME -r RANGE CONFIG`.
- **Edge cache**: `keysEtag(keys)` in `pyrmts` (TS): an order-independent, versioned ETag from the keys a response was built from — option (a). The consumer's route serves it with a short `max-age` and revalidation; a registry swap changes the tag.

### Review fixes (ctbk's review of 813c812)

- **Importability**: 813c812 had staged with `git add -u`, leaving `pyrmts_engine/gc.py` and `keys.test.ts` untracked (any clean checkout failed to import); `source.py` also carried a duplicated `WideShardSource`. Fixed in f7e75f8; a `ci.yml` now runs the Python suite on push (the dist build alone never did).
- **Lazy migration is lazy**: registry rows map to slots by their own fields (`slot_for_record`: tier, shard_dur, period_start → `slot_key`, with `slot_of_any` — hashed *or legacy* key shape — as the fallback for templates with extra placeholders). A pyramid that gains `{hash:N}` sees every legacy row as built and rebuilds nothing; only a genuinely missing slot is built, at a hashed key (test: `test_adding_a_hash_token_to_a_legacy_pyramid_rebuilds_nothing`). `legacy_template` states the assumption: the legacy shape is the template minus the hash token and one adjacent `.`/`-`/`_`.
- **GC safety**: `gc_orphans` refuses a hashless template, a template with no slot placeholders (pure `blobs/{hash}` is shareable across pyramids — not supported), and an empty registry; only hashed-shape keys are ever candidates; a blob without an mtime is never deleted; and the registry is re-read right before deleting so a slot re-pointed at an old orphan (`put_shard` reuses an existing identical object without touching its mtime) is kept (`kept_repointed`). The residual window is the instant between that re-read and the delete — keep `grace` above a fill's duration. Registries: JSONL, `s3://`, or `d1://<database-id>[/<table>]`, always pyramid-scoped (`-n`).
- **Registry scoping**: rows collapse by `(pyramid, tier, shard_dur, period_start)` — the D1 PK — so a shared manifest keeps every pyramid's keys; `lookup(..., pyramid=)` / `RegistryResolver(index, pyramid)` are scoped, and an unscoped lookup that hits several pyramids is refused. Multi-tenant layouts with a dim in the key use one pyramid name per tenant, as D1 already requires. `JsonlShardIndex` parses its file once per instance.
- **Truncated hash as identity**: `{hash:N}` needs N ≥ 8, one token per template, and the full 32 when the hash is the only placeholder; on a HEAD hit `put_shard` compares the backend's etag with the payload's md5 when the etag is an md5 (S3/R2 single-part) and raises on a mismatch instead of registering a key whose bytes differ from the recorded md5.
- **Source resolution**: `WideShardSource(registry=, pyramid_name=)` resolves slots through the registry (unaffected by orphans awaiting GC), falling back to the listing — hashed or legacy shape — for slots without a row (ingest tiles); the map is computed once per source. `build` passes its manifest as the registry.
- **TS**: `tileFromExisting` and `listExpectedShards` refuse a hashed template like `planQuery`; `keysEtag` takes `{ key, md5?, writtenAt? }` entries so a legacy (mutable) key's rewrite changes the tag during a migration, and is 64-bit.
- **adopt** also heals a rewrite whose registration was lost (a hashed blob newer than the slot's row); **canonicalize** reports `unchanged` (no re-registration, no `written_at` bump) when the bytes and the row already match.
- Parity: the Python grammar is ASCII (`[A-Za-z0-9_]`, `[0-9]`) and anchored with `\Z`, like the TS regex.
- Not changed: `D1ShardIndex.lookup` is one query per slot (a batched `lookup_many` is a follow-up if canonicalize over D1 is slow); `run_extension_fill(reconcile=True)` still lists once per tick, as `reconcile_registrations` did.

## Consumer follow-ups

- **RG manifest** (ctbk): drop `shard_written_at` from the validity check once the pyramid's template carries `{hash}`; the manifest is keyed by `(pyramid, key)` and a key's bytes never change.
- **CFW cascade writer** (the base's `pyrmts-cfw` route that writes shards): adopt the same protocol — derive the key from the bytes, put-if-absent, `INSERT OR REPLACE` the row — the TS twin of `put_shard` is a few lines over `substituteKey({ ..., hash })` and R2's `onlyIf`; not built here because no `pyrmts-cfw` route writes shards today.
- **Multi-scan archives** (`multiscan_driver`): archive keys have their own scheme (`{period}--{first_scan_label}`) and are sealed-not-appended already; unaffected.

# Content-hashed shard keys + registry-as-truth

Status: proposed (2026-09-24, from ctbk's rides re-key). Companion: `canonicalize-preserve-layout.md`.

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

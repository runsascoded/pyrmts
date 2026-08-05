# First-class shard invalidation: repair past intervals via the fsck loop

Status: **implemented, pending ctbk burn-in** (spec 2026-07-30, ctbk session; impl 2026-08-04, pyrmts session). Companion consumer spec: ctbk `specs/shard-invalidation-adoption.md` (trigger surfaces, edge-cache versioning, `RAW_FINALITY_S` retirement — the acceptance criterion for this whole effort).

**pyrmts status (2026-08-04): implemented as specced** (journal, not registry-column — see below for why the choice hardened). Landed:

- `pyrmts` (py core): `Storage` grew the CAS + mtime primitives — `get_with_etag`, `put_if_match(key, data, etag)` (etag `None` = create-only / If-None-Match:*; raises `EtagConflict`), `list_with_mtime(prefix)` — on all three backends (`MemStorage` gains a `clock` kwarg + tracked mtimes for tests; `S3Storage` uses `IfMatch`/`IfNoneMatch` conditional puts and the paginator's `LastModified`; `FsStorage` best-effort compare-then-write). `EtagConflict` exported. The engine's `list_existing_with_mtime` now just calls `storage.list_with_mtime` (drops the `_client` sniffing).
- `pyrmts_engine.invalidation` (new module): `Invalidation` dataclass, `journal_key` (`<prefix>_invalidations.json`), `load_invalidations` → `(entries, etag)`, `invalidate(pyramid, interval, *, now=None) -> int`, `stale_keys_for(expected, mtimes, invalidations)`, `prune_spent(pyramid, expected)`. All journal writes are etag-CAS'd with bounded retry — an admin append racing the fill driver's prune can never be dropped (the spec's "lost append is re-issuable" wave-off is closed properly instead). The journal is emptied in place, never deleted: object deletes can't be conditional, so delete-vs-append races would lose appends.
- `discover_gaps(invalidations=…)`: expected shards overlapping an entry newer than their mtime are excluded from `existing` → they join the gap list (same mechanics as `stale_before`'s `split_stale`, interval-scoped). Expected-only scoping per the spec: stray/superseded keys are never touched.
- `run_extension_fill(honor_invalidations=True)` (default on): loads the journal, threads entries into discovery, overwrites only the overlapped keys in place (per-key `overwrite_keys` HEAD-guard bypass — non-invalidated gaps keep their idempotency guard, unlike the global `stale_before` path), and prunes spent entries after the fill (re-listing fresh mtimes).
- CLI: `pyrmts-engine invalidate -r <from-iso>/<to-iso> <config>`.
- Tests (`test_invalidation.py` + `test_storage.py` CAS coverage): overlap staleness with edge-touching exclusion, journal append/roundtrip + CAS-retry-preserves-concurrent-append, end-to-end repair (late datum → invalidate → fine-before-coarse in-place rebuild, byte-identical to a from-scratch build with the datum, journal pruned, stray key + every other object untouched — bytes and mtimes — idempotent re-run), and changed-md5 re-registration (same key, fresh md5).

Deviations from the sketch: `invalidate` takes just `(pyramid, interval, *, now)` — the sketch's `pyramid_name`/`storage` params were redundant (`pyramid` carries storage; the journal key derives from `keyTemplate`); and `invalidate` appends only — pruning needs the expected-cover context only the fill driver has, so "append+prune" lives there. Known edge (accepted): a stale shard whose rebuild returns `empty` (zero rows) isn't rewritten, so its entry is retained and retried each tick — repairs exist because new data landed, so this shouldn't occur in practice.

Note for ctbk (journal-vs-registry): the D1 split-brain incident (`docs/incidents/2026-07-28-d1-rest-split-brain.md`) settles the registry-column alternative for good — invalidation state must live next to the shards in R2 (which never forked), not in D1. Also: R2's conditional-write (If-Match → 412) path should get a live smoke test before the journal takes prod traffic — scaffolding specced in utz (`~/c/utz/specs/s3-live-tests.md`).

**Gate cleared (2026-08-05, ctbk session): R2 If-Match live smoke green.** utz's live suite passed 8/8 against R2 (`s3://ctbk/tmp/utz-s3-tests`, profile `cf`), including the stale-etag conflict and `If-None-Match: *` create-race paths; a direct probe confirmed R2 surfaces `PreconditionFailed` / HTTP 412 on both — byte-identical to AWS, no except-arm changes needed. Findings recorded in the utz spec. **pyrmts is clear to push `main` → `r/main`** (the 6 local commits: invalidation impl, source-readiness done-move, raw-ingest spec + `TiledSource` chassis) and record the new `dist` SHA for ctbk re-pins.

## Motivation

Consumers occasionally get **new data for an already-built interval**: a WAL minute recovered from a secondary source, a backfill correcting a bad ingest window, a redundant poller landing a minute late, or a vocab/denorm repair that changes how old raw rows expand. Today the engine has only two blunt tools:

- `run_extension_fill(stale_before=…)` — a *global* timestamp cutoff: every shard last-built before `stale_before` is rebuilt in place. Right shape (rebuild-in-place, dependency-ordered, no serving gap) but wrong granularity — repairing one minute shouldn't rebuild the world.
- ctbk's `RAW_FINALITY_S = 15*60` wait-branch — the finest-tier `raw_fill` returns `None` for a missing-but-recent raw minute so the build retries next tick, then permanently skips it past 15 min. This conflates two concerns: (a) don't race an in-flight write (needs ~2 min, not 15), and (b) "declare the datum lost" — which stops being a needed concept once late arrivals can repair built shards.

Goal: `invalidate(interval)` as an engine primitive — mark every built shard overlapping `[s, e)` stale; the existing fsck/extension-fill tick rebuilds them **in place**, dependency-ordered (fine → coarse, so coarse rebuilds read repaired fine tiles), re-registering with new md5s. Build-immediately-with-what's-present becomes the only build policy; late data repairs instead of being raced or dropped.

## Design

### Invalidation journal (proposed; registry-column alternative below)

A small JSON doc per pyramid, `<pyramid_prefix>/_invalidations.json`: a list of `{start, end, requested_at}` entries (epoch seconds, `requested_at` = wall-clock of the invalidation request).

- **`invalidate(pyramid, interval, *, storage, now=None)`**: append the entry (read-modify-write; single-writer assumption is fine — requests come from an admin CLI or a cron-serialized worker, and a lost concurrent append is re-issuable).
- **Discovery integration**: `discover_gaps` (or a thin wrapper) loads the journal and, for each entry, treats any *existing* shard whose period overlaps `[start, end)` and whose storage mtime (or registry `built_at`) is `< requested_at` as a gap — same mechanics as `stale_before`'s `split_stale`, scoped to the overlap instead of global. Rebuilds are in-place: key unchanged, content replaced, registration refreshed (md5/bytes/`written_at`).
- **Journal GC**: an entry is spent once no overlapping shard predates its `requested_at`; the fill driver prunes spent entries when it writes. Idempotent by construction — replaying a spent entry finds nothing stale.
- **Ordering**: `sort_by_dependency` already yields fine→coarse; in-place stale rebuilds join the same sorted queue, so a coarse consolidation never rebuilds before the fine tiles it reads. Mid-repair, coarse shards briefly serve pre-repair content (minutes) — no availability gap, monotone convergence.

### Registry-column alternative

Add `invalidated_at` to `pyramid_shards`; `invalidate` = one `UPDATE … WHERE period overlaps AND built_at < ?`. Cleaner queries, but a schema migration on a table with live single-registrar semantics, and discovery currently diffs against *storage* listings (registry rows are reconciled after the fact) — the journal keeps the source of truth in R2 next to the shards and works even for shards that exist but aren't yet registered. ctbk's weak preference: journal. Pyrmts session's call.

### What this does NOT change

- Superseded-but-present shards (GC backlog) overlapping the interval also match the staleness test and get rebuilt or (better) are left to GC — cheapest correct rule: the fill driver rebuilds only shards in the current min-cover + rungs; superseded relics are excluded from serving plans already and GC collects them. Spec point for implementation: staleness applies to *expected* shards only (`listExpectedShards` ∩ journal overlap), not every key on storage.
- Shard identity/keys: unchanged. Content-versioning for caches is consumer-side (see companion spec) — the registry md5 update is the version signal.
- Monoid semantics: a rebuilt shard is just a re-run of the same declarative build over (now-larger) inputs — byte-reproducibility properties are preserved.

## API sketch

```python
def invalidate(pyramid, interval: tuple[datetime, datetime], *, pyramid_name, storage, now=None) -> int: ...  # returns # of entries after append+prune
# run_extension_fill grows: honor journal entries during discovery (default on);
# `stale_before` stays for the global case (vocab refreshes).
```

Tests to lock: overlap staleness (edge-touching periods excluded), fine-before-coarse rebuild order, journal prune on spent entries, idempotent re-run, in-place re-registration (md5 changes, key doesn't), expected-only scoping (superseded keys untouched), byte-identity of a repaired shard vs a from-scratch build with the late datum present.

## Acceptance

ctbk retires `RAW_FINALITY_S` (or shrinks it to a ~2 min anti-race grace): builds proceed with whatever raw minutes exist; a late-landing minute triggers `invalidate` of its 1-minute interval and the next 5-min tick repairs the ≤15 covering fine shards. The "declared lost" concept disappears — absent minutes are just absent, repaired iff their datum ever lands.

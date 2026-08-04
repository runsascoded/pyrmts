# First-class shard invalidation: repair past intervals via the fsck loop

Status: **open** (2026-07-30, ctbk session). Companion consumer spec: ctbk `specs/shard-invalidation-adoption.md` (trigger surfaces, edge-cache versioning, `RAW_FINALITY_S` retirement — the acceptance criterion for this whole effort).

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

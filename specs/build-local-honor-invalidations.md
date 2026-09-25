# `build_local -f` must honor the invalidation journal

## Problem

The shard-invalidation journal (`<prefix>_invalidations.json`, `pyrmts.invalidation.invalidate`) is consumed only by the extension-fill driver (`consolidate.run_extension_fill(honor_invalidations=True)` → `discover_gaps(invalidations=…)` → `stale_keys_for`, then `prune_spent`). `engine.build_local` — what `pyrmts-engine build -f` and every Batch fill run — never reads it: under `fill`, "built" is the listing (hashless) or the registry (hashed template), full stop.

ctbk's monthly rides cadence depends on it. Each new month `M`, rides that started in `M-1` but ended in `M` only become attributable once `M`'s source is published, so `ctbk gbfs rides-extend` journals `[M-1, M)` on the start anchor and then runs `pyrmts-engine batch submit … -f`. The fill ignores the entry, so the spillback never lands.

Evidence:
- ctbk's rides-v5 July 2026 start shards were missing exactly the 1,420 July starts published in `normalized/202608.parquet` (ctbk `specs/rides-rekey.md`, validation gate), even though `rides-v5-extend 202608` had journaled `[2026-07-01, 2026-08-01)`.
- 2026-09-25 `rides-extend 202608` on the hashed `rides/start` pyramid: journal entry written, and the Batch fill logged `277 expected shards, 267 present, 10 missing, 0 fillable` (the 10 being open-period deferrals). Nothing was rebuilt.

## Fix

In `build_local`, when `fill` is set (and whenever `honor_invalidations`, default on, mirroring `run_extension_fill`):

1. `load_invalidations(pyramid)`.
2. Treat every **built** expected slot whose period overlaps an entry and whose build predates the entry (`requested_at` > the slot's write time) as missing, so it is rebuilt:
   - hashless template: write time = the listing mtime (what `stale_keys_for` already uses);
   - hashed template: write time = the registry record's `written_at_ms` (the listing holds orphans; the registry is the truth). The rebuild writes a new hashed key and registers it, so the swap is atomic per slot as usual.
3. After a successful build, `prune_spent` the entries whose overlapping slots were all rebuilt (same semantics as the extension-fill driver), CAS'd.
4. Log the count, like `discover_gaps` does (`invalidated: N built shards overlap journal …`).

Coarse shards overlapping the entry rebuild too (e.g. `1mo@2y` covering July). That's the point: the refold has to reach every rung, the way it does on the Lambda path.

## Tests

- Hashless: build, journal an overlapping range, `build -f` → overlapping shards rebuilt (new mtime / content), non-overlapping untouched, entry pruned.
- Hashed: same, asserting new keys registered for exactly the overlapping slots, and old keys left as orphans (for `gc`).
- An entry requested **before** a slot's last write doesn't rebuild it (already refolded).
- Open-period deferral still applies: an overlapping slot whose source is absent defers, and its entry is **not** pruned.

## Consumer

ctbk will run `rides-extend` for 202609 around mid-October 2026 and needs this pinned before then. No ctbk code changes: it already journals and fills.

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

## Landed

- **`build_local(fill=True, honor_invalidations=True)`** (default on). After the done-set is computed (listing ∪ manifest, or the registry for a hashed template), `load_invalidations` + `stale_keys_for(plan.outputs, write_times, invs)` removes every built slot overlapping an entry newer than its last write, so it joins `missing` and is rebuilt. Coarse rungs included, and the open/closed source-tile checks (deferral, held) apply to these slots exactly as to never-built ones. `BuildResult.invalidated` counts them; the log line is `invalidations: N journal entries; M built shards overlap an entry newer than their build → rebuild`.
- **Write times: `pyrmts_engine.invalidation.slot_write_times`**, keyed by slot key. Hashless = listing mtime. Hashed = the registry row's `written_at_ms`, not the listing: besides orphans, an identical-bytes rebuild reuses the existing object (`put_shard` is put-if-absent), so its mtime never advances; only re-registration marks the slot rebuilt.
- **Prune** after a successful build (no prune if the build raised), CAS'd via `prune_spent`, with the slots written this run marked fresh. Two guards the extension-fill driver lacked: `prune_spent(within=(from, to))` keeps an entry not contained in the fill's range (slots it overlaps outside were never considered), and a fill with a `filter` never prunes (the journal lives under the template's static prefix, shared by every filter value).
- **CLI:** `build -I/--ignore-invalidations` and `batch submit -I` (passed through by `batch.build_command`).

### Also fixed: the extension-fill path on hashed templates

The spec named `run_extension_fill` as the working reference. On a hashed template it had two bugs:

- `discover_gaps` took staleness from the current key's listing mtime, so an identical-bytes repair left the slot stale forever. It now uses `slot_write_times` (registry `written_at`).
- `prune_spent` re-listed storage, keyed by storage key, while expected shards carry slot keys. Every lookup missed, every entry looked spent, and all were pruned, including ones whose slots were never rebuilt. It now passes registry-derived write times and `within=(genesis, now)`. `prune_spent` refuses (raises) rather than listing when given a hashed template without write times.

### Tests

`pyrmts_engine/tests/test_fill_invalidation.py` (9): hashless rebuild of exactly the overlapping shards across rungs, entry pruned, second fill a no-op; an entry requested before the last write rebuilds nothing and is pruned; `honor_invalidations=False`; an entry straddling `to` rebuilt in range but kept; hashed identical-bytes rebuild re-registers exactly the overlapping slots; hashed changed-content rebuild writes new keys and orphans the old ones; an overlapping shard over an absent open tile defers and its entry is kept while a sibling entry is pruned; a filtered fill rebuilds but doesn't prune; `run_extension_fill` on a hashed template keeps an entry it didn't rebuild. Plus `-I` in `test_batch.py`. Python suite 448 passed.

### Cost note for consumers

A rebuilt coarse slot re-walks its whole effective span from the source (`build_local` builds every tier from the base stream). Only coarse slots in the expected min-cover are touched: closed history tiled by max-rung tiles, and trailing smaller rungs near `to`.

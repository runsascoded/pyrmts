// First-class shard invalidation (JS twin of
// `python/pyrmts_engine/src/pyrmts_engine/invalidation.py`; see
// `specs/shard-invalidation.md`).
//
// Mark every built shard overlapping an interval stale; the next fill
// tick (whichever driver the consumer runs) rebuilds them **in place**,
// dependency-ordered (fine → coarse), and prunes spent entries.
//
// The journal is a small JSON doc next to the shards
// (`<pyramid_prefix>_invalidations.json`): a list of
// `{start, end, requestedAt}` entries (epoch seconds). It lives in
// object storage — not the registry — because storage is the source of
// truth for discovery (registry rows are reconciled after the fact,
// and D1 has forked before: ctbk
// `docs/incidents/2026-07-28-d1-rest-split-brain.md`).
//
// All journal writes are etag-CAS'd (`Storage.putIfMatch`) with a
// bounded retry, so a fill-driver prune racing an admin-CLI append
// can never drop the append. The journal is emptied in place, never
// deleted — object deletes can't be made conditional, so a delete
// racing an append could lose it.
//
// Note vs Python: `pyrmts.Pyramid.storage` in JS is a **row-level**
// `StorageBackend` (parquet/D1 backends returning rows), whereas
// Python's `Pyramid.storage` is byte-level `Storage`. So this module
// takes byte-level `storage: Storage` as an explicit parameter rather
// than reading it off the pyramid — the `Pyramid` reference is only
// used for its `keyTemplate` (to derive the journal key).
import { EtagConflict, NotSupported } from './types.js';
export const JOURNAL_BASENAME = '_invalidations.json';
export const CAS_ATTEMPTS = 5;
// `_invalidations.json` under the keyTemplate's static prefix — next to
// the shards, under the same LIST namespace (inert for gap diffing: it
// never matches an expected key).
export function journalKey(pyramid) {
    return pyramid.keyTemplate.split('{')[0] + JOURNAL_BASENAME;
}
function encode(invs) {
    const doc = invs.map(inv => ({
        start: inv.start.getTime() / 1000,
        end: inv.end.getTime() / 1000,
        requested_at: inv.requestedAt.getTime() / 1000,
    }));
    return new TextEncoder().encode(JSON.stringify(doc) + '\n');
}
function decode(bytes) {
    const doc = JSON.parse(new TextDecoder().decode(bytes));
    return doc.map(e => ({
        start: new Date(e.start * 1000),
        end: new Date(e.end * 1000),
        requestedAt: new Date(e.requested_at * 1000),
    }));
}
function requireCas(storage) {
    const { getWithEtag, putIfMatch } = storage;
    if (!getWithEtag || !putIfMatch) {
        throw new NotSupported('invalidation: storage backend must implement getWithEtag + putIfMatch');
    }
    return { getWithEtag, putIfMatch };
}
// `[entries, etag]` — etag for CAS'ing a subsequent rewrite (`null`
// when the journal doesn't exist yet, i.e. create-only).
export async function loadInvalidations(pyramid, storage) {
    const { getWithEtag } = requireCas(storage);
    const [bytes, etag] = await getWithEtag(journalKey(pyramid));
    if (bytes === null)
        return [[], etag];
    return [decode(bytes), etag];
}
// Half-open interval overlap — edge-touching periods are excluded.
export function overlaps(inv, shard) {
    return shard.periodStart < inv.end && inv.start < shard.periodEnd;
}
// Append `[start, end)` to the pyramid's invalidation journal; the next
// extension-fill tick rebuilds every overlapping built shard in place.
// Returns the journal entry count after the append. (Spent entries are
// pruned by the fill driver, which has the expected-cover context this
// function lacks — deviation from the spec sketch's "append+prune".)
export async function invalidate(pyramid, storage, interval, opts = {}) {
    const [start, end] = interval;
    if (!(start < end)) {
        throw new Error(`invalidate: empty interval [${start.toISOString()}, ${end.toISOString()})`);
    }
    const { putIfMatch } = requireCas(storage);
    const entry = {
        start,
        end,
        requestedAt: opts.now ?? new Date(),
    };
    const key = journalKey(pyramid);
    for (let attempt = 0; attempt < CAS_ATTEMPTS; attempt++) {
        const [invs, etag] = await loadInvalidations(pyramid, storage);
        try {
            await putIfMatch(key, encode([...invs, entry]), etag);
        }
        catch (e) {
            if (e instanceof EtagConflict) {
                if (attempt === CAS_ATTEMPTS - 1)
                    throw e;
                continue;
            }
            throw e;
        }
        return invs.length + 1;
    }
    throw new Error('invalidation: unreachable');
}
// Keys of expected shards that exist on storage and are overlapped by a
// journal entry newer than their last build. Staleness applies to
// EXPECTED shards only — superseded/stray keys are GC's concern, not
// the fill's. Unknown mtimes are fresh (backends that can't report
// mtimes shouldn't trigger rebuilds — same rule as Python
// `split_stale`).
export function staleKeysFor(expected, mtimes, invalidations) {
    if (invalidations.length === 0)
        return new Set();
    const out = new Set();
    for (const e of expected) {
        const mtime = mtimes.get(e.key);
        if (mtime === undefined || mtime === null)
            continue;
        for (const inv of invalidations) {
            if (overlaps(inv, e) && mtime < inv.requestedAt) {
                out.add(e.key);
                break;
            }
        }
    }
    return out;
}
// Drop journal entries with no remaining stale overlap (idempotent by
// construction: replaying a spent entry finds nothing stale). Called
// by the fill driver after it writes. Fresh mtimes are re-listed
// unless provided. Returns `[nPruned, nRemaining]`.
export async function pruneSpent(pyramid, storage, expected, opts = {}) {
    const { putIfMatch } = requireCas(storage);
    const mtimes = opts.mtimes ?? await listExistingWithMtime(pyramid, storage);
    const key = journalKey(pyramid);
    for (let attempt = 0; attempt < CAS_ATTEMPTS; attempt++) {
        const [invs, etag] = await loadInvalidations(pyramid, storage);
        if (invs.length === 0)
            return [0, 0];
        const keep = invs.filter(inv => staleKeysFor(expected, mtimes, [inv]).size > 0);
        if (keep.length === invs.length)
            return [0, invs.length];
        try {
            await putIfMatch(key, encode(keep), etag);
        }
        catch (e) {
            if (e instanceof EtagConflict) {
                if (attempt === CAS_ATTEMPTS - 1)
                    throw e;
                continue;
            }
            throw e;
        }
        return [invs.length - keep.length, keep.length];
    }
    throw new Error('invalidation: unreachable');
}
// Helper: LIST the storage under the keyTemplate's static prefix,
// collect `{key: mtime}`. Uses `listWithMtime` if the backend has it;
// otherwise falls back to `list()` with `null` mtimes (treated as
// fresh — backends that can't report mtimes shouldn't trigger
// rebuilds). Mirrors Python `list_existing_with_mtime`.
export async function listExistingWithMtime(pyramid, storage) {
    const prefix = pyramid.keyTemplate.split('{')[0];
    const out = new Map();
    if (storage.listWithMtime) {
        for await (const [key, mtime] of storage.listWithMtime(prefix)) {
            out.set(key, mtime);
        }
    }
    else {
        for await (const key of storage.list(prefix)) {
            out.set(key, null);
        }
    }
    return out;
}
//# sourceMappingURL=invalidation.js.map
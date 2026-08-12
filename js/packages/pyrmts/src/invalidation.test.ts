// JS twin of `python/pyrmts_engine/tests/test_invalidation.py` — the
// primitives (`invalidate` journal append + CAS, `staleKeysFor` overlap
// staleness, `pruneSpent` GC) are exercised in isolation here. The full
// end-to-end fill-and-repair loop lives in Python (there's no JS engine
// port yet); the fold-into-listMissingShards path is covered too.

import { beforeEach, describe, expect, test } from 'vitest'

import { listExpectedShards, listMissingShards, type ExpectedShard } from './gap-discovery.js'
import {
  CAS_ATTEMPTS,
  invalidate,
  journalKey,
  loadInvalidations,
  overlaps,
  pruneSpent,
  staleKeysFor,
  type Invalidation,
} from './invalidation.js'
import { memStorage } from './storage.js'
import { EtagConflict } from './types.js'
import type { Pyramid, Storage } from './types.js'

function utc(y: number, m: number, d: number, h = 0, min = 0): Date {
  return new Date(Date.UTC(y, m - 1, d, h, min))
}

const T18 = utc(2026, 1, 2, 18)
const T_INIT = utc(2026, 1, 2, 19)       // mtime of the genesis build
const T_REQ = utc(2026, 1, 2, 21)        // invalidation requested
const T_REBUILD = utc(2026, 1, 2, 22)    // mtime of the repair tick

// The late-arriving window: inside q@2h [08,10) and h@3h [09,12);
// edge-touching h@3h [06,09) (ends exactly at 09:00) must NOT match.
const W_START = utc(2026, 1, 2, 9)
const W_END = utc(2026, 1, 2, 9, 30)
const FROM = utc(2026, 1, 2, 0)

const Q_KEY = 'pyr/q/2h/2026-01-02T08.parquet'
const H_KEY = 'pyr/h/3h/2026-01-02T09.parquet'
const J_KEY = 'pyr/_invalidations.json'

function makeLadder(storage: Storage): Pyramid {
  return {
    storage: { fetch: async () => { throw new Error('not used') } } as Pyramid['storage'],
    keyTemplate: 'pyr/{tier}/{shard}/{period}.parquet',
    axis: 'time',
    binCol: 'ts',
    dims: [],
    metrics: [{ name: 'n', monoid: 'count' }],
    tiers: [
      { name: 'q', bin: '15min', shards: ['2h'] },
      { name: 'h', bin: '1h', shards: ['3h'] },
    ],
    // `storage` on Pyramid is `StorageBackend` (row-level); byte-level
    // `storage` is passed to invalidation calls explicitly. Attach here
    // as a convenience for the test to keep them together.
    ..._byteStorageHolder(storage),
  }
}

// Attach byte-level storage on a Symbol-keyed side channel so tests
// pass one object around and pluck `storage` back out with `_bs(p)`.
// (Only for tests — production consumers pass `storage` explicitly.)
const BS = Symbol.for('pyrmts.test.byteStorage')
function _byteStorageHolder(storage: Storage): Record<symbol, Storage> {
  return { [BS]: storage }
}
function _bs(pyramid: Pyramid): Storage {
  return (pyramid as unknown as Record<symbol, Storage>)[BS]!
}

describe('journalKey', () => {
  test('is `<static-prefix>_invalidations.json`', () => {
    const pyr = makeLadder(memStorage())
    expect(journalKey(pyr)).toBe(J_KEY)
  })

  test('respects the pyramid.keyTemplate prefix', () => {
    const pyr: Pyramid = {
      ...makeLadder(memStorage()),
      keyTemplate: 'awair-{device_id}/{tier}/{shard}/{period}.parquet',
    }
    // Static prefix stops at the first `{`, so device-scoped templates
    // still yield one journal per pyramid (not per device).
    expect(journalKey(pyr)).toBe('awair-_invalidations.json')
  })
})

describe('overlaps', () => {
  // Same edge-touching exclusion as Python: half-open `[start, end)`.
  test('half-open interval overlap', () => {
    const shard = (s: Date, e: Date): ExpectedShard => ({
      tier: 'x', shardDur: '1h', periodStart: s, periodEnd: e,
      effectiveStart: s, effectiveEnd: e, key: 'k',
    })
    const inv: Invalidation = { start: W_START, end: W_END, requestedAt: T_REQ }
    expect(overlaps(inv, shard(utc(2026, 1, 2, 9), utc(2026, 1, 2, 10)))).toBe(true)
    // Ends exactly at inv.start → no overlap (edge-touching excluded).
    expect(overlaps(inv, shard(utc(2026, 1, 2, 8), W_START))).toBe(false)
    // Starts exactly at inv.end → no overlap.
    expect(overlaps(inv, shard(W_END, utc(2026, 1, 2, 10)))).toBe(false)
    // Fully outside.
    expect(overlaps(inv, shard(utc(2026, 1, 2, 10), utc(2026, 1, 2, 11)))).toBe(false)
  })
})

describe('staleKeysFor', () => {
  // Only shards genuinely overlapping the interval — and built before
  // `requestedAt` — are stale; edge-touching periods are excluded, as
  // are unknown mtimes and post-request builds. JS twin of
  // `test_overlap_staleness_edge_touching`.
  test('overlap × pre-request mtime; edge-touching / unknown / post-request excluded', () => {
    const pyramid = makeLadder(memStorage())
    const expected = listExpectedShards(pyramid, { from: FROM, to: T18 })
    const inv: Invalidation = { start: W_START, end: W_END, requestedAt: T_REQ }
    const mtimes = new Map(expected.map(e => [e.key, T_INIT as Date | null]))
    expect(staleKeysFor(expected, mtimes, [inv])).toEqual(new Set([Q_KEY, H_KEY]))
    // Built after the request → fresh.
    const postMtimes = new Map(expected.map(e => [e.key, T_REBUILD as Date | null]))
    expect(staleKeysFor(expected, postMtimes, [inv])).toEqual(new Set())
    // Unknown mtimes → fresh (can't confirm staleness).
    const nullMtimes = new Map(expected.map(e => [e.key, null as Date | null]))
    expect(staleKeysFor(expected, nullMtimes, [inv])).toEqual(new Set())
    // No invalidations → nothing stale.
    expect(staleKeysFor(expected, mtimes, [])).toEqual(new Set())
  })
})

describe('invalidate + loadInvalidations', () => {
  // Journal append/roundtrip + reject empty interval. JS twin of
  // `test_invalidate_appends_and_journal_roundtrip`.
  test('appends then round-trips via loadInvalidations', async () => {
    const pyramid = makeLadder(memStorage())
    const storage = _bs(pyramid)
    expect(await invalidate(pyramid, storage, [W_START, W_END], { now: T_REQ })).toBe(1)
    const t2 = utc(2026, 1, 2, 21, 5)
    expect(await invalidate(pyramid, storage, [utc(2026, 1, 2, 12), utc(2026, 1, 2, 13)], { now: t2 })).toBe(2)
    const [invs, etag] = await loadInvalidations(pyramid, storage)
    expect(invs).toEqual([
      { start: W_START, end: W_END, requestedAt: T_REQ },
      { start: utc(2026, 1, 2, 12), end: utc(2026, 1, 2, 13), requestedAt: t2 },
    ])
    expect(etag).not.toBeNull()

    // Encoded shape: each entry `{start, end, requested_at}` (snake_case
    // on the wire — matches Python + spec/`shard-invalidation.md`) as
    // epoch seconds.
    const raw = await storage.get(J_KEY)
    expect(raw).not.toBeNull()
    expect(JSON.parse(new TextDecoder().decode(raw!))).toEqual([
      { start: W_START.getTime() / 1000, end: W_END.getTime() / 1000, requested_at: T_REQ.getTime() / 1000 },
      { start: utc(2026, 1, 2, 12).getTime() / 1000, end: utc(2026, 1, 2, 13).getTime() / 1000, requested_at: t2.getTime() / 1000 },
    ])
  })

  test('rejects an empty interval', async () => {
    const pyramid = makeLadder(memStorage())
    await expect(
      invalidate(pyramid, _bs(pyramid), [W_END, W_START], { now: T_REQ }),
    ).rejects.toThrow(
      'invalidate: empty interval [2026-01-02T09:30:00.000Z, 2026-01-02T09:00:00.000Z)',
    )
  })
})

describe('invalidate: CAS retry preserves concurrent append', () => {
  // A prune/append racing this append conflicts the CAS; the retry
  // re-reads and lands on top — the concurrent entry is never dropped.
  // JS twin of `test_invalidate_cas_retry_preserves_concurrent_append`.
  test('losing the first CAS lands on top of the concurrent write', async () => {
    const other: Invalidation = {
      start: utc(2026, 1, 2, 3), end: utc(2026, 1, 2, 4), requestedAt: T_REQ,
    }
    const base = memStorage()
    // Wrap the mem storage: on the first `putIfMatch` for the journal
    // key, land the concurrent entry first, then invalidate our etag.
    let raced = false
    const racing: Storage = {
      ...base,
      async putIfMatch(key, bytes, etag) {
        if (key === J_KEY && !raced) {
          raced = true
          // Concurrent writer lands first → our etag is stale.
          const doc = [{
            start: other.start.getTime() / 1000,
            end: other.end.getTime() / 1000,
            requested_at: other.requestedAt.getTime() / 1000,
          }]
          await base.putIfMatch!(key, new TextEncoder().encode(JSON.stringify(doc) + '\n'), etag)
          // Now our original write must retry.
          throw new EtagConflict('racing')
        }
        await base.putIfMatch!(key, bytes, etag)
      },
    }
    const pyramid = makeLadder(racing)
    const mine: [Date, Date] = [utc(2026, 1, 2, 9), utc(2026, 1, 2, 10)]
    expect(await invalidate(pyramid, racing, mine, { now: T_REQ })).toBe(2)
    const [invs] = await loadInvalidations(pyramid, racing)
    expect(invs).toEqual([
      other,
      { start: mine[0], end: mine[1], requestedAt: T_REQ },
    ])
  })

  test('gives up after CAS_ATTEMPTS retries', async () => {
    const base = memStorage()
    const alwaysRace: Storage = {
      ...base,
      async putIfMatch(_key, _bytes, _etag) {
        throw new EtagConflict('always conflict')
      },
    }
    const pyramid = makeLadder(alwaysRace)
    await expect(
      invalidate(pyramid, alwaysRace, [W_START, W_END], { now: T_REQ }),
    ).rejects.toThrow(EtagConflict)
    expect(CAS_ATTEMPTS).toBe(5)
  })
})

describe('pruneSpent', () => {
  // Prune drops entries whose overlap set is empty (all overlapping
  // shards are fresher than the request). JS twin of the prune slice of
  // `test_invalidation_repair_end_to_end`.
  test('drops spent entries; keeps entries that still have stale overlaps', async () => {
    const clock: [Date] = [T_INIT]
    const storage = memStorage({ clock: () => clock[0] })
    const pyramid = makeLadder(storage)
    const expected = listExpectedShards(pyramid, { from: FROM, to: T18 })

    // Stage 1: append an entry whose overlap includes Q_KEY + H_KEY.
    await invalidate(pyramid, storage, [W_START, W_END], { now: T_REQ })

    // The shards exist and were built before T_REQ.
    for (const e of expected) await storage.put(e.key, new Uint8Array([0]))

    // Nothing rebuilt yet → the entry is still stale, prune is a no-op.
    let [pruned, remaining] = await pruneSpent(pyramid, storage, expected)
    expect([pruned, remaining]).toEqual([0, 1])

    // Stage 2: touch the two overlapping keys with a post-request mtime.
    clock[0] = T_REBUILD
    for (const key of [Q_KEY, H_KEY]) {
      await storage.put(key, new Uint8Array([1]))
    }

    // Now the entry has no remaining stale overlap → pruned.
    ;[pruned, remaining] = await pruneSpent(pyramid, storage, expected)
    expect([pruned, remaining]).toEqual([1, 0])

    // Journal is emptied in place, never deleted.
    expect(await storage.get(J_KEY)).not.toBeNull()
    expect(new TextDecoder().decode((await storage.get(J_KEY))!)).toBe('[]\n')

    // Idempotent: second prune finds nothing.
    ;[pruned, remaining] = await pruneSpent(pyramid, storage, expected)
    expect([pruned, remaining]).toEqual([0, 0])
  })
})

describe('listMissingShards: invalidations parameter', () => {
  // The consumer-facing wiring: `listMissingShards(..., { invalidations })`
  // folds stale-from-invalidation keys into the gap list, mirroring
  // Python `discover_gaps(invalidations=…)`. Mtimes come from the
  // index's `writtenAt` (JS index-driven path).
  //
  // This is the mock ShardIndex — enough surface for `listMissingShards`.
  interface RecordedRow {
    tier: string
    shardDur: string
    periodStart: Date
    periodEnd: Date
    key: string
    writtenAt?: Date
  }

  function mockIndex(recorded: RecordedRow[]) {
    // Only listShards is exercised by the code under test.
    return {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      async listShards(_name: string) {
        return recorded
      },
    } as unknown as import('./shard-index.js').ShardIndex
  }

  let pyramid: Pyramid
  let expected: ExpectedShard[]
  let recorded: RecordedRow[]

  beforeEach(() => {
    pyramid = makeLadder(memStorage())
    expected = listExpectedShards(pyramid, { from: FROM, to: T18 })
    recorded = expected.map(e => ({
      tier: e.tier,
      shardDur: e.shardDur,
      periodStart: e.periodStart,
      periodEnd: e.periodEnd,
      key: e.key,
      writtenAt: T_INIT,
    }))
  })

  test('without invalidations: fully-recorded → no gaps', async () => {
    const missing = await listMissingShards(
      pyramid, 'p', mockIndex(recorded),
      { from: FROM, to: T18 },
    )
    expect(missing).toEqual([])
  })

  test('with an invalidation overlapping two shards: those keys join the gap list', async () => {
    const inv: Invalidation = { start: W_START, end: W_END, requestedAt: T_REQ }
    const missing = await listMissingShards(
      pyramid, 'p', mockIndex(recorded),
      { from: FROM, to: T18 }, {},
      { invalidations: [inv] },
    )
    // T_INIT < T_REQ → both overlapping shards stale.
    expect(missing.map(m => m.key).sort()).toEqual([H_KEY, Q_KEY])
  })

  test('post-request mtimes exclude the invalidation match (freshly rebuilt)', async () => {
    const inv: Invalidation = { start: W_START, end: W_END, requestedAt: T_REQ }
    const rebuiltRecorded = recorded.map(r =>
      (r.key === Q_KEY || r.key === H_KEY) ? { ...r, writtenAt: T_REBUILD } : r,
    )
    const missing = await listMissingShards(
      pyramid, 'p', mockIndex(rebuiltRecorded),
      { from: FROM, to: T18 }, {},
      { invalidations: [inv] },
    )
    expect(missing).toEqual([])
  })

  test('missing-and-stale dedup: a key is emitted at most once', async () => {
    // Drop the H record so it's already `missing`; also invalidate it.
    // The stale-fold step must not double-count.
    const partial = recorded.filter(r => r.key !== H_KEY)
    const inv: Invalidation = { start: W_START, end: W_END, requestedAt: T_REQ }
    const missing = await listMissingShards(
      pyramid, 'p', mockIndex(partial),
      { from: FROM, to: T18 }, {},
      { invalidations: [inv] },
    )
    expect(missing.map(m => m.key).sort()).toEqual([H_KEY, Q_KEY])
  })

  test('unknown writtenAt (older manifests): treated as fresh, not stale', async () => {
    const inv: Invalidation = { start: W_START, end: W_END, requestedAt: T_REQ }
    const noWrittenAt = recorded.map(({ writtenAt: _wat, ...r }) => r)
    const missing = await listMissingShards(
      pyramid, 'p', mockIndex(noWrittenAt),
      { from: FROM, to: T18 }, {},
      { invalidations: [inv] },
    )
    // Backends without mtime can't trigger rebuilds — same rule as
    // Python `split_stale`.
    expect(missing).toEqual([])
  })
})

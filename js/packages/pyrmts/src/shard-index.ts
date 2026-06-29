// `ShardIndex` — abstract backend for watermark lookup + shard-write
// recording. Two impls: `D1ShardIndex` (pyrmts-cfw, indexed table) and
// `ManifestShardIndex` (single JSON blob in storage). The planner takes
// a pre-resolved watermark map (not the index itself), so the index is a
// pure read/write boundary the consumer wires up. See
// `specs/done/unified-shard-ladder.md` §Watermark grid.

import type { Shard } from './types.js'

// Watermark key encoding: `${tier}@${shardDur}` for every entry, no
// special canonical/partial dichotomy. The `@` separator means tier names
// must not contain `@` (validated at pyramid-config time).
export const WATERMARK_KEY_SEPARATOR = '@'

export function encodeWatermarkKey(tier: string, shardDur: Shard): string {
  return `${tier}${WATERMARK_KEY_SEPARATOR}${shardDur}`
}

export interface DecodedWatermarkKey {
  tier: string
  shardDur: Shard
}

export function decodeWatermarkKey(key: string): DecodedWatermarkKey {
  const idx = key.indexOf(WATERMARK_KEY_SEPARATOR)
  if (idx === -1) {
    throw new Error(`decodeWatermarkKey: missing '${WATERMARK_KEY_SEPARATOR}' in '${key}'; expected '\${tier}@\${shardDur}'`)
  }
  return {
    tier: key.slice(0, idx),
    shardDur: key.slice(idx + 1) as Shard,
  }
}

export interface RecordShardInput {
  pyramidName: string
  tier: string
  // Shard duration (e.g. `1h`, `1d`). Always present — no
  // canonical/partial dichotomy in the unified-ladder model.
  shardDur: Shard
  // Half-open `[periodStart, periodEnd)` — end-exclusive matches the
  // existing shard-period convention in `shardPeriodsCovering`.
  periodStart: Date
  periodEnd: Date
  // Pre-resolved storage key (the consumer is responsible for using
  // `pyramid.keyTemplate`; the index just records what was written).
  key: string
}

export interface ShardIndex {
  // Return all watermarks for `pyramidName` as a `Map<encodedKey, Date>`.
  // `Date` is the *end* of the latest sealed shard at that (tier, cadence) —
  // matches the planner's `effectiveWatermarks` semantics today.
  //
  // Implementations should return an empty map for unknown pyramids
  // (not throw) — the planner interprets "no watermark" as "complete
  // through the query range" (today's behavior).
  getWatermarks(pyramidName: string): Promise<Map<string, Date>>

  // Idempotent: re-recording the same `(pyramidName, tier, cadence,
  // periodStart)` overwrites. Concurrent calls for distinct
  // `(tier, cadence)` rows MUST NOT clobber each other (the D1 impl gets
  // this for free via row-level upsert; the manifest impl is single-writer
  // by construction).
  recordShard(input: RecordShardInput): Promise<void>
}

export interface CachedShardIndexOptions {
  // TTL in milliseconds. Default 60_000 (60s — matches ctbk's existing
  // manifest TTL). The cache is per-`pyramidName`; cross-pyramid reads
  // don't share TTL state.
  ttlMs?: number
  // Injectable clock for testability. Defaults to `Date.now`.
  now?: () => number
}

interface CacheEntry {
  value: Map<string, Date>
  // Absolute epoch-ms expiry — `now() >= expiresAt` ⇒ stale.
  expiresAt: number
}

// TTL cache around any `ShardIndex.getWatermarks`. `recordShard`
// delegates and invalidates the pyramid's entry so the next read
// reflects the just-written shard (eventual consistency within the
// TTL window for *other* worker isolates).
//
// In-flight `getWatermarks` calls dedupe: a concurrent second call for
// the same pyramid awaits the first's promise rather than firing a
// duplicate fetch. Cheap correctness win for hot-key workloads.
export class CachedShardIndex implements ShardIndex {
  private readonly cache = new Map<string, CacheEntry>()
  private readonly inflight = new Map<string, Promise<Map<string, Date>>>()
  private readonly ttlMs: number
  private readonly now: () => number

  constructor(private readonly underlying: ShardIndex, opts: CachedShardIndexOptions = {}) {
    this.ttlMs = opts.ttlMs ?? 60_000
    this.now = opts.now ?? Date.now
  }

  async getWatermarks(pyramidName: string): Promise<Map<string, Date>> {
    const cached = this.cache.get(pyramidName)
    if (cached !== undefined && this.now() < cached.expiresAt) {
      return cached.value
    }
    const existing = this.inflight.get(pyramidName)
    if (existing !== undefined) return existing
    const promise = this.underlying.getWatermarks(pyramidName).then(value => {
      this.cache.set(pyramidName, { value, expiresAt: this.now() + this.ttlMs })
      this.inflight.delete(pyramidName)
      return value
    }).catch(err => {
      this.inflight.delete(pyramidName)
      throw err
    })
    this.inflight.set(pyramidName, promise)
    return promise
  }

  async recordShard(input: RecordShardInput): Promise<void> {
    await this.underlying.recordShard(input)
    this.cache.delete(input.pyramidName)
  }

  // Test/diagnostic helper: drop the entire cache. Not part of the
  // `ShardIndex` interface; consumers shouldn't depend on it.
  clear(): void {
    this.cache.clear()
  }
}

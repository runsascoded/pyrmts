// `ShardIndex` — abstract backend for watermark lookup + shard-write
// recording. Two impls: `D1ShardIndex` (pyrmts-cfw, indexed table) and
// `ManifestShardIndex` (single JSON blob in storage). The planner takes
// a pre-resolved watermark map (not the index itself), so the index is a
// pure read/write boundary the consumer wires up. See
// `specs/done/unified-shard-ladder.md` §Watermark grid.
// Watermark key encoding: `${tier}@${shardDur}` for every entry, no
// special canonical/partial dichotomy. The `@` separator means tier names
// must not contain `@` (validated at pyramid-config time).
export const WATERMARK_KEY_SEPARATOR = '@';
export function encodeWatermarkKey(tier, shardDur) {
    return `${tier}${WATERMARK_KEY_SEPARATOR}${shardDur}`;
}
export function decodeWatermarkKey(key) {
    const idx = key.indexOf(WATERMARK_KEY_SEPARATOR);
    if (idx === -1) {
        throw new Error(`decodeWatermarkKey: missing '${WATERMARK_KEY_SEPARATOR}' in '${key}'; expected '\${tier}@\${shardDur}'`);
    }
    return {
        tier: key.slice(0, idx),
        shardDur: key.slice(idx + 1),
    };
}
// TTL cache around any `ShardIndex.getWatermarks`. `recordShard`
// delegates and invalidates the pyramid's entry so the next read
// reflects the just-written shard (eventual consistency within the
// TTL window for *other* worker isolates).
//
// In-flight `getWatermarks` calls dedupe: a concurrent second call for
// the same pyramid awaits the first's promise rather than firing a
// duplicate fetch. Cheap correctness win for hot-key workloads.
export class CachedShardIndex {
    underlying;
    cache = new Map();
    inflight = new Map();
    ttlMs;
    now;
    constructor(underlying, opts = {}) {
        this.underlying = underlying;
        this.ttlMs = opts.ttlMs ?? 60_000;
        this.now = opts.now ?? Date.now;
    }
    async getWatermarks(pyramidName) {
        const cached = this.cache.get(pyramidName);
        if (cached !== undefined && this.now() < cached.expiresAt) {
            return cached.value;
        }
        const existing = this.inflight.get(pyramidName);
        if (existing !== undefined)
            return existing;
        const promise = this.underlying.getWatermarks(pyramidName).then(value => {
            this.cache.set(pyramidName, { value, expiresAt: this.now() + this.ttlMs });
            this.inflight.delete(pyramidName);
            return value;
        }).catch(err => {
            this.inflight.delete(pyramidName);
            throw err;
        });
        this.inflight.set(pyramidName, promise);
        return promise;
    }
    async recordShard(input) {
        await this.underlying.recordShard(input);
        this.cache.delete(input.pyramidName);
    }
    // Test/diagnostic helper: drop the entire cache. Not part of the
    // `ShardIndex` interface; consumers shouldn't depend on it.
    clear() {
        this.cache.clear();
    }
}
//# sourceMappingURL=shard-index.js.map
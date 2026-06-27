// `ManifestShardIndex` — JSON-blob `ShardIndex` impl over the byte-level
// `Storage` interface. Drop-in fallback for consumers without D1
// (non-Cloudflare deploys, simpler stacks, or canonical-shard-only
// pyramids with single-writer cadence).
//
// Format (v1, flat by design — matches the `Map<encodedKey, Date>` shape
// `getWatermarks` returns):
//
//   {
//     "version": 1,
//     "watermarks": {
//       "15m":     1717200000000,
//       "15m@1h":  1717286400000,
//       "1h":      1717200000000
//     },
//     "shards": [ // optional inventory; only present if recordShard
//       { "tier": "15m", "cadence": "1h", "periodStart": 1717286400000,
//         "periodEnd": 1717290000000, "key": "…" }, ...   // …includeInventory: true
//     ],
//     "updatedAt": 1717290000123
//   }
//
// Encoded keys (`${tier}` canonical / `${tier}@${cadence}` partial)
// match the `ShardIndex` contract directly — no per-tier nesting, no
// label parsing. Consumers with the legacy ctbk `{tiers: {<name>:
// {latest_period: "2026-06", partials: …}}}` format migrate via a
// one-shot script (out of scope; the spec recommends D1 anyway for
// the cadence-driven write workloads).
//
// Single-writer assumption: `recordShard` does GET-modify-PUT against
// the manifest blob. Concurrent writers race on the GET/PUT cycle and
// can clobber each other — match the spec's §Watermark index (b)
// "writers serialized externally" constraint. Use `D1ShardIndex` for
// concurrent writers.
import { encodeWatermarkKey, } from './index.js';
const DEFAULT_KEY = (pyramidName) => `pyrmts/${pyramidName}/_manifest.json`;
function resolveKey(manifestKey, pyramidName) {
    if (manifestKey === undefined)
        return DEFAULT_KEY(pyramidName);
    if (typeof manifestKey === 'string')
        return manifestKey;
    return manifestKey(pyramidName);
}
function emptyManifest(now) {
    return { version: 1, watermarks: {}, updatedAt: now };
}
const decoder = new TextDecoder();
const encoder = new TextEncoder();
// Parse a manifest blob. Returns the parsed v1 manifest or `null` on
// any failure (missing version, bad JSON, wrong shape). Callers treat
// null as "start over with an empty manifest" — matches ctbk's existing
// defensive behavior in `loadWatermarks`.
function parseManifest(bytes) {
    let parsed;
    try {
        parsed = JSON.parse(decoder.decode(bytes));
    }
    catch {
        return null;
    }
    if (parsed === null ||
        typeof parsed !== 'object' ||
        Array.isArray(parsed))
        return null;
    const obj = parsed;
    if (obj.version !== 1)
        return null;
    if (obj.watermarks === null || typeof obj.watermarks !== 'object' || Array.isArray(obj.watermarks))
        return null;
    const watermarks = {};
    for (const [k, v] of Object.entries(obj.watermarks)) {
        if (typeof v !== 'number' || !Number.isFinite(v))
            return null;
        watermarks[k] = v;
    }
    const out = {
        version: 1,
        watermarks,
        updatedAt: typeof obj.updatedAt === 'number' ? obj.updatedAt : 0,
    };
    if (Array.isArray(obj.shards)) {
        const shards = [];
        for (const raw of obj.shards) {
            if (raw === null || typeof raw !== 'object' || Array.isArray(raw))
                return null;
            const r = raw;
            if (typeof r.tier !== 'string')
                return null;
            if (r.cadence !== null && typeof r.cadence !== 'string')
                return null;
            if (typeof r.periodStart !== 'number' || typeof r.periodEnd !== 'number')
                return null;
            if (typeof r.key !== 'string')
                return null;
            shards.push({
                tier: r.tier,
                cadence: r.cadence,
                periodStart: r.periodStart,
                periodEnd: r.periodEnd,
                key: r.key,
            });
        }
        out.shards = shards;
    }
    return out;
}
export class ManifestShardIndex {
    storage;
    opts;
    includeInventory;
    now;
    constructor(storage, opts = {}) {
        this.storage = storage;
        this.opts = opts;
        this.includeInventory = opts.includeInventory ?? false;
        this.now = opts.now ?? Date.now;
    }
    async getWatermarks(pyramidName) {
        const key = resolveKey(this.opts.manifestKey, pyramidName);
        const bytes = await this.storage.get(key);
        if (bytes === null)
            return new Map();
        const manifest = parseManifest(bytes);
        if (manifest === null)
            return new Map();
        const out = new Map();
        for (const [k, ms] of Object.entries(manifest.watermarks)) {
            out.set(k, new Date(ms));
        }
        return out;
    }
    async recordShard(input) {
        const key = resolveKey(this.opts.manifestKey, input.pyramidName);
        const existing = await this.storage.get(key);
        const manifest = existing !== null ? parseManifest(existing) : null;
        const now = this.now();
        const m = manifest ?? emptyManifest(now);
        const cadence = input.cadence;
        const encoded = encodeWatermarkKey(input.tier, cadence);
        const periodEndMs = input.periodEnd.getTime();
        const prevMs = m.watermarks[encoded];
        if (prevMs === undefined || periodEndMs > prevMs) {
            m.watermarks[encoded] = periodEndMs;
        }
        if (this.includeInventory) {
            const periodStartMs = input.periodStart.getTime();
            const shards = m.shards ?? [];
            const i = shards.findIndex(s => s.tier === input.tier
                && s.cadence === input.cadence
                && s.periodStart === periodStartMs);
            const record = {
                tier: input.tier,
                cadence: input.cadence,
                periodStart: periodStartMs,
                periodEnd: periodEndMs,
                key: input.key,
            };
            if (i === -1)
                shards.push(record);
            else
                shards[i] = record;
            m.shards = shards;
        }
        m.updatedAt = now;
        await this.storage.put(key, encoder.encode(JSON.stringify(m)));
    }
}
//# sourceMappingURL=manifest-shard-index.js.map
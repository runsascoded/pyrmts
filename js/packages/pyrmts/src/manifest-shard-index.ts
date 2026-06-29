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
//       "15m@1d":  1717200000000,
//       "15m@1h":  1717286400000,
//       "1h@1d":   1717200000000
//     },
//     "shards": [ // optional inventory; only present if recordShard
//       { "tier": "15m", "shardDur": "1h", "periodStart": 1717286400000,
//         "periodEnd": 1717290000000, "key": "…" }, ...   // …includeInventory: true
//     ],
//     "updatedAt": 1717290000123
//   }
//
// Encoded keys are uniformly `${tier}@${shardDur}` — every entry on
// the per-tier shard ladder gets its own watermark row. See
// `specs/done/unified-shard-ladder.md`.
//
// Single-writer assumption: `recordShard` does GET-modify-PUT against
// the manifest blob. Concurrent writers race on the GET/PUT cycle and
// can clobber each other — match the spec's §Watermark index (b)
// "writers serialized externally" constraint. Use `D1ShardIndex` for
// concurrent writers.

import {
  encodeWatermarkKey,
  type RecordShardInput,
  type ShardIndex,
  type Storage,
} from './index.js'

export interface ManifestShardIndexOptions {
  // Storage key resolution. String literal is used directly; function
  // form receives the `pyramidName` for multi-pyramid layouts.
  // Default: `pyrmts/{pyramidName}/_manifest.json`.
  manifestKey?: string | ((pyramidName: string) => string)
  // Include per-shard inventory rows (parallels D1ShardIndex's
  // `pyramid_shards` table). Default false — manifest blob size
  // would grow unbounded over time. Turn on for audit/debug flows.
  includeInventory?: boolean
  // Injectable clock (epoch ms) for `updatedAt`. Default `Date.now`.
  now?: () => number
}

interface ManifestShardRecord {
  tier: string
  shardDur: string
  periodStart: number
  periodEnd: number
  key: string
}

interface ManifestV1 {
  version: 1
  watermarks: Record<string, number>
  shards?: ManifestShardRecord[]
  updatedAt: number
}

const DEFAULT_KEY = (pyramidName: string): string =>
  `pyrmts/${pyramidName}/_manifest.json`

function resolveKey(
  manifestKey: ManifestShardIndexOptions['manifestKey'],
  pyramidName: string,
): string {
  if (manifestKey === undefined) return DEFAULT_KEY(pyramidName)
  if (typeof manifestKey === 'string') return manifestKey
  return manifestKey(pyramidName)
}

function emptyManifest(now: number): ManifestV1 {
  return { version: 1, watermarks: {}, updatedAt: now }
}

const decoder = new TextDecoder()
const encoder = new TextEncoder()

// Parse a manifest blob. Returns the parsed v1 manifest or `null` on
// any failure (missing version, bad JSON, wrong shape). Callers treat
// null as "start over with an empty manifest" — matches ctbk's existing
// defensive behavior in `loadWatermarks`.
function parseManifest(bytes: Uint8Array): ManifestV1 | null {
  let parsed: unknown
  try {
    parsed = JSON.parse(decoder.decode(bytes))
  } catch {
    return null
  }
  if (
    parsed === null ||
    typeof parsed !== 'object' ||
    Array.isArray(parsed)
  ) return null
  const obj = parsed as Record<string, unknown>
  if (obj.version !== 1) return null
  if (obj.watermarks === null || typeof obj.watermarks !== 'object' || Array.isArray(obj.watermarks)) return null
  const watermarks: Record<string, number> = {}
  for (const [k, v] of Object.entries(obj.watermarks as Record<string, unknown>)) {
    if (typeof v !== 'number' || !Number.isFinite(v)) return null
    watermarks[k] = v
  }
  const out: ManifestV1 = {
    version: 1,
    watermarks,
    updatedAt: typeof obj.updatedAt === 'number' ? obj.updatedAt : 0,
  }
  if (Array.isArray(obj.shards)) {
    const shards: ManifestShardRecord[] = []
    for (const raw of obj.shards) {
      if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) return null
      const r = raw as Record<string, unknown>
      if (typeof r.tier !== 'string') return null
      if (typeof r.shardDur !== 'string') return null
      if (typeof r.periodStart !== 'number' || typeof r.periodEnd !== 'number') return null
      if (typeof r.key !== 'string') return null
      shards.push({
        tier: r.tier,
        shardDur: r.shardDur,
        periodStart: r.periodStart,
        periodEnd: r.periodEnd,
        key: r.key,
      })
    }
    out.shards = shards
  }
  return out
}

export class ManifestShardIndex implements ShardIndex {
  private readonly includeInventory: boolean
  private readonly now: () => number

  constructor(
    private readonly storage: Storage,
    private readonly opts: ManifestShardIndexOptions = {},
  ) {
    this.includeInventory = opts.includeInventory ?? false
    this.now = opts.now ?? Date.now
  }

  async getWatermarks(pyramidName: string): Promise<Map<string, Date>> {
    const key = resolveKey(this.opts.manifestKey, pyramidName)
    const bytes = await this.storage.get(key)
    if (bytes === null) return new Map()
    const manifest = parseManifest(bytes)
    if (manifest === null) return new Map()
    const out = new Map<string, Date>()
    for (const [k, ms] of Object.entries(manifest.watermarks)) {
      out.set(k, new Date(ms))
    }
    return out
  }

  async recordShard(input: RecordShardInput): Promise<void> {
    const key = resolveKey(this.opts.manifestKey, input.pyramidName)
    const existing = await this.storage.get(key)
    const manifest = existing !== null ? parseManifest(existing) : null
    const now = this.now()
    const m: ManifestV1 = manifest ?? emptyManifest(now)
    const encoded = encodeWatermarkKey(input.tier, input.shardDur)
    const periodEndMs = input.periodEnd.getTime()
    const prevMs = m.watermarks[encoded]
    if (prevMs === undefined || periodEndMs > prevMs) {
      m.watermarks[encoded] = periodEndMs
    }
    if (this.includeInventory) {
      const periodStartMs = input.periodStart.getTime()
      const shards = m.shards ?? []
      const i = shards.findIndex(s =>
        s.tier === input.tier
        && s.shardDur === input.shardDur
        && s.periodStart === periodStartMs,
      )
      const record: ManifestShardRecord = {
        tier: input.tier,
        shardDur: input.shardDur as string,
        periodStart: periodStartMs,
        periodEnd: periodEndMs,
        key: input.key,
      }
      if (i === -1) shards.push(record)
      else shards[i] = record
      m.shards = shards
    }
    m.updatedAt = now
    await this.storage.put(key, encoder.encode(JSON.stringify(m)))
  }
}

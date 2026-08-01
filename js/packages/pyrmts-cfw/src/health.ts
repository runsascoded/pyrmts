// Pyramid health SDK (`specs/pyrmts-ops-adoption.md` phase 4 — absorbed
// from ctbk `gbfs/api/src/health.ts`'s generic pieces).
//
// - `pyramidCover`: per-tier min-cover status of a registry pyramid —
//   which cover slots are present in the shard registry, which are
//   pending (just-closed, within the write-lag grace window), which are
//   missing, and how many registered shards fell out of the current
//   min-cover (stale — GC candidates). Feeds a /health timeline bar.
// - `BuildProgress` + `getBuildsHealth`: reader for the driver-written
//   build-progress docs (`pyrmts_ops.rebuild.BuildProgress` is the
//   writer — one JSON contract, defined by these types).
// - `readCachedSnapshot` / `computeAndStoreSnapshot`: the
//   cron-refreshed-snapshot pattern for expensive health computations.
//
// Consumers keep their pyramid registry (which pyramids to surface,
// genesis dates, key prefixes) and any app-specific health sections;
// the cover math, doc contract, and cache pattern live here.

import {
  floorToSpan,
  listExpectedShards,
  parseDuration,
  shardBuildableAt,
  type Pyramid,
  type Storage,
} from 'pyrmts'
import type { D1Like } from './d1.js'

/** Per-rung slot in a tier's current min-cover of `[genesis, now)`. In
 *  equilibrium each tier's min-cover is mostly max-rung tiles filling the
 *  closed-history region `[genesis, floor(now, max_rung))`, plus a small
 *  "dust" of finer-rung tiles filling `[floor(now, max_rung), now)`. */
export interface PyramidCoverRung {
  shardDur: string
  role: 'max' | 'dust'
  expected: number   // shards the min-cover requires at this rung
  present: number    // of those, how many are registered
  pending: number    // missing but only just closed (within grace) — cron will land them
}

/** One min-cover slot, for the timeline-bar rendering. Slots are emitted
 *  per-shard (not coalesced) so tile boundaries are visible in the bar. */
export interface PyramidCoverSegment {
  start: string      // ISO
  end: string        // ISO (exclusive)
  shardDur: string
  status: 'present' | 'pending' | 'missing'
  key?: string       // storage key (present segments — click-through)
  // Set on absent segments whose source cover is still open (structural
  // lag — `shardBuildableAt` > period end): the earliest instant a cron
  // tick can land the shard. Tooltip fodder ("waits on /30m@2h until
  // 22:00Z"); omitted for the aligned (majority) case.
  buildableAt?: string
}

/** Per-tier min-cover status. `complete` iff no cover slot is MISSING
 *  (pending slots — just-closed, within the write-lag grace window —
 *  don't break completeness; they'd flap red on every rung boundary
 *  until the next cron tick writes them). */
export interface PyramidTierCoverStatus {
  tier: string
  bin: string
  maxRung: string
  rungs: PyramidCoverRung[]               // ordered oldest → newest
  segments: PyramidCoverSegment[]         // ordered oldest → newest
  totalExpected: number
  totalPresent: number
  totalPending: number
  complete: boolean
  firstMissingPeriod: string | null       // ISO of oldest MISSING cover shard, if any
  lastMaxBoundary: string                 // ISO of floor(now, max_rung) — dust head
  dustAgeSec: number                      // now - lastMaxBoundary, in seconds
  staleShardCount: number                 // registered shards NOT in current min-cover
}

/** Per-pyramid roll-up. `allComplete` iff every tier's min-cover is satisfied. */
export interface PyramidCoverStatus {
  name: string             // registry pyramid name
  genesis: string          // ISO — cover computed over [genesis, now)
  now: string              // ISO snapshot time
  tiers: PyramidTierCoverStatus[]
  totalMissing: number     // sum of MISSING (not pending) across tiers
  totalPending: number
  totalStale: number       // sum of staleShardCount across tiers
  allComplete: boolean
}

/** A cover slot whose period closed within this window and hasn't been
 *  registered yet counts as `pending`, not `missing`. */
export const PENDING_GRACE_MS = 10 * 60_000

export interface PyramidCoverOptions {
  /** Registry pyramid name (the `pyramid` column value). */
  name: string
  /** Cover computed over `[genesis, now)`. */
  genesis: Date
  now?: Date
  pendingGraceMs?: number
  /** Shard-registry table (default `pyramid_shards`). */
  tableName?: string
  /** Shard-duration column (default `shard_dur`; legacy schemas may
   *  pass `cadence` during migration). */
  shardCol?: string
}

/** Min-cover status for one registry pyramid. `pyramid` needs only
 *  `tiers` + `keyTemplate` (all `listExpectedShards` reads). Returns
 *  null when the registry is unavailable or the pyramid has no rows yet
 *  (hidden rather than all-missing). */
export async function pyramidCover(
  db: D1Like,
  pyramid: Pick<Pyramid, 'tiers' | 'keyTemplate'>,
  opts: PyramidCoverOptions,
): Promise<PyramidCoverStatus | null> {
  const {
    name,
    genesis,
    now = new Date(),
    pendingGraceMs = PENDING_GRACE_MS,
    tableName = 'pyramid_shards',
    shardCol = 'shard_dur',
  } = opts
  const tiers = pyramid.tiers
  const largestPerTier: Record<string, string> = Object.fromEntries(
    tiers.map((t) => [t.name, String(t.shards[t.shards.length - 1]!)]),
  )

  type ShardRow = { tier: string; sd: string; period_start: number }
  const shardSql =
    `SELECT tier, ${shardCol} AS sd, period_start ` +
    `FROM ${tableName} WHERE pyramid = ?`

  let shardRows: ShardRow[] = []
  try {
    const s = await db.prepare(shardSql).bind(name).all<ShardRow>()
    shardRows = s.results ?? []
  } catch {
    return null  // registry unavailable
  }
  if (shardRows.length === 0) return null

  // Translate legacy canonical sentinel rows: '' → tier's largest shard.
  const norm = (tier: string, sd: string): string =>
    sd === '' ? (largestPerTier[tier] ?? sd) : sd

  const presentKey = (tier: string, sd: string, periodStartMs: number) =>
    `${tier}\x00${sd}\x00${periodStartMs}`
  const present = new Set<string>()
  const presentByTier: Record<string, number> = {}
  for (const r of shardRows) {
    const sd = norm(r.tier, r.sd)
    present.add(presentKey(r.tier, sd, r.period_start))
    presentByTier[r.tier] = (presentByTier[r.tier] ?? 0) + 1
  }

  const expected = listExpectedShards(pyramid as Pyramid, { from: genesis, to: now })
  const expectedByTier = new Map<string, typeof expected>()
  for (const e of expected) {
    const list = expectedByTier.get(e.tier) ?? []
    list.push(e)
    expectedByTier.set(e.tier, list)
  }

  const tierStatuses: PyramidTierCoverStatus[] = []
  let totalMissing = 0
  let totalPending = 0
  let totalStale = 0

  for (const t of tiers) {
    const maxRung = String(t.shards[t.shards.length - 1]!)
    const lastMaxBoundary = floorToSpan(now, parseDuration(maxRung))
    const cover = expectedByTier.get(t.name) ?? []

    const rungOrder: string[] = []
    const rungAgg: Record<string, { expected: number; present: number; pending: number; role: 'max' | 'dust' }> = {}
    const segments: PyramidCoverSegment[] = []
    let firstMissingPeriod: string | null = null
    let coverPresent = 0
    let coverPending = 0

    for (const slot of cover) {
      const sd = String(slot.shardDur)
      const role: 'max' | 'dust' = sd === maxRung ? 'max' : 'dust'
      if (!(sd in rungAgg)) {
        rungOrder.push(sd)
        rungAgg[sd] = { expected: 0, present: 0, pending: 0, role }
      }
      const agg = rungAgg[sd]!
      agg.expected += 1
      const key = presentKey(t.name, sd, slot.periodStart.getTime())
      let status: PyramidCoverSegment['status']
      let buildableAtIso: string | undefined
      if (present.has(key)) {
        status = 'present'
        agg.present += 1
        coverPresent += 1
      } else {
        // Grace is measured from when a cron tick can first land the
        // shard — later than periodEnd when the source cover is still
        // open (structural lag; `buildableAt == periodEnd` otherwise).
        const buildableAt = shardBuildableAt(pyramid, t.name, slot.periodEnd)
        if (buildableAt.getTime() > slot.periodEnd.getTime()) {
          buildableAtIso = buildableAt.toISOString()
        }
        if (buildableAt.getTime() > now.getTime() - pendingGraceMs) {
          status = 'pending'
          agg.pending += 1
          coverPending += 1
        } else {
          status = 'missing'
          if (firstMissingPeriod === null) {
            firstMissingPeriod = slot.periodStart.toISOString()
          }
        }
      }
      // Clip the head tile to genesis so the bar's x-domain is exact.
      const segStart = slot.effectiveStart > slot.periodStart ? slot.effectiveStart : slot.periodStart
      segments.push({
        start: segStart.toISOString(),
        end: slot.periodEnd.toISOString(),
        shardDur: sd,
        status,
        ...(status === 'present' ? { key: slot.key } : {}),
        ...(buildableAtIso !== undefined ? { buildableAt: buildableAtIso } : {}),
      })
    }

    const rungs: PyramidCoverRung[] = rungOrder.map((sd) => ({
      shardDur: sd,
      role: rungAgg[sd]!.role,
      expected: rungAgg[sd]!.expected,
      present: rungAgg[sd]!.present,
      pending: rungAgg[sd]!.pending,
    }))
    const totalExpected = cover.length
    const tierMissing = totalExpected - coverPresent - coverPending
    const staleShardCount = Math.max(0, (presentByTier[t.name] ?? 0) - coverPresent)

    totalMissing += tierMissing
    totalPending += coverPending
    totalStale += staleShardCount

    tierStatuses.push({
      tier: t.name,
      bin: String(t.bin),
      maxRung,
      rungs,
      segments,
      totalExpected,
      totalPresent: coverPresent,
      totalPending: coverPending,
      complete: tierMissing === 0,
      firstMissingPeriod,
      lastMaxBoundary: lastMaxBoundary.toISOString(),
      dustAgeSec: Math.floor((now.getTime() - lastMaxBoundary.getTime()) / 1000),
      staleShardCount,
    })
  }

  return {
    name,
    genesis: genesis.toISOString(),
    now: now.toISOString(),
    tiers: tierStatuses,
    totalMissing,
    totalPending,
    totalStale,
    allComplete: totalMissing === 0,
  }
}

/** One layer of a driver-written build-progress doc. The writer is
 *  `pyrmts_ops.rebuild.BuildProgress` — these types ARE the contract.
 *  Loose on purpose — health pages render what's present. */
export interface BuildLayer {
  tier: string
  rung: string
  scaffold: boolean
  n: number
  done?: number
  wallS?: number
  status?: Record<string, number>
}

export interface BuildProgress {
  pyramid: string
  driver: string
  startedAt: string
  updatedAt?: string
  status: 'running' | 'done' | 'bounced'
  plan: { layers: number; invocations: number; scaffolds: number }
  byStatus: Record<string, number>
  layers: BuildLayer[]
  currentLayer: BuildLayer | null
}

export interface BuildsHealthOptions {
  /** Progress-doc prefix (the fan-out driver's `progress_prefix` +
   *  nothing — one doc per pyramid lives under it). */
  prefix: string
  /** Max docs to read (default 20). */
  limit?: number
  /** Drop docs idle longer than this (default 7 days). */
  maxIdleMs?: number
  now?: number
}

const decoder = new TextDecoder()

async function getJson<T>(storage: Storage, key: string): Promise<T | null> {
  const bytes = await storage.get(key)
  if (bytes === null) return null
  try {
    return JSON.parse(decoder.decode(bytes)) as T
  } catch {
    return null  // malformed doc — skip
  }
}

/** Recent driver build-progress docs (running builds first, then most
 *  recently updated; anything idle past `maxIdleMs` is dropped). */
export async function getBuildsHealth(
  storage: Storage,
  opts: BuildsHealthOptions,
): Promise<BuildProgress[]> {
  const { prefix, limit = 20, maxIdleMs = 7 * 86_400_000, now = Date.now() } = opts
  const keys: string[] = []
  for await (const key of storage.list(prefix)) {
    keys.push(key)
    if (keys.length >= limit) break
  }
  const out: BuildProgress[] = []
  for (const key of keys) {
    const doc = await getJson<BuildProgress>(storage, key)
    if (doc === null) continue
    const updated = Date.parse(doc.updatedAt ?? doc.startedAt ?? '')
    if (Number.isFinite(updated) && now - updated < maxIdleMs) out.push(doc)
  }
  out.sort((a, b) =>
    Number(b.status === 'running') - Number(a.status === 'running')
    || Date.parse(b.updatedAt ?? '0') - Date.parse(a.updatedAt ?? '0'))
  return out
}

/** Anything cacheable by the snapshot pattern: a `generatedAt` epoch-
 *  seconds stamp. */
export interface SnapshotLike {
  generatedAt: number
}

/** Read a cron-refreshed snapshot if it's fresher than `maxAgeS`; null →
 *  the caller computes live (and should persist the result for the next
 *  request via `computeAndStoreSnapshot`). */
export async function readCachedSnapshot<T extends SnapshotLike>(
  storage: Storage,
  key: string,
  maxAgeS: number,
  now: number = Date.now(),
): Promise<T | null> {
  const snapshot = await getJson<T>(storage, key)
  if (snapshot === null) return null
  const ageS = Math.floor(now / 1000) - (snapshot.generatedAt ?? 0)
  return ageS <= maxAgeS ? snapshot : null
}

const encoder = new TextEncoder()

/** Compute a snapshot and persist it (the cron path). */
export async function computeAndStoreSnapshot<T extends SnapshotLike>(
  storage: Storage,
  key: string,
  compute: () => Promise<T>,
): Promise<T> {
  const snapshot = await compute()
  await storage.put(key, encoder.encode(JSON.stringify(snapshot)))
  return snapshot
}

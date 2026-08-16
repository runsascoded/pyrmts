// Pure cover-status types shared by `pyrmts-cfw` (which computes them —
// `pyramidCover` in its `health.ts`) and `pyrmts-react` (which renders
// them). They live here so the React package stays worker-free: browser
// consumers get the shapes without depending on `pyrmts-cfw`.

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

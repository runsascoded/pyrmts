/** Per-rung slot in a tier's current min-cover of `[genesis, now)`. In
 *  equilibrium each tier's min-cover is mostly max-rung tiles filling the
 *  closed-history region `[genesis, floor(now, max_rung))`, plus a small
 *  "dust" of finer-rung tiles filling `[floor(now, max_rung), now)`. */
export interface PyramidCoverRung {
    shardDur: string;
    role: 'max' | 'dust';
    expected: number;
    present: number;
    pending: number;
}
/** One min-cover slot, for the timeline-bar rendering. Slots are emitted
 *  per-shard (not coalesced) so tile boundaries are visible in the bar. */
export interface PyramidCoverSegment {
    start: string;
    end: string;
    shardDur: string;
    status: 'present' | 'pending' | 'missing';
    key?: string;
    buildableAt?: string;
}
/** Per-tier min-cover status. `complete` iff no cover slot is MISSING
 *  (pending slots — just-closed, within the write-lag grace window —
 *  don't break completeness; they'd flap red on every rung boundary
 *  until the next cron tick writes them). */
export interface PyramidTierCoverStatus {
    tier: string;
    bin: string;
    maxRung: string;
    rungs: PyramidCoverRung[];
    segments: PyramidCoverSegment[];
    totalExpected: number;
    totalPresent: number;
    totalPending: number;
    complete: boolean;
    firstMissingPeriod: string | null;
    lastMaxBoundary: string;
    dustAgeSec: number;
    staleShardCount: number;
}
/** Per-pyramid roll-up. `allComplete` iff every tier's min-cover is satisfied. */
export interface PyramidCoverStatus {
    name: string;
    genesis: string;
    now: string;
    tiers: PyramidTierCoverStatus[];
    totalMissing: number;
    totalPending: number;
    totalStale: number;
    allComplete: boolean;
}
//# sourceMappingURL=cover-status.d.ts.map
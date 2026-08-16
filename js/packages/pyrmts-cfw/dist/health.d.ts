import { type Pyramid, type PyramidCoverRung, type PyramidCoverSegment, type PyramidCoverStatus, type PyramidTierCoverStatus, type Storage } from 'pyrmts';
import type { D1Like } from './d1.js';
export type { PyramidCoverRung, PyramidCoverSegment, PyramidCoverStatus, PyramidTierCoverStatus, };
/** A cover slot whose period closed within this window and hasn't been
 *  registered yet counts as `pending`, not `missing`. */
export declare const PENDING_GRACE_MS: number;
export interface PyramidCoverOptions {
    /** Registry pyramid name (the `pyramid` column value). */
    name: string;
    /** Cover computed over `[genesis, now)`. */
    genesis: Date;
    now?: Date;
    pendingGraceMs?: number;
    /** Shard-registry table (default `pyramid_shards`). */
    tableName?: string;
    /** Shard-duration column (default `shard_dur`; legacy schemas may
     *  pass `cadence` during migration). */
    shardCol?: string;
}
/** Min-cover status for one registry pyramid. `pyramid` needs only
 *  `tiers` + `keyTemplate` (all `listExpectedShards` reads). Returns
 *  null when the registry is unavailable or the pyramid has no rows yet
 *  (hidden rather than all-missing). */
export declare function pyramidCover(db: D1Like, pyramid: Pick<Pyramid, 'tiers' | 'keyTemplate'>, opts: PyramidCoverOptions): Promise<PyramidCoverStatus | null>;
/** One layer of a driver-written build-progress doc. The writer is
 *  `pyrmts_ops.rebuild.BuildProgress` — these types ARE the contract.
 *  Loose on purpose — health pages render what's present. */
export interface BuildLayer {
    tier: string;
    rung: string;
    scaffold: boolean;
    n: number;
    done?: number;
    wallS?: number;
    status?: Record<string, number>;
}
export interface BuildProgress {
    pyramid: string;
    driver: string;
    startedAt: string;
    updatedAt?: string;
    status: 'running' | 'done' | 'bounced';
    plan: {
        layers: number;
        invocations: number;
        scaffolds: number;
    };
    byStatus: Record<string, number>;
    layers: BuildLayer[];
    currentLayer: BuildLayer | null;
}
export interface BuildsHealthOptions {
    /** Progress-doc prefix (the fan-out driver's `progress_prefix` +
     *  nothing — one doc per pyramid lives under it). */
    prefix: string;
    /** Max docs to read (default 20). */
    limit?: number;
    /** Drop docs idle longer than this (default 7 days). */
    maxIdleMs?: number;
    now?: number;
}
/** Recent driver build-progress docs (running builds first, then most
 *  recently updated; anything idle past `maxIdleMs` is dropped). */
export declare function getBuildsHealth(storage: Storage, opts: BuildsHealthOptions): Promise<BuildProgress[]>;
/** Anything cacheable by the snapshot pattern: a `generatedAt` epoch-
 *  seconds stamp. */
export interface SnapshotLike {
    generatedAt: number;
}
/** Read a cron-refreshed snapshot if it's fresher than `maxAgeS`; null →
 *  the caller computes live (and should persist the result for the next
 *  request via `computeAndStoreSnapshot`). */
export declare function readCachedSnapshot<T extends SnapshotLike>(storage: Storage, key: string, maxAgeS: number, now?: number): Promise<T | null>;
/** Compute a snapshot and persist it (the cron path). */
export declare function computeAndStoreSnapshot<T extends SnapshotLike>(storage: Storage, key: string, compute: () => Promise<T>): Promise<T>;
//# sourceMappingURL=health.d.ts.map
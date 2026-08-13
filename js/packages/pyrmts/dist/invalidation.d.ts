import type { ExpectedShard } from './gap-discovery.js';
import type { Pyramid, Storage } from './types.js';
export declare const JOURNAL_BASENAME = "_invalidations.json";
export declare const CAS_ATTEMPTS = 5;
export interface Invalidation {
    start: Date;
    end: Date;
    requestedAt: Date;
}
export declare function journalKey(pyramid: Pyramid): string;
export declare function loadInvalidations(pyramid: Pyramid, storage: Storage): Promise<[Invalidation[], string | null]>;
export declare function overlaps(inv: Invalidation, shard: ExpectedShard): boolean;
export declare function invalidate(pyramid: Pyramid, storage: Storage, interval: [Date, Date], opts?: {
    now?: Date;
}): Promise<number>;
export declare function staleKeysFor(expected: ExpectedShard[], mtimes: Map<string, Date | null>, invalidations: Invalidation[]): Set<string>;
export declare function pruneSpent(pyramid: Pyramid, storage: Storage, expected: ExpectedShard[], opts?: {
    mtimes?: Map<string, Date | null>;
}): Promise<[number, number]>;
export declare function listExistingWithMtime(pyramid: Pyramid, storage: Storage): Promise<Map<string, Date | null>>;
//# sourceMappingURL=invalidation.d.ts.map
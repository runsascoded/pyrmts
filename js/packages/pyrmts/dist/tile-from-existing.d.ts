import type { ExpectedShard } from './gap-discovery.js';
import type { Pyramid, Shard, Tier } from './types.js';
export interface TilingResult {
    picks: Array<{
        rung: Shard;
        key: string;
    }>;
    holes: Array<{
        start: Date;
        end: Date;
    }>;
}
export declare function tileFromExisting(pyramid: Pyramid, tier: Tier, gap: ExpectedShard, keySet: Set<string>, opts: {
    genesis: Date;
    filter?: Record<string, string | number>;
}): TilingResult;
//# sourceMappingURL=tile-from-existing.d.ts.map
import type { Pyramid, Shard } from './types.js';
import type { ShardIndex } from './shard-index.js';
export interface ExpectedShard {
    tier: string;
    shardDur: Shard;
    periodStart: Date;
    periodEnd: Date;
    key: string;
}
export declare function listExpectedShards(pyramid: Pyramid, range: {
    from: Date;
    to: Date;
}, filter?: Record<string, string | number>): ExpectedShard[];
export declare function listMissingShards(pyramid: Pyramid, pyramidName: string, shardIndex: ShardIndex, range: {
    from: Date;
    to: Date;
}, filter?: Record<string, string | number>): Promise<ExpectedShard[]>;
//# sourceMappingURL=gap-discovery.d.ts.map
import type { ShardIndex } from './shard-index.js';
export interface ShardIndexConformanceOptions {
    inventory?: boolean;
}
export declare function assertShardIndexConformance(makeIndex: () => ShardIndex, opts?: ShardIndexConformanceOptions): void;
//# sourceMappingURL=shard-index-conformance.d.ts.map
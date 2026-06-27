import { type RecordShardInput, type ShardIndex, type Storage } from './index.js';
export interface ManifestShardIndexOptions {
    manifestKey?: string | ((pyramidName: string) => string);
    includeInventory?: boolean;
    now?: () => number;
}
export declare class ManifestShardIndex implements ShardIndex {
    private readonly storage;
    private readonly opts;
    private readonly includeInventory;
    private readonly now;
    constructor(storage: Storage, opts?: ManifestShardIndexOptions);
    getWatermarks(pyramidName: string): Promise<Map<string, Date>>;
    recordShard(input: RecordShardInput): Promise<void>;
}
//# sourceMappingURL=manifest-shard-index.d.ts.map
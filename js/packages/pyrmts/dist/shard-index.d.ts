import type { Shard } from './types.js';
export declare const WATERMARK_KEY_SEPARATOR = "@";
export declare function encodeWatermarkKey(tier: string, shardDur: Shard): string;
export interface DecodedWatermarkKey {
    tier: string;
    shardDur: Shard;
}
export declare function decodeWatermarkKey(key: string): DecodedWatermarkKey;
export interface RecordShardInput {
    pyramidName: string;
    tier: string;
    shardDur: Shard;
    periodStart: Date;
    periodEnd: Date;
    key: string;
}
export interface RecordedShard {
    tier: string;
    shardDur: Shard;
    periodStart: Date;
    periodEnd: Date;
    key: string;
    writtenAt?: Date;
}
export interface ListShardsFilter {
    tier?: string;
    range?: {
        from: Date;
        to: Date;
    };
}
export interface ShardIndex {
    getWatermarks(pyramidName: string): Promise<Map<string, Date>>;
    recordShard(input: RecordShardInput): Promise<void>;
    listShards(pyramidName: string, filter?: ListShardsFilter): Promise<RecordedShard[]>;
}
export interface CachedShardIndexOptions {
    ttlMs?: number;
    now?: () => number;
}
export declare class CachedShardIndex implements ShardIndex {
    private readonly underlying;
    private readonly cache;
    private readonly inflight;
    private readonly ttlMs;
    private readonly now;
    constructor(underlying: ShardIndex, opts?: CachedShardIndexOptions);
    getWatermarks(pyramidName: string): Promise<Map<string, Date>>;
    recordShard(input: RecordShardInput): Promise<void>;
    listShards(pyramidName: string, filter?: ListShardsFilter): Promise<RecordedShard[]>;
    clear(): void;
}
//# sourceMappingURL=shard-index.d.ts.map
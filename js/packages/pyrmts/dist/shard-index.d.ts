import type { Duration } from './types.js';
export declare const WATERMARK_KEY_SEPARATOR = "@";
export declare function encodeWatermarkKey(tier: string, cadence: Duration | null): string;
export interface DecodedWatermarkKey {
    tier: string;
    cadence: Duration | null;
}
export declare function decodeWatermarkKey(key: string): DecodedWatermarkKey;
export interface RecordShardInput {
    pyramidName: string;
    tier: string;
    cadence: Duration | null;
    periodStart: Date;
    periodEnd: Date;
    key: string;
}
export interface ShardIndex {
    getWatermarks(pyramidName: string): Promise<Map<string, Date>>;
    recordShard(input: RecordShardInput): Promise<void>;
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
    clear(): void;
}
//# sourceMappingURL=shard-index.d.ts.map
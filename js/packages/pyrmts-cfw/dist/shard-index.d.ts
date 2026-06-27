import { type RecordShardInput, type ShardIndex } from 'pyrmts';
import type { D1Like } from './d1.js';
export interface D1ShardIndexOptions {
    watermarksTable?: string;
    shardsTable?: string;
    skipInventory?: boolean;
    now?: () => number;
}
export declare class D1ShardIndex implements ShardIndex {
    private readonly db;
    private readonly watermarksTable;
    private readonly shardsTable;
    private readonly skipInventory;
    private readonly now;
    constructor(db: D1Like, opts?: D1ShardIndexOptions);
    getWatermarks(pyramidName: string): Promise<Map<string, Date>>;
    recordShard(input: RecordShardInput): Promise<void>;
    static schemaSql(opts?: D1ShardIndexOptions): string[];
}
//# sourceMappingURL=shard-index.d.ts.map
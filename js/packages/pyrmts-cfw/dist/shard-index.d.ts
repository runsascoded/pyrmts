import { type ListShardsFilter, type RecordShardInput, type RecordedShard, type ShardIndex } from 'pyrmts';
import type { D1Like } from './d1.js';
export interface D1ShardIndexOptions {
    watermarksTable?: string;
    shardsTable?: string;
    skipInventory?: boolean;
    now?: () => number;
    extraColumns?: Record<string, string[]>;
}
export interface SchemaObject {
    name: string;
    kind: 'table' | 'index';
    sql: string;
    columns: string[];
}
export interface SchemaDiff {
    ok: boolean;
    missing: string[];
    mismatched: string[];
}
export declare class D1ShardIndex implements ShardIndex {
    private readonly db;
    private readonly watermarksTable;
    private readonly shardsTable;
    private readonly skipInventory;
    private readonly now;
    constructor(db: D1Like, opts?: D1ShardIndexOptions);
    getWatermarks(pyramidName: string): Promise<Map<string, Date>>;
    listShards(pyramidName: string, filter?: ListShardsFilter): Promise<RecordedShard[]>;
    recordShard(input: RecordShardInput): Promise<void>;
    static schemaSql(opts?: D1ShardIndexOptions): string[];
    static schemaObjects(opts?: D1ShardIndexOptions): SchemaObject[];
    static verifySchema(db: D1Like, opts?: D1ShardIndexOptions): Promise<SchemaDiff>;
}
//# sourceMappingURL=shard-index.d.ts.map
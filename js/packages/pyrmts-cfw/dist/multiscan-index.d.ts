import type { MultiScanIndexEntry } from 'pyrmts';
import type { D1Like } from './d1.js';
/** The `CREATE TABLE` a consumer runs to provision the manifest (byte-for-byte
 * the `pyrmts_engine.multiscan_index.multiscan_d1_ddl` shape). */
export declare function multiScanDdl(table?: string): string;
interface D1MultiScanRow {
    dataset: string;
    tier: string;
    shard_dur: string;
    period_start: number;
    period_end: number;
    key: string;
    scans: string;
    encoder: string;
    digests: string | null;
    written_at: number;
}
/** A `pyramid_multiscans` row → a `MultiScanIndexEntry` (parsing the JSON
 * `scans` / `digests` text). */
export declare function multiScanEntryFromRow(r: D1MultiScanRow): MultiScanIndexEntry;
export interface MultiScanD1IndexOptions {
    table?: string;
}
export interface ListMultiScansFilter {
    tier?: string;
    range?: {
        from: number;
        to: number;
    };
}
/** Reads the multi-scan routing manifest from D1. Read-only — the consumer
 * writes rows through its own CF-D1 path (`multiScanDdl` + the row shape). */
export declare class MultiScanD1Index {
    private readonly db;
    private readonly table;
    constructor(db: D1Like, opts?: MultiScanD1IndexOptions);
    listMultiScans(dataset: string, filter?: ListMultiScansFilter): Promise<MultiScanIndexEntry[]>;
}
export {};
//# sourceMappingURL=multiscan-index.d.ts.map
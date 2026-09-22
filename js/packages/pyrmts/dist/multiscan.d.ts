import type { Pyramid, Row } from './types.js';
export declare const MULTISCAN_META_KEY = "pyrmts.multiscan";
export declare const SCAN_COL = "__scan";
export declare const SCAN_LO = "__scan_lo";
export declare const SCAN_HI = "__scan_hi";
export type MultiScanEncoder = 'interval' | 'densify';
/** A consolidated tile plus the ordered member-scan labels its folded indices
 * refer to, and (when present) each scan's content digest. */
export interface MultiScan {
    rows: Row[];
    scans: string[];
    encoder: MultiScanEncoder;
    digests?: Record<string, string>;
}
type Schema = Pick<Pyramid, 'binCol' | 'dims' | 'metrics'>;
/** `[state, ...]` value tuple over the concatenated monoid state columns. */
export type State = unknown[];
/** One over-time point: a scan label and the key's state in that scan (the
 * monoid identity when the key is absent). */
export interface SeriesPoint {
    scan: string;
    state: Row;
}
export declare function keyStateCols(schema: Schema): {
    keyCols: string[];
    stateCols: string[];
};
/** The monoid identity per state column — the fill for an absent cell. Mirrors
 * Python `_identities`: additive (sum/count) → 0, histogram → null. */
export declare function identities(schema: Schema): Record<string, number | null>;
/** Reconstruct member `scan`'s original rows (logical round-trip), sorted
 * `(*dims, binCol)`. Throws if `scan` is not a member. */
export declare function extractScan(ms: MultiScan, schema: Schema, scan: string): Row[];
/** The universal changeset between two scan states (any two row-sets —
 * consolidated-and-extracted or raw): one row per key whose state differs, with
 * before (`__a`) and after (`__b`) state columns. Birth = identity→v, death =
 * v→identity. */
export declare function diffTables(rowsA: Row[], rowsB: Row[], schema: Schema): Row[];
/** The sparse diff of two member scans (same changeset shape as `diffTables`).
 * For `interval`, reads only the keys with a run boundary inside the span —
 * O(changes-in-span). Equivalent to `diffTables(extractScan(a), extractScan(b))`. */
export declare function diffScans(ms: MultiScan, schema: Schema, scanA: string, scanB: string): Row[];
/** A key's value stream across every member scan (the "size over time" line) —
 * `[{ scan, state }]`, absent scans carrying the monoid identity. `key` is a
 * row carrying the key columns (`binCol` + dims). */
export declare function seriesFor(ms: MultiScan, schema: Schema, key: Row): SeriesPoint[];
/** One consolidated-tile row of the routing manifest (`pyramid_multiscans`).
 * `scans` is the ordered member list — the routing key (fold-index =
 * `scans.indexOf(S)`). */
export interface MultiScanIndexEntry {
    dataset: string;
    tier: string;
    shardDur: string;
    periodStart: number;
    periodEnd: number;
    key: string;
    scans: string[];
    encoder: MultiScanEncoder;
    writtenAt: number;
    digests?: Record<string, string>;
}
/** Where a scan's data lives: the archive `key` and the scan's fold index within
 * it. */
export interface ScanLocation {
    key: string;
    foldIndex: number;
    encoder: MultiScanEncoder;
}
/** The routing decision: the archive covering `scan`, or null (the caller then
 * falls back to the single-scan `ShardIndex`). Assumes at most one covering
 * entry per tile (the driver never double-consolidates a scan). */
export declare function resolveScan(entries: MultiScanIndexEntry[], scan: string): ScanLocation | null;
/** Parse the JSONL routing manifest (as `pyrmts_engine.StorageJsonlMultiScanIndex`
 * writes it), optionally filtering to one `dataset` scope. */
export declare function parseMultiScanIndex(bytes: Uint8Array, dataset?: string): MultiScanIndexEntry[];
/** Stitch a key's over-time line across capped-K sealed groups: order the tile's
 * manifest entries by scan span, load each group's shard (via `load` — the
 * consumer's footer-pruned fetch), `seriesFor` within it, and concat in scan
 * order. Keeps the IO in the consumer and the routing/ordering in pyrmts. Pass
 * only one tile's entries (one dataset + tier + period lineage). */
export declare function seriesAcrossGroups(entries: MultiScanIndexEntry[], schema: Schema, key: Row, load: (archiveKey: string) => Promise<MultiScan>): Promise<SeriesPoint[]>;
/** Parse a `MultiScan` from parquet bytes — rows (int64 normalized to number,
 * matching the fetch path) plus the self-describing `pyrmts.multiscan`
 * KV-metadata (encoder / member scans / digests) the Python writer attaches. */
export declare function readMultiScan(bytes: Uint8Array): Promise<MultiScan>;
export {};
//# sourceMappingURL=multiscan.d.ts.map
import type { MetadataCache } from './fetch.js';
import { EtagConflict, type Storage } from './types.js';
export interface WalkCols {
    path: string;
    depth: string;
    size: string;
    count: string;
}
/** pyrmts / cw path indexes: `b` bytes, `o` objects. disk-tree: `size`, `n_desc`. */
export declare const DEFAULT_WALK_COLS: WalkCols;
export interface WalkStats {
    /** Row groups read (RG granularity). */
    requests: number;
    /** Data range GETs actually issued (hyparquet slices, excluding footer reads). */
    gets: number;
    /** Footer range GETs (cold opens only). */
    footerGets: number;
    /** Bytes moved by those GETs. */
    bytes: number;
    rgDecodes: number;
    rgCacheHits: number;
    footerParses: number;
    listings: number;
    expansions: number;
    /** GETs per dependent round (level-synchronous walk). */
    rounds: number[];
    /** Measured wall ms per round. */
    roundMs: number[];
    /** Per-stage ms summed over listings. `fetch` overlaps across concurrent
     * listings (network wait); `decode` and `post` are single-threaded CPU. */
    ms: {
        footer: number;
        locate: number;
        fetch: number;
        decode: number;
        post: number;
    };
}
export declare function newWalkStats(): WalkStats;
/** CPU-ish ms: everything except network wait. */
export declare function cpuMs(s: WalkStats): number;
/** Rounds that issued at least one GET. */
export declare function roundTrips(s: WalkStats): number;
/** Modelled wall: CPU + ⌈GETs / parallel⌉ trips per dependent round × RTT. */
export declare function wallModel(s: WalkStats, rttMs: number, parallel?: number): number;
export interface NodeState {
    size: number;
    count: number;
}
export type Listing = Map<string, NodeState>;
export interface SnapshotReaderOptions {
    cols?: WalkCols;
    stats?: WalkStats;
    /** Decoded-footer cache (see `fetchShardData`); on a hit no `head`, no
     * footer read, and data ranges carry `If-Match`. */
    metadataCache?: MetadataCache;
    /** Keep decoded row groups for this reader's life (one request). Default true. */
    rgCache?: boolean;
    /** Initial bytes-from-EOF for the footer read. */
    initialFetchSize?: number;
}
/** Per-directory children reader over one snapshot parquet. */
export declare class SnapshotReader {
    readonly storage: Storage;
    readonly key: string;
    readonly cols: WalkCols;
    readonly stats: WalkStats;
    private metadata;
    private size;
    private etag;
    private rgLo;
    private rgHi;
    private rgRowStart;
    private rgRows;
    private readonly rgCache;
    private readonly metadataCache;
    private readonly initialFetchSize;
    private opened;
    constructor(storage: Storage, key: string, opts?: SnapshotReaderOptions);
    /** Read (or take from the cache) the footer and build the RG key ranges. */
    open(): Promise<this>;
    private file;
    /** Row groups whose key range intersects `[(depth, lo), (depth, hi))`, as `[first, last)`. */
    private locate;
    private rgSpan;
    private readRun;
    private readRgs;
    private list;
    /** `{size, count}` of one node, or null if absent. */
    node(path: string): Promise<NodeState | null>;
    /** Direct children of `prefix` (a node at `depth`). */
    children(prefix: string, depth: number): Promise<Listing>;
}
export interface DeltaRow {
    path: string;
    /** Levels below the page path (1 = direct child). */
    depth: number;
    status: 'added' | 'removed' | 'changed';
    sizeA: number;
    sizeB: number;
    countA: number;
    countB: number;
    /** A dir we descended into. */
    expanded: boolean;
    /** Differing, not expanded (render floor or budget): change may hide below. */
    pruned: boolean;
}
export interface WalkResult {
    /** Changed rows met, sorted by |Δ| desc. */
    rows: DeltaRow[];
    expansions: number;
    truncated: boolean;
}
export interface WalkOptions {
    /** Render floor in bytes (see `renderFloor`). Default 0 = expand every change. */
    floor?: number;
    /** Expansion backstop. */
    budget?: number;
    order?: 'level' | 'bestfirst';
    /** Expansions in flight per round (each is two listings). */
    parallel?: number;
}
/** Smallest subtree (bytes) that can be drawn: a node's area ≈ its share of
 * the page root × the canvas, so `rootSize × cellPx² / (width × height)`. */
export declare function renderFloor(rootSize: number, widthPx: number, heightPx: number, minCellPx?: number): number;
/** Pruned recursive diff between two snapshots under `pagePath`. */
export declare function walkDiff(ra: SnapshotReader, rb: SnapshotReader, pagePath?: string, opts?: WalkOptions): Promise<WalkResult>;
export { EtagConflict };
//# sourceMappingURL=walkdiff.d.ts.map
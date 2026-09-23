import { type FileMetaData, type RowGroup, type SchemaElement } from 'hyparquet';
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
     * listings (network wait); `decode` includes hyparquet's planning and the
     * fetch wait inside it, so it is an upper bound on decode CPU. */
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
/** What locating a listing needs per row group: its row range and the
 * `(depth, path)` min/max from the footer statistics. JSON-able: a consumer
 * stores these in D1 / a manifest blob so the edge never parses the footer. */
export interface RowGroupSummary {
    rowStart: number;
    numRows: number;
    depthMin: number;
    depthMax: number;
    pathMin: string;
    pathMax: string;
}
/** A pre-computed row-group index in place of the parquet footer. `rowGroup(i)`
 * returns hyparquet's per-group metadata (column-chunk offsets / sizes / codec
 * / encodings — what decoding group `i` needs), served however the consumer
 * likes: all at once from a manifest, or one group at a time from D1. */
export interface RowGroupIndex {
    size: number;
    etag?: string;
    schema: SchemaElement[];
    groups: RowGroupSummary[];
    rowGroup(i: number): RowGroup | Promise<RowGroup>;
}
/** The per-group summaries from a parsed footer — the producer side of
 * `RowGroupIndex.groups`. Requires `(depth, path)` statistics. */
export declare function rowGroupSummaries(metadata: FileMetaData, cols?: WalkCols): RowGroupSummary[];
/** A `RowGroupIndex` over an already-parsed footer (in-memory `rowGroup`),
 * e.g. to build what a producer persists. */
export declare function rowGroupIndexFromMetadata(metadata: FileMetaData, size: number, etag?: string, cols?: WalkCols): RowGroupIndex;
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
    /** Pre-supplied row-group index: no `head`, no footer read or parse; the
     * per-group metadata comes from `rowGroups.rowGroup(i)` on demand. */
    rowGroups?: RowGroupIndex;
    /** hyparquet's fetch coalescing budget for a run of row groups: the max
     * share of a fetch that no selected column chunk needs, and the max bytes
     * per fetch. Default `{ 1, Infinity }` = one GET per run regardless of the
     * unselected columns' share; lower the ratio to trade GETs for bytes. */
    maxOverfetchRatio?: number;
    maxRunBytes?: number;
}
/** Per-directory children reader over one snapshot parquet. */
export declare class SnapshotReader {
    readonly storage: Storage;
    readonly key: string;
    readonly cols: WalkCols;
    readonly stats: WalkStats;
    private schema;
    private size;
    private etag;
    private readonly index;
    private readonly groupMeta;
    private rgLo;
    private rgHi;
    private rgRowStart;
    private rgRows;
    private readonly rgCache;
    private readonly metadataCache;
    private readonly initialFetchSize;
    private readonly maxOverfetchRatio;
    private readonly maxRunBytes;
    private opened;
    constructor(storage: Storage, key: string, opts?: SnapshotReaderOptions);
    /** Build the RG key ranges from the pre-supplied index, the cached footer,
     * or a footer read. */
    open(): Promise<this>;
    /** Per-group metadata: from the parsed footer, or `rowGroups.rowGroup(i)` (memoized). */
    private rowGroupMeta;
    private file;
    /** Row groups whose key range intersects `[(depth, lo), (depth, hi))`, as `[first, last)`. */
    private locate;
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
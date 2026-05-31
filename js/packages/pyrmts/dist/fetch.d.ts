import type { Row } from './monoids.js';
import type { Storage } from './types.js';
export interface FetchOptions {
    binCol?: string;
    range?: {
        from: Date;
        to: Date;
    };
    initialFetchSize?: number;
    tolerate404?: boolean;
    filters?: ColumnFilter[];
    trace?: FetchTrace[];
}
/** One observed `slice(start, end)` against a parquet file. */
export interface FetchTrace {
    /** Storage key (parquet path) the slice was against. */
    key: string;
    /** Inclusive lower byte offset. */
    start: number;
    /** Exclusive upper byte offset. */
    end: number;
    /** `end - start`. Length in bytes of the range that came back. */
    length: number;
    /** Wall-clock milliseconds for the slice (storage call). */
    ms: number;
    /** Bucketed phase the slice happened in: `metadata` (footer / metadata
     *  read) or `data` (column-chunk reads after planning). Heuristic — the
     *  first 1-2 slices per file are the footer; subsequent slices are
     *  column chunks. */
    phase: 'metadata' | 'data';
}
export type ColumnFilter = {
    col: string;
    values: readonly string[] | readonly number[];
} | {
    col: string;
    range: {
        min: number;
        max: number;
    };
};
export declare function fetchShardData(storage: Storage, key: string, opts?: FetchOptions): Promise<Row[]>;
export declare function fetchSegmentRows(storage: Storage, keys: string[], opts?: FetchOptions): Promise<Row[]>;
//# sourceMappingURL=fetch.d.ts.map
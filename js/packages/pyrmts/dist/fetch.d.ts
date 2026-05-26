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
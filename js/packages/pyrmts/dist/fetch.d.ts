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
}
export declare function fetchShardData(storage: Storage, key: string, opts?: FetchOptions): Promise<Row[]>;
export declare function fetchSegmentRows(storage: Storage, keys: string[], opts?: FetchOptions): Promise<Row[]>;
//# sourceMappingURL=fetch.d.ts.map
import { type FileMetaData } from 'hyparquet';
import { type FetchOptionsBase, type Row, type Storage, type StorageBackend } from './types.js';
export interface FetchOptions extends FetchOptionsBase {
    initialFetchSize?: number;
    trace?: FetchTrace[];
    metadataCache?: MetadataCache;
}
/** A cached decoded footer plus what validates it. */
export interface CachedMetadata {
    etag: string;
    size: number;
    metadata: FileMetaData;
}
/** Map-like store for decoded parquet footers, keyed by storage key
 * (`Map<string, CachedMetadata>` satisfies it). */
export interface MetadataCache {
    get(key: string): CachedMetadata | undefined;
    set(key: string, entry: CachedMetadata): void;
    delete?(key: string): void;
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
export declare function fetchShardData(storage: Storage, key: string, opts?: FetchOptions): Promise<Row[]>;
export declare function parquetBackend(storage: Storage, _keyTemplate?: string): StorageBackend<FetchOptions>;
//# sourceMappingURL=fetch.d.ts.map
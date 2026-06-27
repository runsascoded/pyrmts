import type { FetchOptionsBase, StorageBackend } from 'pyrmts';
export interface D1Like {
    prepare(query: string): D1PreparedStatement;
    batch?(statements: D1PreparedStatement[]): Promise<unknown[]>;
}
export interface D1PreparedStatement {
    bind(...values: unknown[]): D1PreparedStatement;
    all<T = Record<string, unknown>>(): Promise<{
        results: T[];
        success?: boolean;
        meta?: unknown;
    }>;
    run?(): Promise<{
        success?: boolean;
        meta?: unknown;
    }>;
}
export interface D1BackendOptions {
    tableTemplate: string;
    selectCols?: readonly string[];
    chunkSize?: number;
}
export declare function d1Backend(db: D1Like, options: D1BackendOptions): StorageBackend<FetchOptionsBase>;
//# sourceMappingURL=d1.d.ts.map
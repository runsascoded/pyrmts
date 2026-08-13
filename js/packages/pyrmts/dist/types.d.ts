export type Axis = 'time' | 'step';
export type TimeUnit = 'min' | 'h' | 'd' | 'mo' | 'y';
export type Duration = `${number}${TimeUnit}`;
export type StepUnit = 'step' | 'steps' | 'ksteps' | 'msteps';
export type StepCount = `${number}${StepUnit}`;
export type RunBoundary = '1run';
export type Bin = Duration | StepCount;
export type Shard = Duration | RunBoundary;
export interface Tier {
    name: string;
    bin: Bin;
    shards: Shard[];
}
export interface Dim {
    name: string;
    type: 'int' | 'string' | 'h3' | 'geohash' | 's2';
}
export type MonoidName = 'sum' | 'count' | 'histogram' | 'topk' | 'botk' | 'hll' | 'tdigest';
export interface Metric {
    name: string;
    monoid: MonoidName;
    config?: Record<string, unknown>;
}
export interface Storage {
    head(key: string): Promise<{
        size: number;
        etag?: string;
    } | null>;
    getRange(key: string, start: number, end: number): Promise<Uint8Array>;
    get(key: string): Promise<Uint8Array | null>;
    put(key: string, bytes: Uint8Array): Promise<void>;
    list(prefix: string): AsyncIterable<string>;
    getWithEtag?(key: string): Promise<[Uint8Array | null, string | null]>;
    putIfMatch?(key: string, bytes: Uint8Array, etag: string | null): Promise<void>;
    listWithMtime?(prefix: string): AsyncIterable<[string, Date | null]>;
}
export declare class EtagConflict extends Error {
    constructor(message: string);
}
export declare class NotSupported extends Error {
    constructor(message: string);
}
export type Row = Record<string, unknown>;
export interface FetchSegment {
    from: Date;
    to: Date;
    shardTier: Tier;
    keys: readonly string[];
}
export interface StorageBackend<Opts = FetchOptionsBase> {
    readonly name: string;
    fetchSegment(segment: FetchSegment, opts?: Opts): Promise<Row[]>;
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
export interface FetchOptionsBase {
    binCol?: string;
    range?: {
        from: Date;
        to: Date;
    };
    filters?: ColumnFilter[];
    tolerate404?: boolean;
}
export interface Pyramid {
    storage: StorageBackend;
    keyTemplate: string;
    axis: Axis;
    binCol: string;
    dims: Dim[];
    metrics: Metric[];
    tiers: Tier[];
    geo?: GeoSpec;
}
export interface GeoSpec {
    cellCol: string;
    resolutions: number[];
}
//# sourceMappingURL=types.d.ts.map
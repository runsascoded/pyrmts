export type Axis = 'time' | 'step';
export type TimeUnit = 'min' | 'h' | 'd' | 'mo' | 'y';
export type Duration = `${number}${TimeUnit}`;
export type StepUnit = 'step' | 'steps' | 'ksteps' | 'msteps';
export type StepCount = `${number}${StepUnit}`;
export type RunBoundary = '1run';
export type Bin = Duration | StepCount;
export type Shard = Duration | RunBoundary | 'all';
export interface Tier {
    name: string;
    bin: Bin;
    shard: Shard;
}
export interface Dim {
    name: string;
    type: 'int' | 'string' | 'h3' | 'geohash';
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
}
export interface Pyramid {
    storage: Storage;
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
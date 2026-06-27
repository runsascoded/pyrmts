import type { Axis, Dim, Duration, GeoSpec, Metric, Pyramid, StorageBackend, Tier } from './types.js';
export interface PyramidConfig {
    storage: {
        type: string;
        [key: string]: unknown;
    };
    keyTemplate: string;
    partialKey?: string;
    axis: Axis;
    binCol: string;
    dims: Dim[];
    metrics: Metric[];
    tiers: Tier[];
    partials?: Duration[];
    geo?: GeoSpec;
}
export declare function parsePyramidYaml(text: string): PyramidConfig;
export declare function pyramidFromConfig(cfg: PyramidConfig, storage: StorageBackend): Pyramid;
//# sourceMappingURL=yaml.d.ts.map
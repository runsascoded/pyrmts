import type { Axis, Dim, GeoSpec, Metric, Pyramid, StorageBackend, Tier } from './types.js';
export interface PyramidConfig {
    storage: {
        type: string;
        [key: string]: unknown;
    };
    keyTemplate: string;
    axis: Axis;
    binCol: string;
    dims: Dim[];
    metrics: Metric[];
    tiers: Tier[];
    geo?: GeoSpec;
}
export declare function parsePyramidYaml(text: string): PyramidConfig;
export declare function validateShardPlaceholder(keyTemplate: string, tiers: Tier[]): void;
export declare function pyramidFromConfig(cfg: PyramidConfig, storage: StorageBackend): Pyramid;
//# sourceMappingURL=yaml.d.ts.map
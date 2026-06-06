import type { BBox, SpatialIndex } from './spatial-index.js';
export interface ConformanceOpts {
    samplePoint: {
        lat: number;
        lng: number;
    };
    sampleBBox: BBox;
    sampleLevel: number;
    coarserLevel: number;
}
export declare function assertSpatialIndex(index: SpatialIndex, opts: ConformanceOpts): void;
//# sourceMappingURL=spatial-index-conformance.d.ts.map
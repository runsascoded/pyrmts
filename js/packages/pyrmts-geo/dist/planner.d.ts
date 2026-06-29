import { type PlanSegment, type QueryPlan, type Tier } from 'pyrmts';
import type { BBox, GeoPyramid, SpatialIndex } from './spatial-index.js';
export type { BBox } from './spatial-index.js';
export declare function bboxToCells(bbox: BBox, level: number): string[];
export interface PlanGeoQueryInput {
    range: {
        from: Date;
        to: Date;
    };
    binBudget: number;
    bbox?: BBox;
    cellBudget?: number;
    outputCells?: {
        res: number;
        cells: readonly string[];
    };
    watermarks?: Record<string, Date>;
    earliestWatermarks?: Record<string, Date>;
    earliestPerShard?: Record<string, Date>;
    filter?: Record<string, string | number>;
    smoothing?: import('pyrmts').SmoothingSpec;
    smoothMode?: import('pyrmts').SmoothMode;
}
export interface GeoPlanSegment extends PlanSegment {
    cells: string[];
}
export interface GeoQueryPlan extends Omit<QueryPlan, 'segments'> {
    outputRes: number;
    outputCells: string[];
    segments: GeoPlanSegment[];
}
export declare function planGeoQuery(pyramid: GeoPyramid, input: PlanGeoQueryInput): GeoQueryPlan;
export declare function filterCellsAndRes(rows: Array<Record<string, unknown>>, cellCol: string, outputRes: number, allowedCells: string[], index?: SpatialIndex): Array<Record<string, unknown>>;
export declare function filterCellsByCover(rows: Array<Record<string, unknown>>, cellCol: string, level: number, cover: import('./spatial-index.js').SpatialSet, index: SpatialIndex): Array<Record<string, unknown>>;
export type { Tier };
//# sourceMappingURL=planner.d.ts.map
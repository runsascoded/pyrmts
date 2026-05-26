import { type PlanSegment, type Pyramid, type QueryPlan, type Tier } from 'pyrmts';
export interface BBox {
    minLat: number;
    maxLat: number;
    minLng: number;
    maxLng: number;
}
export interface PlanGeoQueryInput {
    range: {
        from: Date;
        to: Date;
    };
    binBudget: number;
    bbox: BBox;
    cellBudget: number;
    watermarks?: Record<string, Date>;
    earliestWatermarks?: Record<string, Date>;
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
export declare function planGeoQuery(pyramid: Pyramid, input: PlanGeoQueryInput): GeoQueryPlan;
export declare function bboxToCells(bbox: BBox, res: number): string[];
export declare function filterCellsAndRes(rows: Array<Record<string, unknown>>, cellCol: string, outputRes: number, allowedCells: string[]): Array<Record<string, unknown>>;
export type { Tier };
//# sourceMappingURL=planner.d.ts.map
import type { Row } from './monoids.js';
export interface PivotTallToHistogramOptions {
    histogramCol: string;
    categoryCol: string;
    countCol: string;
    groupBy: string[];
}
export declare function pivotTallToHistogram(rows: Row[], opts: PivotTallToHistogramOptions): Row[];
//# sourceMappingURL=pivot.d.ts.map
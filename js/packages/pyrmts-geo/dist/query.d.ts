import type { Duration, Row, SmoothMode } from 'pyrmts';
import type { BBox } from './planner.js';
export interface FetchPyramidGeoQueryInput {
    url: string;
    range: {
        from: Date;
        to: Date;
    };
    binBudget: number;
    bbox: BBox;
    cellBudget: number;
    filter?: Record<string, string | number>;
    smoothing?: Duration | 'auto' | `auto${number}`;
    smoothMode?: SmoothMode;
    signal?: AbortSignal;
    fetchImpl?: typeof fetch;
}
export interface GeoPlanMeta {
    outputTier?: string;
    outputBin: string;
    outputRes: number;
    outputCells: string[];
    authoritativeEnd: string | null;
    smoothing: {
        smoothBin: string;
        smoothBinCount: number;
        smoothMode: SmoothMode;
        smoothSourceTier: string;
    } | null;
    segments: Array<{
        tier: string;
        from: string;
        to: string;
        reaggregate: boolean;
        keys: string[];
    }>;
}
export interface PyramidGeoQueryResult {
    records: Row[];
    plan: GeoPlanMeta;
}
export declare function fetchPyramidGeoQuery(input: FetchPyramidGeoQueryInput): Promise<PyramidGeoQueryResult>;
export declare function buildGeoQueryUrl(input: Pick<FetchPyramidGeoQueryInput, 'url' | 'range' | 'binBudget' | 'bbox' | 'cellBudget' | 'filter' | 'smoothing' | 'smoothMode'>): string;
//# sourceMappingURL=query.d.ts.map
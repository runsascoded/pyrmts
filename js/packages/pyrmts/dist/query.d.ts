import type { Row } from './monoids.js';
import type { SmoothMode } from './planner.js';
import type { Duration } from './types.js';
export interface FetchPyramidQueryInput {
    url: string;
    range: {
        from: Date;
        to: Date;
    };
    binBudget: number;
    filter?: Record<string, string | number>;
    smoothing?: Duration | 'auto' | `auto${number}`;
    smoothMode?: SmoothMode;
    signal?: AbortSignal;
    fetchImpl?: typeof fetch;
}
export interface PlanMeta {
    outputTier?: string;
    outputBin: string;
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
export interface PyramidQueryResult {
    records: Row[];
    plan: PlanMeta;
}
export declare function fetchPyramidQuery(input: FetchPyramidQueryInput): Promise<PyramidQueryResult>;
export declare function buildQueryUrl(input: Pick<FetchPyramidQueryInput, 'url' | 'range' | 'binBudget' | 'filter' | 'smoothing' | 'smoothMode'>): string;
//# sourceMappingURL=query.d.ts.map
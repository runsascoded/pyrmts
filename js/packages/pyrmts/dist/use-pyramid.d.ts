import type { Row } from './monoids.js';
import { type FetchPyramidQueryInput, type PlanMeta } from './query.js';
export interface UsePyramidInput extends Omit<FetchPyramidQueryInput, 'signal' | 'fetchImpl'> {
}
export interface UsePyramidResult {
    records: Row[];
    plan: PlanMeta | null;
    isLoading: boolean;
    error: Error | null;
}
export declare function usePyramid(input: UsePyramidInput): UsePyramidResult;
//# sourceMappingURL=use-pyramid.d.ts.map
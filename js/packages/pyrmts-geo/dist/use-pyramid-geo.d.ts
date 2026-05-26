import type { Row } from 'pyrmts';
import { type FetchPyramidGeoQueryInput, type GeoPlanMeta } from './query.js';
export interface UsePyramidGeoInput extends Omit<FetchPyramidGeoQueryInput, 'signal' | 'fetchImpl'> {
}
export interface UsePyramidGeoResult {
    records: Row[];
    plan: GeoPlanMeta | null;
    isLoading: boolean;
    error: Error | null;
}
export declare function usePyramidGeo(input: UsePyramidGeoInput): UsePyramidGeoResult;
//# sourceMappingURL=use-pyramid-geo.d.ts.map
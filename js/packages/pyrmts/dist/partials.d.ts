import type { Duration, Pyramid } from './types.js';
export interface ValidatedPartials {
    cadences: Duration[];
    perTier: Record<string, Duration[]>;
}
export declare function validatePartials(pyramid: Pyramid): ValidatedPartials | null;
//# sourceMappingURL=partials.d.ts.map
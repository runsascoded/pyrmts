import type { Pyramid, Shard, Tier } from './types.js';
export interface ValidatedTierLadder {
    tier: Tier;
    shardsMs: Array<{
        shard: Shard;
        ms: number | null;
    }>;
}
export declare function validateLadders(pyramid: Pyramid): ValidatedTierLadder[];
//# sourceMappingURL=ladder.d.ts.map
import type { Pyramid, Tier } from './types.js';
export declare function sourceTierFor(pyramid: Pick<Pyramid, 'tiers'>, tierName: string): Tier | null;
export declare function shardBuildableAt(pyramid: Pick<Pyramid, 'tiers'>, tierName: string, periodEnd: Date): Date;
//# sourceMappingURL=cascade-source.d.ts.map
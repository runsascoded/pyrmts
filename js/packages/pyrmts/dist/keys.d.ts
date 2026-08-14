import type { Pyramid, Shard } from './types.js';
export declare function substituteKey(template: string, values: Record<string, string | number>): string;
export declare function shardKey(pyramid: Pyramid, tierName: string, shardDur: Shard, periodStart: Date, filter?: Record<string, string | number>): string;
//# sourceMappingURL=keys.d.ts.map
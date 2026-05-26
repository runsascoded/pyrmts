import { type Row } from './monoids.js';
import type { QueryPlan } from './planner.js';
import type { Pyramid } from './types.js';
export type { Row } from './monoids.js';
export interface StitchInput {
    pyramid: Pyramid;
    plan: QueryPlan;
    shardRows: Row[][];
}
export declare function stitch(input: StitchInput): Row[];
//# sourceMappingURL=stitch.d.ts.map
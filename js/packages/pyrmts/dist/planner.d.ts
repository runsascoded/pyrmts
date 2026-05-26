import type { Bin, Pyramid, Tier } from './types.js';
export interface PlanQueryInput {
    range: {
        from: Date;
        to: Date;
    };
    binBudget: number;
    watermarks?: Record<string, Date>;
    earliestWatermarks?: Record<string, Date>;
    filter?: Record<string, string | number>;
}
export interface PlanSegment {
    from: Date;
    to: Date;
    shardTier: Tier;
    keys: string[];
    reaggregate: boolean;
}
export interface QueryPlan {
    outputTier: Tier;
    outputBin: Bin;
    segments: PlanSegment[];
    authoritativeEnd: Date | null;
}
export declare function planQuery(pyramid: Pyramid, input: PlanQueryInput): QueryPlan;
//# sourceMappingURL=planner.d.ts.map
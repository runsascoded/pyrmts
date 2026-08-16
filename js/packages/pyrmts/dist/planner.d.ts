import { type RecordedShard } from './shard-index.js';
import { type Bin, type Duration, type PlanLimits, type Pyramid, type Shard, type Tier } from './types.js';
export type SmoothMode = 'centered' | 'trailing';
export type SmoothingSpec = Duration | {
    auto: true;
    multiplier?: number;
};
export interface PlanQueryInput {
    range: {
        from: Date;
        to: Date;
    };
    binBudget: number;
    targetBin?: Duration;
    limits?: PlanLimits;
    watermarks?: Record<string, Date>;
    earliestWatermarks?: Record<string, Date>;
    earliestPerShard?: Record<string, Date>;
    filter?: Record<string, string | number>;
    smoothing?: SmoothingSpec;
    smoothMode?: SmoothMode;
}
export declare const DEFAULT_AUTO_MULTIPLIER = 50;
export interface PlanSegment {
    from: Date;
    to: Date;
    shardTier: Tier;
    shardDur: Shard;
    keys: string[];
    reaggregate: boolean;
}
export interface QueryPlan {
    outputTier?: Tier;
    outputBin: Bin;
    segments: PlanSegment[];
    authoritativeEnd: Date | null;
    visibleRange: {
        from: Date;
        to: Date;
    };
    smoothing: {
        smoothBin: Duration;
        smoothBinCount: number;
        smoothMode: SmoothMode;
        smoothSourceTier: string;
    } | null;
    atomCount: number;
}
export declare function planQuery(pyramid: Pyramid, input: PlanQueryInput): QueryPlan;
export declare function planQueryFromInventory(pyramid: Pyramid, input: PlanQueryInput, registeredShards: RecordedShard[]): QueryPlan;
//# sourceMappingURL=planner.d.ts.map
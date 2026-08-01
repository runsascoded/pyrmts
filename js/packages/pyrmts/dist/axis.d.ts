import type { Duration, Shard, TimeUnit } from './types.js';
export interface ParsedTimeSpan {
    count: number;
    unit: TimeUnit;
}
export declare function parseDuration(s: Duration | string): ParsedTimeSpan;
export declare function fixedDurationMs(d: Duration | string): number;
export declare function addSpan(t: Date, span: ParsedTimeSpan): Date;
export declare function floorToSpan(t: Date, span: ParsedTimeSpan): Date;
export declare function ceilToSpan(t: Date, span: ParsedTimeSpan): Date;
export declare function binsInRange(from: Date, to: Date, bin: Duration): number;
export declare function shardPeriodsCovering(from: Date, to: Date, shard: Shard): {
    start: Date;
    end: Date;
    label: string;
}[];
export declare function formatPeriod(t: Date, span: ParsedTimeSpan): string;
//# sourceMappingURL=axis.d.ts.map
import type { MonoidName, Row } from './types.js';
export type { Row } from './types.js';
export interface Monoid {
    stateSuffixes: string[];
    combine(target: Row, source: Row, metricName: string): void;
    init?(target: Row, metricName: string): void;
}
export declare function getMonoid(name: MonoidName): Monoid;
export declare function stateColumns(monoid: MonoidName, metricName: string): string[];
//# sourceMappingURL=monoids.d.ts.map
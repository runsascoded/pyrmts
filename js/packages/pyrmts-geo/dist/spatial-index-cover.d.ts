import type { MinimalCoverOpts, SpatialIndex, SpatialSet } from './spatial-index.js';
export declare function minimalCover(index: SpatialIndex, include: string[], system: string[], opts?: MinimalCoverOpts): SpatialSet<string>;
export declare function isCellInCover(index: SpatialIndex, cell: string, cover: SpatialSet<string>): boolean;
//# sourceMappingURL=spatial-index-cover.d.ts.map
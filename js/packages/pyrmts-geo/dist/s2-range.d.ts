/** Level of the S2 leaf cells; every level's marker bit sits at
 *  `2 * (LEAF_LEVEL - level)`. */
export declare const S2_LEAF_LEVEL = 30;
/** Marker bit for a cell at `level` — the single "1" bit that
 *  separates real digits from trailing zeros. */
export declare function s2LsbForLevel(level: number): bigint;
/** Extract the level of an S2 cell id — the position of the low-order
 *  1 bit tells us where the marker is. */
export declare function s2LevelOf(id: bigint): number;
/** Cell id of the ancestor at `targetLevel`. Errors if `targetLevel`
 *  is not strictly less than the cell's own level. */
export declare function s2Parent(id: bigint, targetLevel: number): bigint;
/** Parse a token — hex, trailing zeros stripped, or `"X"` for id 0 —
 *  back to a bigint cell id. */
export declare function s2TokenToId(token: string): bigint;
/** Inverse: format a bigint cell id back to a token. */
export declare function s2IdToToken(id: bigint): string;
/** Inclusive `[lo, hi]` range of base-level cell ids that descend from
 *  a parent at some level N ≤ `baseLevel`. When N == baseLevel, the
 *  range collapses to `parent, parent`. */
export type S2CellRange = {
    lo: bigint;
    hi: bigint;
};
/** Merge overlapping or adjacent ranges; output sorted by `lo`.
 *
 *  Grid-agnostic (it only ever compares `{lo, hi}` bigints).
 *
 *  Adjacency check is `a.hi + 1 >= b.lo`. Merging non-adjacent ranges
 *  would also be *correct* (a gap just reads extra row groups), but both
 *  consumers — parquet row-group pruning and SQL `BETWEEN … OR …` —
 *  do less work with minimal, disjoint ranges. */
export declare function mergeRanges(ranges: S2CellRange[]): S2CellRange[];
/** Pairwise intersection of two range sets. Both are typically small
 *  (≤ a few dozen), so the quadratic scan is cheaper than sorting. */
export declare function intersectRanges(a: S2CellRange[], b: S2CellRange[]): S2CellRange[];
/** `[lo, hi]` of `parent`'s descendants at `baseLevel` (see the closed
 *  form in the module docs). */
export declare function s2RangeForCell(parent: bigint, baseLevel: number): S2CellRange;
/** Same, but with tokens on the outside — convenient for callers that
 *  read a shard/parent token from a manifest or client request and want
 *  the base-level range as tokens (for SQL `cellid BETWEEN lo AND hi`
 *  on a TEXT column). */
export declare function s2RangeForCellToken(parentToken: string, baseLevel: number): {
    lo: string;
    hi: string;
};
/** Merged `[lo, hi]` ranges of a whole cover's descendants at
 *  `baseLevel` — the composition every range-predicate consumer wants:
 *  cover cells (e.g. from `s2Index.bboxToCells` or `minimalCover`) in,
 *  disjoint predicate ranges out. Cover cells may sit at mixed levels;
 *  each must be at or above (coarser than) `baseLevel`. Nested /
 *  overlapping cover cells collapse; note *sibling* cells do NOT (their
 *  ranges are separated by a 2·child_lsb gap of finer-level ids), so a
 *  cover of k cells yields up to k ranges. */
export declare function s2RangesForCells(cells: string[], baseLevel: number): S2CellRange[];
//# sourceMappingURL=s2-range.d.ts.map
// S2 cell-id bit math for range-predicate serving: the base-level
// descendants of any S2 cell form one contiguous numeric id range, so a
// cover of coarse cells becomes a small set of `[lo, hi]` ranges usable
// as parquet row-group pruning predicates or SQL `cellid BETWEEN lo AND
// hi` filters (D1/SQLite, on either an INTEGER id or the TEXT token —
// trailing-zero-stripped tokens preserve numeric order under lex
// compare, since `'0'` is the lowest hex char).
//
// Upstreamed from nj-crashes `cells-api/src/s2-range.ts`, where it
// drives both consumers (hyparquet row-group pruning + D1 BETWEEN); its
// closed form is verified there against `nodes2ts` and Python
// `s2sphere`, and here against `s2js`.
//
// S2 cell ID layout (64 bits, MSB → LSB):
//
//     3 face bits | 2N digit bits | 1 marker bit | (60 - 2N) zero bits
//
// where N is the cell's level (0-30). The single "1" marker bit sits at
// position `2 * (30 - N)` and separates the meaningful digits from the
// trailing zero-fill. All 4 leaf-descendants of a level-N cell at level
// (N+1) collapse the parent's marker bit into the top bit of a new
// 2-bit digit, and place a new marker two positions lower.
//
// For a uniform base-level B table sorted numerically, the base-level
// descendants of any ancestor cell P at level N (N < B) share P's bits
// above `2 * (30 - N)` exactly, and vary only in the (B - N) new digits
// between the two markers — hence the contiguous range, with closed
// form:
//
//     parent_lsb = 1 << 2 * (30 - N)
//     child_lsb  = 1 << 2 * (30 - B)
//     lo = P - parent_lsb + child_lsb
//     hi = P + parent_lsb - child_lsb
//
// This module is deliberately dependency-free (pure bigint math, no
// `s2js`): tokens here are the standard S2 hex form (uint64 → 16-char
// lowercase hex, trailing zeros stripped, `"X"` for id 0), identical
// across `s2js`, `nodes2ts`, and Python `s2sphere`, so callers can mix
// it with any of them.
/** Level of the S2 leaf cells; every level's marker bit sits at
 *  `2 * (LEAF_LEVEL - level)`. */
export const S2_LEAF_LEVEL = 30;
/** Marker bit for a cell at `level` — the single "1" bit that
 *  separates real digits from trailing zeros. */
export function s2LsbForLevel(level) {
    if (level < 0 || level > S2_LEAF_LEVEL) {
        throw new Error(`bad S2 level ${level}`);
    }
    return 1n << BigInt(2 * (S2_LEAF_LEVEL - level));
}
/** Extract the level of an S2 cell id — the position of the low-order
 *  1 bit tells us where the marker is. */
export function s2LevelOf(id) {
    if (id === 0n)
        throw new Error('zero cell id has no level');
    // Position of lowest set bit = trailing zero count.
    let tz = 0;
    let v = id;
    while ((v & 1n) === 0n) {
        v >>= 1n;
        tz++;
    }
    // Marker at 2*(30-N) → tz must be even, N = 30 - tz/2.
    if (tz % 2 !== 0)
        throw new Error(`odd marker bit position ${tz} — not a valid cell id`);
    return S2_LEAF_LEVEL - tz / 2;
}
/** Cell id of the ancestor at `targetLevel`. Errors if `targetLevel`
 *  is not strictly less than the cell's own level. */
export function s2Parent(id, targetLevel) {
    const own = s2LevelOf(id);
    if (targetLevel >= own) {
        throw new Error(`target level ${targetLevel} not coarser than cell level ${own}`);
    }
    const lsb = s2LsbForLevel(targetLevel);
    // Clear everything below the target marker, then set the marker.
    return (id & ~(lsb - 1n)) | lsb;
}
/** Parse a token — hex, trailing zeros stripped, or `"X"` for id 0 —
 *  back to a bigint cell id. */
export function s2TokenToId(token) {
    if (token === 'X' || token === 'x')
        return 0n;
    if (token.length === 0 || token.length > 16) {
        throw new Error(`bad S2 token "${token}"`);
    }
    // Right-pad with zeros to 16 chars, then parse as base-16.
    const padded = token + '0'.repeat(16 - token.length);
    return BigInt('0x' + padded);
}
/** Inverse: format a bigint cell id back to a token. */
export function s2IdToToken(id) {
    if (id === 0n)
        return 'X';
    if (id < 0n)
        throw new Error('negative cell id');
    const hex = id.toString(16).padStart(16, '0');
    // A non-zero id must have at least the marker bit → at least one
    // non-zero nibble → the trim result can't be empty.
    return hex.replace(/0+$/, '');
}
/** Merge overlapping or adjacent ranges; output sorted by `lo`.
 *
 *  Grid-agnostic (it only ever compares `{lo, hi}` bigints).
 *
 *  Adjacency check is `a.hi + 1 >= b.lo`. Merging non-adjacent ranges
 *  would also be *correct* (a gap just reads extra row groups), but both
 *  consumers — parquet row-group pruning and SQL `BETWEEN … OR …` —
 *  do less work with minimal, disjoint ranges. */
export function mergeRanges(ranges) {
    if (ranges.length === 0)
        return [];
    const sorted = [...ranges].sort((a, b) => (a.lo < b.lo ? -1 : a.lo > b.lo ? 1 : 0));
    // Copy on the way out: widening `top.hi` in place would otherwise
    // reach back through the shared reference and mutate the caller's
    // range objects.
    const out = [{ ...sorted[0] }];
    for (let i = 1; i < sorted.length; i++) {
        const top = out[out.length - 1];
        const next = sorted[i];
        if (next.lo <= top.hi + 1n) {
            if (next.hi > top.hi)
                top.hi = next.hi;
        }
        else {
            out.push({ ...next });
        }
    }
    return out;
}
/** Pairwise intersection of two range sets. Both are typically small
 *  (≤ a few dozen), so the quadratic scan is cheaper than sorting. */
export function intersectRanges(a, b) {
    const out = [];
    for (const x of a) {
        for (const y of b) {
            const lo = x.lo > y.lo ? x.lo : y.lo;
            const hi = x.hi < y.hi ? x.hi : y.hi;
            if (lo <= hi)
                out.push({ lo, hi });
        }
    }
    return out;
}
/** `[lo, hi]` of `parent`'s descendants at `baseLevel` (see the closed
 *  form in the module docs). */
export function s2RangeForCell(parent, baseLevel) {
    const parentLevel = s2LevelOf(parent);
    if (baseLevel < parentLevel) {
        throw new Error(`baseLevel ${baseLevel} coarser than parent level ${parentLevel}`);
    }
    if (baseLevel === parentLevel) {
        return { lo: parent, hi: parent };
    }
    const parentLsb = s2LsbForLevel(parentLevel);
    const childLsb = s2LsbForLevel(baseLevel);
    return {
        lo: parent - parentLsb + childLsb,
        hi: parent + parentLsb - childLsb,
    };
}
/** Same, but with tokens on the outside — convenient for callers that
 *  read a shard/parent token from a manifest or client request and want
 *  the base-level range as tokens (for SQL `cellid BETWEEN lo AND hi`
 *  on a TEXT column). */
export function s2RangeForCellToken(parentToken, baseLevel) {
    const parent = s2TokenToId(parentToken);
    const { lo, hi } = s2RangeForCell(parent, baseLevel);
    return { lo: s2IdToToken(lo), hi: s2IdToToken(hi) };
}
/** Merged `[lo, hi]` ranges of a whole cover's descendants at
 *  `baseLevel` — the composition every range-predicate consumer wants:
 *  cover cells (e.g. from `s2Index.bboxToCells` or `minimalCover`) in,
 *  disjoint predicate ranges out. Cover cells may sit at mixed levels;
 *  each must be at or above (coarser than) `baseLevel`. Nested /
 *  overlapping cover cells collapse; note *sibling* cells do NOT (their
 *  ranges are separated by a 2·child_lsb gap of finer-level ids), so a
 *  cover of k cells yields up to k ranges. */
export function s2RangesForCells(cells, baseLevel) {
    return mergeRanges(cells.map(c => s2RangeForCell(s2TokenToId(c), baseLevel)));
}
//# sourceMappingURL=s2-range.js.map
// Verify `s2-range.ts`'s pure bigint math against `s2js` on every
// operation it exposes — token round-trip, level extraction, parent
// walk, and range-for-cell. `s2js` is the package's canonical S2 impl
// (what `s2Index` wraps); nj-crashes runs the same suite against
// `nodes2ts` + Python `s2sphere`, so agreement here chains all three.

import { describe, expect, it } from 'vitest'
import { s2 } from 's2js'
import {
  S2_LEAF_LEVEL,
  intersectRanges,
  mergeRanges,
  s2IdToToken,
  s2LevelOf,
  s2LsbForLevel,
  s2Parent,
  s2RangeForCell,
  s2RangeForCellToken,
  s2RangesForCells,
  s2TokenToId,
} from './s2-range.js'

const { cellid, LatLng } = s2

// Reference points spanning two L4 cells (NYC-area vs south-Jersey).
const POINTS: Array<[number, number, string]> = [
  [40.7178, -74.0431, 'Jersey City'],
  [39.9526, -75.1180, 'Camden'],
  [40.2170, -74.7429, 'Trenton'],
  [40.7357, -74.1724, 'Newark'],
  [38.9351, -74.9060, 'Cape May'],
]

/** S2 cell id at `level` for a lat/lng, via s2js (already a bigint). */
function s2jsIdAt(lat: number, lng: number, level: number): bigint {
  return cellid.parent(cellid.fromLatLng(LatLng.fromDegrees(lat, lng)), level)
}

describe('token ↔ id round-trip matches s2js', () => {
  it('agrees on id → token → id at every level for every point', () => {
    for (const [lat, lng] of POINTS) {
      for (const level of [4, 6, 8, 10, 12, 14, 16]) {
        const id = s2jsIdAt(lat, lng, level)
        const token = cellid.toToken(id)
        expect(s2TokenToId(token)).toBe(id)
        expect(s2IdToToken(id)).toBe(token)
      }
    }
  })

  it("handles the special zero-id token 'X'", () => {
    expect(s2TokenToId('X')).toBe(0n)
    expect(s2IdToToken(0n)).toBe('X')
  })
})

describe('level extraction matches s2js', () => {
  it('s2LevelOf(id) == s2js cellid.level(id)', () => {
    for (const [lat, lng] of POINTS) {
      for (const level of [4, 6, 8, 10, 12, 14, 16]) {
        const id = s2jsIdAt(lat, lng, level)
        expect(s2LevelOf(id)).toBe(cellid.level(id))
        expect(s2LevelOf(id)).toBe(level)
      }
    }
  })
})

describe('parent walk matches s2js', () => {
  it('s2Parent(child, targetLevel) == s2js cellid.parent(child, targetLevel)', () => {
    for (const [lat, lng] of POINTS) {
      for (const childLevel of [10, 14, 16]) {
        for (const parentLevel of [4, 6, 8]) {
          const child = s2jsIdAt(lat, lng, childLevel)
          expect(s2Parent(child, parentLevel)).toBe(cellid.parent(child, parentLevel))
        }
      }
    }
  })
})

describe('`s2LsbForLevel` bit position', () => {
  it('level 30 → LSB 1 (leaf marker at bit 0)', () => {
    expect(s2LsbForLevel(S2_LEAF_LEVEL)).toBe(1n)
  })
  it('level 0 → LSB 2^60 (face-level marker at bit 60)', () => {
    expect(s2LsbForLevel(0)).toBe(1n << 60n)
  })
  it('level N → LSB 2^(2*(30-N)) for various levels', () => {
    for (const level of [4, 8, 12, 16]) {
      expect(s2LsbForLevel(level)).toBe(1n << BigInt(2 * (30 - level)))
    }
  })
})

describe('`s2RangeForCell` correctness', () => {
  it('collapses to a single cell when baseLevel == parentLevel', () => {
    for (const [lat, lng] of POINTS) {
      const id = s2jsIdAt(lat, lng, 8)
      expect(s2RangeForCell(id, 8)).toEqual({ lo: id, hi: id })
    }
  })

  it('matches s2js rangeMin/rangeMax exactly at the leaf level', () => {
    for (const [lat, lng] of POINTS) {
      for (const parentLevel of [4, 8, 12]) {
        const parent = s2jsIdAt(lat, lng, parentLevel)
        expect(s2RangeForCell(parent, S2_LEAF_LEVEL)).toEqual({
          lo: cellid.rangeMin(parent),
          hi: cellid.rangeMax(parent),
        })
      }
    }
  })

  it("range at base level 16 CONTAINS the parent's own descendant", () => {
    // Any level-16 cell at the same lat/lng as the parent must sit
    // inside the parent's [lo, hi] range — this is the property that
    // makes the range a valid prefix-pruning filter.
    for (const [lat, lng] of POINTS) {
      for (const parentLevel of [4, 6, 8, 10, 12]) {
        const parent = s2jsIdAt(lat, lng, parentLevel)
        const leaf16 = s2jsIdAt(lat, lng, 16)
        const { lo, hi } = s2RangeForCell(parent, 16)
        expect(leaf16 >= lo && leaf16 <= hi).toBe(true)
      }
    }
  })

  it('range at base level 16 EXCLUDES a level-16 cell from a different level-4 cell', () => {
    // Jersey City and Cape May sit in different level-4 cells; Cape
    // May's level-16 cell must be outside JC's level-4 range.
    const jc4 = s2jsIdAt(40.7178, -74.0431, 4)
    const cmLeaf = s2jsIdAt(38.9351, -74.9060, 16)
    const cm4 = s2jsIdAt(38.9351, -74.9060, 4)
    expect(cm4).not.toBe(jc4)
    const { lo, hi } = s2RangeForCell(jc4, 16)
    expect(cmLeaf < lo || cmLeaf > hi).toBe(true)
  })

  it('range width scales as 4^(baseLevel - parentLevel)', () => {
    // Number of level-16 descendants of a level-12 parent = 4^4 = 256.
    // Adjacent level-16 cells differ by `2 * child_lsb` (the marker bit
    // is stationary; the change is in the digit block above it), so:
    //     count = (hi - lo) / (2 * child_lsb) + 1
    const parent = s2jsIdAt(40.7, -74.0, 12)
    const { lo, hi } = s2RangeForCell(parent, 16)
    const childLsb = s2LsbForLevel(16)
    expect((hi - lo) / (2n * childLsb) + 1n).toBe(4n ** 4n)
  })
})

describe('`s2RangeForCellToken` token-flavored wrapper', () => {
  it("produces bounds that bracket the parent's own base-level descendant, under both bigint and lex-on-token order", () => {
    for (const [lat, lng] of POINTS) {
      const parent = s2jsIdAt(lat, lng, 8)
      const leaf = s2jsIdAt(lat, lng, 16)
      const { lo, hi } = s2RangeForCellToken(cellid.toToken(parent), 16)
      expect({ lo: s2TokenToId(lo), hi: s2TokenToId(hi) }).toEqual(s2RangeForCell(parent, 16))
      // Lex ≤ / ≥ on trailing-zero-stripped tokens is the same order as
      // bigint ≤ / ≥ ('0' is the lowest hex char) — what a SQL TEXT
      // column does under `BETWEEN`.
      const leafToken = cellid.toToken(leaf)
      expect(lo.localeCompare(leafToken) <= 0 && hi.localeCompare(leafToken) >= 0).toBe(true)
    }
  })
})

describe('`mergeRanges`', () => {
  it('merges overlapping and adjacent ranges, sorted by lo, leaving gaps intact', () => {
    expect(mergeRanges([
      { lo: 40n, hi: 50n },
      { lo: 10n, hi: 20n },
      { lo: 21n, hi: 30n },  // adjacent to [10, 20]
      { lo: 15n, hi: 25n },  // overlaps both
    ])).toEqual([
      { lo: 10n, hi: 30n },
      { lo: 40n, hi: 50n },
    ])
  })

  it('returns [] for [] and does not mutate its input', () => {
    expect(mergeRanges([])).toEqual([])
    const input = [{ lo: 1n, hi: 2n }, { lo: 2n, hi: 9n }]
    expect(mergeRanges(input)).toEqual([{ lo: 1n, hi: 9n }])
    expect(input).toEqual([{ lo: 1n, hi: 2n }, { lo: 2n, hi: 9n }])
  })
})

describe('`intersectRanges`', () => {
  it('intersects pairwise, dropping empty overlaps', () => {
    expect(intersectRanges(
      [{ lo: 0n, hi: 10n }, { lo: 20n, hi: 30n }],
      [{ lo: 5n, hi: 25n }],
    )).toEqual([
      { lo: 5n, hi: 10n },
      { lo: 20n, hi: 25n },
    ])
    expect(intersectRanges([{ lo: 0n, hi: 10n }], [{ lo: 11n, hi: 12n }])).toEqual([])
  })
})

describe('`s2RangesForCells`', () => {
  it("4 sibling children's ranges stay disjoint (gap = 2·child_lsb of finer-level ids) and partition the parent's", () => {
    const parent = s2jsIdAt(40.7178, -74.0431, 8)
    const children = Array.from(cellid.children(parent), c => cellid.toToken(c))
    const ranges = s2RangesForCells(children, 16)
    const pRange = s2RangeForCell(parent, 16)
    const childLsb = s2LsbForLevel(16)
    // Ordered, disjoint, flush with the parent's bounds; each gap holds
    // only ids at levels finer than 16, so no base-level id is lost.
    expect(ranges.map(r => r.lo)).toEqual([
      pRange.lo,
      ranges[0].hi + 2n * childLsb,
      ranges[1].hi + 2n * childLsb,
      ranges[2].hi + 2n * childLsb,
    ])
    expect(ranges[3].hi).toBe(pRange.hi)
    // Base-level id counts (stride 2·child_lsb): 4 quarters = the whole.
    const count = (r: { lo: bigint; hi: bigint }) => (r.hi - r.lo) / (2n * childLsb) + 1n
    expect(ranges.map(count)).toEqual([4n ** 7n, 4n ** 7n, 4n ** 7n, 4n ** 7n])
  })

  it("a cover cell nested inside another merges away (child's range ⊂ parent's)", () => {
    const parent = s2jsIdAt(40.7178, -74.0431, 8)
    const child = s2jsIdAt(40.7178, -74.0431, 10)  // descendant of `parent`
    expect(s2RangesForCells([cellid.toToken(parent), cellid.toToken(child)], 16))
      .toEqual([s2RangeForCell(parent, 16)])
  })

  it('mixed-level cover cells each contribute their own range', () => {
    const jc8 = s2jsIdAt(40.7178, -74.0431, 8)
    const cm12 = s2jsIdAt(38.9351, -74.9060, 12)
    const ranges = s2RangesForCells([cellid.toToken(jc8), cellid.toToken(cm12)], 16)
    const expected = mergeRanges([s2RangeForCell(jc8, 16), s2RangeForCell(cm12, 16)])
    expect(ranges).toEqual(expected)
    expect(ranges.length).toBe(2)
  })
})

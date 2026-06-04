// Tests for the backend-agnostic `minimalCover` DP.
//
// Tests primarily use `s2Index` (S2's quadtree gives exact lineage and
// clean 4-children partition — perfect tree structure to test the DP).
// Spec test plan §Phase 3:
//   - Small hand-cases (all-include, 1-exclude, 3-of-4, single-leaf)
//   - Cross-level grandparent-swap win vs naive bottom-up greedy
//   - DP optimality vs exhaustive brute force on small trees
//   - Lineage-disjoint output
//   - `allowSubtraction = false` → empty `exclude[]`
//   - Cover correctness: ∀ station ∈ system,
//     `isCellInCover(station, cover) === include.has(station)`

import { s2 } from 's2js'
import { describe, expect, test } from 'vitest'
import { filterCellsByCover } from './planner.js'
import { s2Index } from './s2-index.js'
import { isCellInCover, minimalCover } from './spatial-index-cover.js'

const { cellid, LatLng } = s2

// Build a balanced S2 fixture rooted at an NYC-area cell:
//   P at level L
//     C[i] at level L+1 (i ∈ 0..3)
//       GC[i][j] at level L+2 (j ∈ 0..3)
// 1 + 4 + 16 = 21 cells.
function s2Fixture(L: number = 10) {
  const nyc = LatLng.fromDegrees(40.74, -73.99)
  const leaf = cellid.fromLatLng(nyc)
  const Pci = cellid.parent(leaf, L)
  const P = cellid.toToken(Pci)
  const C = cellid.children(Pci).map(c => cellid.toToken(c))
  const GC: string[][] = []
  for (const cToken of C) {
    const cci = cellid.fromToken(cToken)
    GC.push(cellid.children(cci).map(g => cellid.toToken(g)))
  }
  return { P, C, GC }
}

describe('minimalCover: hand cases (S2)', () => {
  test('all 4 children in include → promote to parent (1 op)', () => {
    const { P, C } = s2Fixture()
    const cover = minimalCover(s2Index, C, C)
    expect(cover).toEqual({ include: [P], exclude: [] })
  })

  test('1 of 4 children in include → no promotion possible (1 op)', () => {
    const { C } = s2Fixture()
    const include = [C[0]!]
    const cover = minimalCover(s2Index, include, C)
    expect(cover).toEqual({ include: [C[0]], exclude: [] })
  })

  test('2 of 4 → explicit wins over via_parent (2 ops)', () => {
    const { C } = s2Fixture()
    const include = [C[0]!, C[1]!]
    const cover = minimalCover(s2Index, include, C)
    // explicit: 2 ops. via_parent: [(P,+),(C[2],-),(C[3],-)] = 3 ops.
    // explicit wins (≤).
    expect(cover.include.length).toBe(2)
    expect(cover.exclude.length).toBe(0)
    expect(new Set(cover.include)).toEqual(new Set([C[0], C[1]]))
  })

  test('3 of 4 → via_parent wins (2 ops: +P, -excluded)', () => {
    const { P, C } = s2Fixture()
    const include = [C[0]!, C[1]!, C[2]!]
    const cover = minimalCover(s2Index, include, C)
    expect(cover).toEqual({ include: [P], exclude: [C[3]] })
  })

  test('empty include → empty cover', () => {
    const { C } = s2Fixture()
    expect(minimalCover(s2Index, [], C)).toEqual({ include: [], exclude: [] })
  })
})

describe('minimalCover: cross-level grandparent swap (DP beats greedy)', () => {
  // GP has 4 children C[0..3]. Each C[i] has 4 grandchildren GC[i][0..3].
  // include = all 4 of C[0..2] (12 grandchildren) + 2 of C[3]'s 4 (=14).
  //
  // Naive bottom-up greedy (decide promote-or-not at each parent
  // independently):
  //   - C[0..2]: pure-include → promote → 1 op each = 3 ops
  //   - C[3]: mixed 2-of-4 → can't promote → list 2 leaves = 2 ops
  //   - Total: 5 ops.
  //
  // DP catches the GP-level swap:
  //   - via_parent at GP = [(GP,+)] + Σ encode_exclude(C[i])
  //   - encode_exclude(C[0..2]) = [] (no excludes under fully-include)
  //   - encode_exclude(C[3]) = 2 ops (list 2 excluded leaves)
  //   - via_parent total = 1 + 0 + 0 + 0 + 2 = 3 ops. DP picks this.
  test('14-of-16 grandchildren → DP gives 3 ops; greedy would give 5', () => {
    const { P, GC } = s2Fixture(10)
    const include = [
      ...GC[0]!,             // 4 leaves
      ...GC[1]!,             // 4 leaves
      ...GC[2]!,             // 4 leaves
      GC[3]![0]!, GC[3]![1]!, // 2 leaves
    ]
    const system = GC.flat()
    const cover = minimalCover(s2Index, include, system)
    const opCount = cover.include.length + cover.exclude.length
    expect(opCount).toBe(3)
    expect(cover.include).toEqual([P])
    expect(new Set(cover.exclude)).toEqual(new Set([GC[3]![2], GC[3]![3]]))
  })
})

describe('minimalCover: cover correctness (∀ station: in-cover iff in-include)', () => {
  test('14-of-16 case: every leaf correctly classified', () => {
    const { GC } = s2Fixture(10)
    const include = [
      ...GC[0]!,
      ...GC[1]!,
      ...GC[2]!,
      GC[3]![0]!, GC[3]![1]!,
    ]
    const system = GC.flat()
    const cover = minimalCover(s2Index, include, system)
    const includeSet = new Set(include)
    for (const station of system) {
      expect(isCellInCover(s2Index, station, cover)).toBe(includeSet.has(station))
    }
  })

  test('3-of-4 case: every leaf correctly classified', () => {
    const { C } = s2Fixture()
    const include = [C[0]!, C[1]!, C[2]!]
    const cover = minimalCover(s2Index, include, C)
    const includeSet = new Set(include)
    for (const station of C) {
      expect(isCellInCover(s2Index, station, cover)).toBe(includeSet.has(station))
    }
  })
})

describe('minimalCover: lineage-disjoint output', () => {
  // For S2, lineage-disjoint = no cell in include is an ancestor of
  // another, and same for exclude. Test via s2js's `cellid.contains`.
  function isAncestorOf(ancestor: string, descendant: string): boolean {
    if (ancestor === descendant) return false
    const a = cellid.fromToken(ancestor)
    const d = cellid.fromToken(descendant)
    return cellid.contains(a, d)
  }

  test('cover.include has no internal ancestor/descendant pairs', () => {
    const { GC } = s2Fixture(10)
    const include = [...GC[0]!, ...GC[1]!, ...GC[2]!, GC[3]![0]!, GC[3]![1]!]
    const system = GC.flat()
    const cover = minimalCover(s2Index, include, system)
    for (let i = 0; i < cover.include.length; i++) {
      for (let j = i + 1; j < cover.include.length; j++) {
        expect(isAncestorOf(cover.include[i]!, cover.include[j]!)).toBe(false)
        expect(isAncestorOf(cover.include[j]!, cover.include[i]!)).toBe(false)
      }
    }
  })

  test('cover.exclude has no internal ancestor/descendant pairs', () => {
    const { GC } = s2Fixture(10)
    const include = [...GC[0]!, ...GC[1]!, ...GC[2]!, GC[3]![0]!, GC[3]![1]!]
    const system = GC.flat()
    const cover = minimalCover(s2Index, include, system)
    for (let i = 0; i < cover.exclude.length; i++) {
      for (let j = i + 1; j < cover.exclude.length; j++) {
        expect(isAncestorOf(cover.exclude[i]!, cover.exclude[j]!)).toBe(false)
        expect(isAncestorOf(cover.exclude[j]!, cover.exclude[i]!)).toBe(false)
      }
    }
  })
})

describe('minimalCover: coarsestLevel opt', () => {
  // `coarsestLevel` caps the bottom-up walk: the cover may emit ops at
  // that level but won't roll any coarser. Use case: pyramids that
  // materialize only a sub-range of levels can't query coarser cells.
  test('all 16 grandchildren, coarsestLevel = parents → 4 parents (no GP)', () => {
    const { C, GC } = s2Fixture(10)  // P at 10, C at 11, GC at 12
    const include = GC.flat()
    const cover = minimalCover(s2Index, include, include, { coarsestLevel: 11 })
    // Without cap: would roll to [P] (level 10) = 1 op.
    // With cap at 11: rolls up to C[0..3] (level 11) but no further = 4 ops.
    expect(new Set(cover.include)).toEqual(new Set(C))
    expect(cover.exclude).toEqual([])
  })

  test('all 4 children, coarsestLevel = child level → no promotion', () => {
    const { C } = s2Fixture(10)  // P at 10, C at 11
    const cover = minimalCover(s2Index, C, C, { coarsestLevel: 11 })
    expect(new Set(cover.include)).toEqual(new Set(C))
    expect(cover.exclude).toEqual([])
  })

  test('all 4 children, coarsestLevel = parent level → promote', () => {
    const { P, C } = s2Fixture(10)  // P at 10, C at 11
    const cover = minimalCover(s2Index, C, C, { coarsestLevel: 10 })
    expect(cover).toEqual({ include: [P], exclude: [] })
  })

  test('coarsestLevel undefined matches no-cap behavior', () => {
    const { GC } = s2Fixture(10)
    const include = GC.flat()
    const noCap = minimalCover(s2Index, include, include)
    const explicit = minimalCover(s2Index, include, include, { coarsestLevel: undefined })
    expect(noCap).toEqual(explicit)
  })

  test('coarsestLevel preserves cover correctness (3-of-4 leaves)', () => {
    const { C, GC } = s2Fixture(10)
    const include = [GC[0]![0]!, GC[0]![1]!, GC[0]![2]!]  // 3 of C[0]'s 4 leaves
    const system = GC.flat()
    const cover = minimalCover(s2Index, include, system, { coarsestLevel: 11 })
    // Without cap: +C[0], -GC[0][3] (2 ops at levels 11 and 12). With
    // cap=11: same — cap permits level 11 cells, DP still picks the
    // subtract trick. Verify membership matches.
    const includeSet = new Set(include)
    for (const station of system) {
      expect(isCellInCover(s2Index, station, cover)).toBe(includeSet.has(station))
    }
    // And no cell in the cover is coarser than the cap.
    for (const c of [...cover.include, ...cover.exclude]) {
      expect(s2Index.cellLevel(c)).toBeGreaterThanOrEqual(11)
    }
  })
})

describe('minimalCover: allowSubtraction = false', () => {
  test('pure-union mode never emits excludes', () => {
    const { P, C } = s2Fixture()
    const include = [C[0]!, C[1]!, C[2]!]
    const cover = minimalCover(s2Index, include, C, { allowSubtraction: false })
    expect(cover.exclude).toEqual([])
    // Pure-union 3-of-4 → can't promote (excludeCount > 0 at P);
    // returns the 3 cells.
    expect(new Set(cover.include)).toEqual(new Set([C[0], C[1], C[2]]))
  })

  test('pure-union promotes when all 4 children are in include', () => {
    const { P, C } = s2Fixture()
    const cover = minimalCover(s2Index, C, C, { allowSubtraction: false })
    expect(cover).toEqual({ include: [P], exclude: [] })
  })
})

// Brute-force optimality: exhaustively enumerate all (cell, +/-)
// assignments on the relevant tree and find the minimum |ops| of any
// valid cover. The DP must match.
describe('minimalCover → filterCellsByCover end-to-end', () => {
  // Phase 4 integration: build a synthetic shard of rows at one level,
  // compute a minimalCover from a station subset, filter rows by the
  // cover, verify the kept rows are exactly the ones whose cells are in
  // the include subset.
  test('14-of-16 case: filterCellsByCover keeps exactly the include leaves', () => {
    const { GC } = s2Fixture(10)
    const allLeaves = GC.flat()
    const include = [
      ...GC[0]!,
      ...GC[1]!,
      ...GC[2]!,
      GC[3]![0]!, GC[3]![1]!,
    ]
    const cover = minimalCover(s2Index, include, allLeaves)
    // Simulate parquet rows: one per leaf, with a `cell` column.
    const rows = allLeaves.map(leaf => ({ cell: leaf, count: 1 }))
    const filtered = filterCellsByCover(rows, 'cell', 12, cover, s2Index)
    const keptCells = new Set(filtered.map(r => r.cell as string))
    expect(keptCells).toEqual(new Set(include))
  })

  test('multi-level shard rows: only level-r rows are kept (wrong-level dropped)', () => {
    // A real pyrmts shard has rows at MULTIPLE materialized resolutions.
    // filterCellsByCover should drop rows whose cell isn't at the query
    // level (delegated to cellInSet's level gate).
    const { P, C, GC } = s2Fixture(10)
    const rows = [
      { cell: P, level: 10, count: 100 },           // wrong level
      ...C.map(c => ({ cell: c, level: 11, count: 25 })),  // wrong level
      ...GC.flat().map(g => ({ cell: g, level: 12, count: 1 })),  // right level
    ]
    const include = GC[0]!  // all 4 grandchildren under C[0]
    const cover = minimalCover(s2Index, include, GC.flat())
    const filtered = filterCellsByCover(rows, 'cell', 12, cover, s2Index)
    const keptCells = new Set(filtered.map(r => r.cell as string))
    expect(keptCells).toEqual(new Set(include))
  })

  test('cover with one-level promotion: rows under promoted cell all kept', () => {
    const { P, C, GC } = s2Fixture(10)
    // include all 16 grandchildren → minimalCover promotes to P
    const include = GC.flat()
    const cover = minimalCover(s2Index, include, include)
    expect(cover.include).toEqual([P])
    // filterCellsByCover at level 12: each grandchild's lineage walks
    // up through C[i] then to P (which is in include) → all kept.
    const rows = include.map(g => ({ cell: g, count: 1 }))
    const filtered = filterCellsByCover(rows, 'cell', 12, cover, s2Index)
    expect(filtered.length).toBe(16)
  })
})

describe('minimalCover: brute-force optimality (small trees)', () => {
  // Lineage descendant check (same as cellid.contains on S2 tokens).
  function isDescendantOf(leaf: string, ancestor: string): boolean {
    return cellid.contains(cellid.fromToken(ancestor), cellid.fromToken(leaf))
  }

  // Compute the cover set for a candidate op list: a station S is in the
  // cover iff (∃ include cell I containing S) ∧ (∀ exclude cell E not
  // containing S).
  function coverPredicate(
    ops: Array<{ cell: string; op: '+' | '-' }>,
    station: string,
  ): boolean {
    const inc = ops.filter(o => o.op === '+').map(o => o.cell)
    const exc = ops.filter(o => o.op === '-').map(o => o.cell)
    if (!inc.some(c => c === station || isDescendantOf(station, c))) return false
    if (exc.some(c => c === station || isDescendantOf(station, c))) return false
    return true
  }

  function exhaustiveMinOps(
    relevantCells: string[],
    include: string[],
    system: string[],
  ): number {
    const includeSet = new Set(include)
    const N = relevantCells.length
    if (N > 12) throw new Error(`brute force capped at 12 cells; got ${N}`)
    let minOps = Infinity
    // 3^N enumerations: each cell is + / - / skip.
    const total = Math.pow(3, N)
    for (let mask = 0; mask < total; mask++) {
      const ops: Array<{ cell: string; op: '+' | '-' }> = []
      let m = mask
      for (let i = 0; i < N; i++) {
        const state = m % 3
        m = Math.floor(m / 3)
        if (state === 1) ops.push({ cell: relevantCells[i]!, op: '+' })
        else if (state === 2) ops.push({ cell: relevantCells[i]!, op: '-' })
      }
      if (ops.length >= minOps) continue
      // Check the candidate covers exactly `include`.
      let valid = true
      for (const s of system) {
        if (coverPredicate(ops, s) !== includeSet.has(s)) { valid = false; break }
      }
      if (valid) minOps = ops.length
    }
    return minOps
  }

  // Use a 1-level S2 subtree (1 parent + 4 children = 5 cells). Test
  // every non-empty subset of include vs the parent set.
  test('1-parent + 4-children: DP matches brute force for all 16 include subsets', () => {
    const { P, C } = s2Fixture()
    const relevantCells = [P, ...C]
    const system = C as string[]
    for (let mask = 1; mask < 16; mask++) {
      const include = C.filter((_, i) => (mask >> i) & 1)
      const dpCover = minimalCover(s2Index, include, system)
      const dpOps = dpCover.include.length + dpCover.exclude.length
      const bruteOps = exhaustiveMinOps(relevantCells, include, system)
      expect({ mask, include, dpOps, bruteOps }).toEqual({ mask, include, dpOps, bruteOps: dpOps })
    }
  })

  // 2-level S2 subtree (1 GP + 4 parents + 16 leaves = 21 cells) — too
  // big for full brute force, but we can prune to 6 cells per round and
  // test on subsets that span 2 levels.
  // Approach: pick a single parent's subtree (1 parent + 4 leaves = 5
  // cells); test every include subset of its 4 leaves.
  test('1-parent subtree of 2-level fixture: DP matches brute force for all 16 leaf subsets', () => {
    const { C, GC } = s2Fixture()
    const C0 = C[0]!
    const C0Leaves = GC[0]!
    const relevantCells = [C0, ...C0Leaves]
    const system = C0Leaves
    for (let mask = 1; mask < 16; mask++) {
      const include = C0Leaves.filter((_, i) => (mask >> i) & 1)
      const dpCover = minimalCover(s2Index, include, system)
      const dpOps = dpCover.include.length + dpCover.exclude.length
      const bruteOps = exhaustiveMinOps(relevantCells, include, system)
      expect({ mask, include, dpOps, bruteOps }).toEqual({ mask, include, dpOps, bruteOps: dpOps })
    }
  })

  // The hardest: a 2-level subtree (1 GP + 4 parents + a few leaves =
  // up to ~8 cells). Pick a single parent + its 4 leaves + their
  // grandparent's other parents (covered only at the leaf level here),
  // to keep relevantCells ≤ 12.
  test('2-level subtree (3 parents + 4 leaves under one) for hand-picked include subsets', () => {
    const { P, C, GC } = s2Fixture()
    // System = leaves of C[0] + the other 3 parents as leaves
    // (we treat C[1..3] as leaves for the brute force, since their
    // own children aren't in `system`).
    const system = [...GC[0]!, C[1]!, C[2]!, C[3]!]
    // Relevant cells for brute force: P + C[0..3] + GC[0][0..3] = 9 cells
    const relevantCells = [P, ...C, ...GC[0]!]
    // Test a handful of hand-picked includes.
    const masks = [
      [...GC[0]!],                            // 4 leaves under C[0] → +C[0] (1 op)
      [...GC[0]!, C[1]!],                     // 4 leaves under C[0] + C[1] → +C[0], +C[1] (2 ops)
      [...GC[0]!, C[1]!, C[2]!],              // → +C[0..2] (3) OR +P,-C[3] (2)
      [...GC[0]!, C[1]!, C[2]!, C[3]!],       // all → +P (1)
      [GC[0]![0]!, GC[0]![1]!, GC[0]![2]!],   // 3-of-4 leaves under C[0] → +C[0],-GC[0][3] (2)
    ]
    for (const include of masks) {
      const dpCover = minimalCover(s2Index, include, system)
      const dpOps = dpCover.include.length + dpCover.exclude.length
      const bruteOps = exhaustiveMinOps(relevantCells, include, system)
      expect({ include, dpOps, bruteOps }).toEqual({ include, dpOps, bruteOps: dpOps })
    }
  })
})

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

describe('minimalCover: mixed-level systems (LUC cells)', () => {
  // ctbk station "LUC" systems have one cell per station at whatever
  // level makes it unique — levels 10–20 mixed in one call. Pinned
  // tokens below are real ctbk cells: '89c2574b3'/'89c2574b5' are L16
  // siblings under '89c2574b4' (L15) → '89c2574b' (L14) → '89c2574c'
  // (L13); '89c2574c4' (L15) → '89c2574d' (L14) merges the first chain
  // at '89c2574c'.

  test('mixed-level siblings: include one of two L16 siblings', () => {
    const cover = minimalCover(s2Index, ['89c2574b3'], ['89c2574b3', '89c2574b5'])
    expect(cover).toEqual({ include: ['89c2574b3'], exclude: [] })
  })

  test('mixed-level coarsening: rolls to the L14 divergence child (stall-exit used to stop at L15)', () => {
    const cover = minimalCover(s2Index, ['89c2574b3'], ['89c2574b3', '89c2574c4'])
    expect(cover).toEqual({ include: ['89c2574b'], exclude: [] })
  })

  test('sparse-deep: two L20 leaves sharing an L12 ancestor → the L13 divergence child, no stall', () => {
    const nyc = LatLng.fromDegrees(40.74, -73.99)
    const a12 = cellid.parent(cellid.fromLatLng(nyc), 12)
    const [c13a, c13b] = cellid.children(a12)
    const leafUnder = (ci: bigint, level: number): bigint => {
      let cur = ci
      while (cellid.level(cur) < level) cur = cellid.children(cur)[0]!
      return cur
    }
    const leafA = cellid.toToken(leafUnder(c13a!, 20))
    const leafB = cellid.toToken(leafUnder(c13b!, 20))
    const cover = minimalCover(s2Index, [leafA], [leafA, leafB])
    expect(cover).toEqual({ include: [cellid.toToken(c13a!)], exclude: [] })
  })

  test('mixed-level subtraction: 3-of-4 L16 children + distant L14 cell → +parent −child', () => {
    const nyc = LatLng.fromDegrees(40.74, -73.99)
    const p15 = cellid.parent(cellid.fromLatLng(nyc), 15)
    const kids = cellid.children(p15).map(c => cellid.toToken(c))
    // A different L14 child of the L13 ancestor — makes `system` span
    // levels 14 and 16.
    const a13 = cellid.parent(p15, 13)
    const p14 = cellid.toToken(cellid.parent(p15, 14))
    const d14 = cellid.children(a13).map(c => cellid.toToken(c)).find(t => t !== p14)!
    const include = [kids[0]!, kids[1]!, kids[2]!]
    const cover = minimalCover(s2Index, include, [...kids, d14])
    expect(cover).toEqual({ include: [cellid.toToken(p15)], exclude: [kids[3]] })
  })

  test('coarsestLevel respected with mixed levels: stays at L15', () => {
    const cover = minimalCover(s2Index, ['89c2574b3'], ['89c2574b3', '89c2574c4'], { coarsestLevel: 15 })
    expect(cover).toEqual({ include: ['89c2574b4'], exclude: [] })
  })

  test('ancestor in system throws, naming both tokens', () => {
    expect(() => minimalCover(s2Index, ['89c259b23'], ['89c259b23', '89c259b24'])).toThrow(
      "minimalCover: system cells must be mutually disjoint; '89c259b24' is an ancestor of '89c259b23'",
    )
  })

  test('deep-nested ancestor in system: error names the original system cell, not the chain cell', () => {
    // '89c2574c' (L13) is three levels above '89c2574b3' (L16); the walk
    // hits it via the intermediate '89c2574b' (L14) chain cell.
    expect(() => minimalCover(s2Index, ['89c2574b3'], ['89c2574b3', '89c2574c'])).toThrow(
      "minimalCover: system cells must be mutually disjoint; '89c2574c' is an ancestor of '89c2574b3'",
    )
  })
})

describe('minimalCover: ragged vocabulary at scale', () => {
  // The unit cases above pin the DP on hand-built systems of a handful of
  // cells. This one models the *shape* real consumers have and those cases
  // don't: a station vocabulary of a couple thousand cells, each at
  // whatever level makes it unique — ragged across L11-L19 in a single
  // call. That's where both original defects bit (counts stranded below an
  // already-linked ancestor → empty cover; stall-exit firing on the first
  // zero-merge round → no coarsening).
  //
  // It exists because no consumer exercises mixed-level systems today, so
  // this is the acceptance evidence for `specs/minimal-cover-mixed-levels.md`
  // rather than a downstream adoption. Generated deterministically (LCG,
  // fixed seed) so the pinned counts are stable.

  const N = 2340          // ctbk's station count, for shape realism
  const FINEST = 20
  const COARSEST_UNIQUE = 10

  // Deterministic points over an NYC-sized box.
  function points(n: number): Array<{ lat: number; lng: number }> {
    let x = 20260817
    const next = (): number => {
      x = (x * 1103515245 + 12345) % 2147483648
      return x / 2147483648
    }
    return Array.from({ length: n }, () => ({
      lat: 40.55 + next() * 0.40,
      lng: -74.10 + next() * 0.36,
    }))
  }

  // "LUC"-style vocabulary: each point gets its shallowest ancestor (from
  // L10 down) that no other point shares. Lineage-disjoint by construction.
  const leaves = points(N).map(p => cellid.fromLatLng(LatLng.fromDegrees(p.lat, p.lng)))
  const cellOf: string[] = new Array(N)
  for (let level = COARSEST_UNIQUE; level <= FINEST; level++) {
    const byToken = new Map<string, number[]>()
    leaves.forEach((leaf, i) => {
      if (cellOf[i] !== undefined) return
      const tok = cellid.toToken(cellid.parent(leaf, level))
      const bucket = byToken.get(tok)
      if (bucket === undefined) byToken.set(tok, [i])
      else bucket.push(i)
    })
    for (const [tok, idxs] of byToken) {
      if (idxs.length === 1 || level === FINEST) for (const i of idxs) cellOf[i] = tok
    }
  }
  const system = [...new Set(cellOf)]

  test('the fixture is a genuinely ragged, lineage-disjoint vocabulary', () => {
    const levels = system.map(t => cellid.level(cellid.fromToken(t)))
    expect([Math.min(...levels), Math.max(...levels)]).toEqual([11, 19])
    expect([system.length, new Set(system).size]).toEqual([2340, 2340])
    // Disjointness: no member is an ancestor of another. (`minimalCover`
    // throws on violations, so this pins the fixture, not the DP.)
    const members = new Set(system)
    const nested = system.filter(t => {
      let cur = cellid.fromToken(t)
      while (cellid.level(cur) > 0) {
        cur = cellid.parent(cur, cellid.level(cur) - 1)
        if (members.has(cellid.toToken(cur))) return true
      }
      return false
    })
    expect(nested).toEqual([])
  })

  // A contiguous slice of the vocabulary stands in for "everything except
  // these" — the shape a region query takes once most of the map is wanted.
  const omitted = new Set(system.slice(0, 140))
  const include = system.filter(t => !omitted.has(t))

  test('a large subset covers compactly, and every membership decision is right', () => {
    const cover = minimalCover(s2Index, include, system, { allowSubtraction: true })

    // Defect 1 produced an *empty* cover for a non-empty include; defect 2
    // left a forest of near-leaf roots. Both would show up as term count.
    // 2200 wanted members collapse to 1 include + 138 excludes — the DP
    // finds it cheaper to name the root and subtract, which is exactly the
    // ± reasoning the whole cover exists for.
    expect([include.length, cover.include.length, cover.exclude.length]).toEqual([2200, 1, 138])

    // The actual contract: cover membership matches the include set
    // exactly, for every member of the vocabulary.
    const wanted = new Set(include)
    const misclassified = system.filter(t => isCellInCover(s2Index, t, cover) !== wanted.has(t))
    expect(misclassified).toEqual([])
  })

  test('coarsestLevel caps the roll-up, but cannot lift a system finer than the ladder', () => {
    const cover = minimalCover(s2Index, include, system, {
      allowSubtraction: true,
      coarsestLevel: 15,
    })

    // Terms the DP *rolled up to* respect the cap. Terms that are system
    // members already coarser than it pass through untouched — the walk
    // stops at them, it can't invent finer cells that aren't in the
    // vocabulary. Consumers whose vocabulary is finer or coarser than their
    // materialized ladder have to fix the system, not the cover: this knob
    // bounds one direction only.
    const passthrough = cover.include.concat(cover.exclude).filter(t => system.includes(t))
    const rolledUp = cover.include.concat(cover.exclude).filter(t => !system.includes(t))
    expect(rolledUp.every(t => cellid.level(cellid.fromToken(t)) >= 15)).toBe(true)
    expect(passthrough.some(t => cellid.level(cellid.fromToken(t)) < 15)).toBe(true)

    const wanted = new Set(include)
    const misclassified = system.filter(t => isCellInCover(s2Index, t, cover) !== wanted.has(t))
    expect(misclassified).toEqual([])
  })

  test('a member missing from the vocabulary cannot be back-filled with a fixed-level cell', () => {
    // The lesson from ctbk's `_`-alias stations: an entry with no
    // vocabulary member of its own looks like it wants a lat/lng fallback
    // cell. It can't have one — a fixed-level cell lands as an *ancestor*
    // of the finer members nested inside it, which is precisely what the
    // disjointness check rejects. The only correct handling is mapping the
    // entry onto the member it aliases.
    const deep = system.find(t => cellid.level(cellid.fromToken(t)) >= 17)!
    const fallback = cellid.toToken(cellid.parent(cellid.fromToken(deep), 15))
    expect(() => minimalCover(s2Index, [deep], [...system, fallback])).toThrow(
      `minimalCover: system cells must be mutually disjoint; '${fallback}' is an ancestor of '${deep}'`,
    )
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

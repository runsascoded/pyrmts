// Tests for vocab-restricted ± covers (`specs/vocab-restricted-covers.md`).
//
// Spec test plan:
//   - DP optimality on the drop-LUC counter-example (parent with four
//     partial children: `P − Σ outsiders` beats per-child forms)
//   - Random-forest fuzz vs brute-force term enumeration (small N)
//   - Positive-only mode = exact union (empty `exclude`)
//   - Value arithmetic: Σ include − Σ exclude ≡ Σ wanted stations
//     (the monoid form of "cover rows ≡ union of identity-row queries")
//   - `buildVocabGraph`: ragged vocab (skipped levels), outside-vocab
//     stations as root leaves, duplicate/unknown key errors

import { s2 } from 's2js'
import { describe, expect, test } from 'vitest'
import { s2Index } from './s2-index.js'
import {
  buildVocabGraph,
  vocabCover,
  type SpatialSet,
  type VocabGraph,
  type VocabNode,
} from './index.js'

const { cellid, LatLng } = s2

// S2 fixture rooted over NYC: P (L12) → C[0..3] (L13) → GC[i][0..3] (L14).
// Stations live at the GC cells; the vocab is ragged subsets of {P, C, GC}.
function s2Fixture() {
  const leaf = cellid.fromLatLng(LatLng.fromDegrees(40.74, -73.99))
  const Pci = cellid.parent(leaf, 12)
  const P = cellid.toToken(Pci)
  const C = cellid.children(Pci).map(c => cellid.toToken(c))
  const GC = C.map(c => cellid.children(cellid.fromToken(c)).map(g => cellid.toToken(g)))
  return { P, C, GC }
}

// Nearest-signed-ancestor evaluation of a cover over the graph — which
// station keys does this ± term list select?
function selected(graph: VocabGraph, cover: SpatialSet<string>): Set<string> {
  const inc = new Set(cover.include)
  const exc = new Set(cover.exclude)
  const out = new Set<string>()
  const walk = (n: VocabNode, sign: '+' | '-' | null): void => {
    const s = inc.has(n.term) ? '+' : exc.has(n.term) ? '-' : sign
    if (n.isLeaf && s === '+') out.add(n.term)
    n.children.forEach(c => walk(c, s))
  }
  graph.roots.forEach(r => walk(r, null))
  return out
}

// The counter-example graph: vocab {P, C[0..3]}, 4 stations per C child.
function counterExample() {
  const { P, C, GC } = s2Fixture()
  const leaves = C.flatMap((_, i) =>
    [0, 1, 2, 3].map(j => ({ key: `s:${i}-${j}`, cell: GC[i]![j]! })),
  )
  const graph = buildVocabGraph(s2Index, [P, ...C], leaves)
  return { P, C, graph, leaves }
}

describe('vocabCover: ± DP', () => {
  test('drop-LUC counter-example: P − 4 outsiders (5 terms) beats per-child (8 terms)', () => {
    const { P, graph } = counterExample()
    // 3 of 4 stations wanted in every child. Per-child optimum is
    // 2 terms each (C[i] − s:i-3) = 8; the DP finds the grandparent
    // swap: [P] − the 4 unwanted stations = 5 terms.
    const wanted = [0, 1, 2, 3].flatMap(i => [0, 1, 2].map(j => `s:${i}-${j}`))
    const cover = vocabCover(graph, wanted)
    expect(cover).toEqual({
      include: [P],
      exclude: ['s:0-3', 's:1-3', 's:2-3', 's:3-3'],
    })
    expect(selected(graph, cover)).toEqual(new Set(wanted))
  })

  test('all stations wanted → the root cell alone', () => {
    const { P, graph, leaves } = counterExample()
    expect(vocabCover(graph, leaves.map(l => l.key))).toEqual({ include: [P], exclude: [] })
  })

  test('empty wanted → empty cover', () => {
    const { graph } = counterExample()
    expect(vocabCover(graph, [])).toEqual({ include: [], exclude: [] })
  })

  test('one station → its identity key', () => {
    const { graph } = counterExample()
    expect(vocabCover(graph, ['s:2-1'])).toEqual({ include: ['s:2-1'], exclude: [] })
  })

  test('unknown wanted key throws (a dropped station is an undercount)', () => {
    const { graph } = counterExample()
    expect(() => vocabCover(graph, ['s:0-0', 's:nope'])).toThrowError(
      'vocabCover: 1 wanted key(s) not in the vocab graph: s:nope',
    )
  })
})

describe('vocabCover: positive-only mode', () => {
  test('exact union: fully-wanted cells + identity keys, no excludes', () => {
    const { C, graph } = counterExample()
    // C0 fully wanted; C1 partially (2 of 4); C2 none; C3 one.
    const wanted = [
      's:0-0', 's:0-1', 's:0-2', 's:0-3',
      's:1-0', 's:1-1',
      's:3-2',
    ]
    const cover = vocabCover(graph, wanted, { positiveOnly: true })
    expect(cover).toEqual({
      include: [C[0], 's:1-0', 's:1-1', 's:3-2'],
      exclude: [],
    })
    expect(selected(graph, cover)).toEqual(new Set(wanted))
  })

  test('the counter-example set stays exclude-free (10 terms, not 5)', () => {
    const { graph } = counterExample()
    const wanted = [0, 1, 2, 3].flatMap(i => [0, 1, 2].map(j => `s:${i}-${j}`))
    const cover = vocabCover(graph, wanted, { positiveOnly: true })
    expect(cover.exclude).toEqual([])
    expect(cover.include.length).toBe(12)  // no fully-wanted cell → all keys
    expect(selected(graph, cover)).toEqual(new Set(wanted))
  })
})

describe('vocabCover: value arithmetic (monoid exactness)', () => {
  // Station values are distinct powers of two, so any wrong subset —
  // double-count, undercount, sign error — changes the sum.
  test('Σ include − Σ exclude ≡ Σ wanted stations, both modes', () => {
    const { graph, leaves } = counterExample()
    const value = new Map(leaves.map((l, i) => [l.key, 2 ** i]))
    const rowValue = (n: VocabNode): number =>
      n.isLeaf ? value.get(n.term)! : n.children.reduce((s, c) => s + rowValue(c), 0)
    const rows = new Map<string, number>()
    const index = (n: VocabNode): void => {
      rows.set(n.term, rowValue(n))
      n.children.forEach(index)
    }
    graph.roots.forEach(index)

    const wanted = [0, 1, 2, 3].flatMap(i => [0, 1, 2].map(j => `s:${i}-${j}`))
    const expected = wanted.reduce((s, k) => s + value.get(k)!, 0)
    for (const opts of [{}, { positiveOnly: true }]) {
      const cover = vocabCover(graph, wanted, opts)
      const got = cover.include.reduce((s, t) => s + rows.get(t)!, 0)
        - cover.exclude.reduce((s, t) => s + rows.get(t)!, 0)
      expect(got).toBe(expected)
    }
  })
})

describe('vocabCover: fuzz vs brute force', () => {
  function lcg(seed: number): () => number {
    let s = seed >>> 0
    return () => ((s = (s * 1664525 + 1013904223) >>> 0) / 2 ** 32)
  }

  // Random forest of ≤ maxNodes abstract nodes (interior = cells,
  // childless = station leaves). Terms are synthetic — the DP never
  // looks at geometry.
  function randomGraph(rand: () => number, maxNodes: number): VocabGraph {
    let n = 0
    const mk = (depth: number): VocabNode => {
      const id = n++
      const nKids = depth >= 3 || n >= maxNodes ? 0 : Math.floor(rand() * 4)
      const node: VocabNode = { term: `t${id}`, isLeaf: false, children: [] }
      for (let i = 0; i < nKids && n < maxNodes; i++) node.children.push(mk(depth + 1))
      if (node.children.length === 0) node.isLeaf = true
      node.term = node.isLeaf ? `s:${id}` : `c${id}`
      return node
    }
    const roots: VocabNode[] = []
    while (n < maxNodes && (roots.length === 0 || rand() < 0.3)) roots.push(mk(0))
    return { roots }
  }

  function allNodes(graph: VocabGraph): VocabNode[] {
    const out: VocabNode[] = []
    const walk = (node: VocabNode): void => {
      out.push(node)
      node.children.forEach(walk)
    }
    graph.roots.forEach(walk)
    return out
  }

  // Minimum |terms| over every ± assignment to every node term that
  // selects exactly `wanted` (nearest-signed-ancestor semantics).
  function bruteMin(graph: VocabGraph, wanted: Set<string>, signs: ('+' | '-')[]): number {
    const nodes = allNodes(graph)
    let best = Infinity
    const assign = new Map<string, '+' | '-'>()
    const evalAssign = (): boolean => {
      const cover = {
        include: [...assign].filter(([, s]) => s === '+').map(([t]) => t),
        exclude: [...assign].filter(([, s]) => s === '-').map(([t]) => t),
      }
      const sel = selected(graph, cover)
      return sel.size === wanted.size && [...wanted].every(k => sel.has(k))
    }
    const rec = (i: number): void => {
      if (assign.size >= best) return
      if (i === nodes.length) {
        if (evalAssign()) best = assign.size
        return
      }
      rec(i + 1)
      for (const s of signs) {
        assign.set(nodes[i]!.term, s)
        rec(i + 1)
        assign.delete(nodes[i]!.term)
      }
    }
    rec(0)
    return best
  }

  test('± DP is optimal and exact on random forests', () => {
    const rand = lcg(20260728)
    for (let trial = 0; trial < 30; trial++) {
      const graph = randomGraph(rand, 9)
      const leaves = allNodes(graph).filter(n => n.isLeaf).map(n => n.term)
      const wanted = new Set(leaves.filter(() => rand() < 0.5))
      const cover = vocabCover(graph, wanted)
      expect(selected(graph, cover)).toEqual(wanted)
      const nTerms = cover.include.length + cover.exclude.length
      expect(nTerms).toBe(bruteMin(graph, wanted, ['+', '-']))
    }
  })

  test('positive-only DP is optimal among exclude-free covers', () => {
    const rand = lcg(741)
    for (let trial = 0; trial < 30; trial++) {
      const graph = randomGraph(rand, 9)
      const leaves = allNodes(graph).filter(n => n.isLeaf).map(n => n.term)
      const wanted = new Set(leaves.filter(() => rand() < 0.5))
      const cover = vocabCover(graph, wanted, { positiveOnly: true })
      expect(cover.exclude).toEqual([])
      expect(selected(graph, cover)).toEqual(wanted)
      expect(cover.include.length).toBe(bruteMin(graph, wanted, ['+']))
    }
  })
})

describe('buildVocabGraph', () => {
  test('ragged vocab: leaves classify under the finest present ancestor across skipped levels', () => {
    const { P, GC } = s2Fixture()
    // Vocab skips L13 entirely: {P (L12), GC[0][0] (L14)}. A station in
    // GC[0][0] nests under it; a station elsewhere under C[1] falls
    // through to P.
    const graph = buildVocabGraph(s2Index, [P, GC[0]![0]!], [
      { key: 's:a', cell: GC[0]![0]! },
      { key: 's:b', cell: GC[1]![0]! },
    ])
    expect(vocabCover(graph, ['s:a'])).toEqual({ include: [GC[0]![0]], exclude: [] })
    expect(vocabCover(graph, ['s:b'])).toEqual({ include: ['s:b'], exclude: [] })
    expect(vocabCover(graph, ['s:a', 's:b'])).toEqual({ include: [P], exclude: [] })
  })

  test('station outside every vocab cell becomes a root leaf, selectable by key', () => {
    const { P } = s2Fixture()
    const la = cellid.toToken(cellid.parent(cellid.fromLatLng(LatLng.fromDegrees(34.05, -118.24)), 14))
    const graph = buildVocabGraph(s2Index, [P], [{ key: 's:la', cell: la }])
    expect(graph.roots.length).toBe(2)
    expect(vocabCover(graph, ['s:la'])).toEqual({ include: ['s:la'], exclude: [] })
  })

  test('duplicate leaf key throws', () => {
    const { P, GC } = s2Fixture()
    expect(() => buildVocabGraph(s2Index, [P], [
      { key: 's:a', cell: GC[0]![0]! },
      { key: 's:a', cell: GC[0]![1]! },
    ])).toThrowError('buildVocabGraph: duplicate leaf key s:a')
  })
})

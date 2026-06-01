// Backend-agnostic `minimalCover` DP for the `SpatialIndex` interface.
//
// Per `specs/pluggable-spatial-backend.md`, the DP is a mutually-recursive
// pair `encode_include` / `encode_exclude` over the relevant subtree of
// cells spanning `system`:
//
//   encode_include(C):
//     - if C has no include points:   return []
//     - if C has no exclude points:   return [(C, +)]
//     - else (mixed):                 min(
//         explicit  = Σ encode_include(child),
//         via_parent = [(C, +)] ++ Σ encode_exclude(child)
//       )
//
//   encode_exclude(C):  -- dual
//     - if C has no exclude points:   return []
//     - if C has no include points:   return [(C, -)]
//     - else:                         min(
//         explicit  = Σ encode_exclude(child),
//         via_parent = [(C, -)] ++ Σ encode_include(child)
//       )
//
// Two visits per cell ⇒ O(|relevant tree|) total. Always exact, always
// optimal for the |ops| objective.
//
// **Not just bottom-up greedy.** The DP catches grandparent-level swaps
// that bottom-up greedy misses: e.g., 3 of GP's 4 children fully-include
// plus 1 child with 2-of-4 excludes →
//   greedy: 3×(child,+) + 2×(leaf,+) = 5 ops
//   DP:    [(GP,+), (excluded_leaf_1,-), (excluded_leaf_2,-)] = 3 ops
//
// The algorithm doesn't care about the geometry — only the structure of
// the relevant subtree. Branching factor (S2: 4, H3: 7, H13/T4 deferred)
// is implicit in the tree built by `buildTree`. ⇒ same DP works for any
// SpatialIndex backend.

import type {
  MinimalCoverOpts,
  SpatialIndex,
  SpatialSet,
} from './spatial-index.js'

interface CellNode {
  cell: string
  level: number
  parent: CellNode | null
  children: CellNode[]
  // Roll-up counts of leaves rooted at this cell.
  includeCount: number
  excludeCount: number
}

interface Op {
  cell: string
  op: '+' | '-'
}

export function minimalCover(
  index: SpatialIndex,
  include: string[],
  system: string[],
  opts: MinimalCoverOpts = {},
): SpatialSet<string> {
  if (include.length === 0) return { include: [], exclude: [] }
  const allowSubtraction = opts.allowSubtraction ?? true
  // Include cells are implicitly part of system.
  const systemUnion = Array.from(new Set([...system, ...include]))
  const roots = buildTree(index, include, systemUnion)
  const ops: Op[] = []
  for (const root of roots) {
    ops.push(...(allowSubtraction ? encodeInclude(root) : encodeIncludePureUnion(root)))
  }
  return {
    include: ops.filter(o => o.op === '+').map(o => o.cell),
    exclude: ops.filter(o => o.op === '-').map(o => o.cell),
  }
}

// Walk up from each `system` cell, creating ancestor nodes on demand,
// until the frontier collapses to a common root (or stalls at the
// backend's root level). Returns the forest of relevant roots.
function buildTree(
  index: SpatialIndex,
  include: string[],
  system: string[],
): CellNode[] {
  const includeSet = new Set(include)
  const nodes = new Map<string, CellNode>()

  for (const cell of system) {
    nodes.set(cell, {
      cell,
      level: index.cellLevel(cell),
      parent: null,
      children: [],
      includeCount: includeSet.has(cell) ? 1 : 0,
      excludeCount: includeSet.has(cell) ? 0 : 1,
    })
  }

  let frontier = new Set(system)
  let prevSize = -1
  while (frontier.size > 1 && frontier.size !== prevSize) {
    prevSize = frontier.size
    const newFrontier = new Set<string>()
    for (const cellId of frontier) {
      const node = nodes.get(cellId)!
      if (node.level === 0) {
        // Backend root — keep as a forest-root candidate.
        newFrontier.add(cellId)
        continue
      }
      const parentCell = index.cellToParent(cellId)
      let parentNode = nodes.get(parentCell)
      if (parentNode === undefined) {
        parentNode = {
          cell: parentCell,
          level: index.cellLevel(parentCell),
          parent: null,
          children: [],
          includeCount: 0,
          excludeCount: 0,
        }
        nodes.set(parentCell, parentNode)
      }
      if (node.parent !== parentNode) {
        node.parent = parentNode
        parentNode.children.push(node)
        parentNode.includeCount += node.includeCount
        parentNode.excludeCount += node.excludeCount
      }
      newFrontier.add(parentCell)
    }
    frontier = newFrontier
  }

  const roots: CellNode[] = []
  for (const node of nodes.values()) {
    if (node.parent === null) roots.push(node)
  }
  return roots
}

function encodeInclude(node: CellNode): Op[] {
  if (node.includeCount === 0) return []
  if (node.excludeCount === 0) return [{ cell: node.cell, op: '+' }]
  // Mixed.
  const explicit = node.children.flatMap(encodeInclude)
  const viaParent: Op[] = [
    { cell: node.cell, op: '+' },
    ...node.children.flatMap(encodeExclude),
  ]
  return explicit.length <= viaParent.length ? explicit : viaParent
}

function encodeExclude(node: CellNode): Op[] {
  if (node.excludeCount === 0) return []
  if (node.includeCount === 0) return [{ cell: node.cell, op: '-' }]
  // Mixed.
  const explicit = node.children.flatMap(encodeExclude)
  const viaParent: Op[] = [
    { cell: node.cell, op: '-' },
    ...node.children.flatMap(encodeInclude),
  ]
  return explicit.length <= viaParent.length ? explicit : viaParent
}

// Pure-union mode (allowSubtraction = false): standard compactCells-style
// greedy. Promote to parent only when every system descendant under the
// parent is in include (no excludes). Approximate spatially for
// non-strict-quadtree backends (H3's children don't perfectly tile parent
// because of BTs); for S2 this is exact.
function encodeIncludePureUnion(node: CellNode): Op[] {
  if (node.includeCount === 0) return []
  if (node.excludeCount === 0) return [{ cell: node.cell, op: '+' }]
  return node.children.flatMap(encodeIncludePureUnion)
}

// Lineage-based membership check. Walks from `cell` up the parent chain,
// returning true on the first include hit, false on the first exclude
// hit. (The nearest ancestor in the cover decides — that's what makes
// `[(P,+), (child,-)]` work: stations under `child` hit `-` first.)
//
// Powers the mixed-resolution `cellInSet`.
export function isCellInCover(
  index: SpatialIndex,
  cell: string,
  cover: SpatialSet<string>,
): boolean {
  const inc = new Set(cover.include)
  const exc = new Set(cover.exclude)
  let cur = cell
  for (;;) {
    if (exc.has(cur)) return false
    if (inc.has(cur)) return true
    if (index.cellLevel(cur) === 0) return false
    cur = index.cellToParent(cur)
  }
}

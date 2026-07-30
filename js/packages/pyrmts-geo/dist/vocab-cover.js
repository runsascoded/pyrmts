// Vocab-restricted ± covers (`specs/vocab-restricted-covers.md`): region
// queries over a pyramid that stores rows only at a *frozen vocabulary* —
// a ragged set of cells plus one identity row per station. A cover from
// `minimalCover` (arbitrary cells at the pyramid's resolutions) silently
// undercounts there: cover cells not in the vocabulary match no rows.
//
// `vocabCover` runs the same two-function ± DP, but over a consumer-built
// **containment forest of vocabulary members** (`VocabGraph`): interior
// nodes are vocab cells, leaves are stations (term = the identity key,
// e.g. `s:<short_name>`), and every emitted term is a graph node — so the
// output is directly usable as `cells` / `cells.exclude` query params.
// The DP itself is geometry- and index-free (any containment forest, any
// fan-out — H3 vocabularies get it free); geometry enters only in
// `buildVocabGraph`, which classifies each cell/station under its nearest
// containing vocab ancestor via the backend's parent walk.
//
// Exactness rests on the vocabulary's aggregation invariant: a vocab
// cell's row aggregates exactly the stations under it, and a node's graph
// children partition those stations — so `node − Σ(unwanted under node)`
// selects precisely the wanted subset.
//
//   pos(node) = min(Σ pos(children), 1 + Σ neg(children))   # node − complement
//   neg(node) = min(Σ neg(children), 1 + Σ pos(children))   # dual
//   leaf: pos = (wanted ? [key+] : []); neg dual
//
// Per-node greedy is NOT optimal (a parent with several mostly-wanted
// children: `parent − Σ outsiders` beats per-child forms); the DP is,
// for the |terms| objective.
//
// **Positive-only mode** (`{ positiveOnly: true }`): the complement
// branch is disabled — output is the exact union of fully-wanted vocab
// cells + wanted identity keys, `exclude` always empty. For serving
// paths that refuse sign-flip arithmetic (ctbk avail's histogram
// lineage-walk filter); still vocab-restricted, just less compressed.
/** Build the containment forest of a stored vocabulary: vocab cells nest
 *  under their nearest containing vocab ancestor (the vocabulary may be
 *  ragged — levels can be skipped); each station leaf attaches under the
 *  finest vocab cell containing its `cell`. Cells/stations contained by
 *  no vocab member become forest roots (a root leaf is still selectable
 *  via its identity key). */
export function buildVocabGraph(index, cells, leaves) {
    const vocab = new Map();
    for (const cell of cells) {
        if (!vocab.has(cell))
            vocab.set(cell, { term: cell, isLeaf: false, children: [] });
    }
    // Nearest vocab member on `cell`'s ancestor chain (including itself).
    const containingVocab = (cell) => {
        let cur = cell;
        for (;;) {
            if (vocab.has(cur))
                return cur;
            if (index.cellLevel(cur) === 0)
                return null;
            cur = index.cellToParent(cur);
        }
    };
    const roots = [];
    for (const [cell, node] of vocab) {
        const anc = index.cellLevel(cell) === 0 ? null : containingVocab(index.cellToParent(cell));
        if (anc === null)
            roots.push(node);
        else
            vocab.get(anc).children.push(node);
    }
    const seen = new Set();
    for (const { key, cell } of leaves) {
        if (seen.has(key))
            throw new Error(`buildVocabGraph: duplicate leaf key ${key}`);
        seen.add(key);
        const node = { term: key, isLeaf: true, children: [] };
        const anc = containingVocab(cell);
        if (anc === null)
            roots.push(node);
        else
            vocab.get(anc).children.push(node);
    }
    return { roots };
}
/** Minimal ± term list selecting exactly the `wanted` stations (leaf
 *  keys) — every term a vocabulary member. Throws on wanted keys absent
 *  from the graph (a silently-dropped station is an undercount). */
export function vocabCover(graph, wanted, opts = {}) {
    const want = new Set(wanted);
    const leafTerms = new Set();
    const collectLeaves = (n) => {
        if (n.isLeaf)
            leafTerms.add(n.term);
        n.children.forEach(collectLeaves);
    };
    graph.roots.forEach(collectLeaves);
    const unknown = [...want].filter(k => !leafTerms.has(k));
    if (unknown.length) {
        throw new Error(`vocabCover: ${unknown.length} wanted key(s) not in the vocab graph: `
            + unknown.slice(0, 5).join(', ') + (unknown.length > 5 ? ', …' : ''));
    }
    const counts = new Map();
    const count = (n) => {
        let w = 0;
        let u = 0;
        if (n.isLeaf) {
            if (want.has(n.term))
                w = 1;
            else
                u = 1;
        }
        for (const c of n.children) {
            const cc = count(c);
            w += cc.w;
            u += cc.u;
        }
        const res = { w, u };
        counts.set(n, res);
        return res;
    };
    graph.roots.forEach(count);
    // Memoized: a node's pos/neg can each be demanded from both the
    // parent's pos and neg branches.
    const posMemo = new Map();
    const negMemo = new Map();
    const pos = (n) => {
        const hit = posMemo.get(n);
        if (hit !== undefined)
            return hit;
        const { w, u } = counts.get(n);
        let out;
        if (w === 0)
            out = [];
        else if (u === 0)
            out = [{ term: n.term, op: '+' }];
        else {
            const explicit = n.children.flatMap(pos);
            if (opts.positiveOnly)
                out = explicit;
            else {
                const viaParent = [{ term: n.term, op: '+' }, ...n.children.flatMap(neg)];
                out = explicit.length <= viaParent.length ? explicit : viaParent;
            }
        }
        posMemo.set(n, out);
        return out;
    };
    const neg = (n) => {
        const hit = negMemo.get(n);
        if (hit !== undefined)
            return hit;
        const { w, u } = counts.get(n);
        let out;
        if (u === 0)
            out = [];
        else if (w === 0)
            out = [{ term: n.term, op: '-' }];
        else {
            const explicit = n.children.flatMap(neg);
            const viaParent = [{ term: n.term, op: '-' }, ...n.children.flatMap(pos)];
            out = explicit.length <= viaParent.length ? explicit : viaParent;
        }
        negMemo.set(n, out);
        return out;
    };
    const ops = graph.roots.flatMap(pos);
    return {
        include: ops.filter(o => o.op === '+').map(o => o.term),
        exclude: ops.filter(o => o.op === '-').map(o => o.term),
    };
}
//# sourceMappingURL=vocab-cover.js.map
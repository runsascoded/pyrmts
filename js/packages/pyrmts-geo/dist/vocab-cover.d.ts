import type { SpatialIndex, SpatialSet } from './spatial-index.js';
export interface VocabLeaf {
    /** Query term for the station — its identity key (e.g. `s:<name>`). */
    key: string;
    /** Any cell containing the station (typically its finest-level cell);
     *  `buildVocabGraph` attaches the leaf under the finest vocab cell on
     *  this cell's ancestor chain. */
    cell: string;
}
export interface VocabNode {
    /** Query term: a vocab cell id, or a leaf's identity key. */
    term: string;
    /** True for station leaves (the only nodes `wanted` may name). */
    isLeaf: boolean;
    children: VocabNode[];
}
export interface VocabGraph {
    roots: VocabNode[];
}
export interface VocabCoverOpts {
    /** Disable the complement branch: output is the exact union of
     *  fully-wanted vocab cells + wanted identity keys (empty `exclude`). */
    positiveOnly?: boolean;
}
/** Build the containment forest of a stored vocabulary: vocab cells nest
 *  under their nearest containing vocab ancestor (the vocabulary may be
 *  ragged — levels can be skipped); each station leaf attaches under the
 *  finest vocab cell containing its `cell`. Cells/stations contained by
 *  no vocab member become forest roots (a root leaf is still selectable
 *  via its identity key). */
export declare function buildVocabGraph(index: SpatialIndex, cells: string[], leaves: VocabLeaf[]): VocabGraph;
/** Minimal ± term list selecting exactly the `wanted` stations (leaf
 *  keys) — every term a vocabulary member. Throws on wanted keys absent
 *  from the graph (a silently-dropped station is an undercount). */
export declare function vocabCover(graph: VocabGraph, wanted: Iterable<string>, opts?: VocabCoverOpts): SpatialSet<string>;
//# sourceMappingURL=vocab-cover.d.ts.map
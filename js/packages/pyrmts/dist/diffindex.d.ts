import { type State } from './multiscan.js';
import type { Pyramid, Row } from './types.js';
type Schema = Pick<Pyramid, 'binCol' | 'dims' | 'metrics'>;
export interface ChangeEntry {
    keyRow: Row;
    sa: State;
    sb: State;
}
/** A changeset keyed by the JSON-encoded key tuple. */
export type Changeset = Map<string, ChangeEntry>;
/** Parse changeset rows (`key_cols` + `{c}__a`/`{c}__b`) into a `Changeset`. */
export declare function changesetFromRows(rows: Row[], schema: Schema): Changeset;
/** Materialize a `Changeset` as rows, sorted `(*dims, binCol)` — the same
 * shape `diffTables` / `diffScans` return. */
export declare function changesetToRows(cs: Changeset, schema: Schema): Row[];
/** The changeset from `rowsA` to `rowsB` (two scan states) as a `Changeset` —
 * the dict twin of `diffTables`; absent → monoid identity. */
export declare function changesetBetween(rowsA: Row[], rowsB: Row[], schema: Schema): Changeset;
/** Compose `left` over `(a, m]` with `right` over `(m, b]` → net over `(a, b]`.
 * A key in both chains `left.before → right.after` (dropped if equal — churn
 * that cancels); a key in one passes through. Not invertible: compose only
 * disjoint spans. Mirrors Python `compose_changesets`. */
export declare function composeChangesets(left: Changeset, right: Changeset): Changeset;
/** The disjoint dyadic blocks composing `(i, j]` as `[level, start]` pairs —
 * block `(level, start)` = net change from scan `start` to `start + 2^level`.
 * popcount(j−i) = O(log) blocks. Mirrors Python `jumps`. */
export declare function jumps(i: number, j: number): Array<[number, number]>;
/** Diff between any two scans, composed from only the O(log) jump nodes.
 * `scans` is the index's ordered label list (from `index.json`); `loadNode`
 * fetches node `(level, i)` as changeset rows (the consumer's storage read). If
 * `a` is after `b`, the forward changeset is computed and each before/after
 * swapped (a single changeset reverses; only composition is non-invertible). */
export declare function diffOverSpan(scans: string[], schema: Schema, a: string, b: string, loadNode: (level: number, i: number) => Promise<Row[]>): Promise<Row[]>;
/** Parse a persisted node parquet (as `DiffIndexStore` writes it) into
 * changeset rows, int64 normalized to number like the fetch path. */
export declare function readChangesetNode(bytes: Uint8Array): Promise<Row[]>;
/** Parse the store's `index.json` → ordered scan labels. */
export declare function parseDiffIndexManifest(bytes: Uint8Array, dataset?: string): string[];
export {};
//# sourceMappingURL=diffindex.d.ts.map
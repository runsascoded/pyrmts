import type { PyramidCoverStatus } from 'pyrmts';
/** One searchable shard entry. Superset of use-kbd's `OmnibarLinkEntry`
 *  (declared structurally so this module doesn't import `use-kbd`):
 *  `search` is the precomputed lowercase haystack, stripped before the
 *  entry is handed to the omnibar. */
export interface ShardSearchEntry {
    id: string;
    label: string;
    description: string;
    group: string;
    href: string;
    search: string;
}
export interface ShardSearchOptions {
    /** Deep-link target for a shard's storage key (e.g. `` key => `/files/${key}` ``). */
    hrefFor: (key: string) => string;
    /** Display name for a pyramid (e.g. map registry name → device name).
     *  Defaults to the pyramid name itself. */
    pyramidLabel?: (pyramidName: string) => string;
    /** Omnibar group; default `'Shards'`. */
    group?: string;
}
/** Entries for every present min-cover slot with a registered key:
 *  `label` = `<pyramid> · <tier>/<rung> · <start day>`, `description` =
 *  full key, searchable by any of those parts (see `filterShardEntries`). */
export declare function shardSearchEntries(covers: PyramidCoverStatus[], opts: ShardSearchOptions): ShardSearchEntry[];
/** Multi-term AND filter: every whitespace-separated query term must match
 *  somewhere in an entry's `search` haystack ("gym m3 2026-07" narrows
 *  progressively). Returns the requested page with `search` stripped —
 *  the shape use-kbd's sync `filter` endpoint expects. */
export declare function filterShardEntries(entries: ShardSearchEntry[], query: string, { offset, limit }: {
    offset: number;
    limit: number;
}): {
    entries: Omit<ShardSearchEntry, 'search'>[];
    total: number;
    hasMore: boolean;
};
//# sourceMappingURL=shard-search.d.ts.map
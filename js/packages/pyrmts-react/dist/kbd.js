// `pyrmts-react/kbd` — omnibar shard search (`use-kbd` peer required;
// subpath export so consumers without use-kbd don't pull it).
import { useMemo } from 'react';
import { useOmnibarEndpoint } from 'use-kbd';
import { filterShardEntries, shardSearchEntries, } from './shard-search.js';
export { filterShardEntries, shardSearchEntries };
/**
 * Registers a sync omnibar endpoint (⌘K) over every present pyramid
 * shard: min-cover slots with registered keys, plus `extraEntries`.
 * Selecting an entry navigates to `hrefFor(key)` — conventionally a
 * `/files/*` browser's parquet viewer for that storage object.
 *
 * Searchable by pyramid label, tier, rung, period, or any key substring
 * (e.g. "gym m3", "137496 2026-07", "raw/1d").
 */
export function useShardOmnibarEndpoint(covers, opts) {
    const { hrefFor, pyramidLabel, extraEntries, id = 'shards', group = 'Shards' } = opts;
    const entries = useMemo(() => [
        ...shardSearchEntries(covers ?? [], {
            hrefFor,
            ...(pyramidLabel !== undefined ? { pyramidLabel } : {}),
            group,
        }),
        ...(extraEntries ?? []),
    ], [covers, hrefFor, pyramidLabel, extraEntries, group]);
    useOmnibarEndpoint(id, {
        filter: (query, { offset, limit }) => filterShardEntries(entries, query, { offset, limit }),
        group,
        pagination: 'scroll',
    });
}
//# sourceMappingURL=kbd.js.map
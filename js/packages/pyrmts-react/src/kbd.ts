// `pyrmts-react/kbd` — omnibar shard search (`use-kbd` peer required;
// subpath export so consumers without use-kbd don't pull it).

import { useMemo } from 'react'
import { useOmnibarEndpoint } from 'use-kbd'
import type { PyramidCoverStatus } from 'pyrmts'
import {
  filterShardEntries,
  shardSearchEntries,
  type ShardSearchEntry,
  type ShardSearchOptions,
} from './shard-search.js'

export { filterShardEntries, shardSearchEntries }
export type { ShardSearchEntry, ShardSearchOptions }

export interface ShardOmnibarOptions extends ShardSearchOptions {
  /** Extra entries appended after the cover-derived ones — e.g. live raw
   *  tips (streaming-tip shards bypass the registry, so the cover doesn't
   *  know them). Build with the `ShardSearchEntry` shape (`search` = the
   *  lowercase haystack to match against). */
  extraEntries?: ShardSearchEntry[]
  /** Endpoint id; default `'shards'`. */
  id?: string
}

/**
 * Registers a sync omnibar endpoint (⌘K) over every present pyramid
 * shard: min-cover slots with registered keys, plus `extraEntries`.
 * Selecting an entry navigates to `hrefFor(key)` — conventionally a
 * `/files/*` browser's parquet viewer for that storage object.
 *
 * Searchable by pyramid label, tier, rung, period, or any key substring
 * (e.g. "gym m3", "137496 2026-07", "raw/1d").
 */
export function useShardOmnibarEndpoint(
  covers: PyramidCoverStatus[] | undefined,
  opts: ShardOmnibarOptions,
): void {
  const { hrefFor, pyramidLabel, extraEntries, id = 'shards', group = 'Shards' } = opts
  const entries = useMemo(
    () => [
      ...shardSearchEntries(covers ?? [], {
        hrefFor,
        ...(pyramidLabel !== undefined ? { pyramidLabel } : {}),
        group,
      }),
      ...(extraEntries ?? []),
    ],
    [covers, hrefFor, pyramidLabel, extraEntries, group],
  )
  useOmnibarEndpoint(id, {
    filter: (query, { offset, limit }) => filterShardEntries(entries, query, { offset, limit }),
    group,
    pagination: 'scroll',
  })
}

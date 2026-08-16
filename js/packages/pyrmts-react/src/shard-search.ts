// Pure omnibar-entry construction + filtering for shard search — kept
// React-free (and `use-kbd`-free) so it's unit-testable and reusable;
// `kbd.ts` wraps it in the `useOmnibarEndpoint` hook.

import type { PyramidCoverStatus } from 'pyrmts'

/** One searchable shard entry. Superset of use-kbd's `OmnibarLinkEntry`
 *  (declared structurally so this module doesn't import `use-kbd`):
 *  `search` is the precomputed lowercase haystack, stripped before the
 *  entry is handed to the omnibar. */
export interface ShardSearchEntry {
  id: string
  label: string
  description: string
  group: string
  href: string
  search: string
}

export interface ShardSearchOptions {
  /** Deep-link target for a shard's storage key (e.g. `` key => `/files/${key}` ``). */
  hrefFor: (key: string) => string
  /** Display name for a pyramid (e.g. map registry name → device name).
   *  Defaults to the pyramid name itself. */
  pyramidLabel?: (pyramidName: string) => string
  /** Omnibar group; default `'Shards'`. */
  group?: string
}

/** Entries for every present min-cover slot with a registered key:
 *  `label` = `<pyramid> · <tier>/<rung> · <start day>`, `description` =
 *  full key, searchable by any of those parts (see `filterShardEntries`). */
export function shardSearchEntries(
  covers: PyramidCoverStatus[],
  opts: ShardSearchOptions,
): ShardSearchEntry[] {
  const { hrefFor, pyramidLabel, group = 'Shards' } = opts
  const out: ShardSearchEntry[] = []
  for (const cover of covers) {
    const name = pyramidLabel?.(cover.name) ?? cover.name
    for (const t of cover.tiers) {
      for (const s of t.segments) {
        if (s.key === undefined) continue
        const label = `${name} · ${t.tier}/${s.shardDur} · ${s.start.slice(0, 10)}`
        out.push({
          id: `shard:${s.key}`,
          label,
          description: s.key,
          group,
          href: hrefFor(s.key),
          search: `${label} ${s.key}`.toLowerCase(),
        })
      }
    }
  }
  return out
}

/** Multi-term AND filter: every whitespace-separated query term must match
 *  somewhere in an entry's `search` haystack ("gym m3 2026-07" narrows
 *  progressively). Returns the requested page with `search` stripped —
 *  the shape use-kbd's sync `filter` endpoint expects. */
export function filterShardEntries(
  entries: ShardSearchEntry[],
  query: string,
  { offset, limit }: { offset: number; limit: number },
): { entries: Omit<ShardSearchEntry, 'search'>[]; total: number; hasMore: boolean } {
  const terms = query.toLowerCase().split(/\s+/).filter(Boolean)
  const matches = entries.filter(e => terms.every(t => e.search.includes(t)))
  return {
    entries: matches.slice(offset, offset + limit).map(({ search: _s, ...e }) => e),
    total: matches.length,
    hasMore: offset + limit < matches.length,
  }
}

// Pure omnibar-entry construction + filtering for shard search — kept
// React-free (and `use-kbd`-free) so it's unit-testable and reusable;
// `kbd.ts` wraps it in the `useOmnibarEndpoint` hook.
/** Entries for every present min-cover slot with a registered key:
 *  `label` = `<pyramid> · <tier>/<rung> · <start day>`, `description` =
 *  full key, searchable by any of those parts (see `filterShardEntries`). */
export function shardSearchEntries(covers, opts) {
    const { hrefFor, pyramidLabel, group = 'Shards' } = opts;
    const out = [];
    for (const cover of covers) {
        const name = pyramidLabel?.(cover.name) ?? cover.name;
        for (const t of cover.tiers) {
            for (const s of t.segments) {
                if (s.key === undefined)
                    continue;
                const label = `${name} · ${t.tier}/${s.shardDur} · ${s.start.slice(0, 10)}`;
                out.push({
                    id: `shard:${s.key}`,
                    label,
                    description: s.key,
                    group,
                    href: hrefFor(s.key),
                    search: `${label} ${s.key}`.toLowerCase(),
                });
            }
        }
    }
    return out;
}
/** Multi-term AND filter: every whitespace-separated query term must match
 *  somewhere in an entry's `search` haystack ("gym m3 2026-07" narrows
 *  progressively). Returns the requested page with `search` stripped —
 *  the shape use-kbd's sync `filter` endpoint expects. */
export function filterShardEntries(entries, query, { offset, limit }) {
    const terms = query.toLowerCase().split(/\s+/).filter(Boolean);
    const matches = entries.filter(e => terms.every(t => e.search.includes(t)));
    return {
        entries: matches.slice(offset, offset + limit).map(({ search: _s, ...e }) => e),
        total: matches.length,
        hasMore: offset + limit < matches.length,
    };
}
//# sourceMappingURL=shard-search.js.map
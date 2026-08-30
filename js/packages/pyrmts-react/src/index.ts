// pyrmts-react — shared health-page React components for pyrmts.
// Omnibar shard search lives in the `pyrmts-react/kbd` subpath export
// (requires the optional `use-kbd` peer).

export { CoverTimeline, coverageWindow, monthGridlines, spotlightClass } from './cover-timeline.js'
export type { CoverTimelineProps, ExtraTip, Gridline, RungKey } from './cover-timeline.js'

export { filterShardEntries, shardSearchEntries } from './shard-search.js'
export type { ShardSearchEntry, ShardSearchOptions } from './shard-search.js'

export const VERSION = '0.0.0'

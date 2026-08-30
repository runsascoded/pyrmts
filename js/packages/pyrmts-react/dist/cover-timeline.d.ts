import type { PyramidTierCoverStatus } from 'pyrmts';
/** An extra present-but-unregistered segment to overlay on one tier's row —
 *  e.g. a streaming-tip writer's current shard (Lambda-owned, bypasses the
 *  shard registry, so it's outside `pyramidCover`'s min-cover). Drawn
 *  dimmer (`--pyrmts-tip`); the cover math stays unaware of it. */
export interface ExtraTip {
    tier: string;
    shardDur: string;
    start: number;
    end: number;
    key?: string;
    uploaded?: number;
    label?: string;
}
/** Identifies one rung — the join key between an external stats row and
 *  the timeline segments it describes. */
export interface RungKey {
    tier: string;
    shardDur: string;
}
/** Class suffix that spotlights `highlight`'s rung: its segments stay lit
 *  (`tt-hl`) while every other segment fades back (`tt-faded`). Empty string
 *  (no spotlight) when `highlight` is null — the default rendering. */
export declare function spotlightClass(highlight: RungKey | null | undefined, tier: string, shardDur: string): string;
export interface CoverTimelineProps {
    tiers: PyramidTierCoverStatus[];
    genesis: number;
    now: number;
    extraTips?: ExtraTip[];
    /** When set, keyed slots get `cursor: pointer` + click-through (default
     *  navigation via `location.assign(hrefFor(key))`) and the tooltip
     *  appends a "click to browse" hint. */
    hrefFor?: (key: string) => string;
    /** Overrides the default navigation; receives the slot's storage key. */
    onShardClick?: (key: string) => void;
    /** Rung to spotlight (e.g. hovered in a sibling stats table): its
     *  segments stay lit while every other segment fades back. `null` = no
     *  spotlight (the default, unchanged rendering). */
    highlight?: RungKey | null;
}
/** First-of-month gridline instants covering `[genesis, now]`, starting at
 *  the first-of-month at-or-before `genesis` (the leading line may fall
 *  left of the axis — the SVG clips it). January lines are `major` and
 *  labeled with the year; other months get short-month labels. */
export interface Gridline {
    t: number;
    label: string;
    major: boolean;
}
export declare function monthGridlines(genesis: number, now: number): Gridline[];
/** A pyramid's rendering window from the cover's `[genesis, now)` bounds:
 *  genesis extended left by max(1 day, 2% of the span) so the first shard
 *  doesn't hug the axis. */
export declare function coverageWindow(genesisTs: number, now: number): {
    genesis: number;
    now: number;
};
/**
 * Coverage timeline for one pyramid, rendered from `pyramidCover`
 * min-cover segments: one row per tier, one rectangle per cover slot,
 * colored by status (present / pending / missing). Slots are per-shard,
 * so rung boundaries show as strokes; the uncovered head right of the
 * last slot (the current open period) stays background-colored.
 *
 * Every slot gets a floating tooltip — one shared `useFloating` instance,
 * re-anchored to the hovered rect; not per-rect, which would mount
 * hundreds of hook instances per timeline.
 *
 * X-axis maps `[genesis, now]` → `[0, 1000]` in the SVG viewBox so the
 * bar scales to whatever CSS width the container has.
 *
 * Styling: import `pyrmts-react/styles.css` (status colors are CSS vars —
 * `--pyrmts-present`, `--pyrmts-pending`, `--pyrmts-missing`,
 * `--pyrmts-tip` — override to retheme).
 */
export declare function CoverTimeline({ tiers, genesis, now, extraTips, hrefFor, onShardClick, highlight }: CoverTimelineProps): import("react/jsx-runtime").JSX.Element;
//# sourceMappingURL=cover-timeline.d.ts.map
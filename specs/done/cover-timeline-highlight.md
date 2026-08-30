# Add a `highlight` prop to `pyrmts-react` `CoverTimeline` (rung↔timeline spotlight)

Source: awair session, 2026-08-30. awair is adopting `pyrmts-react` (shipped `55825c1` dist). The `shard-search`/`kbd` half swapped in cleanly and replaced awair's hand-rolled `useShardSearch`. The `CoverTimeline` half can't yet: awair's `TierTimeline` (the component `CoverTimeline` was ported from) has since grown a **rung↔timeline hover spotlight** that `CoverTimeline` lacks, so adopting verbatim would regress it. This spec upstreams that feature so awair can drop its fork.

## The feature

awair's `/health` renders a per-rung **stats table** (one row per `(tier, shardDur)`) directly under each device's `CoverTimeline`. Hovering a stats row **spotlights that rung's timeline segments** and fades the rest — the visual join between the table and the timeline. It reads far better than the alternatives (a rung is often hundreds of sub-pixel-wide rects, so outlining the needles is worse than dimming the haystack).

Concretely, add to `CoverTimeline`:

```ts
/** Identifies one rung — the join key between an external stats row and
 *  the timeline segments it describes. */
export interface RungKey {
  tier: string
  shardDur: string
}

export interface CoverTimelineProps {
  // ...existing...
  /** Rung to spotlight (e.g. hovered in a sibling stats table): its
   *  segments stay lit while every other segment fades back. `null` = no
   *  spotlight (the default, unchanged rendering). */
  highlight?: RungKey | null
}
```

Rendering (verbatim from awair `www/src/components/TierTimeline.tsx`):

```ts
const spotlight = (tier: string, shardDur: string) => highlight == null ? ''
  : highlight.tier === tier && highlight.shardDur === shardDur ? ' tt-hl' : ' tt-faded'

const segClass = (status: string, key: string | undefined, tier: string, shardDur: string) =>
  `tt-seg-${status}${key !== undefined && clickable ? ' tt-clickable' : ''}${spotlight(tier, shardDur)}`
```

- Each segment rect: `className={segClass(s.status, s.key, t.tier, s.shardDur)}`.
- The raw extra-tip rect: append `spotlight(rawTier, finestRung)` so a `raw` rung's tip lights with its slots. (awair passes `spotlight('raw', '1d')`.)
- The tier label `<text>`: `className={`tt-label${highlight?.tier === t.tier ? ' tt-label-hl' : ''}`}`.

Default `highlight` to `null` in the destructure so existing callers (and the `55825c1` build) render identically — this is purely additive.

## Styles (`pyrmts-react/styles.css`)

Add, keyed off the existing `.tier-timeline` scope, using overridable vars:

```css
.tier-timeline .tier-timeline-svg rect { transition: opacity 0.12s ease; }
.tier-timeline .tt-faded { opacity: var(--pyrmts-faded-opacity, 0.15); }
.tier-timeline .tt-hl { filter: brightness(1.2); }
.tier-timeline .tt-label-hl {
  fill: var(--pyrmts-text-primary, currentColor);
  font-weight: 700;
}
```

Unrelated but worth folding in while here: awair overrides the `now` marker to **blue** (`--pyrmts-now: #3b82f6`) because the default red (`#ef4444`) collides with `missing` and reads as a coverage gap at the right edge. The var already exists — no change needed, just confirming the override path works (it's the reason `--pyrmts-now` is a var). awair will set it in its own scope.

## Reference implementation

awair drives it from the parent, one `highlight` state per device timeline:

```tsx
const [highlight, setHighlight] = useState<RungKey | null>(null)
// ...
<CoverTimeline tiers={...} genesis={...} now={...} highlight={highlight} />
// stats-table row:
<tr
  onPointerEnter={() => setHighlight({ tier: r.tier, shardDur: r.shardDur })}
  onPointerLeave={() => setHighlight(null)}
>
```

Full current source: awair `www/src/components/TierTimeline.tsx` (`RungKey`, `spotlight`, `segClass`) and `HealthPage.scss` (`.tt-hl` / `.tt-faded` / `.tt-label-hl` / the rect transition). The port is line-for-line except awair still uses a single `rawTip?: RawTip` where `CoverTimeline` took `extraTips?: ExtraTip[]` — the spotlight logic is identical either way.

## After it lands

1. Publish a `pyrmts-react` dist build carrying this (a new `dist`-branch SHA).
2. awair pins that SHA, swaps `TierTimeline` → `CoverTimeline`, wires `highlight` from `HealthPage`'s `DeviceCoverage`, sets `--pyrmts-now: #3b82f6`, and deletes `www/src/components/TierTimeline.tsx` + its tests. That closes the last hand-maintained fork of pyrmts-react in awair.

`react-health-components.md` (piece 3/4 of the original spec) can note the highlight join as part of `CoverTimeline`'s surface once merged.

## Outcome (landed 2026-08-30)

Implemented in `pyrmts-react` (`js/packages/pyrmts-react/`):
- `cover-timeline.tsx`: added `RungKey` interface, the `highlight?: RungKey | null` prop (defaults to `null` in the destructure — purely additive, `55825c1` callers render identically), and an **exported pure `spotlightClass(highlight, tier, shardDur)`** helper (rather than a component-local closure) so the classifier is unit-testable. Wired into all three sites: segment rects (`segClass(status, key, t.tier, s.shardDur)`), the extra-tip rects (`spotlightClass(highlight, tipSeg.tier, tipSeg.shardDur)`), and the tier label (`tt-label-hl`).
  - Generalization note: awair pins `spotlight('raw', '1d')` on its single `rawTip`; here each `ExtraTip` already carries its own `tier`/`shardDur`, so the tip lights via `spotlightClass(highlight, tipSeg.tier, tipSeg.shardDur)` — same semantics, no hardcoded rung.
- `styles.css`: the rect `transition: opacity`, `.tt-faded` (`--pyrmts-faded-opacity`, 0.15), `.tt-hl` (`brightness(1.2)`), `.tt-label-hl`, all scoped under `.tier-timeline`.
- `index.ts`: re-exports `spotlightClass` (value) and `RungKey` (type).
- `cover-timeline.test.ts`: exact-equality tests for `spotlightClass` (null/undefined → `''`; match → ` tt-hl`; same-tier-other-rung and other-tier-same-shardDur → ` tt-faded`).

Verified: root `tsc -b` clean, `pnpm exec vitest run` → 546 passed (+2). Visual behavior (fade/brightness) is verified downstream in awair's `/health` after it `pds l`-links or pins the dist — `CoverTimeline` has no standalone render harness here.

Handoff: once a `pyrmts-react` dist SHA carrying this is cut, awair pins it, swaps `TierTimeline` → `CoverTimeline`, wires `highlight` from `HealthPage`'s `DeviceCoverage`, sets `--pyrmts-now: #3b82f6`, and deletes `www/src/components/TierTimeline.tsx` + tests.

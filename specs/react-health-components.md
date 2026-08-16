# `pyrmts-react`: shared health-page React components (cover timeline, shard tooltips/click-through, omnibar shard search)

## Motivation

ctbk and awair now each hand-roll the same health-page UI over `pyrmts-cfw.health.pyramidCover` output:

- **Cover timeline**: per-tier SVG bars, one rect per min-cover slot, colored by `present | pending | missing` (ctbk: `www/src/pages/Health.tsx` cover bar; awair: `www/src/components/TierTimeline.tsx`).
- **Shard tooltips**: hover metadata per slot (ctbk: `ShardTip` with lazy parquet-footer reads; awair: single shared `@floating-ui/react` tooltip re-anchored to the hovered rect).
- **File-tree click-through**: present slots deep-link to an `@rdub/file-tree` browser at `/files/<r2-key>` (ctbk: `<Link to={`/files/${s.key}`}>` wrappers; awair: `onClick` → `location.assign`). Both mount `createHandlers(R2Store(bucket, {prefixes}))` worker-side and `<FileTree store={HttpStore(...)}>` client-side.
- **Omnibar shard search** (awair only, new): `use-kbd` `useOmnibarEndpoint` sync filter over every present shard key (from `covers` segments + live raw tips), entries `href`-linking into `/files/*`. ⌘K → "gym m3 32d" → open that shard in the parquet viewer. ctbk has no equivalent yet — this is the piece it would gain for free.

Every future pyrmts consumer will want all four. Extract the React pieces into a new `js/packages/pyrmts-react` package so a health page gets them by composition instead of copy-paste.

## Reference implementation

awair, as of awair@d8539c4 (see `www/src/components/TierTimeline.tsx`, `www/src/hooks/useShardSearch.ts`, `www/src/components/ShardOmnibar.tsx`, `www/src/components/FilesPage.tsx`, `cfw/serve/src/index.ts` `/files/` mount). Notable choices worth carrying over:

- **One floating tooltip per timeline, not per rect**: a single `useFloating` instance whose reference is re-anchored via `refs.setReference(e.currentTarget)` on `pointerenter`. A per-rect `<Tooltip>` wrapper would mount hundreds of hook instances per timeline.
- **Segments straight from `PyramidCoverSegment[]`**: `start`/`end` ISO parse → x-scale over `[genesis - 2% pad, now]`; per-shard slots (not coalesced) so rung/tile boundaries render as strokes.
- **Live-tip overlay**: streaming-tip consumers (Lambda-style writers that bypass the shard registry) pass unregistered-but-present tip segments separately (awair: `rawTip` prop, drawn dimmer). The cover math shouldn't know about them; the renderer should.
- **Omnibar entries**: `label` = `${deviceName} · ${tier}/${shardDur} · ${startDay}`, `description` = full key, `href` = `hrefFor(key)`; multi-term AND filter over `label + key` lowercased; sync `filter` endpoint (instant, no debounce), `pagination: 'scroll'`.

## Proposed package: `js/packages/pyrmts-react`

Peer deps: `react`, `@floating-ui/react`. `use-kbd` must NOT be a dep of the core — omnibar helpers live in a subpath export so consumers without use-kbd don't pull it:

- `pyrmts-react` (core):
  - `<CoverTimeline tiers={PyramidTierCoverStatus[]} genesis now extraTips? hrefFor?={(key) => string} onShardClick? />` — the SVG cover bars + shared floating tooltip. When `hrefFor` is set, keyed slots get `cursor: pointer` + navigation; tooltip appends a "click to browse" hint.
  - `coverageWindow(genesisMs, nowMs)` — left-pad helper.
  - A small default stylesheet (`pyrmts-react/styles.css`) with the status colors as CSS vars so consumers can retheme (`--pyrmts-present`, `--pyrmts-pending`, `--pyrmts-missing`, `--pyrmts-tip`).
- `pyrmts-react/kbd` (requires `use-kbd` peer):
  - `useShardOmnibarEndpoint(covers: PyramidCoverStatus[], opts: { hrefFor: (key: string) => string; deviceName?: (pyramidName: string) => string; extraEntries?; group?; id? })` — registers the sync endpoint described above.

Types come from `pyrmts-cfw` (`PyramidCoverStatus` etc.) — either import them as a dep or (better, to keep pyrmts-react worker-free) move the pure cover-status *types* into `pyrmts` (or a shared types module) that both `pyrmts-cfw` and `pyrmts-react` re-export. TBD at impl time; don't duplicate the interfaces.

Out of scope: `@rdub/file-tree` stays an independent generic package (it has no pyrmts knowledge); the coupling is only the `hrefFor` convention (`/files/<key>`). The worker-side `/files` mount is 5 lines of consumer code and doesn't need wrapping.

## Adoption

1. Implement `pyrmts-react`, port awair's components into it ≈verbatim (they were written prop-injected for this).
2. awair: replace local `TierTimeline` + `useShardSearch` with the package; keep `ShardOmnibar`/`FilesPage` as thin app wiring.
3. ctbk: swap the hand-rolled Health.tsx cover bar for `<CoverTimeline>`; add `useShardOmnibarEndpoint` (net-new capability there).

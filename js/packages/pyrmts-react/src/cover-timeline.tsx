// Coverage timeline over `pyramidCover` min-cover segments
// (`specs/react-health-components.md`; ported ≈verbatim from awair
// `www/src/components/TierTimeline.tsx`).

import {
  autoUpdate,
  flip,
  FloatingPortal,
  offset,
  shift,
  useFloating,
} from '@floating-ui/react'
import { useState } from 'react'
import type { PyramidTierCoverStatus } from 'pyrmts'

const MS_PER_DAY = 86_400_000

/** An extra present-but-unregistered segment to overlay on one tier's row —
 *  e.g. a streaming-tip writer's current shard (Lambda-owned, bypasses the
 *  shard registry, so it's outside `pyramidCover`'s min-cover). Drawn
 *  dimmer (`--pyrmts-tip`); the cover math stays unaware of it. */
export interface ExtraTip {
  tier: string       // tier row to draw on
  shardDur: string   // tooltip fodder
  start: number      // ms
  end: number        // ms (exclusive)
  key?: string       // storage key (click-through, if the consumer serves it)
  uploaded?: number  // ms mtime, shown in the tooltip when set
  label?: string     // tooltip status label; default 'live tip'
}

export interface CoverTimelineProps {
  tiers: PyramidTierCoverStatus[]
  genesis: number    // ms — left edge (pre-padded; see `coverageWindow`)
  now: number        // ms — right edge + "now" marker
  extraTips?: ExtraTip[]
  /** When set, keyed slots get `cursor: pointer` + click-through (default
   *  navigation via `location.assign(hrefFor(key))`) and the tooltip
   *  appends a "click to browse" hint. */
  hrefFor?: (key: string) => string
  /** Overrides the default navigation; receives the slot's storage key. */
  onShardClick?: (key: string) => void
}

/** Hovered-segment payload for the (single, shared) floating tooltip. */
interface TipState {
  tier: string
  shardDur: string
  status: string
  start?: string
  end?: string
  key?: string | undefined
  buildableAt?: string | undefined
  uploaded?: number
}

const fmtDay = (iso: string) => iso.slice(0, 10)

/** First-of-month gridline instants covering `[genesis, now]`, starting at
 *  the first-of-month at-or-before `genesis` (the leading line may fall
 *  left of the axis — the SVG clips it). January lines are `major` and
 *  labeled with the year; other months get short-month labels. */
export interface Gridline {
  t: number
  label: string
  major: boolean
}

export function monthGridlines(genesis: number, now: number): Gridline[] {
  const out: Gridline[] = []
  const start = new Date(genesis)
  start.setUTCDate(1)
  start.setUTCHours(0, 0, 0, 0)
  for (let t = start.getTime(); t <= now; ) {
    const d = new Date(t)
    const isJan = d.getUTCMonth() === 0
    const label = isJan ? String(d.getUTCFullYear()) : d.toLocaleString('en-US', { month: 'short', timeZone: 'UTC' })
    out.push({ t, label, major: isJan })
    d.setUTCMonth(d.getUTCMonth() + 1)
    t = d.getTime()
  }
  return out
}

/** A pyramid's rendering window from the cover's `[genesis, now)` bounds:
 *  genesis extended left by max(1 day, 2% of the span) so the first shard
 *  doesn't hug the axis. */
export function coverageWindow(genesisTs: number, now: number): { genesis: number; now: number } {
  const spanDays = (now - genesisTs) / MS_PER_DAY
  const pad = Math.max(1, spanDays * 0.02) * MS_PER_DAY
  return { genesis: genesisTs - pad, now }
}

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
export function CoverTimeline({ tiers, genesis, now, extraTips, hrefFor, onShardClick }: CoverTimelineProps) {
  const [tip, setTip] = useState<TipState | null>(null)
  const { refs, floatingStyles } = useFloating({
    open: tip !== null,
    placement: 'top',
    middleware: [offset(6), flip(), shift({ padding: 8 })],
    whileElementsMounted: autoUpdate,
  })
  const hoverProps = (state: TipState) => ({
    onPointerEnter: (e: React.PointerEvent<SVGRectElement>) => {
      refs.setReference(e.currentTarget)
      setTip(state)
    },
    onPointerLeave: () => setTip(null),
  })
  const clickable = hrefFor !== undefined || onShardClick !== undefined
  const clickProps = (key: string | undefined) => key === undefined || !clickable ? {} : {
    onClick: () => {
      if (onShardClick !== undefined) onShardClick(key)
      else window.location.assign(hrefFor!(key))
    },
  }
  const segClass = (status: string, key: string | undefined) =>
    `tt-seg-${status}${key !== undefined && clickable ? ' tt-clickable' : ''}`

  const range = Math.max(1, now - genesis)
  const toX = (t: number) => ((t - genesis) / range) * 1000

  const rowH = 14
  const rowGap = 3
  const labelW = 42
  const svgW = 1000  // viewBox width; CSS scales to 100%.
  const rows = tiers.length
  const svgH = rows * (rowH + rowGap)

  const gridlines = monthGridlines(genesis, now)
  const tipsFor = (tier: string) => (extraTips ?? []).filter(t => t.tier === tier)
  const anyTips = (extraTips ?? []).length > 0

  return (
    <div className="tier-timeline">
      <svg
        viewBox={`0 0 ${svgW + labelW} ${svgH + 14}`}
        preserveAspectRatio="none"
        className="tier-timeline-svg"
        aria-label="Coverage timeline"
      >
        {/* Month gridlines behind everything else. */}
        <g className="tt-grid">
          {gridlines.map((g, i) => (
            <line
              key={i}
              x1={labelW + toX(g.t)}
              x2={labelW + toX(g.t)}
              y1={0}
              y2={svgH}
              className={g.major ? 'tt-grid-major' : 'tt-grid-minor'}
            />
          ))}
        </g>

        {/* One row per tier. */}
        {tiers.map((t, i) => {
          const y = i * (rowH + rowGap)
          return (
            <g key={t.tier} className="tt-row">
              {/* Left-side tier label (SVG text so it scales with the bar). */}
              <text
                x={labelW - 4}
                y={y + rowH - 3}
                textAnchor="end"
                className="tt-label"
              >
                {t.tier}
              </text>
              {/* Row background — the "outside cover" color (open head). */}
              <rect
                x={labelW}
                y={y}
                width={svgW}
                height={rowH}
                className="tt-bg"
              />
              {t.segments.map(s => {
                const start = Date.parse(s.start)
                const end = Date.parse(s.end)
                const x0 = toX(Math.max(start, genesis))
                const x1 = toX(Math.min(end, now))
                const w = Math.max(0.3, x1 - x0)
                return (
                  <rect
                    key={s.start}
                    x={labelW + x0}
                    y={y}
                    width={w}
                    height={rowH}
                    className={segClass(s.status, s.key)}
                    {...hoverProps({
                      tier: t.tier,
                      shardDur: s.shardDur,
                      status: s.status,
                      start: s.start,
                      end: s.end,
                      key: s.key,
                      buildableAt: s.buildableAt,
                    })}
                    {...clickProps(s.key)}
                  />
                )
              })}
              {tipsFor(t.tier).map(tipSeg => (
                <rect
                  key={`tip:${tipSeg.start}`}
                  x={labelW + toX(Math.max(tipSeg.start, genesis))}
                  y={y}
                  width={Math.max(0.3, toX(Math.min(tipSeg.end, now)) - toX(Math.max(tipSeg.start, genesis)))}
                  height={rowH}
                  className={`tt-seg-tip${tipSeg.key !== undefined && clickable ? ' tt-clickable' : ''}`}
                  {...hoverProps({
                    tier: tipSeg.tier,
                    shardDur: tipSeg.shardDur,
                    status: tipSeg.label ?? 'live tip',
                    key: tipSeg.key,
                    ...(tipSeg.uploaded !== undefined ? { uploaded: tipSeg.uploaded } : {}),
                  })}
                  {...clickProps(tipSeg.key)}
                />
              ))}
            </g>
          )
        })}

        {/* "Now" marker */}
        <line
          x1={labelW + toX(now)}
          x2={labelW + toX(now)}
          y1={0}
          y2={svgH}
          className="tt-now"
        />

        {/* Month labels — below all rows. */}
        <g className="tt-axis">
          {gridlines.map((g, i) => (
            <text
              key={i}
              x={labelW + toX(g.t) + 2}
              y={svgH + 10}
              className={g.major ? 'tt-axis-major' : 'tt-axis-minor'}
            >
              {g.label}
            </text>
          ))}
        </g>
      </svg>
      {tip !== null && (
        <FloatingPortal>
          <div ref={refs.setFloating} style={floatingStyles} className="tt-tooltip">
            <div className="tt-tooltip-title">
              {tip.tier} · {tip.shardDur} · <span className={`tt-status-${tip.status.replace(/\s/g, '-')}`}>{tip.status}</span>
            </div>
            {tip.start !== undefined && tip.end !== undefined && (
              <div>{fmtDay(tip.start)} → {fmtDay(tip.end)}</div>
            )}
            {tip.uploaded !== undefined && (
              <div>uploaded {new Date(tip.uploaded).toISOString().slice(0, 19)}Z</div>
            )}
            {tip.buildableAt !== undefined && (
              <div>buildable at {tip.buildableAt.slice(0, 16)}Z</div>
            )}
            {tip.key !== undefined && (
              <>
                <div className="tt-tooltip-key">{tip.key}</div>
                {clickable && <div className="tt-tooltip-hint">click to browse</div>}
              </>
            )}
          </div>
        </FloatingPortal>
      )}
      <div className="tt-legend">
        <span className="tt-legend-item"><span className="tt-legend-swatch tt-present-swatch" /> present</span>
        <span className="tt-legend-item"><span className="tt-legend-swatch tt-pending-swatch" /> pending</span>
        <span className="tt-legend-item"><span className="tt-legend-swatch tt-missing-swatch" /> missing</span>
        {anyTips && (
          <span className="tt-legend-item"><span className="tt-legend-swatch tt-tip-swatch" /> live tip</span>
        )}
        <span className="tt-legend-item"><span className="tt-legend-swatch tt-now-swatch" /> now</span>
      </div>
    </div>
  )
}

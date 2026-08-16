import { jsx as _jsx, jsxs as _jsxs, Fragment as _Fragment } from "react/jsx-runtime";
// Coverage timeline over `pyramidCover` min-cover segments
// (`specs/react-health-components.md`; ported ≈verbatim from awair
// `www/src/components/TierTimeline.tsx`).
import { autoUpdate, flip, FloatingPortal, offset, shift, useFloating, } from '@floating-ui/react';
import { useState } from 'react';
const MS_PER_DAY = 86_400_000;
const fmtDay = (iso) => iso.slice(0, 10);
export function monthGridlines(genesis, now) {
    const out = [];
    const start = new Date(genesis);
    start.setUTCDate(1);
    start.setUTCHours(0, 0, 0, 0);
    for (let t = start.getTime(); t <= now;) {
        const d = new Date(t);
        const isJan = d.getUTCMonth() === 0;
        const label = isJan ? String(d.getUTCFullYear()) : d.toLocaleString('en-US', { month: 'short', timeZone: 'UTC' });
        out.push({ t, label, major: isJan });
        d.setUTCMonth(d.getUTCMonth() + 1);
        t = d.getTime();
    }
    return out;
}
/** A pyramid's rendering window from the cover's `[genesis, now)` bounds:
 *  genesis extended left by max(1 day, 2% of the span) so the first shard
 *  doesn't hug the axis. */
export function coverageWindow(genesisTs, now) {
    const spanDays = (now - genesisTs) / MS_PER_DAY;
    const pad = Math.max(1, spanDays * 0.02) * MS_PER_DAY;
    return { genesis: genesisTs - pad, now };
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
export function CoverTimeline({ tiers, genesis, now, extraTips, hrefFor, onShardClick }) {
    const [tip, setTip] = useState(null);
    const { refs, floatingStyles } = useFloating({
        open: tip !== null,
        placement: 'top',
        middleware: [offset(6), flip(), shift({ padding: 8 })],
        whileElementsMounted: autoUpdate,
    });
    const hoverProps = (state) => ({
        onPointerEnter: (e) => {
            refs.setReference(e.currentTarget);
            setTip(state);
        },
        onPointerLeave: () => setTip(null),
    });
    const clickable = hrefFor !== undefined || onShardClick !== undefined;
    const clickProps = (key) => key === undefined || !clickable ? {} : {
        onClick: () => {
            if (onShardClick !== undefined)
                onShardClick(key);
            else
                window.location.assign(hrefFor(key));
        },
    };
    const segClass = (status, key) => `tt-seg-${status}${key !== undefined && clickable ? ' tt-clickable' : ''}`;
    const range = Math.max(1, now - genesis);
    const toX = (t) => ((t - genesis) / range) * 1000;
    const rowH = 14;
    const rowGap = 3;
    const labelW = 42;
    const svgW = 1000; // viewBox width; CSS scales to 100%.
    const rows = tiers.length;
    const svgH = rows * (rowH + rowGap);
    const gridlines = monthGridlines(genesis, now);
    const tipsFor = (tier) => (extraTips ?? []).filter(t => t.tier === tier);
    const anyTips = (extraTips ?? []).length > 0;
    return (_jsxs("div", { className: "tier-timeline", children: [_jsxs("svg", { viewBox: `0 0 ${svgW + labelW} ${svgH + 14}`, preserveAspectRatio: "none", className: "tier-timeline-svg", "aria-label": "Coverage timeline", children: [_jsx("g", { className: "tt-grid", children: gridlines.map((g, i) => (_jsx("line", { x1: labelW + toX(g.t), x2: labelW + toX(g.t), y1: 0, y2: svgH, className: g.major ? 'tt-grid-major' : 'tt-grid-minor' }, i))) }), tiers.map((t, i) => {
                        const y = i * (rowH + rowGap);
                        return (_jsxs("g", { className: "tt-row", children: [_jsx("text", { x: labelW - 4, y: y + rowH - 3, textAnchor: "end", className: "tt-label", children: t.tier }), _jsx("rect", { x: labelW, y: y, width: svgW, height: rowH, className: "tt-bg" }), t.segments.map(s => {
                                    const start = Date.parse(s.start);
                                    const end = Date.parse(s.end);
                                    const x0 = toX(Math.max(start, genesis));
                                    const x1 = toX(Math.min(end, now));
                                    const w = Math.max(0.3, x1 - x0);
                                    return (_jsx("rect", { x: labelW + x0, y: y, width: w, height: rowH, className: segClass(s.status, s.key), ...hoverProps({
                                            tier: t.tier,
                                            shardDur: s.shardDur,
                                            status: s.status,
                                            start: s.start,
                                            end: s.end,
                                            key: s.key,
                                            buildableAt: s.buildableAt,
                                        }), ...clickProps(s.key) }, s.start));
                                }), tipsFor(t.tier).map(tipSeg => (_jsx("rect", { x: labelW + toX(Math.max(tipSeg.start, genesis)), y: y, width: Math.max(0.3, toX(Math.min(tipSeg.end, now)) - toX(Math.max(tipSeg.start, genesis))), height: rowH, className: `tt-seg-tip${tipSeg.key !== undefined && clickable ? ' tt-clickable' : ''}`, ...hoverProps({
                                        tier: tipSeg.tier,
                                        shardDur: tipSeg.shardDur,
                                        status: tipSeg.label ?? 'live tip',
                                        key: tipSeg.key,
                                        ...(tipSeg.uploaded !== undefined ? { uploaded: tipSeg.uploaded } : {}),
                                    }), ...clickProps(tipSeg.key) }, `tip:${tipSeg.start}`)))] }, t.tier));
                    }), _jsx("line", { x1: labelW + toX(now), x2: labelW + toX(now), y1: 0, y2: svgH, className: "tt-now" }), _jsx("g", { className: "tt-axis", children: gridlines.map((g, i) => (_jsx("text", { x: labelW + toX(g.t) + 2, y: svgH + 10, className: g.major ? 'tt-axis-major' : 'tt-axis-minor', children: g.label }, i))) })] }), tip !== null && (_jsx(FloatingPortal, { children: _jsxs("div", { ref: refs.setFloating, style: floatingStyles, className: "tt-tooltip", children: [_jsxs("div", { className: "tt-tooltip-title", children: [tip.tier, " \u00B7 ", tip.shardDur, " \u00B7 ", _jsx("span", { className: `tt-status-${tip.status.replace(/\s/g, '-')}`, children: tip.status })] }), tip.start !== undefined && tip.end !== undefined && (_jsxs("div", { children: [fmtDay(tip.start), " \u2192 ", fmtDay(tip.end)] })), tip.uploaded !== undefined && (_jsxs("div", { children: ["uploaded ", new Date(tip.uploaded).toISOString().slice(0, 19), "Z"] })), tip.buildableAt !== undefined && (_jsxs("div", { children: ["buildable at ", tip.buildableAt.slice(0, 16), "Z"] })), tip.key !== undefined && (_jsxs(_Fragment, { children: [_jsx("div", { className: "tt-tooltip-key", children: tip.key }), clickable && _jsx("div", { className: "tt-tooltip-hint", children: "click to browse" })] }))] }) })), _jsxs("div", { className: "tt-legend", children: [_jsxs("span", { className: "tt-legend-item", children: [_jsx("span", { className: "tt-legend-swatch tt-present-swatch" }), " present"] }), _jsxs("span", { className: "tt-legend-item", children: [_jsx("span", { className: "tt-legend-swatch tt-pending-swatch" }), " pending"] }), _jsxs("span", { className: "tt-legend-item", children: [_jsx("span", { className: "tt-legend-swatch tt-missing-swatch" }), " missing"] }), anyTips && (_jsxs("span", { className: "tt-legend-item", children: [_jsx("span", { className: "tt-legend-swatch tt-tip-swatch" }), " live tip"] })), _jsxs("span", { className: "tt-legend-item", children: [_jsx("span", { className: "tt-legend-swatch tt-now-swatch" }), " now"] })] })] }));
}
//# sourceMappingURL=cover-timeline.js.map
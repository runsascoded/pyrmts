// Same-tier consolidation tiling: cover a coarse-rung gap period from
// EXISTING finer-rung shards of the same tier. Twin of Python
// `pyrmts_engine.consolidate.tile_from_existing`
// (`specs/js-calendar-same-tier-tiling.md`; Python side:
// `specs/done/calendar-rung-consolidation.md`).
//
// The walk is deliberately narrow — no reads, no writes. Consumers own
// fetching each pick's bytes, concatenating rows (bins match by
// definition — same tier, no rebin), sorting, and writing the output
// shard. Uncovered `holes` are app policy (fetch from a tip layout,
// raise past month-close, etc.).
import { addSpan, ceilToSpan, nominalMs, parseDuration } from './axis.js';
import { shardKey } from './keys.js';
// Greedy largest-first tiling of `gap`'s period from existing same-tier
// shards (`keySet` — snapshot of the caller's listing). The prescriptive
// expected cover is wrong for this problem: it demands largest-fitting
// sub-rungs that no min-cover ever materialized; what's actually on
// storage is whatever mix of rungs history produced.
//
// Each rung's aligned slots within [segStart, segEnd) — the epoch grid
// for fixed rungs, calendar boundaries for `mo`/`y` (each slot's width
// varies with the cursor: Feb ≠ Aug). Divisibility chaining ⇒ seg
// boundaries align to some rung ≤ the current one; misaligned
// leading/trailing parts descend to finer rungs.
//
// Pre-genesis segments are dropped. `filter` supplies values for extra
// `{dim_name}` placeholders in the keyTemplate (must match how the
// caller's `keySet` keys were derived).
export function tileFromExisting(pyramid, tier, gap, keySet, opts) {
    const filter = opts.filter ?? {};
    const rungs = tier.shards.filter(r => nominalMs(r) < nominalMs(gap.shardDur));
    const picks = [];
    const holes = [];
    const tile = (segStart, segEnd, idx) => {
        if (segEnd <= opts.genesis)
            return;
        if (idx < 0) {
            holes.push({ start: segStart, end: segEnd });
            return;
        }
        const rung = rungs[idx];
        const span = parseDuration(rung);
        let cur = segStart;
        let slot = ceilToSpan(segStart, span);
        while (slot < segEnd) {
            const nxt = addSpan(slot, span);
            if (nxt > segEnd)
                break;
            if (cur < slot)
                tile(cur, slot, idx - 1);
            const key = shardKey(pyramid, tier.name, rung, slot, filter);
            if (keySet.has(key))
                picks.push({ rung, key });
            else
                tile(slot, nxt, idx - 1);
            cur = nxt;
            slot = nxt;
        }
        if (cur < segEnd)
            tile(cur, segEnd, idx - 1);
    };
    tile(gap.periodStart, gap.periodEnd, rungs.length - 1);
    return { picks, holes };
}
//# sourceMappingURL=tile-from-existing.js.map
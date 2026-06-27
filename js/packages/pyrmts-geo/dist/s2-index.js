// S2 `SpatialIndex` implementation. Wraps `s2js` — the pure-TS port of
// Google's `golang/geo` reference implementation. Runs in Cloudflare
// Workers (no Node-only deps).
//
// **Recommended primary backend.** S2's structural wins over H3:
//   - Branching factor 4 vs 7.
//   - Exact lineage: `cellToParent(cellAt(L, r), r-1) === cellAt(L, r-1)`
//     for every L. No BT mismatches at any level transition.
//   - `bboxToCells` uses S2's native `RegionCoverer`.
//   - `minimalCover` is exact (clean quadtree DP, no approximation).
//
// Cell IDs are S2 hex tokens (`toToken`/`fromToken` round-trip). The
// `SpatialIndex<string>` contract uses tokens for storage + JSON-key
// stability; internal ops use `bigint` `CellID`.
import { s2 } from 's2js';
import { isCellInCover, minimalCover as runMinimalCover } from './spatial-index-cover.js';
const { cellid, LatLng, Rect, RegionCoverer } = s2;
// `bboxToCells` upper bound. S2's `RegionCoverer` uses `maxCells` both as
// an output cap and as a work-bound: each candidate carries ~100-200 bytes
// of `S2CellID` + priority-queue metadata, so the previous 1M cap could
// peak at 100-200 MB and OOM under the V8 / CF Worker 128 MB heap limit.
//
// 10k bounds peak heap to ~1-2 MB. Real covers are well under 1k cells
// (NYC bbox at S2 L15 covered by ~few-hundred cells; callers' explicit
// `cellBudget` is typically 1024), so 10k gives the candidate-priority-
// queue ~10× headroom for refinement without blowing the heap. If a
// future caller legitimately needs more, expose `maxCells` as an optional
// `bboxToCells` argument.
const COVERER_MAX_CELLS = 10_000;
export const s2Index = {
    name: 's2',
    maxLevel: 30,
    latLngToCell(lat, lng, level) {
        const leaf = cellid.fromLatLng(LatLng.fromDegrees(lat, lng));
        return cellid.toToken(cellid.parent(leaf, level));
    },
    cellLevel(cell) {
        return cellid.level(cellid.fromToken(cell));
    },
    cellToParent(cell, targetLevel) {
        const ci = cellid.fromToken(cell);
        const target = targetLevel ?? (cellid.level(ci) - 1);
        if (target < 0)
            throw new Error(`s2Index.cellToParent: target level ${target} < 0`);
        return cellid.toToken(cellid.parent(ci, target));
    },
    bboxToCells(bbox, level) {
        const sw = LatLng.fromDegrees(bbox.minLat, bbox.minLng);
        const ne = LatLng.fromDegrees(bbox.maxLat, bbox.maxLng);
        const rect = Rect.fromLatLng(sw).addPoint(ne);
        const coverer = new RegionCoverer({
            minLevel: level,
            maxLevel: level,
            maxCells: COVERER_MAX_CELLS,
        });
        const union = coverer.covering(rect);
        return Array.from(union, (ci) => cellid.toToken(ci));
    },
    // Lineage-aware membership: walks up from `cell`, returning true on
    // the first include hit, false on the first exclude hit. Mixed-
    // resolution sets (e.g., `minimalCover` output) work naturally — an
    // include cell at a coarser level "covers" all its descendants at
    // `cell`'s level. The `level` parameter gates wrong-level rows.
    cellInSet(cell, level, set) {
        if (cellid.level(cellid.fromToken(cell)) !== level)
            return false;
        return isCellInCover(s2Index, cell, set);
    },
    minimalCover(include, system, opts) {
        return runMinimalCover(s2Index, include, system, opts);
    },
};
//# sourceMappingURL=s2-index.js.map
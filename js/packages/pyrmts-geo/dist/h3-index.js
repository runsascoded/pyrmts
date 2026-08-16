// H3 `SpatialIndex` implementation. Wraps `h3-js`.
//
// **TEST-ONLY — not exported from the package index, and no shipped code
// path may import this module.** `h3-js` declares no `sideEffects`, so any
// reachable reference to `h3Index` pins ~195 KB (minified) of it into every
// consumer bundle; keeping this module a leaf is what lets it shake out.
// `h3-js` is a devDependency for the same reason.
//
// It survives as the *second* `SpatialIndex` implementation: the
// conformance suite (`spatial-index-conformance.ts`) runs the same contract
// against both this and `s2Index`, which is the only thing keeping the
// interface from silently collapsing into "whatever S2 happens to do".
// H13/T4 are deferred indefinitely (H13's recursive boundary-triangle
// geometry doesn't decompose cleanly), so there is no other candidate.
//
// It is *not* a serving backend. H3 lineage walks have BT mismatches at
// every level transition for ~7% of points, so multi-level covers via
// `minimalCover` here are approximate — exact multi-resolution aggregation
// is unachievable on H3, which is why the last H3-keyed pyramids were
// retired downstream. Use `s2Index`.
import { cellToParent as h3CellToParent, getResolution, latLngToCell as h3LatLngToCell, polygonToCells, } from 'h3-js';
import { isCellInCover, minimalCover as runMinimalCover } from './spatial-index-cover.js';
export const h3Index = {
    name: 'h3',
    maxLevel: 15,
    latLngToCell(lat, lng, level) {
        return h3LatLngToCell(lat, lng, level);
    },
    cellLevel(cell) {
        return getResolution(cell);
    },
    cellToParent(cell, level) {
        const target = level ?? getResolution(cell) - 1;
        return h3CellToParent(cell, target);
    },
    bboxToCells(bbox, level) {
        const polygon = [
            [bbox.minLat, bbox.minLng],
            [bbox.minLat, bbox.maxLng],
            [bbox.maxLat, bbox.maxLng],
            [bbox.maxLat, bbox.minLng],
            [bbox.minLat, bbox.minLng],
        ];
        return polygonToCells(polygon, level);
    },
    // Lineage-aware membership: walks up from `cell`, returning true on
    // first include hit, false on first exclude hit. The `level` parameter
    // still gates wrong-level rows.
    //
    // Caveat: H3's parent chain is BT-affected for ~7% of points at every
    // level transition. Exact for covers built from H3-lineage operations
    // (e.g., `minimalCover`'s tree DP, which uses the same parent chain)
    // but approximate against geographically-defined covers — use s2Index
    // for that.
    cellInSet(cell, level, set) {
        if (getResolution(cell) !== level)
            return false;
        return isCellInCover(h3Index, cell, set);
    },
    minimalCover(include, system, opts) {
        // Backend-agnostic DP. H3 lineage walks have BT mismatches at every
        // level transition (~7% of points) — outputs are approximate. Use
        // s2Index for exact mixed-resolution covers.
        return runMinimalCover(h3Index, include, system, opts);
    },
};
//# sourceMappingURL=h3-index.js.map
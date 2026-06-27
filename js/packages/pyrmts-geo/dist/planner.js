// Joint time-×-space query planner. Layers spatial-index cell selection on
// top of pyrmts core's time-axis planner.
//
// Storage model (shared with the core planner): one shard per (time_tier,
// period). Geo resolutions are not separate shards — every materialized
// resolution lives inside every shard, sorted by the spatial cell column
// (cell IDs encode resolution so the sort naturally clusters rows by
// resolution-then-spatial-locality). Predicate pushdown handles the
// spatial filter at read time.
//
// The planner consumes `Pyramid.geo` via the `SpatialIndex` abstraction
// (`spatial-index.ts`); pyramids without an explicit `index` default to
// `h3Index` (back-compat for rides-v1 / avail-v2 / etc).
import { planQuery, } from 'pyrmts';
import { getSpatialIndex, h3Index } from './h3-index.js';
// Standalone bbox→cells helper. Thin wrapper over `h3Index.bboxToCells`
// for back-compat; pyramids with a non-H3 `index` should call
// `pyramid.geo.index.bboxToCells(...)` directly (or rely on the planner,
// which already does).
export function bboxToCells(bbox, level) {
    return h3Index.bboxToCells(bbox, level);
}
export function planGeoQuery(pyramid, input) {
    if (pyramid.geo === undefined) {
        throw new Error('planGeoQuery: pyramid has no `geo` config (use planQuery for time-only)');
    }
    const resolutions = pyramid.geo.resolutions;
    if (resolutions.length === 0) {
        throw new Error('planGeoQuery: pyramid.geo.resolutions is empty');
    }
    const haveBBox = input.bbox !== undefined;
    const haveCells = input.outputCells !== undefined;
    if (!haveBBox && !haveCells) {
        throw new Error('planGeoQuery: pass either `bbox` or `outputCells`');
    }
    if (haveBBox && haveCells) {
        throw new Error('planGeoQuery: pass `bbox` xor `outputCells`, not both');
    }
    if (haveBBox && input.cellBudget === undefined) {
        throw new Error('planGeoQuery: `cellBudget` required when `bbox` is provided');
    }
    const index = getSpatialIndex(pyramid);
    // Delegate the time-axis decisions to the core planner.
    const timePlan = planQuery(pyramid, {
        range: input.range,
        binBudget: input.binBudget,
        ...(input.watermarks !== undefined ? { watermarks: input.watermarks } : {}),
        ...(input.earliestWatermarks !== undefined ? { earliestWatermarks: input.earliestWatermarks } : {}),
        ...(input.earliestPerCadence !== undefined ? { earliestPerCadence: input.earliestPerCadence } : {}),
        ...(input.filter !== undefined ? { filter: input.filter } : {}),
        ...(input.smoothing !== undefined ? { smoothing: input.smoothing } : {}),
        ...(input.smoothMode !== undefined ? { smoothMode: input.smoothMode } : {}),
    });
    // Pick the finest materialized resolution whose cells-in-bbox fits the
    // cell budget. `resolutions` is finest-first. Skipped when the caller
    // supplied `outputCells` directly.
    const { outputRes, outputCells } = input.outputCells !== undefined
        ? { outputRes: input.outputCells.res, outputCells: [...input.outputCells.cells] }
        : pickResolution(index, input.bbox, resolutions, input.cellBudget);
    // Each segment carries the same cell list — every materialized resolution
    // lives in every shard, so the cell predicate is the same regardless of
    // which (finer) time tier the segment reads from.
    const segments = timePlan.segments.map(seg => ({
        from: seg.from,
        to: seg.to,
        shardTier: seg.shardTier,
        shardCadence: seg.shardCadence,
        keys: seg.keys,
        reaggregate: seg.reaggregate,
        cells: outputCells,
    }));
    return {
        ...(timePlan.outputTier !== undefined ? { outputTier: timePlan.outputTier } : {}),
        outputBin: timePlan.outputBin,
        outputRes,
        outputCells,
        segments,
        authoritativeEnd: timePlan.authoritativeEnd,
        visibleRange: timePlan.visibleRange,
        smoothing: timePlan.smoothing,
    };
}
function pickResolution(index, bbox, resolutions, cellBudget) {
    // Try finest → coarsest (resolutions is finest-first); pick first that
    // yields a non-empty cell list within the budget. An empty result at some
    // resolution means the bbox is smaller than that resolution's centroid
    // spacing — skip; coarser will catch it.
    let lastNonEmpty;
    for (const res of resolutions) {
        const cells = index.bboxToCells(bbox, res);
        if (cells.length === 0)
            continue;
        if (cells.length <= cellBudget) {
            return { outputRes: res, outputCells: cells };
        }
        lastNonEmpty = { res, cells };
    }
    if (lastNonEmpty !== undefined) {
        // None fit the budget; return the coarsest that yielded data.
        return { outputRes: lastNonEmpty.res, outputCells: lastNonEmpty.cells };
    }
    // No resolution yielded any cells (bbox smaller than every materialized
    // cell). Fall back to the single cell containing the bbox center at the
    // coarsest resolution.
    const coarsest = resolutions[resolutions.length - 1];
    const centerLat = (bbox.minLat + bbox.maxLat) / 2;
    const centerLng = (bbox.minLng + bbox.maxLng) / 2;
    return {
        outputRes: coarsest,
        outputCells: [index.latLngToCell(centerLat, centerLng, coarsest)],
    };
}
// Filter fetched rows to a chosen geo resolution + allowed cell set. Drops
// rows whose cell is at a different resolution (every shard has multiple)
// or whose cell isn't in the bbox-covering set. Stitch consumes the result.
// `index` defaults to `h3Index` for back-compat with pre-SpatialIndex
// callers; pyrmts-geo's planner threads the pyramid's actual index through.
export function filterCellsAndRes(rows, cellCol, outputRes, allowedCells, index = h3Index) {
    const allowed = new Set(allowedCells);
    return rows.filter(r => {
        const cell = r[cellCol];
        if (typeof cell !== 'string')
            return false;
        if (index.cellLevel(cell) !== outputRes)
            return false;
        return allowed.has(cell);
    });
}
// Filter fetched rows by a mixed-resolution `SpatialSet` cover (e.g.,
// `minimalCover` output). Each row's cell is checked against the cover
// via lineage walks (`SpatialIndex.cellInSet`): include cells "cover"
// their descendants; exclude cells (more specific in the lineage) win
// over ancestor include cells.
//
// Use this when the query's spatial filter is defined by a station set
// (run through `minimalCover` to compact) rather than a bbox.
export function filterCellsByCover(rows, cellCol, level, cover, index) {
    return rows.filter(r => {
        const cell = r[cellCol];
        if (typeof cell !== 'string')
            return false;
        return index.cellInSet(cell, level, cover);
    });
}
//# sourceMappingURL=planner.js.map
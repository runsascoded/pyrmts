// pyrmts — multi-scale timeseries pyramids.
// See ../../../../SPEC.md.
export { EtagConflict, NotSupported, PlanLimitError } from './types.js';
export { addSpan, binsInRange, ceilToSpan, floorToSpan, formatPeriod, nominalMs, parseDuration, shardPeriodsCovering, } from './axis.js';
export { shardBuildableAt, sourceTierFor } from './cascade-source.js';
export { DEFAULT_AUTO_MULTIPLIER, planQuery, planQueryFromInventory } from './planner.js';
export { getMonoid, stateColumns } from './monoids.js';
export { stitch } from './stitch.js';
export { pivotTallToHistogram } from './pivot.js';
export { fetchShardData, parquetBackend } from './fetch.js';
export { validateLadders } from './ladder.js';
export { CachedShardIndex, WATERMARK_KEY_SEPARATOR, decodeWatermarkKey, encodeWatermarkKey, } from './shard-index.js';
export { listExpectedShards, listMissingShards } from './gap-discovery.js';
export { CAS_ATTEMPTS, JOURNAL_BASENAME, invalidate, journalKey, listExistingWithMtime, loadInvalidations, overlaps, pruneSpent, staleKeysFor, } from './invalidation.js';
export { hashWidth, keyPattern, parseKey, shardKey, slotKey, slotOf, substituteKey, templateHasHash, validateKeyTemplate, } from './keys.js';
export { tileFromExisting } from './tile-from-existing.js';
export { ManifestShardIndex } from './manifest-shard-index.js';
// `assertShardIndexConformance` is exported separately as
// `pyrmts/test-utils` so consumers importing the main module don't pull
// vitest into their runtime bundle.
export { httpStorage, memStorage } from './storage.js';
export { DEFAULT_WALK_COLS, SnapshotReader, cpuMs, newWalkStats, renderFloor, roundTrips, rowGroupIndexFromMetadata, rowGroupSummaries, walkDiff, wallModel, } from './walkdiff.js';
export { parsePyramidYaml, pyramidFromConfig } from './yaml.js';
export { MULTISCAN_META_KEY, SCAN_COL, SCAN_HI, SCAN_LO, diffScans, diffTables, extractScan, identities, keyStateCols, parseMultiScanIndex, readMultiScan, resolveScan, seriesAcrossGroups, seriesFor, } from './multiscan.js';
export { alignedBlocks, changesetBetween, changesetFromRows, changesetToRows, composeChangesets, diffOverSpan, parseDiffIndexManifest, readChangesetNode, } from './diffindex.js';
export { buildQueryUrl, fetchPyramidQuery } from './query.js';
export const VERSION = '0.0.0';
//# sourceMappingURL=index.js.map
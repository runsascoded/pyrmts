// Parse a pyramid YAML config into a Pyramid (modulo Storage, which is
// runtime-wired by the caller — different consumers want different bindings).
//
// Defaults:
//   - axis: 'time'
//   - binCol: 'ts'
//
// See SPEC.md §YAML schema for the format. Example:
//
//   storage:
//     type: r2
//     bucket: 380nwk
//     key: 'awair-{device_id}/{tier}/{shard}/{period}.parquet'
//   dims:
//     - { name: device_id, type: int }
//   metrics:
//     - { name: temp, monoid: sum }
//   tiers:
//     - { name: raw, bin: 1min, shards: [1h, 1mo] }
//     - { name: h1,  bin: 1h,   shards: [1d, 1mo] }
import { parse as parseYaml } from 'yaml';
const VALID_AXES = new Set(['time', 'step']);
const VALID_DIM_TYPES = new Set(['int', 'string', 'h3', 'geohash', 's2']);
const VALID_MONOIDS = new Set([
    'sum', 'count', 'histogram', 'topk', 'botk', 'hll', 'tdigest',
]);
export function parsePyramidYaml(text) {
    const raw = parseYaml(text);
    if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) {
        throw new Error('parsePyramidYaml: top-level must be a mapping');
    }
    const root = raw;
    const storage = parseStorageBlock(root.storage);
    const { keyTemplate, storageMeta } = storage;
    const axis = root.axis ?? 'time';
    if (!VALID_AXES.has(axis)) {
        throw new Error(`parsePyramidYaml: invalid axis '${axis}' (want 'time' or 'step')`);
    }
    const binCol = typeof root.binCol === 'string' ? root.binCol : 'ts';
    const cfg = {
        storage: storageMeta,
        keyTemplate,
        axis,
        binCol,
        dims: parseDims(root.dims),
        metrics: parseMetrics(root.metrics),
        tiers: parseTiers(root.tiers),
    };
    if (root.geo !== undefined)
        cfg.geo = parseGeo(root.geo);
    validateShardPlaceholder(cfg.keyTemplate, cfg.tiers);
    return cfg;
}
// A multi-rung tier's per-shard label is the only thing that keeps two
// rungs starting on the same period (e.g. `4d`+`32d`, both aligned on
// `2026-08-07`) from writing to the same key and silently clobbering
// each other. `formatPeriod` produces identical text for the shared
// start, so `{shard}` in the keyTemplate is what disambiguates them.
// Single-rung tiers don't need it (one label per period).
export function validateShardPlaceholder(keyTemplate, tiers) {
    if (keyTemplate.includes('{shard}'))
        return;
    for (const tier of tiers) {
        if (tier.shards.length > 1) {
            throw new Error(`parsePyramidYaml: tier '${tier.name}' has a multi-rung ladder ` +
                `(${JSON.stringify(tier.shards)}) but keyTemplate '${keyTemplate}' ` +
                `is missing the '{shard}' placeholder — rungs starting on the same ` +
                `period would collide on one key. Add '{shard}' to the template ` +
                `(e.g. '.../{tier}/{shard}/{period}.parquet') or collapse the tier ` +
                `to a single shard rung.`);
        }
    }
}
// Sanity bound on declared levels. The backend isn't known at parse time
// (`geo.index` is a TS object, not YAML), so this is the widest level any
// supported backend addresses — S2's 30. It was 15 (H3's max) until
// 2026-08-16, which would have rejected any S2 pyramid declaring levels
// 16-30 — e.g. ctbk's per-station LUC cells, which reach level 20.
const MAX_GEO_LEVEL = 30;
function parseGeo(raw) {
    if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) {
        throw new Error('parsePyramidYaml: `geo` must be a mapping');
    }
    const g = raw;
    const cellCol = typeof g.cellCol === 'string' ? g.cellCol : 'h3_cell';
    if (!Array.isArray(g.resolutions) || g.resolutions.length === 0) {
        throw new Error('parsePyramidYaml: `geo.resolutions` must be a non-empty array of integers');
    }
    const resolutions = g.resolutions.map((r, i) => {
        if (typeof r !== 'number' || !Number.isInteger(r) || r < 0 || r > MAX_GEO_LEVEL) {
            throw new Error(`parsePyramidYaml: geo.resolutions[${i}] must be an integer 0-${MAX_GEO_LEVEL} (got ${String(r)})`);
        }
        return r;
    });
    // Validate finest-first ordering (descending).
    for (let i = 1; i < resolutions.length; i++) {
        if (resolutions[i] >= resolutions[i - 1]) {
            throw new Error(`parsePyramidYaml: geo.resolutions must be finest-first (descending); got ${resolutions.join(', ')}`);
        }
    }
    return { cellCol, resolutions };
}
// Materialize a full Pyramid by wiring in a StorageBackend. Re-validates
// the `{shard}` placeholder guard so a hand-built PyramidConfig
// (bypassing `parsePyramidYaml`) still can't reach downstream fill/serve
// code with a collision-prone template.
export function pyramidFromConfig(cfg, storage) {
    validateShardPlaceholder(cfg.keyTemplate, cfg.tiers);
    const p = {
        storage,
        keyTemplate: cfg.keyTemplate,
        axis: cfg.axis,
        binCol: cfg.binCol,
        dims: cfg.dims,
        metrics: cfg.metrics,
        tiers: cfg.tiers,
    };
    if (cfg.geo !== undefined)
        p.geo = cfg.geo;
    return p;
}
function parseStorageBlock(raw) {
    if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) {
        throw new Error('parsePyramidYaml: `storage` must be a mapping');
    }
    const s = raw;
    if (typeof s.type !== 'string') {
        throw new Error('parsePyramidYaml: `storage.type` must be a string');
    }
    if (typeof s.key !== 'string') {
        throw new Error('parsePyramidYaml: `storage.key` (key template) must be a string');
    }
    const { key, ...rest } = s;
    return {
        keyTemplate: key,
        storageMeta: { ...rest, type: s.type },
    };
}
function parseDims(raw) {
    if (!Array.isArray(raw)) {
        throw new Error('parsePyramidYaml: `dims` must be an array');
    }
    return raw.map((d, i) => {
        if (d === null || typeof d !== 'object' || Array.isArray(d)) {
            throw new Error(`parsePyramidYaml: dims[${i}] must be a mapping`);
        }
        const dd = d;
        if (typeof dd.name !== 'string') {
            throw new Error(`parsePyramidYaml: dims[${i}].name must be a string`);
        }
        if (typeof dd.type !== 'string' || !VALID_DIM_TYPES.has(dd.type)) {
            throw new Error(`parsePyramidYaml: dims[${i}].type '${String(dd.type)}' invalid (want one of int/string/h3/geohash/s2)`);
        }
        return { name: dd.name, type: dd.type };
    });
}
function parseMetrics(raw) {
    if (!Array.isArray(raw)) {
        throw new Error('parsePyramidYaml: `metrics` must be an array');
    }
    return raw.map((m, i) => {
        if (m === null || typeof m !== 'object' || Array.isArray(m)) {
            throw new Error(`parsePyramidYaml: metrics[${i}] must be a mapping`);
        }
        const mm = m;
        if (typeof mm.name !== 'string') {
            throw new Error(`parsePyramidYaml: metrics[${i}].name must be a string`);
        }
        if (typeof mm.monoid !== 'string' || !VALID_MONOIDS.has(mm.monoid)) {
            throw new Error(`parsePyramidYaml: metrics[${i}].monoid '${String(mm.monoid)}' invalid`);
        }
        const out = { name: mm.name, monoid: mm.monoid };
        if (mm.config !== undefined) {
            if (mm.config === null || typeof mm.config !== 'object' || Array.isArray(mm.config)) {
                throw new Error(`parsePyramidYaml: metrics[${i}].config must be a mapping`);
            }
            out.config = mm.config;
        }
        return out;
    });
}
function parseTiers(raw) {
    if (!Array.isArray(raw) || raw.length === 0) {
        throw new Error('parsePyramidYaml: `tiers` must be a non-empty array');
    }
    return raw.map((t, i) => {
        if (t === null || typeof t !== 'object' || Array.isArray(t)) {
            throw new Error(`parsePyramidYaml: tiers[${i}] must be a mapping`);
        }
        const tt = t;
        if (typeof tt.name !== 'string') {
            throw new Error(`parsePyramidYaml: tiers[${i}].name must be a string`);
        }
        if (typeof tt.bin !== 'string') {
            throw new Error(`parsePyramidYaml: tiers[${i}].bin must be a string`);
        }
        if (!Array.isArray(tt.shards) || tt.shards.length === 0) {
            throw new Error(`parsePyramidYaml: tiers[${i}].shards must be a non-empty array of Shard strings`);
        }
        const shards = tt.shards.map((s, j) => {
            if (typeof s !== 'string') {
                throw new Error(`parsePyramidYaml: tiers[${i}].shards[${j}] must be a string (got ${String(s)})`);
            }
            return s;
        });
        return {
            name: tt.name,
            bin: tt.bin,
            shards,
        };
    });
}
//# sourceMappingURL=yaml.js.map
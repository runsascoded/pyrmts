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
//     key: 'awair-{device_id}/{tier}/{period}.parquet'
//   dims:
//     - { name: device_id, type: int }
//   metrics:
//     - { name: temp, monoid: sum }
//   tiers:
//     - { name: raw, bin: 1min, shard: 1mo }
//     - { name: h1,  bin: 1h,   shard: 1mo }

import { parse as parseYaml } from 'yaml'
import type {
  Axis,
  Bin,
  Dim,
  Metric,
  MonoidName,
  Pyramid,
  Shard,
  Storage,
  Tier,
} from './types.js'

// Pyramid config with Storage stripped out — the caller wires Storage based
// on `storage.type` and runtime bindings (R2 binding, fs root, etc.).
export interface PyramidConfig {
  storage: { type: string; [key: string]: unknown }
  keyTemplate: string
  axis: Axis
  binCol: string
  dims: Dim[]
  metrics: Metric[]
  tiers: Tier[]
}

const VALID_AXES = new Set<Axis>(['time', 'step'])
const VALID_DIM_TYPES = new Set<Dim['type']>(['int', 'string', 'h3', 'geohash'])
const VALID_MONOIDS = new Set<MonoidName>([
  'sum', 'count', 'histogram', 'topk', 'botk', 'hll', 'tdigest',
])

export function parsePyramidYaml(text: string): PyramidConfig {
  const raw = parseYaml(text)
  if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) {
    throw new Error('parsePyramidYaml: top-level must be a mapping')
  }
  const root = raw as Record<string, unknown>

  const storage = parseStorageBlock(root.storage)
  const { keyTemplate, storageMeta } = storage

  const axis = (root.axis as Axis | undefined) ?? 'time'
  if (!VALID_AXES.has(axis)) {
    throw new Error(`parsePyramidYaml: invalid axis '${axis}' (want 'time' or 'step')`)
  }

  const binCol = typeof root.binCol === 'string' ? root.binCol : 'ts'

  return {
    storage: storageMeta,
    keyTemplate,
    axis,
    binCol,
    dims: parseDims(root.dims),
    metrics: parseMetrics(root.metrics),
    tiers: parseTiers(root.tiers),
  }
}

// Materialize a full Pyramid by wiring in a Storage impl.
export function pyramidFromConfig(cfg: PyramidConfig, storage: Storage): Pyramid {
  return {
    storage,
    keyTemplate: cfg.keyTemplate,
    axis: cfg.axis,
    binCol: cfg.binCol,
    dims: cfg.dims,
    metrics: cfg.metrics,
    tiers: cfg.tiers,
  }
}

function parseStorageBlock(
  raw: unknown,
): { keyTemplate: string; storageMeta: PyramidConfig['storage'] } {
  if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) {
    throw new Error('parsePyramidYaml: `storage` must be a mapping')
  }
  const s = raw as Record<string, unknown>
  if (typeof s.type !== 'string') {
    throw new Error('parsePyramidYaml: `storage.type` must be a string')
  }
  if (typeof s.key !== 'string') {
    throw new Error('parsePyramidYaml: `storage.key` (key template) must be a string')
  }
  const { key, ...rest } = s
  return {
    keyTemplate: key,
    storageMeta: { ...(rest as Record<string, unknown>), type: s.type },
  }
}

function parseDims(raw: unknown): Dim[] {
  if (!Array.isArray(raw)) {
    throw new Error('parsePyramidYaml: `dims` must be an array')
  }
  return raw.map((d, i) => {
    if (d === null || typeof d !== 'object' || Array.isArray(d)) {
      throw new Error(`parsePyramidYaml: dims[${i}] must be a mapping`)
    }
    const dd = d as Record<string, unknown>
    if (typeof dd.name !== 'string') {
      throw new Error(`parsePyramidYaml: dims[${i}].name must be a string`)
    }
    if (typeof dd.type !== 'string' || !VALID_DIM_TYPES.has(dd.type as Dim['type'])) {
      throw new Error(
        `parsePyramidYaml: dims[${i}].type '${String(dd.type)}' invalid (want one of int/string/h3/geohash)`,
      )
    }
    return { name: dd.name, type: dd.type as Dim['type'] }
  })
}

function parseMetrics(raw: unknown): Metric[] {
  if (!Array.isArray(raw)) {
    throw new Error('parsePyramidYaml: `metrics` must be an array')
  }
  return raw.map((m, i) => {
    if (m === null || typeof m !== 'object' || Array.isArray(m)) {
      throw new Error(`parsePyramidYaml: metrics[${i}] must be a mapping`)
    }
    const mm = m as Record<string, unknown>
    if (typeof mm.name !== 'string') {
      throw new Error(`parsePyramidYaml: metrics[${i}].name must be a string`)
    }
    if (typeof mm.monoid !== 'string' || !VALID_MONOIDS.has(mm.monoid as MonoidName)) {
      throw new Error(
        `parsePyramidYaml: metrics[${i}].monoid '${String(mm.monoid)}' invalid`,
      )
    }
    const out: Metric = { name: mm.name, monoid: mm.monoid as MonoidName }
    if (mm.config !== undefined) {
      if (mm.config === null || typeof mm.config !== 'object' || Array.isArray(mm.config)) {
        throw new Error(`parsePyramidYaml: metrics[${i}].config must be a mapping`)
      }
      out.config = mm.config as Record<string, unknown>
    }
    return out
  })
}

function parseTiers(raw: unknown): Tier[] {
  if (!Array.isArray(raw) || raw.length === 0) {
    throw new Error('parsePyramidYaml: `tiers` must be a non-empty array')
  }
  return raw.map((t, i) => {
    if (t === null || typeof t !== 'object' || Array.isArray(t)) {
      throw new Error(`parsePyramidYaml: tiers[${i}] must be a mapping`)
    }
    const tt = t as Record<string, unknown>
    if (typeof tt.name !== 'string') {
      throw new Error(`parsePyramidYaml: tiers[${i}].name must be a string`)
    }
    if (typeof tt.bin !== 'string') {
      throw new Error(`parsePyramidYaml: tiers[${i}].bin must be a string`)
    }
    if (typeof tt.shard !== 'string') {
      throw new Error(`parsePyramidYaml: tiers[${i}].shard must be a string`)
    }
    return {
      name: tt.name,
      bin: tt.bin as Bin,
      shard: tt.shard as Shard,
    }
  })
}

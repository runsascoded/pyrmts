// Monoid catalog. Each monoid defines (a) the column-suffix layout for how
// its state is stored alongside a metric, and (b) an associative+commutative
// combine that merges two states.
//
// Storage convention: for metric named `foo` with monoid M (suffixes ['_a',
// '_b']), the parquet shard has columns `foo_a` and `foo_b`. Stitcher reads
// both, combines element-wise.
//
// v0.1 ships `sum` and `count`. `histogram`/`topk`/`hll`/`tdigest` follow
// the same shape but aren't implemented yet.

import type { MonoidName } from './types.js'

export type Row = Record<string, unknown>

export interface Monoid {
  // Column suffixes for state storage. Single-column monoids use [''], so
  // the storage column is just the metric name.
  stateSuffixes: string[]
  // Combine `source`'s state into `target` in place, for the given metric.
  combine(target: Row, source: Row, metricName: string): void
  // Optional: normalize this metric's state in a freshly-created aggregate
  // row. Stitcher calls this once when an output bin first sees data, so
  // monoids with object-typed state (`histogram`) can detach from the
  // source row (avoid aliasing) and parse JSON-string forms transparently.
  init?(target: Row, metricName: string): void
}

const SUM_SUFFIXES = ['_n', '_sum', '_sumsq']

const sum: Monoid = {
  stateSuffixes: SUM_SUFFIXES,
  combine(target, source, name) {
    for (const suffix of SUM_SUFFIXES) {
      const col = `${name}${suffix}`
      const t = (target[col] as number | undefined) ?? 0
      const s = (source[col] as number | undefined) ?? 0
      target[col] = t + s
    }
  },
}

const count: Monoid = {
  stateSuffixes: [''],
  combine(target, source, name) {
    const t = (target[name] as number | undefined) ?? 0
    const s = (source[name] as number | undefined) ?? 0
    target[name] = t + s
  },
}

// Histogram: state is a `{ category: count }` map. Stored as one column
// (JSON parquet logical type recommended — hyparquet round-trips JS objects
// transparently). Combine merges maps, summing overlapping keys.
//
// Keys are coerced to strings (JS object keys are strings anyway, and JSON
// can't represent non-string keys). Consumers with int-valued categories
// (e.g. ctbk avail's `num_bikes_available`) should stringify when building.
const histogram: Monoid = {
  stateSuffixes: [''],
  init(target, name) {
    target[name] = parseHist(target[name])
  },
  combine(target, source, name) {
    const t = parseHist(target[name])
    const s = parseHist(source[name])
    for (const k in s) {
      t[k] = (t[k] ?? 0) + s[k]!
    }
    target[name] = t
  },
}

function parseHist(v: unknown): Record<string, number> {
  if (v === null || v === undefined) return {}
  if (typeof v === 'string') return JSON.parse(v) as Record<string, number>
  if (typeof v === 'object') return { ...(v as Record<string, number>) }
  throw new Error(`histogram: unexpected value type ${typeof v} (got ${String(v)})`)
}

const MONOIDS = {
  sum,
  count,
  histogram,
} satisfies Partial<Record<MonoidName, Monoid>>

export function getMonoid(name: MonoidName): Monoid {
  const m = (MONOIDS as Partial<Record<MonoidName, Monoid>>)[name]
  if (!m) throw new Error(`Monoid '${name}' not yet implemented`)
  return m
}

// Column names a metric occupies in a shard, given its monoid.
export function stateColumns(monoid: MonoidName, metricName: string): string[] {
  return getMonoid(monoid).stateSuffixes.map(s => `${metricName}${s}`)
}

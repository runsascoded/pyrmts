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

const MONOIDS = {
  sum,
  count,
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

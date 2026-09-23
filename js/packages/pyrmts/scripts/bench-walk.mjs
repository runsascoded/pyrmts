#!/usr/bin/env node
// Run the index-free diff walk (`walkDiff`) over HTTP and report what it
// measured: expansions, listings, GETs, bytes, per-round wall, CPU split.
//
//   pnpm -C packages/pyrmts build
//   node scripts/bench-walk.mjs --base http://127.0.0.1:3729/ \
//     --a 2026-09-15T1201/path-index.parquet --b 2026-09-16T0001/path-index.parquet \
//     [--root marin-us-east-02a/tmp] [--width 1400 --height 340 --cell 4] \
//     [--order level|bestfirst] [--parallel 8] [--repeat 2] [--cols path,depth,b,o] [--json]
//
// Against a remote origin (a public R2 bucket, Pages, ...) `--base` is that
// origin: every number is then a real network measurement.

import { parseArgs } from 'node:util'
import { SnapshotReader, cpuMs, httpStorage, newWalkStats, renderFloor, roundTrips, walkDiff } from 'pyrmts'

const { values: v } = parseArgs({
  options: {
    base: { type: 'string' },
    a: { type: 'string' },
    b: { type: 'string' },
    root: { type: 'string', default: '' },
    width: { type: 'string', default: '1400' },
    height: { type: 'string', default: '340' },
    cell: { type: 'string', default: '4' },
    order: { type: 'string', default: 'level' },
    parallel: { type: 'string', default: '8' },
    repeat: { type: 'string', default: '2' },
    cols: { type: 'string', default: 'path,depth,b,o' },
    json: { type: 'boolean', default: false },
  },
})
if (!v.base || !v.a || !v.b) {
  console.error('usage: bench-walk.mjs --base URL --a KEY --b KEY [--root PATH] ...')
  process.exit(2)
}
const [path, depth, size, count] = v.cols.split(',')
const cols = { path, depth, size, count }
const storage = httpStorage(v.base)
const metadataCache = new Map()
const root = v.root
const w = Number(v.width), h = Number(v.height), cell = Number(v.cell)

const probe = await new SnapshotReader(storage, v.b, { cols, rgCache: false }).open()
let rootSize
if (root) {
  const n = await probe.node(root)
  if (!n) { console.error(`root ${root} not found in scan B`); process.exit(1) }
  rootSize = n.size
} else {
  rootSize = [...(await probe.children('', 0)).values()].reduce((s, n) => s + n.size, 0)
}
const floor = renderFloor(rootSize, w, h, cell)
console.error(`root '${root}': ${rootSize.toLocaleString()} bytes; floor ${floor.toLocaleString()} bytes (${w}x${h}, ${cell}px cells)`)

const runs = []
for (let k = 0; k < Number(v.repeat); k++) {
  const stats = newWalkStats()
  const ra = new SnapshotReader(storage, v.a, { cols, stats, metadataCache })
  const rb = new SnapshotReader(storage, v.b, { cols, stats, metadataCache })
  const t0 = performance.now()
  const res = await walkDiff(ra, rb, root, { floor, order: v.order, parallel: Number(v.parallel) })
  const wall = performance.now() - t0
  const run = {
    run: k, rows: res.rows.length, expansions: res.expansions, truncated: res.truncated,
    listings: stats.listings, rgReads: stats.requests, gets: stats.gets, footerGets: stats.footerGets, bytes: stats.bytes,
    rgDecodes: stats.rgDecodes, rgCacheHits: stats.rgCacheHits, footerParses: stats.footerParses,
    rounds: stats.rounds, roundTrips: roundTrips(stats), roundMs: stats.roundMs.map(x => Math.round(x)),
    cpuMs: Math.round(cpuMs(stats)), ms: Object.fromEntries(Object.entries(stats.ms).map(([k, x]) => [k, Math.round(x)])),
    wallMs: Math.round(wall),
  }
  runs.push(run)
  console.error(
    `run ${k}: ${run.rows} rows, ${run.expansions} expansions, ${run.listings} listings, ` +
    `${run.rgReads} RG reads in ${run.gets} GETs / ${(run.bytes / 1e6).toFixed(1)} MB (cache hits ${run.rgCacheHits}), ` +
    `footer parses ${run.footerParses} (${run.footerGets} GETs); ${run.roundTrips} dependent rounds; cpu ${run.cpuMs} ms ` +
    `(footer ${run.ms.footer}, locate ${run.ms.locate}, decode ${run.ms.decode}, post ${run.ms.post}; fetch wait ${run.ms.fetch}); wall ${run.wallMs} ms; ` +
    `round ms [${run.roundMs.join(', ')}]${run.truncated ? ' [budget-cut]' : ''}`,
  )
}
if (v.json) console.log(JSON.stringify({ base: v.base, a: v.a, b: v.b, root, rootSize, floor, canvas: [w, h], cell, runs }, null, 2))

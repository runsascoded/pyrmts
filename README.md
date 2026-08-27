# `pyrmts`

> Multi-scale timeseries pyramids.

Pre-compute (shard × bin)-tier aggregates of time-series data once; serve any range × bin-budget query in O(log) bins from edge.

Polyglot: Python for the build side (`python/`), TypeScript for the serve + frontend (`js/`). Shared YAML schema constructs the same data model on both sides.

## Status

| | Side | Status |
|---|---|---|
| **`pyrmts`** core | TS + Python | shipping — used by [awair](https://github.com/runsascoded/awair), [ctbk](https://github.com/ryan-williams/ctbk) |
| **`pyrmts-cfw`** | TS | shipping — CFW serve handler |
| **`pyrmts-geo`** | TS | shipping — `s2Index` is the backend; see [`js/packages/pyrmts-geo/README.md`](./js/packages/pyrmts-geo/README.md) |
| **`pyrmts-engine`** | Python | shipping — build/fill engine, parallel window executor, Batch packaging |
| **`pyrmts-ops`** | Python | shipping — fan-out rebuild driver, GC, Lambda deployer |
| **`pyrmts-react`** | TS | built, unadopted — health/cover React components |

See [`SPEC.md`](./SPEC.md) for the original design + the four consumer projects.

## Layout

```
python/         # uv workspace
├── pyrmts/         PyPI: pyrmts         — writer, CLI, core types
├── pyrmts_engine/  PyPI: pyrmts-engine  — build/fill engine, window executor
└── pyrmts_ops/     PyPI: pyrmts-ops     — rebuild driver, GC, deployers

js/             # pnpm workspace
└── packages/
    ├── pyrmts/         npm: pyrmts         — planner, types, FE hook
    ├── pyrmts-cfw/     npm: pyrmts-cfw     — CFW serving helpers
    ├── pyrmts-geo/     npm: pyrmts-geo     — spatial extension (S2)
    └── pyrmts-react/   npm: pyrmts-react   — health/cover components
```

## Spatial backends (`pyrmts-geo`)

- **`s2Index`** — the backend. S2 quadtree (branching 4), exact lineage, optimal `minimalCover` DP. Built on [`s2js`](https://github.com/missinglink/s2js); Cloudflare-Workers compatible.
- **`h3Index`** — **test-only**, not exported. It survives as the second implementation the conformance suite runs the `SpatialIndex` contract against, which is what keeps the interface from collapsing into "whatever S2 does". H3 is not a serving backend: Boundary-Triangle mismatches affect ~7% of points at each level transition, so exact multi-resolution aggregation is unachievable on it.

Used by [ctbk](https://github.com/ryan-williams/ctbk) (S2 multi-resolution rides). [nj-crashes](https://github.com/hudcostreets/nj-crashes) completed its own H3→S2 migration (2026-08-23); its S2 cell-id range math is upstreamed here (`pyrmts-geo` `s2-range.ts`), and adoption of the cover/planner half is specced in its `specs/pyrmts-geo-adoption.md`.

## Consume from npm dist branch

`pyrmts`, `pyrmts-cfw`, `pyrmts-geo`, and `pyrmts-react` publish to the `dist` branch via a [GHA workflow](./.github/workflows/build-dist.yml) on every push to `main`. Add to your `package.json`:

```json
{
  "dependencies": {
    "pyrmts": "github:runsascoded/pyrmts#dist&path:/js/packages/pyrmts",
    "pyrmts-cfw": "github:runsascoded/pyrmts#dist&path:/js/packages/pyrmts-cfw",
    "pyrmts-geo": "github:runsascoded/pyrmts#dist&path:/js/packages/pyrmts-geo"
  }
}
```

Pin to a specific SHA by replacing `dist` with the dist-branch commit SHA.

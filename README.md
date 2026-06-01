# `pyrmts`

> Multi-scale timeseries pyramids.

Pre-compute (shard × bin)-tier aggregates of time-series data once; serve any range × bin-budget query in O(log) bins from edge.

Polyglot: Python for the build side (`python/`), TypeScript for the serve + frontend (`js/`). Shared YAML schema constructs the same data model on both sides.

## Status

| | Side | Status |
|---|---|---|
| **`pyrmts`** core | TS + Python | shipping — used by [awair](https://github.com/runsascoded/awair), [ctbk](https://github.com/ryan-williams/ctbk) |
| **`pyrmts-cfw`** | TS | shipping — CFW serve handler |
| **`pyrmts-geo`** | TS | shipping — `s2Index` primary, `h3Index` fixed-level legacy; see [`js/packages/pyrmts-geo/README.md`](./js/packages/pyrmts-geo/README.md) |
| **`pyrmts_geo`** Python | Python | not yet implemented (TS side is the active path) |

See [`SPEC.md`](./SPEC.md) for the original design + the four consumer projects.

## Layout

```
python/         # uv workspace
├── pyrmts/         PyPI: pyrmts        — build, CLI, core types
└── pyrmts_geo/     PyPI: pyrmts-geo    — spatial extension (placeholder)

js/             # pnpm workspace
└── packages/
    ├── pyrmts/         npm: pyrmts         — planner, types, FE hook
    ├── pyrmts-cfw/     npm: pyrmts-cfw     — CFW serving helpers
    └── pyrmts-geo/     npm: pyrmts-geo     — spatial extension (h3 + s2)
```

## Spatial backends (`pyrmts-geo`)

- **`s2Index`** — recommended primary. S2 quadtree (branching 4), exact lineage, optimal `minimalCover` DP. Built on [`s2js`](https://github.com/missinglink/s2js); Cloudflare-Workers compatible.
- **`h3Index`** — default for back-compat with H3-based pyramids. Recommended for **fixed-level** queries; mixed-resolution `minimalCover` is approximate (H3 Boundary-Triangle mismatches affect ~7% of points at each level transition).

Used by [ctbk](https://github.com/ryan-williams/ctbk) (S2 multi-resolution rides) and [crashes](https://github.com/runsascoded/crashes) (H3 fixed-level).

## Consume from npm dist branch

`pyrmts`, `pyrmts-cfw`, and `pyrmts-geo` publish to the `dist` branch via a [GHA workflow](./.github/workflows/build-dist.yml) on every push to `main`. Add to your `package.json`:

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

# `pyrmts`

> Multi-scale timeseries pyramids.

Pre-compute (shard × bin)-tier aggregates of time-series data once; serve any range × bin-budget query in O(log) bins from edge.

Polyglot: Python for the build side (`python/`), TypeScript for the serve + frontend (`js/`). Shared YAML schema constructs the same data model on both sides.

**Status**: pre-implementation. See [`SPEC.md`](./SPEC.md) for the design and the four consumer projects driving it.

## Layout

```
python/         # uv workspace
├── pyrmts/         PyPI: pyrmts        — build, CLI, core types
└── pyrmts_geo/     PyPI: pyrmts-geo    — spatial extension

js/             # pnpm workspace
└── packages/
    ├── pyrmts/         npm: pyrmts         — planner, types, FE hook
    ├── pyrmts-cfw/     npm: pyrmts-cfw     — CFW serving helpers
    └── pyrmts-geo/     npm: pyrmts-geo     — spatial extension
```

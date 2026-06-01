# `pyrmts-geo` (Python)

Spatial extension for [`pyrmts`](../pyrmts): H3 / S2 indexing alongside the time pyramid.

**Status**: placeholder. The TypeScript side ([`js/packages/pyrmts-geo`](../../js/packages/pyrmts-geo)) is the active path — ships `s2Index` (primary) and `h3Index` (legacy/fixed-level), with the `minimalCover` DP for mixed-resolution station-set filtering.

The Python side will land when there's a Python consumer that needs query-time geo ops (planner, `minimalCover`, filtering). The shard *writer* side already lives in `pyrmts.write_tier_parquet` (in the `pyrmts` package — geo-agnostic; takes a `cellCol` sort hint), so build-time pyramids work today without a separate Python geo extension.

See [`../../SPEC.md`](../../SPEC.md) for design context.

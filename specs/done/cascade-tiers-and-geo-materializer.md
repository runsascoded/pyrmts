# Spec: `cascade_tiers` + `materialize_resolutions` helpers

> Status: **done** (2026-05-25). Both shipped in Python (`pyrmts.cascade_tiers`
> in `dac9d5b`/`012538e`, `pyrmts_geo.materialize_resolutions` in `adafc2b`).
> TS ports deferred — ctbk's port is Python-side, and no current TS consumer
> needs them yet.
>
> From ctbk's avail-pyramid-v2 work (`~/c/hccs/ctbk/specs/avail-pyramid-v2.md`).
> Two pure, generic helpers needed by the ctbk Cascade-to-pyrmts port.
> Both are project-agnostic and useful to awair / tomat / any future pyrmts
> consumer building tier pyramids bottom-up with geo.

## Resolution

- `pyrmts.cascade_tiers` shipped per §1 with one API rename: `range` →
  `time_range` (avoid shadowing the Python builtin). Added a `filter` kwarg
  for awair-style multi-tenant key templates (extra `keyTemplate` vars).
  8 tests covering histogram/sum/count monoids, idempotency, explicit
  `derive_from`, and empty-source no-op.
- `pyrmts_geo.materialize_resolutions` shipped per §2 as written. Optional
  `MaterializeStats` sidecar (the "stats sidecar" the spec floated) tracks
  `rows_in` / `rows_out` / `dropped`. 7 tests.
- TS ports skipped for now; if/when a TS consumer wants offline builders
  they can be added by mirroring the Python API.

## 1. `cascade_tiers`

### Motivation

Given a pyrmts pyramid declared with N tiers (finest → coarsest), and
the **finest tier** already populated for some time range, build every
coarser tier by combining the finest's shards via the pyramid's monoid
catalog.

This is exactly what offline pyramid builders need: write the
finest-granularity tier from raw data, then let pyrmts cascade the
rest. Removes a category of consumer boilerplate (per-tier loops with
ad-hoc combine logic).

### API

```py
# pyrmts (python)
from pyrmts import cascade_tiers, Pyramid

cascade_tiers(
    pyramid: Pyramid,
    range: tuple[datetime, datetime],
    finest_tier: str = pyramid.tiers[0].name,    # default to the first
    storage_write: Storage | None = None,        # default = pyramid.storage
    overwrite: bool = False,                     # default: skip-if-exists
    derive_from: dict[str, str] | None = None,   # tier_name → its source-tier name (default: next-finer)
    concurrency: int = 1,
) -> CascadeResult
```

```ts
// pyrmts (ts) — same shape, async iterables for streaming the input
import { cascadeTiers } from 'pyrmts'
```

### Behavior

For each tier above `finest_tier` (coarsest-walking):
1. Compute the shard list covering `range` (via `shardPeriodsCovering`).
2. For each shard:
   a. Identify the input shards from `derive_from[tier_name]` (default
      next-finer tier).
   b. Read input rows, group by `(binStart_floored_to_this_tier_bin, ...pyramid.dims)`,
      apply each metric's monoid `combine` across grouped rows.
   c. Write the output parquet.
3. Honor `overwrite=False`: HEAD the output first; skip if present + row
   count matches what would be produced (cheap sentinel).

### Constraints

- Pure axis arithmetic + monoid application. No project specifics
  (h3, GBFS, etc.) — those belong to consumers.
- Streaming-friendly. Don't materialize all rows of all shards in RAM
  if the source is large.
- Idempotent. Concurrent invocations on disjoint shards must not race.

### Non-goals

- Doesn't write the finest tier. Consumers do that — finest-tier shape
  is project-specific (h3 materialization, raw event interpretation,
  etc.).
- Doesn't handle watermarks. Caller passes `range` explicitly.

## 2. `materialize_resolutions` (pyrmts-geo)

### Motivation

Most geo-pyramid consumers have point-level raw data (per-station,
per-ride, per-event) with `(lat, lng)` coordinates, and want to write
shards with rows at every materialized h3 resolution. Today every
consumer (ctbk's `avail_geo.py`) reimplements the same loop:

```py
for row in rows:
    for res in pyramid.geo.resolutions:
        cell = h3.latlng_to_cell(row.lat, row.lng, res)
        emit({**row, 'h3_cell': cell, '_res': res})
```

Hoist this into `pyrmts-geo`.

### API

```py
# pyrmts_geo (python)
from pyrmts_geo import materialize_resolutions

materialize_resolutions(
    rows: Iterable[Row],
    geo: GeoSpec,                                 # cellCol, resolutions
    lat_lng: Callable[[Row], tuple[float, float] | None],
) -> Iterable[Row]:
    """For each input row, emit one row per materialized h3 resolution
    with `geo.cellCol` set. Drops rows whose `lat_lng(row)` returns
    None (with a count returned in a stats sidecar?)."""
```

```ts
// pyrmts-geo (ts) — same shape, AsyncIterable
import { materializeResolutions } from 'pyrmts-geo'
```

### Behavior

- Pure h3 + groupby logic. No I/O.
- Yields rows in the input order × resolution-finest-first per input
  row. Caller is responsible for downstream sorting (by `(h3_cell, dt,
  ...dims)` per pyrmts-geo shard convention).
- Optional: stats sidecar reporting `dropped_no_latlng` count.

### Non-goals

- Doesn't aggregate / combine. Pure expansion: 1 row in → N resolutions
  out. Aggregation happens via `cascade_tiers` or directly by the
  consumer.

## Sequencing

Both helpers are blocking-but-not-on-each-other for the ctbk port:
- `cascade_tiers` unblocks ctbk's coarser-tier builds (otherwise ctbk
  writes a bespoke per-tier loop).
- `materialize_resolutions` unblocks ctbk's 1m-tier write (otherwise
  ctbk hand-rolls the lat→h3 expansion, which it currently does).

Order doesn't matter; either ships first, the other can land later.

If both ship same week, ctbk's v2 builder is straightforward.

## References

- `~/c/hccs/ctbk/specs/avail-pyramid-v2.md` — the consumer port that
  motivates these helpers
- `~/c/hccs/ctbk/ctbk/avail_geo.py` — current PoC build with inline
  h3 materialization (model for `materialize_resolutions`)
- `~/c/hccs/ctbk/gbfs/cascade/src/index.ts` — Cascade's per-tier loop
  (model for `cascade_tiers`, with sum monoid instead of histogram)

# pyrmts engine capability: id-map-keyed identity rollup (canonical `s:` rows)

Status: **done** (2026-09-14) — all pyrmts-side deliverables landed and tested: Python transform + config, JS config twin, and the `pyrmts-engine canonicalize` driver (suites green: `pyrmts` 130, `pyrmts_ops` 50, `pyrmts_engine` 124, JS 553). The only open item is ctbk's own adoption (their repo). The pyrmts-side half of the one-vocab design accepted by both sessions in [`../ctbk-serve-time-canonicalization.md`](../ctbk-serve-time-canonicalization.md). ctbk owns the ingest change (emit raw `s:<raw_id>` instead of canonical) and the serve/audit UI; pyrmts owns exactly **one** capability, quoted from that spec:

> given the id-map, emit one summed canonical `s:` row per canonical class present in a shard (a declared pyramid input → DVX dep, shard-scoped invalidation).

## Where this sits in the existing engine

- A shard's rows are keyed `(binCol, *dims)`; `cascade_tiers` (`cascade.py`) only **re-bins time** — it groups by `(floor(bin), *dims)` and monoid-combines, preserving the `dims` values exactly.
- The station identity `s:<id>` and the s2 rollup cells share **one ragged-vocab column** — the geo `cellCol` (`GeoSpec.cellCol`). `vocab-cover.ts` already treats it that way: interior nodes are vocab cells (s2), leaves are stations (`s:<key>`), and its ± DP is geometry-free over any forest.
- Geo/identity rollups are materialized at **ingest** (the finest rung the caller populates), not by `cascade` — cascade never introduces new `cellCol` values.

So the canonical identity rollup is naturally a **purely additive, idempotent pass over a built shard** — it introduces new `cellCol` values (`c:<canonical>` rows) but touches no existing build code. cascade keeps operating on raw rows and stays **id-map-independent** (cacheable); canonical rows are a per-shard overlay that can be re-derived independently.

## Contract

### Token namespaces (raw vs canonical are disjoint)

- **Raw leaves**: `s:<raw_id>` — as reported on the ride. cascade carries these up the time ladder unchanged; id-map-independent.
- **Canonical rollups**: a **distinct** prefix, `c:<canonical_id>` (recommended; opaque to the engine — it uses whatever the map's *values* are). Disjoint from `s:` so a canonical row never collides with / double-counts a raw row.
- **s2 cells**: untouched (never appear as a map key or value).

Keeping the two namespaces disjoint is what makes the pass idempotent and the serve selection clean (raw view = `s:` tokens; canonical view = `c:` tokens + `s:` tokens that are not map keys — the unmerged stations, which are their own canonical without a duplicate row).

### The id-map

`{raw_token: canonical_token}` (or bare ids with a declared prefix applied). **Partial**: only merged constituents appear — an unmerged station is absent and serves as its own canonical (no `c:` row, no row bloat). So the extra rows are `+1 c:` per *merged cluster* per shard (~25 clusters for ctbk), not per station.

### The transform (idempotent)

`recanonicalize_table(table, id_map, *, col, pyramid) -> pa.Table`:

1. **Drop** any existing rows whose `col` value is in the canonical namespace (a prior `c:` row) — so re-running with a new map *replaces* rather than accumulates. Identifying the namespace: rows whose `col` value appears as a *value* in `id_map` (or by the declared canonical prefix).
2. From the surviving rows, take those whose `col` value is a **map key**; regroup by `(bin, id_map[key], *other-dims)` and monoid-combine (reusing the same `get_monoid(...).combine` machinery as `_combine_to_bin`).
3. **Append** the resulting canonical rows to the table; return.

Properties:
- **Idempotent / RGIP**: `recanonicalize(recanonicalize(t, m), m) == recanonicalize(t, m)` byte-for-byte (step 1 strips the prior `c:` rows first). Round-trips cleanly for a fixed map.
- **No source re-pull, no raw re-aggregation**: the surviving raw `s:` rows *in the shard* are the regen source. A map change reads the shard, strips `c:`, re-derives from raw, rewrites. This is the whole reactivity payoff — a local file rewrite, never a Batch genesis→now.
- **Per-rung independence**: applied to each built rung's shard from *that rung's* raw rows, so no re-cascade is needed on a map change.

### Config — a declared pyramid input

New optional block, twinned Python/TS (mirrors the `geo:` precedent):

```yaml
identityRollup:
  col: cell                    # the cellCol/vocab column holding s:/c: tokens (defaults to geo.cellCol)
  map: station-id-map.json     # declared input (storage key or path) → DVX dep
  canonicalPrefix: "c:"        # canonical-rollup namespace (drop-and-rebuild marker)
```

The transform needs only the col, the map (`{raw_token: canonical_token}`), and the canonical prefix — the raw set is defined by the map's *keys*, so no `rawPrefix` is needed. `canonicalPrefix` is the drop marker: existing rows whose `col` begins with it are stripped and rebuilt (idempotency + correct handling of a *changed* map, where a stale canonical token may no longer be a map value).

- `types.py`: `@dataclass(frozen=True) IdentityRollup(col, map, canonicalPrefix)`; `Pyramid.identity_rollup: IdentityRollup | None = None`.
- `yaml.py`: `_parse_identity_rollup`, wired like `_parse_geo`.
- The `map` field being a declared input is what lets the harness/DVX treat the id-map as a dependency of the canonical rows (shard-scoped invalidation below). The engine loads the map (`pyramid.storage.get` or a passed dict) and passes it to the transform.

### Reactive re-derive — a direct pass, NOT the invalidation journal

An earlier draft here proposed routing an id-map change through the time-interval invalidation journal (`invalidation.py`, `_invalidations.json`) as a "map-only fast path" in the fill tick. **That is wrong** and the engine work corrected it: a journal-driven rebuild of a **finest** rung goes through `raw_fill` in `materialize_extension_shard` (`consolidate.py`) — i.e. a *source re-pull*, the Batch cost we're avoiding. The journal can't distinguish "map changed" from "data changed" anyway (entries are bare `[start, end, requested_at]` intervals).

So the reactive path is a **direct pass** that reads existing shards and re-derives canonical rows from the raw `s:` leaves *already in them* — never touching the source or the fill/journal machinery: `pyrmts-engine canonicalize -r <from>/<to> <config>` → `canonicalize_shards`. Run it after a build to materialize the canonical level, and re-run it over the affected span after an id-map change (idempotent for a fixed map). Precise station→shard scoping (vs. the coarse range) is a later optimization.

## Deliverables (this repo)

**Python core (`pyrmts`)** — DONE. The whole pure capability, testable without JS:
- `types.py` + `yaml.py`: the `identityRollup` config (`IdentityRollup(col, map, canonicalPrefix)`, `_parse_identity_rollup` guarded like `_parse_geo`, propagated through `pyramid_from_config`). ✓
- `canonicalize.py`: `recanonicalize_table(...)` (pure, idempotent) + `canonicalize_shards(pyramid, id_map, time_range, ...)` (reads each existing shard, recanonicalizes, writes — parallel, mirroring `cascade_tiers`' shard iteration). Exported from `__init__`. ✓
- Tests (`test_canonicalize.py`, `test_yaml.py`): a merged cluster sums; raw + s2 rows preserved untouched; canonical rows group by `(bin, *other-dims)`; idempotent re-run is byte-identical; a new map replaces (not accumulates) `c:` rows; unmerged station gets no `c:` row; histogram monoid merges maps; `canonicalize_shards` rewrites present shards / skips missing; config parse + defaults + errors. ✓ (10 new)

**`pyrmts_engine` (in-repo) — DONE (the direct pass).** `pyrmts-engine canonicalize -r <from>/<to> [-m <local-map>] [-j N] [-F k=v] <config>`: loads the declared `identityRollup.map` (an `s3://` URL or a storage key relative to the pyramid's storage; `-m` overrides with a local JSON path), then runs `canonicalize_shards` over the range. `_load_id_map` validates the map is `{raw_token: canonical_token}`. Tests (`test_cli.py`): a merged cluster gains its summed `c:` row while raw/unmerged rows stay; a second pass is byte-identical (idempotent). ✓

*Future optimization (not needed to unblock ctbk):* fold the canonical derive into `materialize_extension_shard`'s write (add `c:` rows to `combined` before `write_tier_parquet`) so a normal build carries canonical rows without a second read/write pass — deferred until it's shown to matter, and gated on preserving any non-schema shard columns.

**JS/TS** — much smaller than first assumed (serve-path survey, 2026-09-14):
- **No serve change in pyrmts.** pyrmts's `serve.ts`/`planner.ts` never call `vocabCover` — the `s:`-vs-cell (hence canonical-vs-raw) selection lives entirely in `vocabCover`, which the **consumer app (ctbk)** invokes and feeds into `planGeoQuery({ outputCells })`. pyrmts's row-matching (`filterCellsByCover` → `cellInSet` lineage walk) is cover-agnostic. So canonical-by-default / `?raw=1` audit selection is **ctbk-side** (`rides_v1.ts`, `avail_geo.ts` + their `vocabCover` call), not a pyrmts capability.
- **No JS transform twin.** Tile building/rollup is Python-only; JS is read/serve + planning ports (`cascade-source.ts`, `tile-from-existing.ts` are "no reads, no writes" planners). Nothing to mirror.
- **Config twin only, for parser parity** (`pyrmts/src/types.ts`, `yaml.ts`, `index.ts`): DONE. `IdentityRollup` interface + optional `identityRollup?` on `Pyramid`/`PyramidConfig`, `parseIdentityRollup` guarded like `parseGeo`, propagated in `pyramidFromConfig`, exported from the package index; 6 new `yaml.test.ts` cases mirroring the Python ones. JS ignores unknown YAML keys, so this is parity (honoring the twinned-parser convention), not correctness.

**Publish**: the config twin touches JS packages → new dist SHA; the Python engine transform → new `pyrmts-engine` image tag. ctbk pins both. (If we chose to skip the JS parity twin, no dist re-cut — but the codebase's twinning convention argues for it.)

## Open questions

- **Canonical token identity**: does ctbk's canonical id reuse a constituent's id (e.g. the latest) or a synthetic one? The `c:` prefix makes either safe (disjoint from `s:`), but confirm ctbk's `station-id-map.json` value convention so `rawPrefix`/`canonicalPrefix` match.
- **Station→shard invalidation scoping**: coarse active-span interval (reuses today's journal, over-invalidates a bit) vs. a station→shard index for exact scoping. Start coarse.
- **Where the map is loaded**: storage key under the pyramid prefix (travels with the shards, natural DVX dep) vs. a config-relative path. Leaning storage key.
- **Does `canonicalize_shards` run as an explicit stage, or fold into the engine fill driver's per-shard finalize?** Core exposes the pure transform either way; the driver wiring is a `pyrmts_engine` follow-up.

## ctbk contract answer (2026-09-14, from the ctbk session)

**`station-id-map.json` value convention: bare canonical ids, no prefix** — it's
`{alias_id: canonical_id}` with values like `6148.02`, `HB102`, `JC115` (3907
entries). So under adoption ctbk emits raw leaves as `s:<raw_id>` and the
canonical rollup applies `canonicalPrefix: "c:"` to the map *values* at build
time → `c:<canonical_id>` rows, disjoint from `s:` as required. Confirmed clean.

Two ctbk-side notes for adoption (not pyrmts's concern, recorded so they're not lost):
- The map currently contains **identity self-maps** (e.g. `1234.56 → 1234.56`).
  Per the partial-map contract (unmerged station = its own canonical, no `c:`
  row), ctbk must strip `k == v` entries before feeding `identityRollup.map`,
  else the transform emits a `c:` row duplicating one `s:` leaf (harmless sum,
  but needless row bloat). ctbk owns this filter.
- ctbk keys must switch: today the pyramid is `s:<canonical>` (canonicalized at
  ingest); post-adoption the raw leaves are `s:<raw>` and canonical is `c:<canon>`,
  so the FE/worker canonical-default query targets `c:` tokens (via `vocabCover`),
  `?raw=1` targets `s:`.

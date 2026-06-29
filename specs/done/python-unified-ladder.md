# pyrmts Python: unified-shard-ladder catch-up

Bring `python/pyrmts/` up to the unified-shard-ladder data model the JS
side just shipped (see `specs/unified-shard-ladder.md`). Without this,
downstream consumers (notably ctbk's `ctbk/pyramid_cascade/` + its
`configs/pyramids/avail.yaml`) can't move off the old `Tier.shard: str`
singular form, and `avail.yaml` literally won't parse if rewritten to
the new `shards: [...]` shape.

## Scope: what's in vs what's out

Python pyrmts is behind on **multiple** JS refactors (partial-shards,
`ShardIndex`, planner grid-walk, per-(tier,cadence) earliest
watermarks, unified ladder). Of these:

- **In scope (this spec):** the unified-ladder data-model rewrite.
  Enough to materialize per-tier ladder shards from a YAML spec.
- **Out of scope (deferred indefinitely):** `ShardIndex` interface,
  `D1ShardIndex`, `ManifestShardIndex`, planner grid-walk, planQuery
  extensions, per-(tier,cadence) earliest watermarks. These are
  query-time concerns; Python pyrmts only materializes (the CFW serves
  queries via JS). No Python consumer needs them today.

Out-of-scope items get specs of their own if/when a Python query
consumer materializes. Don't speculatively port them.

## What changes

### Types (`types.py`)

```python
@dataclass(frozen=True)
class Tier:
    name: str
    bin: str
    shards: tuple[str, ...]   # was: shard: str
```

`shards` is ordered smallest → largest, divisibility-chained (each
rung's duration divides the next). Validation in `parse_pyramid_yaml`.

No new types needed; `Shard` isn't a Python-side enum.

### YAML (`yaml.py`)

`parse_pyramid_yaml`:
- Accept `shards: [5min, 10min, ..., 1d]` per tier instead of
  `shard: 1d`.
- Reject the old `shard:` singular form with a clear error (no BC
  shim — hard cut, mirroring JS).
- Validate divisibility-chained ladders (each rung divides next).
- Validate `tier.bin` divides `tier.shards[0]`.

### Keys (`keys.py`)

`substitute_key` currently fills `{tier}`, `{period}`, plus any
`filter` extras. Needs to accept `{shard}` too — the new keyTemplate
for ctbk is `avail-v3/{tier}/{shard}/{period}.parquet`.

Concretely: `substitute_key` is already substring-based; just need
callers to pass `shard=` and an explicit test that
`{shard}`-templated keys substitute correctly.

### Cascade (`cascade.py`)

This is the substantive rewrite. Old model: tier-pair derivation —
"build tier T from tier T-1 (or `derive_from[T]`)". New model:
per-tier ladder walk — "build T's `shards[i]` from T's `shards[i-1]`
(or from the finer tier's largest shard if i == 0)".

New shape:

```python
def cascade_tiers(
    pyramid: Pyramid,
    time_range: tuple[datetime, datetime],
    finest_tier: str | None = None,
    storage_write=None,
    overwrite: bool = False,
    concurrency: int = 1,
    filter: dict | None = None,
) -> CascadeResult:
    finest = finest_tier or pyramid.tiers[0].name
    finest_idx = pyramid.tier_index(finest)

    for tier_idx in range(finest_idx, len(pyramid.tiers)):
        tier = pyramid.tiers[tier_idx]
        for shard_idx, shard_dur in enumerate(tier.shards):
            if tier_idx == finest_idx and shard_idx == 0:
                continue          # caller materialized this
            src_tier, src_shard = _pick_inputs(
                pyramid, tier_idx, shard_idx,
            )
            _cascade_one_rung(
                pyramid, tier, shard_dur,
                src_tier, src_shard,
                time_range, storage_write or pyramid.storage,
                overwrite, concurrency, filter or {},
                result,
            )
    return result
```

`_pick_inputs(pyramid, tier_idx, shard_idx)`:
- `shard_idx > 0`: source is `(tier, tier.shards[shard_idx - 1])` —
  combine N smaller shards into one larger.
- `shard_idx == 0` (smallest rung of tier T, T > finest): source is
  `(tier_{T-1}, tier_{T-1}.shards[-1])` — promote from finer tier's
  largest shard, applying the bin-floor at the new tier's `bin`.

Drop `derive_from` — replaced by the ladder declaration in YAML. If
some pyramid genuinely needs cross-tier derivation overrides, add it
back later as a `Tier.derive_from` field; YAGNI for now.

`_combine_to_bin` stays mostly as-is (still groups by
`(floor(bin), *dims)` + applies monoids). The only diff: it now also
handles same-tier combination (N smaller shards into one larger of
the same tier) — but since `tier.bin` is unchanged within a tier, the
floor step is a no-op for same-tier promotion. Single code path.

### Writer (`writer.py`)

`write_tier_parquet`: signature change — accepts `shard_dur` and uses
it in `keyTemplate` substitution. (ctbk's `ctbk/avail_v3.py` calls
this; the call site needs the new arg.)

### Storage helpers (`storage.py`)

No structural change — just ensure `FsStorage` / `S3Storage` /
`MemStorage` round-trip through the new key shape correctly. Likely a
no-op; storage is dumb-by-design.

### Tests

Update + add:
- `test_writer.py`: `write_tier_parquet` with `shard_dur` substitution
- `test_keys.py`: `{shard}` substitution
- `test_cascade.py`: per-tier ladder cascade (replace tier-pair tests)
  - small ladder: `[1h, 1d]` → 24× 1h shards combine to 1× 1d
  - cross-tier promotion: `1min` finest → `5min` tier's `[5min, 1h, 1d]`
    works end-to-end
- New: `test_yaml.py` (if not present) — `shards: [...]` parsing,
  validation errors for non-divisible chains / old `shard:` singular form

Aim for parity with the JS conformance suite where the conformance
applies (cascade output equivalence between Python + JS on a shared
fixture). Defer if it doubles the work; existing test coverage is the
floor.

## Migration in this repo

No callers inside the pyrmts repo to migrate — `python/pyrmts_geo/` is
the only intra-repo consumer. Check whether it builds against `Tier`
directly:

```bash
grep -rn "tier.shard\b\|\.shard\b" python/pyrmts_geo/src
```

If yes, port those call sites (likely just one or two — pyrmts-geo is
small).

## Downstream

After this lands + pyrmts dists are published:
- ctbk pins bumped (`pds gh -r <sha> pyrmts pyrmts-cfw pyrmts-geo` for
  JS; `pyproject.toml` `pyrmts` pin for Python).
- ctbk rewrites `configs/pyramids/avail.yaml` to new shape (see
  `~/c/hccs/ctbk/specs/avail-yaml-source-of-truth.md`).
- ctbk's `ctbk/pyramid_cascade/` adopts new `cascade_tiers` signature.

## Acceptance

- `pytest python/pyrmts/tests` green
- A small fixture pyramid (1min bin, ladder `[5min, 1h, 1d]`) builds
  end-to-end via `cascade_tiers` and outputs equal-modulo-RG to the
  same pyramid built by the JS compactor on the same input.
- ctbk's `ctbk/pyramid_cascade/cli.py` runs against the new
  `avail.yaml` and produces shards at the new paths.

## Out-of-scope follow-ups

If future Python consumers need them (none today), separate specs for:
- `ShardIndex` + `ManifestShardIndex` Python ports
- Per-(tier,shard_dur) earliest watermarks
- Planner grid-walk port

Don't write these speculatively.

## Resolution

Implemented 2026-06-29. 53/53 `pyrmts` tests + 7/7 `pyrmts_geo` tests pass.

Deviations from the spec as drafted:

- **`writer.py` left unchanged.** The spec said
  `write_tier_parquet` would gain a `shard_dur` arg, but that helper
  doesn't do key-template substitution — it just writes parquet bytes to
  a destination. The `keyTemplate` substitution happens in `cascade.py`'s
  `substitute_key` calls (which now pass `shard=`). External consumers
  (ctbk's `ctbk/avail_v3.py`) construct the key themselves; if they want
  `{shard}` in their template they pass `shard=` into their own
  `substitute_key`.
- **Ladder validation is lenient on calendar/fixed mixes.** Mirrors JS
  `validateLadders` (`js/.../ladder.ts`): a `mo`/`y` rung in the ladder
  records as `null`-ms, and divisibility checks against fixed-width
  neighbors are skipped at that boundary. So `shards: [1d, 1mo]` is
  accepted (variable-month width makes integer-ms comparison
  meaningless). Both ends of a fixed-fixed transition must still divide;
  both ends of a calendar-calendar transition must still divide in
  months.
- **`axis.py`: dropped the `shard == 'all'` sentinel branch** —
  `shard_periods_covering` now always parses the shard string as a
  Duration, matching the JS-side `'all'` removal. Use a comically-large
  Duration (`'120y'`) to express "single shard covering everything".
- **`cascade.py`: dropped the `derive_from` argument** as planned (the
  ladder declaration in YAML now drives all input selection). The
  corresponding test was rewritten as
  `test_cascade_multi_rung_with_cross_tier_promotion` exercising mixed
  same-tier + cross-tier promotion.
- **No structural change to `storage.py`.** Storage is dumb-by-design;
  the new key shape passes through unchanged.

`pyrmts_geo` required no changes — it doesn't touch `tier.shard` /
`tier.shards`, only `GeoSpec`.

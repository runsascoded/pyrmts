# pyrmts_ops adoption: pull ctbk's generic ladder-ops machinery down into pyrmts

Status: **open** (2026-07-28, ctbk session — distilled from a full cross-repo factoring audit). Goal state: a pyrmts consumer (ctbk, awair, crashes) provides exactly **(a)** a pyramid config YAML, **(b)** one raw→base-tier ingester function, **(c)** storage/D1 bindings + env, **(d)** thin CLI/handler shims — and pyrmts owns everything else. The audit found pyrmts core already clean (no consumer assumptions in shipped source; `dims`/`metrics`/`geo` abstractions real; S2 properly isolated in `pyrmts-geo`) — the debt is one-directional: ~2,000 lines of generic ops code stranded in `ctbk/pyramid_cascade/` + `gbfs/`, coupled only via module-level constants (`AVAIL_METRICS`, `['s2_cell']`, `AVAIL_GENESIS`, `R2_BUCKET`, `gbfs/` key strings) that should be read off `Pyramid`/config or injected.

Ordering below is dependency-driven; each phase ends with ctbk deleting the moved code and re-importing from pyrmts (ctbk session handles its side per phase — coordinate via this spec's status lines).

## Phase 1 — foundations (S, ~½ day): `storage_from_cfg` + Python D1 client + per-tier YAML extras

**Status: done pyrmts-side (2026-07-28)** — ctbk can now delete its copies and flip imports:

- `pyrmts.storage.storage_from_cfg(storage_cfg, *, profile=None)` (exported from `pyrmts`): R2 env chain (`R2_*`, `CLOUDFLARE_ACCOUNT_ID`-derived endpoint) → named-profile creds (never `AWS_*` env on the profile path — the 20-vs-32-char key trap) → plain `S3Storage` (generic-AWS fallthrough; ctbk's version raised here, so pass `profile='cf'` to keep must-have-R2-creds behavior). Also honors `storage.prefix` (ctbk's copy didn't).
- `pyrmts.d1`: `d1_query` + `register_shard` — same request/row shape as ctbk's `d1_http` and `pyrmts-cfw/src/d1.ts` (request shape locked by tests). Differences from ctbk's copy: no `ctbk-gbfs` database-id default (`D1_DATABASE_ID` env or param — keep the default ctbk-side if wanted), `table` param on `register_shard`, `account_id`/`api_token` overridable per call, and no stderr log line (library stays silent; wrap ctbk-side if the log matters). `pyrmts_engine.D1ShardIndex` now delegates to it (dupe deleted) and grew a `table` param.
- Per-tier YAML extras first-class: `Tier.rg_size` (tier `rg_size:` > `defaults.rg_size` > None = writer heuristic — the built-in 2048 was ctbk policy, keep it consumer-side) and `Tier.lambda_shards` (chain-validated at parse as one combined ladder with `shards`). `pyrmts.merge_lambda_shards(cfg) -> PyramidConfig` replaces the YAML-text-in/YAML-text-out version — it's config-level now; re-dump ctbk-side if a YAML string is still needed. Note: ctbk's `parse_rg_sizes` also read a `base:` block — pyrmts has no such concept (tiers only), so keep that shim ctbk-side if any config still uses it.
- Cross-cutting item landed early: `DimType` gained `'s2'` (python + JS twins, validation + messages).
- Not moved (ctbk-side deletions per spec): `lite.py:dur_min`, `_hist.py`.

- `ctbk/pyramid_cascade/storage.py:storage_from_cfg` → `pyrmts.storage` (S3/R2 factory honoring `R2_*` env + a named AWS profile param, default `profile=None`). Its own docstring argues it belongs here (bare `S3Storage` picks the wrong creds).
- `ctbk/pyramid_cascade/d1_http.py` → `pyrmts` (new module, e.g. `pyrmts.d1`): `d1_query` + `register_shard` over CF's REST API — the missing Python peer of `pyrmts-cfw/src/d1.ts`, already writing the identical `pyramid_shards` schema. Database id / table name become params (no `ctbk-gbfs` default).
- `ctbk/pyramid_cascade/config.py:parse_rg_sizes` → `pyrmts.yaml`: preserve per-tier extras (`rg_size`) first-class in `parse_pyramid_yaml` instead of a consumer-side re-parse. Also fold `merge_lambda_shards`/`parse_lambda_shards` (from `lambda_exec.py`) here: `lambda_shards` is a generic ladder-extension concept, and both the engine harness and the Lambda planner need the merged view.
- Dedup: `lite.py:dur_min` re-implements `pyrmts.axis.parse_duration`; ctbk's `_hist.py` is a stale fork of `pyrmts_engine.longform`'s copy — delete both ctbk-side.

## Phase 2 — engine absorbs the cascade planner (M, ~2 days): fsck discovery + materialize

- `fsck.py` discovery half (`discover_gaps`, `diff_with_existing`, `sort_by_dependency`, `group_by_tier_rung`, `split_stale`, `list_existing_with_mtime`) → `pyrmts_engine` (Python twin of JS `gap-discovery.ts`; the engine's `-f` LIST/diff machinery already overlaps — unify on one implementation).
- `materialize.py` generic parts (`source_tier_for`, `plan_source_cover_single_tier`, `_preaggregate_to_tier_bin`, `materialize_shard` skeleton, `emit_d1_insert_sql`) → `pyrmts_engine`. Parameterize `dim_names`/`metric_cols` off `pyramid.dims`/`pyramid.metrics` (currently hardwired `['s2_cell']`/`AVAIL_METRICS`).
- `lambda_exec.py` generic ~70% (`_tile_from_existing`, `_overlap_cover`, `materialize_extension_shard`, `run_extension_fill`, `run_single_gap`, `encode_gap`/`decode_gap`) → `pyrmts_engine`. The coupling seam: `_fill_hole_raw` / `_fill_hole_cross_tier` become injected strategies (`RawIngest`, `CrossTierRebin` protocols) — ctbk keeps its GBFS/vocab implementations; the tiling/cover/concat skeleton moves.
- Note: this phase is also where **same-tier consolidation** naturally lands engine-side (the `_tile_from_existing` concat path IS it) — coordinate with that roadmap item rather than porting then re-porting.

## Phase 3 — ops drivers (M–L, ~2-3 days): fan-out driver + progress + Lambda deployer → new `pyrmts_ops` package

- `rebuild.py` (`run_rebuild`, `expand_scaffolds`, `fill_safe_rung`, `print_plan`, `_estimate_layer`, `_Progress`, `touch_tick_function`) → `pyrmts_ops`. Function names, cost-model constants, progress key prefix, env-bump var become a config object. `_Progress`'s JSON doc is a **shared contract** with the TS `BuildProgress` type (phase 4) — define it once.
- `gbfs/lambda/deploy.py` skeleton (`build_zip` venv-vendoring, IAM/function/EventBridge upserts) → `pyrmts_ops.aws.deploy_pyramid_lambda(func_name, handler, bundle, schedule, env, ...)`; ctbk's deploy.py shrinks to a config invocation. `handler.py`'s dispatch (config select / single-gap / discovery+GC branch) → `pyrmts_ops.lambda_entry(event, ingester=..., configs=...)`, leaving consumer handlers ~5 lines.
- `gc.py` (`gc_sweep`, `covering_parent`) → `pyrmts_ops`, params `(pyramid, genesis, registry, raw_prefixes)`.
- `engine_check.py` compare harness (`_compare_streaming`, `canonical_long`, `compare_shards`, `aligned_range`) → `pyrmts_engine.validate` (generic content-equality validation for any consumer's builds).
- `orchestrator.py` → `pyrmts_engine.orchestrator` (near drop-in already; fix `_merge_long` to dispatch on `Metric.monoid`; `ingester_target` string import stays the injection seam). Consider whether `pyrmts_engine`'s newer executor supersedes parts of it — delete rather than move anything redundant.

## Phase 4 — TS serving/health SDK (M, ~1-1.5 days)

- `gbfs/api/src/health.ts` `pyramidCover`/`getPyramidsHealth` → `pyrmts-cfw` as `pyramidCover(db, pyramid, {genesis, now, pendingGraceMs, tableName})` returning the existing `PyramidCoverStatus` shape; ctbk keeps the `HEALTH_PYRAMIDS` registry + feed/compaction/tripdata sections.
- `BuildProgress` type + `getBuildsHealth` (R2 progress-doc reader) → `pyrmts-cfw`, matching phase 3's `_Progress` writer.
- Snapshot-cache-to-R2 pattern (`readCachedHealthSnapshot`/`computeAndStoreHealthSnapshot`) → small `pyrmts-cfw` util; consumer composes which sections go in.

## Cross-cutting

- **Genesis**: consumers pass it explicitly everywhere (already the convention; kill `AVAIL_GENESIS` imports from moved code).
- **Minor reverse-direction items** (audit): `DimType` lacks `'s2'` (S2 rides via `geo.cellCol` — confirm intentional or add for symmetry); `pyrmts_engine/cli.py` `--source-shard` help references "ctbk-style" (cosmetic; the min-cover default largely mooted the flag).
- **Acceptance per phase**: ctbk's copy deleted, imports flipped, existing ctbk behavior byte/content-identical (the compare harness — itself moving in phase 3 — is the tool); pyrmts tests cover the moved logic with a non-ctbk fixture pyramid (proving no residual avail-shape assumptions).
- End state check: stand up a **toy second consumer** in pyrmts' test suite (tiny synthetic "weather" pyramid: config + 20-line ingester) exercising ingest → cascade → fill → GC → health-cover end-to-end. That fixture is the SDK-cleanliness regression test.

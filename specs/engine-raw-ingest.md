# Engine raw-ingest source: build the base tier from consumer raw archives

Status: **open** (2026-08-04, ctbk session — written at the pyrmts session's request; it owns the chassis refactor, ctbk owns the parse hook + layout facts below). Motivation: the one remaining Lambda-exclusive capability. With it, ctbk's `avail-v6` regen (LU re-attribution + drop-LUC keys, ctbk `specs/lu-attribution.md` + `specs/drop-luc-station-keys.md`) becomes a **single Batch job** (~40 min-scale, per the proven ~34 min / ~$2 cascade envelope) instead of a Lambda fan-out `/1m` phase (~2h / $10–15) + Batch cascade.

## Agreement with the pyrmts session's read (2026-08-04 assessment)

The `Source` seam already admits this (`read_window(start, end) -> long frame`; `provides is None` → engine writes every rung including base). What's missing is the production chassis — tile cache, single-flight, readahead `prefetch`, `evict_before` watermark, `cache_bytes` admission, `coverage()` accounting — all currently welded to `WideShardSource`. Plan endorsed: factor a **generic tiled-source base** parameterized by (a) window → tile keys/periods, (b) blob → long-frame parse hook. ctbk's raw source becomes a small parse function plugged into that base.

## The ctbk facts (what this spec exists to pin down)

### Read representation: daily status parquets, NOT WAL JSONs, NOT minute parquets

Three candidate representations; only one is right:

1. **Daily status parquets** — `gbfs/status/<YYYY-MM-DD>.parquet` (GHA daily compaction). **The canonical raw archive and the engine's source.** Verified 2026-08-04: complete genesis→yesterday (119/119 days, 2026-04-07 → 2026-08-03), ~15 MB / ~3.5 M rows / 4 RGs per day, **~1.8 GB full history**. One row per (snapshot, station); columns: `station_id`, the 5 count metrics + 3 `is_*` flags, `last_reported`, **`ts`** (the feed's `last_updated` — authoritative time-of-record), `polled_at`; sorted `(ts, station_id)`. From 2026-08-04: `vehicle_types_available` as a nullable **JSON string** column (deterministically serialized; null for pre-v2 rows — ctbk hardened the compactor for the mixed era 2026-08-04).
2. **Raw WAL JSONs** (`gbfs/status/<date>/<HH-MM>.json`) — ~1440 tiny objects/day, ~170k+ total: read-amplification poison for a bulk build. Only relevance: the uncompacted tail (today). **Recommendation: the engine never reads them** — clamp build ranges to the compaction watermark (last daily parquet); the tip beyond it belongs to the cascade Lambda tick, per the standing doctrine (engine = bulk, tick = tip).
3. **Minute parquets** (`gbfs/avail/agg=1m/cons=1m/…`) — **unusable for re-attribution**: their columns are `(station_id, dt, metrics…)` with `dt` *baked* by `buildMinuteShard` — pre-2026-08-04 history is poll-attributed, and the raw `ts` is not present. Do not read these for a v6 build.

Tile unit = one day-parquet. 119 tiles × 15 MB with day-granular readahead slots naturally into the chassis; whole-history working set fits trivially inside the Batch task's budget.

### Parse hook (ctbk-owned, per-window)

Given a day tile's frame and a requested window `[start, end)`:

1. Filter rows to `ts ∈ [start, end)`.
2. Dedupe exact duplicates on `(ts, station_id)` — the pre-v2 poller re-recorded an unchanged LU under consecutive poll-minute keys, so day frames contain literal duplicate rows (same-LU content is byte-identical, verified upstream — either copy is fine; keep first).
3. Per `(floor(ts/60), station_id)`, keep the max-`ts` row — "state as of end of bin" (two LUs in one calendar minute = boundary jitter; ctbk `specs/lu-attribution.md` § binning model).
4. `dt = floor(ts/60) * 60`. `polled_at` is operational metadata — never used for attribution.
5. Expand `station_id` → key rows per the pyramid's dim config (today: LUC vocab chains, exactly what the Lambda's `raw_fill` does; under v6: fixed L10–L13 ancestors + `s:<short_name>` per drop-LUC). The expansion is config/vocab-driven — the engine base class needs no geo knowledge.

### Coverage semantics (the design question, settled)

Two-level rule — this is the whole answer to "absent ≠ error":

- **Missing day-parquet post-genesis = hard error** (compaction gap or wrong prefix — both real faults; the archive is verifiably complete today, keep it that way). This is the analogue of `max_missing_source`, at day granularity.
- **Missing minutes *within* a day = legitimate, always.** Upstream skips generations (120–125 s LU deltas; measured ~0.3 %/day, bursty — 3 of 4 skips one evening clustered in 20 min), boundary jitter leaves LU-less calendar minutes, and the old poller's cron shed 1–4 %. An empty bin yields no rows; no per-minute missing accounting, no threshold, no warning. (ctbk's miss-monitoring already classifies holes by LU-delta forensics upstream of the archive; the engine must not re-litigate it.)

### Determinism

Daily parquets are immutable once written (byte-stable; regenerated only by explicit backfill). Clamping the range end to the compaction watermark makes re-runs read identical bytes → the engine's existing byte-identity test discipline applies unchanged. No listing-snapshot machinery needed; refuse (or clamp with a log line) ranges extending past the watermark.

### Era notes (so nobody trips on them later)

- WAL **key names** switched from poll-minute to LU-minute at poller-v2 cutover (2026-08-04T02:58Z). Irrelevant to this spec — daily parquets carry `ts` per row and the parse hook never consults key names — but stated so the "key minute == dt minute" property isn't assumed for pre-v2 data.
- `ts` is present and authoritative back to the first record (2026-04-07T01:16Z, drift 77 s) — the whole archive re-attributes; there is no partial-history caveat.

## API shape (suggestion, pyrmts session's call)

```python
class TiledSource(Source):  # the factored chassis
    # subclass/param hooks:
    def tiles_for(self, start, end) -> list[Tile]: ...   # Tile = (key, period)
    def parse(self, blob, window) -> pl.DataFrame: ...   # → long form
    # base provides: cache, single-flight, prefetch, evict_before,
    # cache_bytes, coverage() (tile-granular missing accounting)

# ctbk side (in ctbk, not pyrmts): DailyStatusSource(TiledSource) with the
# parse hook above; wired via `ctbk gbfs engine config`/`submit` like
# WideShardSource is today, selected when the build range starts at genesis
# (or via an explicit flag).
```

`coverage()` reports at tile granularity (missing *days*); the `provides=None` path already makes the engine emit `/1m` and cascade upward.

## Acceptance

1. Chassis refactor: `WideShardSource` re-expressed over the tiled base, existing engine byte-identity tests green (the refactor's honesty check, as the pyrmts session put it).
2. A pyrmts-side synthetic raw source (toy fixture, no ctbk shapes) proving: base-tier emission, two-level coverage (missing tile raises; empty window inside a tile yields empty), dedupe-then-max-ts parse contract exercised via the hook.
3. ctbk side (separate, after dist lands): `DailyStatusSource` + a scratch-prefix `avail-v6-engine-check` build over a short range, content-compared against the same range built by the Lambda fan-out path with LU attribution — the v4-engine-check playbook re-run.

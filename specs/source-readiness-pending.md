# Source readiness: expected-but-not-yet-buildable shards are `pending`, not `missing`

Status: **open** (2026-08-01, ctbk session). Companion specs: `shard-invalidation.md` (orthogonal — that one repairs the *past*, this one classifies the *future*; both shrink "missing" down to "actually wrong"), `engine-min-cover-source.md` (background on strict-cascade source selection, implemented).

## Motivating incident (2026-07-31, ctbk avail-v3 + avail-v5)

`/health` (`pyramidCover`) reported `totalMissing=1` on both pyramids for ~40 min and paged the soak monitor. The "missing" shard was `/1h@3h [18:00,21:00)`: it becomes **expected** the moment its own period closes (21:00), but it's built by cross-tier rebin from the `/30m` tier, whose smallest rung is `2h` — the covering source tile `[20:00,22:00)` doesn't close until 22:00. For ~1 h the shard is expected-but-unfillable (the Lambda tick logged `no_inputs` on it every 5 min); the 22:03 tick wrote source then target in one dependency-ordered pass. Self-healing, structural, and recurring: in the ctbk ladder it happens for every `@3h` slot ending on an odd hour (03/09/15/21 UTC), ~4×/day, ~1 h each.

Two consumers mis-handle this class today:

1. **`pyramidCover`** calls it `missing` (red segment on /health, `totalMissing` trips monitors). Semantically it's `pending` — nothing is wrong, the build *cannot* exist yet.
2. **`run_extension_fill`** includes it in `ext_gaps` and burns a `no_inputs` attempt every tick until the source closes (log noise; pollutes `by_status` with failures that aren't).

## The rule: `buildableAt`

A shard `[s, e)` at tier `T` is buildable once its strict-cascade source cover can be complete. Recursively:

```
buildableAt(T, e):
  S = sourceTierFor(T)              # None for the base tier (raw-ingest territory)
  if S is None: return e
  e' = ceilToSpan(e, S.shards[0])   # end of the smallest-rung source tile containing e⁻
  return max(e', buildableAt(S, e'))
```

- `sourceTierFor` = the engine's `materialize.source_tier_for`: largest tier `S` with `bin(S) < bin(T)` and `bin(S) | bin(T)`. Needs a TS port (doesn't exist in `pyrmts` js yet).
- `ceilToSpan(t, span)` = `t` if span-aligned, else the next span boundary (`floorToSpan(t, span) + span`). Also new in TS.
- **Why the smallest rung is the binding constraint**: the tail of `[s, e)` needs an existing source tile overlapping every instant up to `e`; each tier's rung list is a divisibility chain, so for any coarser rung `r` (incl. `lambda_shards` extensions), `ceilToSpan(e, r) ≥ ceilToSpan(e, S.shards[0])` — no coarser tile closes earlier. Assumption (holds in the current engine, confirmed by the incident): tiles exist only at period close — there are no partial/in-progress tiles.
- The recursion covers transitive lag (source tile's own sources), and terminates at the base tier. In practice one level suffices for current ladders, but it's cheap and correct.
- For most (tier, rung) pairs `buildableAt(T, e) == e` — every rung ending is aligned to the source tier's smallest rung. In the ctbk avail ladder the **only** misaligned pair is `/1h@3h` (1h tier sources from 30m tier, `r_min = 2h`, and `2h ∤ 3h`-endings on odd hours). That enumeration makes a good parity test: sweep every (tier, rung, ending) over a few days of the avail-v5 ladder and assert the lag set is exactly `{/1h@3h at odd-hour ends: +1h}`.

## `pyrmts` (js core)

- `export function sourceTierFor(pyramid, tierName): Tier | null` — port of `source_tier_for` (same "malformed ladder" throw for a non-base tier with no divisor).
- `export function ceilToSpan(t: Date, span: ParsedTimeSpan): Date` — in `axis.ts` next to `floorToSpan`.
- `export function shardBuildableAt(pyramid, tierName, periodEnd: Date): Date` — the recursion above. Pure function of the ladder; memoization optional (call sites are per-cover-slot, trivially cheap).

## `pyrmts-cfw` (`pyramidCover`)

Replace the pending test (health.ts ~line 190):

```ts
// before
} else if (slot.periodEnd.getTime() > now.getTime() - pendingGraceMs) {
// after
} else if (shardBuildableAt(pyramid, t.name, slot.periodEnd).getTime() > now.getTime() - pendingGraceMs) {
```

- Grace is now measured from **buildableAt** (when a cron tick can first land it), not from periodEnd — same semantics as before for the aligned (majority) case, since `buildableAt == periodEnd` there.
- Optionally annotate the segment when the two differ, for /health tooltips: `buildableAt?: string` (ISO) on `PyramidCoverSegment`, set only when `buildableAt > periodEnd`. Status stays `'pending'` — no new enum value, existing UIs keep working; the annotation lets a tooltip say "waits on /30m@2h until 22:00Z".
- Counts/`complete`/`firstMissingPeriod` logic unchanged — these slots just move from the missing bucket to the pending bucket.

## `pyrmts_engine` (parity)

- `materialize.buildable_at(pyramid, tier_name, period_end) -> datetime` — same recursion, next to `source_tier_for`.
- `run_extension_fill`: partition `ext_gaps` further — gaps with `buildable_at(g) > now` are **not ready**; exclude them from the fill loop and report separately: `err(f"fillable gaps: {len(ext_gaps)} of {len(gaps)} total missing ({n_not_ready} not ready — source cover open)")`. They rejoin naturally once ready; the same-tick dependency ordering already builds source-then-target in one pass at readiness (the observed 22:03 behavior), so nothing else changes.
- `discover_gaps` itself stays unchanged (it answers "what's absent", not "what's actionable") — the filter lives in the driver, like the existing pre-genesis exclusion.
- `run_single_gap` (explicit fan-out ask): leave as-is — an explicit request for a not-ready gap fails with `no_inputs` today, which is an honest answer; not worth a special status.

## Tests

- `shardBuildableAt` / `buildable_at`: aligned rung → `== periodEnd`; `/1h@3h` odd-hour ending → next even hour; recursion on a synthetic 3-tier ladder with two levels of misalignment; base tier → `e`; malformed ladder throws.
- TS↔py parity: same ladder + endings sweep, identical results (the avail-v5 enumeration above).
- `pyramidCover`: a fixture registry with the `/1h@3h [18,21)` slot absent at now=21:30 → slot `pending` (with `buildableAt: 22:00`), tier `complete`, `totalMissing` 0; same fixture at now=22:15 (past buildableAt + grace) → `missing`.
- `run_extension_fill` dry-run at 21:30 → gap counted not-ready, zero attempts; at 22:05 → source and target both filled in one pass (existing behavior, now asserted).

## Acceptance / ctbk adoption (recorded here for the ctbk side)

- Bump `gbfs/api` pins to the dist with this change (no ctbk code change needed — `pyramidCover` signature is unchanged; the optional `buildableAt` segment field is additive and the /health FE tooltip can adopt it whenever).
- /health stops showing the 4×/day red "missing" segment during structural-lag windows (amber pending instead); the Lambda tick log loses its recurring `no_inputs` entries for this class.
- ctbk's soak monitor drops its special-cased structural-lag thresholds ("≥2 missing OR 1 missing >81 min") back to the simple rule: **any** sustained `totalMissing > 0` is a real problem.

# Engine missing-source: classify by period, not by ratio

Status: **done (2026-08-28)** — implemented `72f2552`, validated by ctbk on editable installs, pushed to `r/main`. Written by the ctbk session (2026-08-28).

**Addendum (2026-09-07) — fill mode defers shards over an absent open tile.** `72f2552` only forgave the absence *after* the build: an uncapped `-f` on a tiled source still planned the trailing rungs over the day-in-progress as fillable, wrote them as 0-row shards, and every later fill then read them as built (ctbk: 28 empty `smg-v1` trailing shards, 3 inside `avail-v6`'s live tip, 12 per `rides-v5` anchor over the unpublished month). Now, before the window walk, `build_local(fill=True)` asks a tiled source for `tiles_for(from, to)`, LISTs the open ones (`period.end > to`), and moves every missing shard whose effective period overlaps an *absent* open tile from `fillable` to `deferred` — reported as `fill: N deferred (open-period source absent: <key> [<start>, <end>)): …`, counted in `BuildResult.deferred`, not an error. A later fill (after the tile lands) builds exactly those shards. Tests: `test_raw_source.py::test_fill_defers_shards_over_absent_open_tile` (two-fill round trip).

**Addendum 2 (2026-09-07) — absent CLOSED tiles fail fast, before the walk.** The same pre-walk check now covers every tile the fill would read, not just the open ones, and answers presence through a new `TiledSource.present_keys(tiles)` hook (default: one LIST of the pyramid's storage under the tiles' common prefix; ctbk's rides source overrides it with its `available_months` S3 listing, because its tiles don't live on the pyramid's R2 — the previous open-tile LIST of R2 only got the right answer there by accident). Missing shards over an absent closed tile are *held*; if the held tiles exceed `max_missing_source` over the tiles the fill would read, `build_local` raises `SourceCoverageError` before any window is walked — `fill: N held (closed-period source absent: <key> [<start>, <end>)): …` then `… NOTHING was written — cap the range before the hole, or raise max_missing_source …` (exit 4 from the CLI). Within tolerance, closed holes build through as empty and the post-walk ratio reports them exactly as before. `strict_open_periods=True` treats open tiles as closed, so under it an absent open tile is held, not deferred (test renamed to `::test_fill_strict_open_periods_holds_absent_open_tile`). This retires the "guard fires after the outputs are written" non-goal below for fill mode on tiled sources: the ctbk rides case (uncapped `-f`, unpublished closed month) now exits 4 with zero relics instead of 12 per anchor plus a sweep. Tests: `::test_fill_fails_fast_over_absent_closed_tile`, `::test_fill_builds_through_tolerated_closed_hole`, `::test_fill_prefers_present_keys_over_storage_listing`.

Implementation notes: `TiledSource` now records absent *tiles* (key + period) — `coverage()` keeps its keys-only shape, and a new `missing_tiles()` exposes the periods. `build_local` classifies before applying the ratio: absent tiles with `period.end > to` are excluded from both sides, reported as `expected-absent (open period): <key> [<start>, <end>)` on stderr, and counted in a new `BuildResult.expected_absent`; `strict_open_periods=False` is the `build_local` kwarg (the spec's flag, inverted-default as specced), surfaced as `build -o/--strict-open-periods` and forwarded through `batch submit --strict-open-periods` / `build_command_args`. The clock is literally the range's `to` — no wall-clock enters the engine, so builds stay deterministic and a capped rebuild over closed history gets strict behavior for free (every absent period then satisfies `end <= to`). All four contract tests landed in `test_engine.py` (forgiven open tile; open-excluded 1/24 alongside a closed hole; denominator-0 single-source shape; `--strict` restoring 1/25), each also asserting outputs/registrations are unchanged by classification.

`build_local`'s `max_missing_source` is a **fraction**, and no value of it can express the one rule a maintained pyramid actually wants: *a source period that hasn't happened yet (or is still open) may be absent; any other absence is a real hole.* The ratio conflates the two, and because its denominator is the sources **this fill reads** — not all history — the same legitimate absence lands anywhere between 1% and 100% depending on how big the gap set happens to be.

## Evidence (both from ctbk, six weeks apart)

| date | fill | sources read | absent | ratio | `max_missing_source` | outcome |
|---|---|---|---|---|---|---|
| 2026-08-20 | monthly cadence, `rides-v5-{start,end}`, uncapped `-f` | 1 | `normalized/202608.parquet` | **1.00** | 0.01 | rc=4 |
| 2026-08-28 | `1mo`-rung backfill, same anchors, uncapped `-f` | 8 | `normalized/202608.parquet` | **0.125** | 0.01 | rc=4 |

Same absent object, same legitimate cause — Citi Bike publishes a month's tripdata mid-*following* month, so an uncapped fill whose expected cover reaches `now` always wants a source that cannot exist yet. ctbk's `rides-v5-extend` had picked `0.01` assuming a denominator of ~160 (all history); under `-f` the denominator is the gap set, so the tolerance was two orders of magnitude off and would have needed to be `>0.125` in one case and `1.0` in the other. `1.0` disables the guard entirely.

## Why this belongs in pyrmts, not the caller

The caller can only pass a scalar. **The engine is the only place that knows which periods are absent and where they sit relative to wall-clock `now`** — that comparison happens inside source discovery, after the LIST, and is never surfaced. A caller wanting "tolerate only the open period" would have to re-derive the engine's own tile selection to compute the right fraction, and re-derive it *per fill*, since the denominator moves with the gap set. That's the engine's private knowledge, so the classification has to live there.

The corollary matters as much: the guard currently fires **after** the outputs are written and registered. A tripped guard doesn't prevent a bad build — it reports one, and the exit code then aborts whatever the caller had queued next. In ctbk that meant a fill that had correctly built and registered all 14 shards still skipped its relic sweep and its RG-manifest backfill, and (because the CI step ordering puts the fill before the deploy) **silently skipped the production frontend deploy for 12 days**. A guard whose false positives are this expensive should not be a heuristic.

## Contract

Classify each absent source by its period, then apply the ratio to the remainder:

1. **Open / future periods.** An absent source whose period `[start, end)` satisfies `end > now` is **expected-absent**: excluded from both numerator and denominator of `max_missing_source`, and reported separately (`expected-absent (open period): normalized/202608.parquet [2026-08-01, 2026-09-01)`). This is the whole fix for the ctbk case.
2. **Everything else** — an absent source whose period closed in the past — counts toward the ratio exactly as today. A GC'd rung, a filter typo, or a wrong-rung read still trips the guard at the same threshold, which is the behavior worth keeping.
3. **`now`** is the engine's existing range clock (the same one an uncapped `-f` uses to compute expected cover), so no new parameter and no clock skew between "what we expected to cover" and "what we forgive".
4. **Escape hatch**: a flag (`--strict-open-periods`, default off) restores today's behavior for callers that genuinely want an open period to fail — e.g. a rebuild that should only ever run over closed history.

Nothing about the ratio's semantics changes for closed periods, so existing callers keep their tuning.

## Why not the alternatives

- **Cap the fill range at the last closed period.** ctbk tried this; the code comment records the result: capping at month-end leaves coarse-rung holes, because a tier whose ladder reaches `16y` wants tiles that legitimately extend past the last closed month. The uncapped range is load-bearing.
- **Raise `max_missing_source` per call site.** No single value works across fills (0.125 vs 1.00 above), and any value high enough for the single-source case disables the guard.
- **Move the check to the caller.** Requires re-deriving engine-private tile selection; see above.

## Tests

- Uncapped `-f` over a range ending mid-open-period, source for that period absent → exit 0, `expected-absent` line naming the period bounds, ratio computed over the remaining sources.
- Same fill, additionally missing one **closed** source → exit 4, ratio counts 1 missing over the closed-source denominator (the open one still excluded from both).
- Single-source fill (the 2026-08-20 shape: gap set = one open month) → exit 0, denominator 0, no division-by-zero.
- `--strict-open-periods` on the first case → exit 4, restoring current behavior.
- Outputs written/registered identically in all cases — the guard's classification must not alter what gets built.

## Non-goals

- Changing *when* the guard fires relative to writing outputs. Reporting-after-writing is a separate question; this spec only stops the false positives. *(Superseded for fill mode on tiled sources by Addendum 2: the check runs before the walk and writes nothing when it trips.)*
- Teaching the engine which sources a publisher *will* eventually produce. "Period is open" is a wall-clock fact; "upstream is late on a closed month" is a real hole and should keep failing.

## ctbk-side interim (already landed, keep until this ships)

- `ctbk gbfs engine submit -t/--max-missing` exposes the knob that was previously reachable only from inside `rides-v5-extend` (`dd0d72d8`).
- The denominator is documented at both call sites, including the arithmetic error that produced `0.01` (`e6fd7a40`).
- `ctbk gbfs rides-v5-sweep` is a standalone command, so a tripped guard's skipped relic sweep can be run by hand (`dd0d72d8`).

When this lands, ctbk drops the tolerance from `rides-v5-extend` entirely rather than retuning it.

## Outcome

- **pyrmts**: `72f2552` (2026-08-28), on `r/main`. Python packages install from git; no dist artifact is involved (the JS dist built at the same SHA is `612f144`, unrelated to this change).
- **ctbk validation** (editable installs of the Python trio, before the push): ctbk suite 62 passed, pyrmts suite 226 passed, both flags surfacing (`build -o/--strict-open-periods` and the `batch submit --strict-open-periods` passthrough), and ctbk's real submit path renders `pyrmts-engine batch submit … -f -t 1m --max-missing 0.01` with no `--strict-open-periods` — i.e. the monthly cadence gets the non-strict default.
- **Deviation from the spec, agreed better**: the spec said classify against wall-clock `now`; the implementation uses the range's `to`. Wall-clock inside the engine would let two runs of the same uncapped fill classify differently across a period boundary, breaking the byte-determinism the engine maintains elsewhere — and the "capped rebuild over closed history" case then gets strict behavior for free (every absent period satisfies `end <= to`) instead of needing the escape hatch. `--strict-open-periods` stays for the genuinely-strict caller. `coverage()` also kept its keys-only shape, with `missing_tiles()` added alongside, so nothing downstream of `coverage()` changed.
- **ctbk-side remaining** (theirs): re-pin the Python trio past `6378cdd`, then drop `max_missing` from `rides-v5-extend` entirely rather than retuning it. That stops the Sept 20 monthly run failing on the absent open month — the failure that silently skipped the www deploy for 12 days — and unsticks `specs/gc-legacy-pyramids.md`, whose avail<6 GC gates on the first fully-auto month.

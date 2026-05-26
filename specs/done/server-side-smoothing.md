# Server-side smoothing (rolling-mean over the monoid)

> Status: **done v0.1** (2026-05-26). Planner extended with `smoothing` +
> `smoothMode`; stitcher gained a rolling-window pass that emits parallel
> `<metric>_smooth_<state>` columns; serve handlers parse `?smooth=` +
> `?smooth_mode=`; FE hook + `fetchPyramidQuery` plumb the params through.
> 17 new tests (planner: 8, stitch: 5, serve: 4); 132 → 149.
>
> ## Resolution
>
> Followed the spec exactly on the **API shape**, **edge-buffer**, **snap-nearest**,
> **`centered` default**, and **flat `_smooth_<state>` columns**. Deviations and
> decisions worth flagging:
>
> 1. **`QueryPlan` carries `visibleRange` and `smoothing`.** Stitcher trims to
>    `visibleRange` regardless of smoothing (no-op when no buffer was added).
>    The added `smoothSourceTier` field on `plan.smoothing` reports the tier
>    the rolling pass ran against — always equals `outputTier.name` for v0.1
>    but exists so the §4 tier-downshift optimization can land later without
>    changing the response shape.
> 2. **Watermark interaction**: `effectiveWatermarks` is computed against the
>    *extended* planning window (`plannedTo`), not the original `to`. This
>    lets the trailing buffer pull bins past the visible range. The
>    `earliestWatermarks` clamp at the leading edge degrades the buffer
>    gracefully (no error, just less context).
> 3. **Auto-mode default multiplier = 50** (matches awair's observed feel).
>    Snap is **nearest**, clamped to `[1, floor(visibleBins/4)]` — the upper
>    clamp prevents smoothing from dominating the visible range. The clamp
>    can bite for short query ranges: e.g. a 5-bin visible window can't
>    smooth at all (cap = 1 = no-op). Tunable later via a planner option if
>    real consumers hit this.
> 4. **`smoothBinCount == 1` mirrors raw cols into `_smooth_<state>`** rather
>    than omitting them — consumers can always read `_smooth_*` without
>    conditional plumbing on whether smoothing was meaningful.
> 5. **Rolling pass is O(rows × N)** per metric (full recompute per window).
>    The spec's "prefix-sum" optimization isn't applied yet; deferred until
>    we see real perf issues in the wild. Even at the spec's worst-case
>    62M-combine scenario it's in the 500ms-2s range in V8.
> 6. **Tier downshift (§4) deferred** entirely; recorded as `smoothSourceTier`
>    on the plan for future implementation. Will need a `smoothTier` segment
>    list and an interpolation/alignment step in the stitcher.
> 7. **Histogram smoothing is "correct but probably not useful"** as the spec
>    warned — combine just merges histogram state across the window. We don't
>    block or warn; consumers can ignore it.
> 8. **Cross-pkg ports**: pyrmts-geo's `planGeoQuery` and `serveGeoQuery` both
>    plumb `smoothing` / `smoothMode` through (mirroring cfw). `pyrmts-geo`
>    re-exports `SmoothMode`/`SmoothingSpec` types via `pyrmts`.
>
> Awair migration: change `useMultiDeviceAggregation` to read
> `record.<metric>_smooth_<state>` directly and drop its `pltly` smoothing
> plumbing and `fetchAwairData`'s `lookbackMinutes` extension.

## Goal

Move rolling-window smoothing into pyrmts. Today, consumers (awair) fetch
raw monoid bins, then run client-side smoothing (rolling-mean over N bins)
in JS. Pyrmts is uniquely well-positioned to do this on the server side —
the monoid abstraction is the right primitive, and pyrmts has tier
information consumers don't (so it can optimize wide-window smoothing by
sourcing from a coarser tier instead of re-aggregating raw).

## Conceptual fit

A smoothed bin at index `i` over window `W` is the monoid combine of bins
`[i - N, i]` (trailing) or `[i - N/2, i + N/2]` (centered), where
`N = round(W / outputBin)`. For the `sum` monoid (`{n, sum, sumsq}`), this
produces both a smoothed *mean* (`sum/n`) and a smoothed *stddev*
(`√((sumsq - sum²/n) / n)`) directly, which strictly subsumes what JS-side
rolling-mean does today (mean only, or mean ± hand-computed band).

Other monoids: `count` rolling-sum works. `histogram` doesn't quite (you'd
combine state-by-state, which is correct but the output isn't really "a
smoothed histogram" — punt). `topk`/`hll`/`tdigest` are non-trivial; out
of scope for v0.1.

## API

### Query params

```
GET /q?from=…&to=…&device_id=…&bin_budget=…&smooth=4h
```

- `smooth=<Duration>` — explicit width (e.g. `1h`, `30min`, `1d`, `1w`).
  Snapped to the nearest `N × outputBin` where N is an integer ≥ 1.
- `smooth=auto` — server picks based on the resolved `bin_budget` →
  `outputBin`. Default multiplier `50` (see `multiplier` below).
- `smooth=auto<N>` — auto with an explicit multiplier, e.g. `auto25`,
  `auto100`.
- `smooth_mode=trailing|centered` — default `centered`. (Awair will
  probably want `centered` for chart display; trailing is useful for
  online/streaming-style usage.)
- Omitted → no smoothing (current behavior).

### Response shape

Each row gets parallel `_smooth_<suffix>` columns per state column of each
metric:

```json
{
  "records": [
    {
      "ts": 1779706800000,
      "device_id": 17617,
      "temp_n": 5,         "temp_sum": 325.6,    "temp_sumsq": 21210.8,
      "temp_smooth_n": 60, "temp_smooth_sum": 3902.4, "temp_smooth_sumsq": 254211.3,
      …
    }
  ],
  "plan": {
    "outputTier": "m5",
    "outputBin": "5min",
    "smoothBin": "1h",        // resolved value (after snapping)
    "smoothMode": "centered",
    "authoritativeEnd": "…"
  }
}
```

Consumers compute smoothed mean as `temp_smooth_sum / temp_smooth_n` and
optionally stddev from `temp_smooth_sumsq`.

Auto-mode reports `smoothBin` back so the consumer UI can label
the trace honestly: "Auto (50× = 1h)".

## Implementation

### 1. Plan-level extension

`PlanQueryInput` grows two fields:
```ts
smoothing?: Duration | { auto: true; multiplier?: number }
smoothMode?: 'trailing' | 'centered'  // default 'centered'
```

`planQuery` resolves smoothing → an integer `smoothBinCount` (number of
output-tier bins per smoothed value) and tracks the snapped width on
`QueryPlan.smoothBin`. For `centered`, this also widens the segments by
`smoothBinCount / 2` bins on each side (so smoothing has full context at
the visible edges).

### 2. Edge buffer

Currently `planQuery` produces segments covering `[from, to)`. With
smoothing, it extends to `[from - smoothBinCount/2 × outputBin, to +
smoothBinCount/2 × outputBin]` (centered) or `[from - smoothBinCount ×
outputBin, to]` (trailing).

The stitch step then trims output rows back to the visible range — the
edge bins were only fetched for smoothing context, not for display.

### 3. Stitcher applies the rolling-window combine

After `stitch` produces the merged row sequence at output bin granularity,
a new pass (`smoothInPlace` or similar) walks the sequence and for each
visible row, combines the N surrounding rows using the metric's monoid.
Writes the result into `<metric>_smooth_<suffix>` columns.

For sum monoid, this is just three rolling-window sums over `_n`/`_sum`/
`_sumsq`. O(rows) per metric using a window-sum optimization (subtract
leaving bin, add entering bin).

### 4. The sneaky optimization: tier downshift for wide windows

Smoothing window much wider than output bin → consider sourcing the
smoothed series from a *coarser tier* than the output.

Example: query at `bin_budget=2000` over 1 year. Output tier = `d1`
(daily bins, ~365 rows). User asks `smooth=30d`. Naive: 30 d1-bins per
smoothed value (cheap, ~365 windows × 30 combines).

Now consider: 1d view at `bin_budget=1500`, `bin=1min`,
`smooth=1mo` (= ~43k bins per smoothed value). That's 1440 windows × 43k
combines = 62M combines. Vs. using `d1` tier internally for the smoothing
series only: ~30 combines per visible bin. Same math result; 4 orders of
magnitude cheaper.

Heuristic: when `smoothBinCount` exceeds some threshold (e.g. 100), look
for the coarsest tier where `smoothWidth / tier.bin ≤ threshold` and use
*that* tier for the smoothing series. Output's raw column comes from the
chosen output tier as usual; output's `_smooth_` columns come from the
downshift tier.

When raw and smoothing tiers differ, the smoothing tier's bins need to be
re-aligned to the output bin grid (i.e. linearly interpolated or
nearest-neighbor'd from the coarser tier to the finer output grid). For
visualization this is fine; the smoothed line is meant to be visually
"smooth" anyway.

For v0.1 this optimization can be skipped; just always smooth at output
tier. Even at 43k combines × 1440 windows that's well under 1s in JS, so
it's a perf concern only at extreme zoom-out + tight binning.

### 5. Snapping smoothing width

Given a requested width `W` (or `multiplier * outputBin` for auto), snap
to the nearest representable width:

```ts
const NICE_MS = [
  1 * MIN, 5 * MIN, 15 * MIN, 30 * MIN,
  1 * HOUR, 2 * HOUR, 3 * HOUR, 6 * HOUR, 12 * HOUR,
  1 * DAY, 2 * DAY, 7 * DAY, 14 * DAY, 30 * DAY,
]
function snap(w: number, outputBinMs: number): number {
  // Closest "nice" width that's also an integer multiple of outputBinMs.
  // Clamp so result is at least 1 outputBin and at most query-range/4 to
  // avoid pathological cases (smoothing dominating the visible range).
}
```

## Awair migration

After this lands, awair drops:

- `useMultiDeviceAggregation`'s `smoothingWindowSize` plumbing through to
  pltly
- The two smoothed-data traces it builds from pltly's smoothed output
- `fetchAwairData`'s `lookbackMinutes` extension (server handles edge
  buffer)
- `detectInputBinMs` heuristic in `useMultiDeviceAggregation` (server
  reports `outputBin` directly in plan)

Replaces them with: PyrmtsSource adds `?smooth=…` to its query, returns
the smoothed columns as `record.<metric>_smooth_mean` / `_stddev`, chart
draws one trace from `<metric>` and overlays one from `<metric>_smooth`.

## Open questions

- **Auto-width default multiplier**: I proposed 50 (matches awair's
  observed natural behavior). Worth a 2nd opinion from the pyrmts session.
- **Snap up or nearest?** I lean nearest. If user types `?smooth=4h` and
  output bin is 5min, nearest of `[45min, 50min, 55min, 1h, …]` would be
  4h exact (= 48 × 5min); fine. If they ask `smooth=37min` at the same
  bin, nearest snap → 35min (= 7 × 5min) or 40min (= 8 × 5min). Either
  works.
- **`smooth_mode` default**: I picked `centered` since that's the chart
  default everywhere. Trailing has uses (live dashboards that don't want
  to "see the future") — probably opt-in.
- **Stddev semantics for non-sum monoids**: punt. `sum` is the only
  monoid that's fully meaningful here.
- **Should pyrmts also serve quantile bands** (p10/p90 etc.) using
  tdigest? Separate spec — depends on whether tdigest monoid gets
  implemented.
- **`/q` response getting big** with extra columns. Worth thinking about
  a `?columns=` projection (only request the metrics you care about) as a
  separate optimization.

## Cross-references

- `~/c/awair/www/src/hooks/useMultiDeviceAggregation.ts` — current
  client-side smoothing (via pltly's `useMultiSeriesAggregation`)
- `~/c/awair/www/src/services/awairService.ts:fetchAwairData` —
  `lookbackMinutes` edge-buffer extension that this spec subsumes
- `~/c/pyrmts/specs/multiscale-timeseries-v2.md` — base monoid model

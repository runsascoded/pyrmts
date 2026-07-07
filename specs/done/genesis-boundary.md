# Genesis-boundary: prune pre-genesis + expose effective period for partials

## Resolution

Implemented in JS `js/packages/pyrmts/src/gap-discovery.ts` and Python
`python/pyrmts/src/pyrmts/gap_discovery.py`. Landed on top of the
minimal-cover restructure in [`9d16761` "gap-discovery: emit minimal
cover (max-shards + trailing rungs)"] — which is exactly the
`coverForTier` structure the spec was written against, so both fixes
apply as described.

**Fix A ("prune fully-pre-genesis shards") is load-bearing on the
trailing greedy walk.** When `firstMax === lastMax` (short range within
a single max-shard tile), the trailing walk starts at
`cur = lastMax = floor(to, maxSpan)`, which may lie years before
`from`. On the ctbk avail-v3 `/3d` tier (`shards: [3d, 30d, 90d, 1440d,
4320d]`) with `from = 2026-04-08`, `to = 2026-06-30`:

- `firstMax = lastMax = floor(2026-04-08, 4320d) = 2017-04-24`
- Max walk: no tiles emitted.
- Trailing walk from `2017-04-24`: picks `1440d` (2017-04-24 is
  1440d-aligned) → `[2017-04-24, 2021-04-03)` — fully pre-genesis.
  Then `[2021-04-03, 2025-03-13)` — fully pre-genesis. Fix A skips
  both. Continues walking with smaller rungs until it reaches a tile
  straddling `from`, then normally past `from`.

The guard `if (rungEnd > from) out.push(...)` in the trailing walk
implements Fix A. In the max-shard walk it's not needed (`next > from`
is guaranteed by definition of `floorToSpan`), so the guard lives only
on the trailing branch.

**Fix B ("expose `effectiveStart` / `effectiveEnd`") is the coverage-
math primitive.** Both JS `ExpectedShard` and Python `ExpectedShard`
now carry `effective{Start,End}` (Python: `effective_{start,end}`)
clipped to `[range.from, range.to)`. Materializers count sources whose
period intersects `[effectiveStart, effectiveEnd)`, not the raw shard
period. `makeExpected` takes `from` / `to` args and does the clip; the
minimal-cover walk only ever produces one straddling tile (the first
max-shard tile when `from` isn't max-shard-aligned; or the first
non-pruned trailing tile when `firstMax === lastMax`). No tile can
straddle `to` (max walk stops at `lastMax ≤ to`; trailing walk uses
`rEnd ≤ to`), so `effectiveEnd === periodEnd` invariantly for every
emitted tile — the field is still populated for uniformity but never
does clipping.

The diff key in `listMissingShards` is unchanged — `(tier, shardDur,
periodStart)` still identifies a unique expected row; `effectiveStart`
doesn't participate in identity, only in the materializer's coverage
math.

**No `Pyramid.genesis` config field** (as noted in Non-goals) — the
spec is right that `range.from` already carries the information.

Tests added (JS + Python parity):
- `effective_equals_period_when_shards_fully_inside_range` — sanity.
- `max_shard_tile_straddling_from_clips_effective_start_to_from` —
  1d/1mo ladder, `[Jun 15, Jul 1)` → one 1mo tile straddling `from`.
- `trailing_walk_prunes_fully_pre_genesis_tiles` — ctbk `/3d` shape,
  asserts no emitted tile has `periodEnd ≤ from` and that the first
  emitted tile straddles `from` with `effectiveStart === from`.
- `no_emitted_tile_has_period_end_past_to` — contract that the minimal-
  cover walk never overshoots `to`, so `effectiveEnd === periodEnd`
  always.

`listMissingShards` and its inventory-off throw path are unchanged from
the gap-discovery spec.

[`9d16761` "gap-discovery: emit minimal cover (max-shards + trailing rungs)"]:
https://github.com/runsascoded/pyrmts/commit/9d16761

## Consumer rollout (unchanged from spec)

Same as spec §"Consumer rollout": ctbk avail-v3 bumps its `pyrmts` pin,
`writeShard` reads `effectiveStart` / `effectiveEnd` instead of raw
`periodStart` / `periodEnd`, and can delete its local `AVAIL_GENESIS`
constant (still passed to `range.from`, of course).

---


Gap-discovery today deliberately emits shards whose period overhangs
`range.from` (a.k.a. "genesis") — the docstring notes:

> *"The first tile may start before `from` — the shard's notional
> period contains pre-genesis time the materializer just leaves empty."*

That contract works for materializers that *can* produce empty pre-
genesis bins (e.g. Python `pyramid_cascade` on a batch host). But
production consumers now include CFW-hosted `writeShard` loops with a
strict "every declared source key must exist on storage" invariant —
they treat any missing source as `no_inputs` and bounce off the shard
every tick. Two categories of nuisance emerge:

1. **Fully-pre-genesis shards.** A shard whose `periodEnd ≤ range.from`
   can never contain data. Today `coverForTier` emits it anyway
   because it's the largest-fitting rung at that cursor position.
   Example: ctbk avail-v3 `/3d` tier (max shard `4320d` ≈ 11.8 y).
   `floorToSpan(2026-04-07T01:15Z, 4320d) = 2017-04-24`; the trailing
   greedy walk emits `/3d/1440d/2017-04-24` (period 2017-04-24 →
   2021-04-03, entirely pre-genesis) and `/3d/1440d/2021-04-03`
   (2021-04-03 → 2025-03-13, entirely pre-genesis) before it reaches
   any tile that intersects `[from, to]`.

2. **Genesis-straddling shards.** A shard whose period contains
   `range.from` strictly inside it (`periodStart < from < periodEnd`).
   The shard is valid — it has real data over `[from, periodEnd)` —
   but a strict materializer sees `inputsExpected = shardDurMin /
   sourceRungMin` sources and only some of them exist post-`from`.
   Example: `/1m@1d/2026-04-07` (period T00 → T24) with genesis
   T01:15Z; sources `/1m@3h/T00-T01:15` don't exist so all 8 sources
   report as HEAD-miss → `no_inputs`.

Consumers can't work around this cleanly because gap-discovery
doesn't tell them *which* sub-range of a shard is expected to have
data. Today they either write a strict materializer (perpetual bounce)
or a lax one (records shards with holes as authoritative — silent
data corruption).

## Fixes

Two orthogonal changes, both in `gap-discovery.ts`:

### A. Prune fully-pre-genesis shards

In `coverForTier`, skip any shard whose `periodEnd ≤ from`. Applies to
both the max-shard-tile loop and the trailing greedy walk. Zero
information loss — such shards' periods contain no in-range time.

```ts
function coverForTier(pyramid, tier, from, to, filter, out) {
  const maxShard = tier.shards[tier.shards.length - 1]!
  const maxSpan = parseDuration(maxShard)
  const lastMax = floorToSpan(to, maxSpan)
  const firstMax = floorToSpan(from, maxSpan)

  let cur = firstMax
  while (cur < lastMax) {
    const next = addSpan(cur, maxSpan)
    if (next > from) {                                    // ← new
      out.push(makeExpected(pyramid, tier, maxShard, cur, next, filter))
    }
    cur = next
  }

  cur = lastMax
  const nonMax = tier.shards.slice(0, -1)
  while (cur < to) {
    const chosen = largestFittingRung(nonMax, cur, to)
    if (chosen === null) break
    const [rung, rungEnd] = chosen
    if (rungEnd > from) {                                 // ← new
      out.push(makeExpected(pyramid, tier, rung, cur, rungEnd, filter))
    }
    cur = rungEnd
  }
}
```

Tests to add:
- Range `[T=2026-04-07, T=2026-07-07]` on a tier with max shard
  `4320d` — no shard with `periodEnd ≤ from` in output.
- Boundary: shard with `periodEnd === from` is skipped (half-open
  interval; empty intersection).
- Range where genesis lies inside the first max-shard tile — the
  tile is still emitted (its `periodEnd > from`).

### B. Expose `effectiveStart` / `effectiveEnd` on `ExpectedShard`

For shards that straddle `from` or `to`, tell the materializer which
sub-range is expected to be materially covered. Materializers use it
to compute `inputsExpected` correctly (count only sources whose
period intersects the effective range) and can record partial shards
as authoritative-for-what-exists without silently recording holes.

```ts
export interface ExpectedShard {
  tier: string
  shardDur: Shard
  periodStart: Date        // shard-aligned, unchanged
  periodEnd: Date          // exclusive, shard-aligned, unchanged
  // Intersection of `[periodStart, periodEnd)` with `[range.from,
  // range.to)`. Equal to the period for shards fully inside the range;
  // clipped for shards that straddle either boundary. Materializers
  // that require full source coverage should count sources whose
  // period intersects `[effectiveStart, effectiveEnd)`, not the raw
  // shard period.
  effectiveStart: Date
  effectiveEnd: Date
  key: string
}
```

`makeExpected` sets:

```ts
effectiveStart: new Date(Math.max(periodStart.getTime(), from.getTime())),
effectiveEnd:   new Date(Math.min(periodEnd.getTime(),   to.getTime())),
```

Tests:
- Shard fully inside range → `effective === period`.
- Shard straddling `from` → `effectiveStart === from`,
  `effectiveEnd === periodEnd`.
- Shard straddling `to` (edge-of-now case) → `effectiveEnd === to`.
- Shard fully outside range → not emitted (per A).

Consumer note: `listMissingShards`'s diff key is unchanged —
`(tier, shardDur, periodStart)` still identifies a unique expected
row. `effectiveStart` doesn't participate in identity, only in the
materializer's coverage math.

## Non-goals

- **No partial-shard flag on `RecordedShard`.** Whether a shard is
  "partial-at-head" is derivable from `(periodStart < genesis)` at
  query time; consumers that care can compute it. This spec is about
  the *materializer* correctly identifying required sources, not
  about the *reader* distinguishing partial vs full shards.
- **No step-down at genesis boundary.** The materializer accepts the
  straddling shard as-is and fills what exists; no sub-rung tiling
  of the pre-genesis sub-range. This matches the pyramid's natural
  structure (biggest shard walking back from `to`) and avoids a
  parallel rung-descent algorithm.
- **No config-level genesis field.** Consumers already pass
  `range.from` — gap-discovery has all it needs. Adding a
  `pyramid.genesis` field would duplicate that.

## Python parity

`python/pyrmts` has an equivalent `list_expected_shards` (see the
gap-discovery spec's Python API section). Apply the same two fixes
there for parity — but the primary consumer is ctbk avail-v3 CFW
(TypeScript), so the TS side is the p0.

## Consumer rollout (ctbk gbfs/cascade)

Once shipped in pyrmts + dist branch:

1. Bump `pyrmts` pin in `gbfs/cascade/package.json`.
2. `writeShard` (`gbfs/cascade/src/avail3/cascade.ts`) uses
   `expected.effectiveStart` / `effectiveEnd` when computing
   `inputsExpected`:
   - `/1m` smallest-rung path: iterate raw minutes in
     `[effectiveStart, effectiveEnd)`, not `[periodStart,
     periodEnd)`.
   - Coarser same-tier / heterogeneous cover path: filter source
     candidate keys to those whose period intersects
     `[effectiveStart, effectiveEnd)`.
3. Delete the ctbk-side genesis constant `AVAIL_GENESIS` — consumers
   already pass `range.from` to `converge()`, which threads through
   to gap-discovery. (Well, keep it as the value passed to `range.from`.)
4. Verify `/avail3?dryRun=1` returns only shards that intersect
   `[genesis, now]` and that `/1m@1d/2026-04-07` writes as partial
   with `inputsPresent === inputsExpected` for the intersection.

## Related

- [[gap-discovery]] — introduces `listExpectedShards` / `listMissingShards`.
- [[tolerate-missing-shards]] — dual on the *read* side (query
  tolerates 404s for pre-earliest-watermark shards). Genesis-boundary
  handles the same "some periods have no data" reality on the *write*
  side.
- [[per-cadence-earliest]] — per-`(tier, cadence)` earliest
  watermarks. Different concern (post-genesis coverage gaps per rung),
  but the `effectiveStart` field could later interact with per-cadence
  earliest-watermarks if we want gap-discovery to also skip shards
  entirely before a rung's earliest watermark.

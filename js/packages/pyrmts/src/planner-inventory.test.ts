// Tests for `planQueryFromInventory` — the min-cover-aware planner that
// walks registered inventory rather than synthesizing keys from the
// ladder × per-`(tier, shardDur)` watermark grid. See
// `specs/done/inventory-driven-read-walk.md`.

import { describe, expect, test } from 'vitest'
import { planQuery, planQueryFromInventory } from './planner.js'
import type { RecordedShard } from './shard-index.js'
import type { Pyramid, Storage } from './types.js'

const mockStorage: Storage = {
  head: async () => null,
  getRange: async () => new Uint8Array(),
  get: async () => null,
  put: async () => {},
  list: async function* () {},
}

const d = (iso: string): Date => new Date(iso)

interface SegmentSnapshot {
  from: string
  to: string
  tier: string
  shardDur: string
  keys: string[]
  reaggregate: boolean
}

function segments(plan: ReturnType<typeof planQueryFromInventory>): SegmentSnapshot[] {
  return plan.segments.map(s => ({
    from: s.from.toISOString(),
    to: s.to.toISOString(),
    tier: s.shardTier.name,
    shardDur: s.shardDur,
    keys: s.keys,
    reaggregate: s.reaggregate,
  }))
}

// Ladder-shaped pyramid mirroring ctbk avail-v3's 15m tier (the incident
// tier). Two coarser tiers below for cross-tier fall-through tests.
const availLike: Pyramid = {
  storage: mockStorage,
  keyTemplate: 'avail-v3/{tier}/{shard}/{period}.parquet',
  axis: 'time',
  binCol: 'ts',
  dims: [],
  metrics: [{ name: 'n', monoid: 'sum' }],
  tiers: [
    { name: '15m', bin: '15min', shards: ['1h', '3h', '1d'] },
    { name: '1h',  bin: '1h',    shards: ['1d'] },
    { name: '1d',  bin: '1d',    shards: ['7d'] },
  ],
}

// Helper to build a RecordedShard tersely.
function shard(tier: string, shardDur: string, periodStart: string, periodEnd: string): RecordedShard {
  const start = d(periodStart)
  const end = d(periodEnd)
  const per = start.toISOString().slice(0, 13)  // rough key label; not asserted on
  return {
    tier,
    shardDur: shardDur as never,
    periodStart: start,
    periodEnd: end,
    key: `avail-v3/${tier}/${shardDur}/${per}.parquet`,
  }
}

describe('planQueryFromInventory: min-cover phantom-key regression', () => {
  // ctbk incident 2026-07-10: /15m@1h@2026-07-09T23:00 was never
  // materialized (superseded by /15m@3h@2026-07-09T21:00 at 00:00), but
  // the 15m@1h watermark advanced past midnight so `planQuery` picked
  // the phantom key. Confirm both:
  //   1. `planQuery` (old, watermark-only) synthesizes the phantom key.
  //   2. `planQueryFromInventory` picks the coarser tile that actually
  //      exists (or falls through cleanly when nothing does).

  test('planQuery synthesizes the phantom `1h@23:00` key (bug demo)', () => {
    // Cursor lands at T23:00 in the 15m tier. Watermarks say 1h is sealed
    // past midnight but inventory only has 3h@21:00. Old planner picks
    // 1h first? — no, largest first, so 3h@21:00 would win. Bug repros
    // when the tier has no coarser rung than 1h at 15m tier's ladder OR
    // when the coarser rung's within-tier watermark clips it out.
    //
    // Here we contrive scenario (b): declare `15m@3h` watermark BELOW
    // cursor via within-tier propagation (a smaller-shardDur watermark
    // that clips it). `15m@1h` declared at 22:00 clips `15m@3h` to
    // 22:00, so cursor 23:00 skips 3h → tries 1h → floor(23, 1h) = 23,
    // effective (from `15m@1h`?) — hmm within-tier propagation says
    // smaller bounds larger, so `15m@1h`'s effective isn't clipped from
    // 22:00 back down. Actually `15m@1h`'s declared IS 22:00, so
    // effective = 22:00, cursor 23:00 > 22, skips 1h too.
    //
    // Simpler repro: declare only `15m@1h` = 24:00 and give the 15m
    // tier a SINGLE-rung ladder so 1h is the largest. Then cursor at
    // 23:00 tries 1h first, picks phantom.
    const singleRungAvail: Pyramid = {
      ...availLike,
      tiers: [
        { name: '15m', bin: '15min', shards: ['1h'] },
      ],
    }
    const plan = planQuery(singleRungAvail, {
      range: { from: d('2026-07-09T23:00:00Z'), to: d('2026-07-10T00:00:00Z') },
      binBudget: 4,  // 4 * 15min = 60min — outputs 15m tier
      watermarks: { '15m@1h': d('2026-07-10T00:00:00Z') },
    })
    // Old planner synthesizes the phantom.
    expect(plan.segments.map(s => s.keys)).toEqual([
      ['avail-v3/15m/1h/2026-07-09T23.parquet'],
    ])
  })

  test('planQueryFromInventory: empty inventory yields no segments (no phantom)', () => {
    const singleRungAvail: Pyramid = {
      ...availLike,
      tiers: [
        { name: '15m', bin: '15min', shards: ['1h'] },
      ],
    }
    const plan = planQueryFromInventory(
      singleRungAvail,
      {
        range: { from: d('2026-07-09T23:00:00Z'), to: d('2026-07-10T00:00:00Z') },
        binBudget: 4,
        watermarks: { '15m@1h': d('2026-07-10T00:00:00Z') },
      },
      [],  // no registered tiles
    )
    expect(segments(plan)).toEqual([])
  })

  test('planQueryFromInventory: picks the registered coarser tile, ignoring absent finer rung', () => {
    // Inventory has 15m/3h/21:00 (period 21-24) but NOT 15m/1h/23:00 —
    // the min-cover writer consolidated the trailing 1h dust into 3h.
    // Watermarks lie: 15m@1h advanced past midnight even though the tile
    // is missing. New planner must ignore the watermark and pick the 3h
    // tile from inventory.
    const plan = planQueryFromInventory(
      availLike,
      {
        range: { from: d('2026-07-09T23:00:00Z'), to: d('2026-07-10T00:00:00Z') },
        binBudget: 4,
        watermarks: {
          '15m@1h': d('2026-07-10T00:00:00Z'),   // phantom-inducing
          '15m@3h': d('2026-07-10T00:00:00Z'),
        },
      },
      [
        shard('15m', '3h', '2026-07-09T21:00:00Z', '2026-07-10T00:00:00Z'),
      ],
    )
    expect(segments(plan)).toEqual([
      {
        from: '2026-07-09T23:00:00.000Z',
        to: '2026-07-10T00:00:00.000Z',
        tier: '15m',
        shardDur: '3h',
        keys: ['avail-v3/15m/3h/2026-07-09T21.parquet'],
        reaggregate: false,
      },
    ])
  })

  test('planQueryFromInventory: prefers largest shardDur when multiple registered rows overlap (stale-not-yet-GC\'d)', () => {
    // Post-consolidation state: 15m/3h/21:00 is the current cover;
    // 15m/1h/22:00 is a stale row that hasn't been GC\'d yet. Both
    // registered, both cover cursor 22:30. Tiebreak (largest shardDur)
    // picks 3h.
    const plan = planQueryFromInventory(
      availLike,
      {
        range: { from: d('2026-07-09T22:30:00Z'), to: d('2026-07-09T23:00:00Z') },
        binBudget: 4,
      },
      [
        shard('15m', '1h', '2026-07-09T22:00:00Z', '2026-07-09T23:00:00Z'),
        shard('15m', '3h', '2026-07-09T21:00:00Z', '2026-07-10T00:00:00Z'),
      ],
    )
    expect(segments(plan)).toEqual([
      {
        from: '2026-07-09T22:30:00.000Z',
        to: '2026-07-09T23:00:00.000Z',
        tier: '15m',
        shardDur: '3h',
        keys: ['avail-v3/15m/3h/2026-07-09T21.parquet'],
        reaggregate: false,
      },
    ])
  })

  test('planQueryFromInventory: falls through to next-finer tier when output tier has no covering row', () => {
    // 15m tier has NO registered tile for the query range, but the
    // finer 1h tier does. (Tiers list is finest-first, so "finer" =
    // index 0. Here output tier is 15m at index 0; "finer" tier doesn't
    // exist. We invert by picking a coarser output tier via a smaller
    // budget so pickTier chooses 1h, then walk falls back to raw 15m.)
    // Simpler: give 1h tier a registered tile and let the walk pick it.
    //
    // Actually the walk goes outputIdx → 0 (finest). So with output=1h
    // (idx=1) and 15m (idx=0) as fallback, if 1h has no covering row
    // but 15m does, the walk picks 15m and reaggregates.
    const plan = planQueryFromInventory(
      availLike,
      {
        range: { from: d('2026-07-09T22:00:00Z'), to: d('2026-07-09T23:00:00Z') },
        binBudget: 1,  // 1h range at 1h bin = 1 bin → 1h tier is output
      },
      [
        shard('15m', '3h', '2026-07-09T21:00:00Z', '2026-07-10T00:00:00Z'),
        // No 1h tier tile.
      ],
    )
    expect(plan.outputTier?.name).toBe('1h')
    expect(segments(plan)).toEqual([
      {
        from: '2026-07-09T22:00:00.000Z',
        to: '2026-07-09T23:00:00.000Z',
        tier: '15m',
        shardDur: '3h',
        keys: ['avail-v3/15m/3h/2026-07-09T21.parquet'],
        reaggregate: true,
      },
    ])
  })

  test('planQueryFromInventory: earliestPerShard gate skips too-early registered rows', () => {
    // A registered tile whose periodStart precedes its
    // `earliestPerShard` marker is skipped — matches `planQuery`'s
    // partial-shard-ladder semantics.
    const plan = planQueryFromInventory(
      availLike,
      {
        range: { from: d('2026-07-09T22:00:00Z'), to: d('2026-07-09T23:00:00Z') },
        binBudget: 4,
        earliestPerShard: {
          '15m@3h': d('2026-07-10T00:00:00Z'),  // 3h rung only trusted from tomorrow
        },
      },
      [
        shard('15m', '3h', '2026-07-09T21:00:00Z', '2026-07-10T00:00:00Z'),
      ],
    )
    // No coverage: 3h tile gated out; no other tile registered.
    expect(segments(plan)).toEqual([])
  })

  test('planQueryFromInventory: authoritativeEnd is watermark-derived, unchanged from planQuery', () => {
    const plan = planQueryFromInventory(
      availLike,
      {
        range: { from: d('2026-07-09T22:00:00Z'), to: d('2026-07-10T02:00:00Z') },
        binBudget: 16,
        watermarks: {
          '15m@1h': d('2026-07-10T00:00:00Z'),
        },
      },
      [
        shard('15m', '3h', '2026-07-09T21:00:00Z', '2026-07-10T00:00:00Z'),
      ],
    )
    expect(plan.authoritativeEnd?.toISOString()).toBe('2026-07-10T00:00:00.000Z')
  })

  test('planQueryFromInventory: ragged path also plans from inventory', () => {
    // targetBin = 30min. Ladder eligible tiers: 15m (15min), 1h (1h),
    // 1d (1d) — those with fixed-width bins ≤ 30min are just 15m.
    // Decomposition: 30min = 2 * 15min atoms. Both atoms must be
    // covered by a registered 15m tier tile. Inventory has one 3h tile
    // (covering the whole range) — the DP picks two 15min atoms and
    // both find the 3h tile as covering.
    const plan = planQueryFromInventory(
      availLike,
      {
        range: { from: d('2026-07-09T22:00:00Z'), to: d('2026-07-09T22:30:00Z') },
        binBudget: 1,
        targetBin: '30min',
      },
      [
        shard('15m', '3h', '2026-07-09T21:00:00Z', '2026-07-10T00:00:00Z'),
      ],
    )
    // One segment for the coalesced 30min at 15m tier, keyed to the 3h tile.
    expect(segments(plan)).toEqual([
      {
        from: '2026-07-09T22:00:00.000Z',
        to: '2026-07-09T22:30:00.000Z',
        tier: '15m',
        shardDur: '3h',
        keys: ['avail-v3/15m/3h/2026-07-09T21.parquet'],
        reaggregate: true,  // 15m bin != targetBin 30min
      },
    ])
  })

  test('planQueryFromInventory: ragged skips output bins with no registered coverage', () => {
    // No inventory anywhere; ragged path emits zero segments (rather
    // than throwing like `planRagged` would when eligibleTiers is
    // empty).
    const plan = planQueryFromInventory(
      availLike,
      {
        range: { from: d('2026-07-09T22:00:00Z'), to: d('2026-07-09T22:30:00Z') },
        binBudget: 1,
        targetBin: '30min',
      },
      [],
    )
    expect(segments(plan)).toEqual([])
  })
})

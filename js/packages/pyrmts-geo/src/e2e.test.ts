// End-to-end geo pipeline: parquet shard with multiple resolutions →
// memStorage → planGeoQuery → fetchSegmentRows → filterCellsAndRes →
// stitch. Validates the full read path for a ctbk-trips-style pyramid.

import { latLngToCell } from 'h3-js'
import { parquetWriteBuffer } from 'hyparquet-writer'
import {
  fetchSegmentRows,
  memStorage,
  stitch,
  type Pyramid,
} from 'pyrmts'
import { describe, expect, test } from 'vitest'
import { filterCellsAndRes, planGeoQuery } from './planner.js'

const ms = (iso: string): number => new Date(iso).getTime()

// Three "stations" spread across lower Manhattan. At res 9 each gets a
// distinct cell; at res 7 they cluster into 1-2 cells; at res 5 they all
// share one cell.
const STATIONS = [
  { id: 's1', lat: 40.7128, lng: -74.0060 },   // financial district
  { id: 's2', lat: 40.7484, lng: -73.9857 },   // empire state
  { id: 's3', lat: 40.7794, lng: -73.9632 },   // upper east
]

// Build an h1 shard for 2026-01 covering 2 hours × 3 stations, materialized
// at res 9, 7, and 5. Each (cell, ts) row carries a `count` (trips).
function ctbkTripsParquet(): Uint8Array {
  const RESES = [9, 7, 5]
  const HOURS = [ms('2026-01-01T00:00:00Z'), ms('2026-01-01T01:00:00Z')]

  // Per-row data we'll write out as parallel column arrays.
  const tsCol: bigint[] = []
  const cellCol: string[] = []
  const countCol: number[] = []

  // For each hour, aggregate per (cell-at-res) → trips. We just give each
  // station a fixed per-hour count for simplicity.
  const stationTrips: Record<string, number> = { s1: 10, s2: 20, s3: 30 }

  for (const tsMs of HOURS) {
    for (const res of RESES) {
      const cellTotals: Record<string, number> = {}
      for (const station of STATIONS) {
        const cell = latLngToCell(station.lat, station.lng, res)
        cellTotals[cell] = (cellTotals[cell] ?? 0) + stationTrips[station.id]!
      }
      for (const [cell, count] of Object.entries(cellTotals)) {
        tsCol.push(BigInt(tsMs))
        cellCol.push(cell)
        countCol.push(count)
      }
    }
  }

  const buf = parquetWriteBuffer({
    columnData: [
      { name: 'ts', type: 'INT64', data: tsCol },
      { name: 'h3_cell', type: 'STRING', data: cellCol },
      { name: 'count', type: 'INT32', data: countCol },
    ],
  })
  return new Uint8Array(buf)
}

function buildPyramid(): Pyramid {
  const data = new Map<string, Uint8Array>()
  data.set('trips/h1/2026-01.parquet', ctbkTripsParquet())
  return {
    storage: memStorage(data),
    keyTemplate: 'trips/{tier}/{period}.parquet',
    axis: 'time',
    binCol: 'ts',
    dims: [],
    metrics: [{ name: 'count', monoid: 'count' }],
    tiers: [
      { name: 'h1', bin: '1h', shard: '1mo' },
      { name: 'd1', bin: '1d', shard: '1y' },
    ],
    geo: { cellCol: 'h3_cell', resolutions: [9, 7, 5] },
  }
}

// Lower-Manhattan-ish bbox spanning all three stations.
const BBOX = { minLat: 40.70, maxLat: 40.79, minLng: -74.02, maxLng: -73.95 }

describe('pyrmts-geo end-to-end', () => {
  test('plan → fetch → filter (res 9) → stitch yields per-station rows', async () => {
    const pyramid = buildPyramid()
    const plan = planGeoQuery(pyramid, {
      range: { from: new Date('2026-01-01T00:00:00Z'), to: new Date('2026-01-01T02:00:00Z') },
      binBudget: 100,
      bbox: BBOX,
      cellBudget: 1000,    // big enough to fit res-9 cells in this bbox
    })
    expect(plan.outputRes).toBe(9)
    expect(plan.outputTier.name).toBe('h1')

    const rows = await fetchSegmentRows(pyramid.storage, plan.segments[0]!.keys)
    const filtered = filterCellsAndRes(rows, 'h3_cell', plan.outputRes, plan.outputCells)

    // The shard contains all 3 stations' cells at res 9 — they should all
    // pass the filter (assuming they're in BBOX, which they are).
    const cellsAtRes9 = STATIONS.map(s => latLngToCell(s.lat, s.lng, 9))

    const stitched = stitch({
      pyramid: { ...pyramid, dims: [{ name: 'h3_cell', type: 'h3' }] },   // group by cell
      plan: {
        outputTier: plan.outputTier,
        outputBin: plan.outputBin,
        segments: plan.segments.map(s => ({
          from: s.from, to: s.to, shardTier: s.shardTier, keys: s.keys, reaggregate: s.reaggregate,
        })),
        authoritativeEnd: plan.authoritativeEnd,
      },
      shardRows: [filtered],
    })

    // 3 cells × 2 hours = 6 output rows, one per (cell, hour).
    expect(stitched).toHaveLength(6)
    const byHour = Object.groupBy(stitched, r => String(r.ts))
    expect(Object.keys(byHour)).toHaveLength(2)
    for (const key of Object.keys(byHour)) {
      const hourRows = byHour[key]!
      expect(hourRows).toHaveLength(3)
      const cells = new Set(hourRows.map(r => r.h3_cell))
      expect(cells).toEqual(new Set(cellsAtRes9))
      // Total trips per hour = 10 + 20 + 30 = 60
      const total = hourRows.reduce((s, r) => s + (r.count as number), 0)
      expect(total).toBe(60)
    }
  })

  test('coarser cellBudget picks res 7 and aggregates stations sharing a cell', async () => {
    const pyramid = buildPyramid()
    const plan = planGeoQuery(pyramid, {
      range: { from: new Date('2026-01-01T00:00:00Z'), to: new Date('2026-01-01T02:00:00Z') },
      binBudget: 100,
      bbox: BBOX,
      cellBudget: 3,    // res 9 has ~10 cells in bbox → won't fit; res 7 likely fits
    })
    expect(plan.outputRes).toBe(7)

    const rows = await fetchSegmentRows(pyramid.storage, plan.segments[0]!.keys)
    const filtered = filterCellsAndRes(rows, 'h3_cell', plan.outputRes, plan.outputCells)

    const stitched = stitch({
      pyramid: { ...pyramid, dims: [{ name: 'h3_cell', type: 'h3' }] },
      plan: {
        outputTier: plan.outputTier,
        outputBin: plan.outputBin,
        segments: plan.segments.map(s => ({
          from: s.from, to: s.to, shardTier: s.shardTier, keys: s.keys, reaggregate: s.reaggregate,
        })),
        authoritativeEnd: plan.authoritativeEnd,
      },
      shardRows: [filtered],
    })

    // At res 7, stations cluster into 1-3 cells. Whatever the exact count,
    // total trips per hour must still be 60 (10 + 20 + 30).
    const byHour = Object.groupBy(stitched, r => String(r.ts))
    for (const key of Object.keys(byHour)) {
      const total = byHour[key]!.reduce((s, r) => s + (r.count as number), 0)
      expect(total).toBe(60)
    }
  })
})

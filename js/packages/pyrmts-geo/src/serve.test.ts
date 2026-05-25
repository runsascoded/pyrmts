import { latLngToCell } from 'h3-js'
import { parquetWriteBuffer } from 'hyparquet-writer'
import { memStorage, type Pyramid } from 'pyrmts'
import { describe, expect, test } from 'vitest'
import { serveGeoQuery } from './serve.js'

const ms = (iso: string): number => new Date(iso).getTime()

const STATIONS = [
  { id: 's1', lat: 40.7128, lng: -74.0060 },
  { id: 's2', lat: 40.7484, lng: -73.9857 },
  { id: 's3', lat: 40.7794, lng: -73.9632 },
]

function tripsParquet(): Uint8Array {
  const RESES = [9, 7, 5]
  const HOURS = [ms('2026-01-01T00:00:00Z'), ms('2026-01-01T01:00:00Z')]
  const STATION_TRIPS: Record<string, number> = { s1: 10, s2: 20, s3: 30 }

  const tsCol: bigint[] = []
  const cellCol: string[] = []
  const countCol: number[] = []
  for (const tsMs of HOURS) {
    for (const res of RESES) {
      const cellTotals: Record<string, number> = {}
      for (const s of STATIONS) {
        const cell = latLngToCell(s.lat, s.lng, res)
        cellTotals[cell] = (cellTotals[cell] ?? 0) + STATION_TRIPS[s.id]!
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
  data.set('trips/h1/2026-01.parquet', tripsParquet())
  return {
    storage: memStorage(data),
    keyTemplate: 'trips/{tier}/{period}.parquet',
    axis: 'time',
    binCol: 'ts',
    dims: [{ name: 'h3_cell', type: 'h3' }],
    metrics: [{ name: 'count', monoid: 'count' }],
    tiers: [
      { name: 'h1', bin: '1h', shard: '1mo' },
      { name: 'd1', bin: '1d', shard: '1y' },
    ],
    geo: { cellCol: 'h3_cell', resolutions: [9, 7, 5] },
  }
}

const BBOX_PARAM = '40.70,-74.02,40.79,-73.95'

interface GeoResponse {
  records: Array<Record<string, unknown>>
  plan: {
    outputTier: string
    outputBin: string
    outputRes: number
    outputCells: string[]
    authoritativeEnd: string | null
    segments: Array<{ tier: string; from: string; to: string; reaggregate: boolean; keys: string[] }>
  }
}

describe('serveGeoQuery', () => {
  test('happy path returns records + geo plan metadata', async () => {
    const res = await serveGeoQuery({
      pyramid: buildPyramid(),
      request: new Request(
        'https://serve.example/?'
        + 'from=2026-01-01T00:00:00Z'
        + '&to=2026-01-01T02:00:00Z'
        + '&bin_budget=100'
        + `&bbox=${BBOX_PARAM}`
        + '&cell_budget=1000',
      ),
    })
    expect(res.status).toBe(200)
    const body = await res.json() as GeoResponse

    expect(body.plan.outputRes).toBe(9)
    expect(body.plan.outputTier).toBe('h1')
    expect(body.plan.segments).toHaveLength(1)
    expect(body.plan.outputCells.length).toBeGreaterThan(0)

    // 3 distinct stations at res 9 × 2 hours = 6 rows; total trips per hour = 60.
    expect(body.records).toHaveLength(6)
    const byHour: Record<string, number> = {}
    for (const r of body.records) {
      const ts = String(r.ts)
      byHour[ts] = (byHour[ts] ?? 0) + (r.count as number)
    }
    expect(Object.values(byHour)).toEqual([60, 60])
  })

  test('returns 400 on missing bbox', async () => {
    const res = await serveGeoQuery({
      pyramid: buildPyramid(),
      request: new Request(
        'https://serve.example/?from=2026-01-01T00:00:00Z&to=2026-01-01T02:00:00Z',
      ),
    })
    expect(res.status).toBe(400)
    expect(await res.json()).toEqual({
      error: 'bbox query param required (minLat,minLng,maxLat,maxLng)',
    })
  })

  test('returns 400 on malformed bbox', async () => {
    const res = await serveGeoQuery({
      pyramid: buildPyramid(),
      request: new Request(
        `https://serve.example/?from=2026-01-01T00:00:00Z&to=2026-01-01T02:00:00Z&bbox=oops`,
      ),
    })
    expect(res.status).toBe(400)
  })

  test('returns 400 on missing from/to', async () => {
    const res = await serveGeoQuery({
      pyramid: buildPyramid(),
      request: new Request(
        `https://serve.example/?bbox=${BBOX_PARAM}`,
      ),
    })
    expect(res.status).toBe(400)
    expect(await res.json()).toEqual({
      error: 'from and to query params required (ISO-8601)',
    })
  })

  test('returns 400 on invalid cell_budget', async () => {
    const res = await serveGeoQuery({
      pyramid: buildPyramid(),
      request: new Request(
        'https://serve.example/?'
        + 'from=2026-01-01T00:00:00Z'
        + '&to=2026-01-01T02:00:00Z'
        + `&bbox=${BBOX_PARAM}`
        + '&cell_budget=abc',
      ),
    })
    expect(res.status).toBe(400)
    expect(await res.json()).toEqual({ error: "invalid cell_budget 'abc'" })
  })

  test('returns 500 on pyramid with no geo config', async () => {
    const pyramid = buildPyramid()
    delete pyramid.geo
    const res = await serveGeoQuery({
      pyramid,
      request: new Request(
        `https://serve.example/?from=2026-01-01T00:00:00Z&to=2026-01-01T02:00:00Z&bbox=${BBOX_PARAM}`,
      ),
    })
    expect(res.status).toBe(500)
    expect(await res.json()).toEqual({
      error: 'serveGeoQuery: pyramid has no `geo` config',
    })
  })

  test('coarser cell_budget downshifts to res 7', async () => {
    const res = await serveGeoQuery({
      pyramid: buildPyramid(),
      request: new Request(
        'https://serve.example/?'
        + 'from=2026-01-01T00:00:00Z'
        + '&to=2026-01-01T02:00:00Z'
        + `&bbox=${BBOX_PARAM}`
        + '&cell_budget=3',
      ),
    })
    expect(res.status).toBe(200)
    const body = await res.json() as GeoResponse
    expect(body.plan.outputRes).toBe(7)
    // Hour totals still 60 (10 + 20 + 30) regardless of cell clustering at res 7.
    const byHour: Record<string, number> = {}
    for (const r of body.records) {
      byHour[String(r.ts)] = (byHour[String(r.ts)] ?? 0) + (r.count as number)
    }
    expect(Object.values(byHour)).toEqual([60, 60])
  })

  test('CORS header when enabled', async () => {
    const res = await serveGeoQuery({
      pyramid: buildPyramid(),
      request: new Request(
        'https://serve.example/?'
        + 'from=2026-01-01T00:00:00Z'
        + '&to=2026-01-01T02:00:00Z'
        + `&bbox=${BBOX_PARAM}`
        + '&cell_budget=100',
      ),
      cors: true,
    })
    expect(res.headers.get('access-control-allow-origin')).toBe('*')
  })

  test('tolerateMissingShards + earliestWatermarks plumb through to the planner / fetcher', async () => {
    // Pyramid storage with no shards at all — every fetch would 404. With
    // tolerateMissingShards we get an empty result (200) instead of an error.
    // earliestWatermarks pushes the planner's segment past the query range so
    // no shard keys are emitted at all, demonstrating the planner-level skip.
    const pyramid: Pyramid = { ...buildPyramid(), storage: memStorage() }
    const url = 'https://serve.example/?'
      + 'from=2026-01-01T00:00:00Z'
      + '&to=2026-01-01T02:00:00Z'
      + `&bbox=${BBOX_PARAM}`
      + '&cell_budget=100'

    const errRes = await serveGeoQuery({ pyramid, request: new Request(url) })
    expect(errRes.status).toBe(400)

    const tolerantRes = await serveGeoQuery({
      pyramid,
      request: new Request(url),
      tolerateMissingShards: true,
    })
    expect(tolerantRes.status).toBe(200)
    expect(((await tolerantRes.json()) as GeoResponse).records).toEqual([])

    const clampedRes = await serveGeoQuery({
      pyramid,
      request: new Request(url),
      earliestWatermarks: { h1: new Date('2030-01-01T00:00:00Z') },
    })
    expect(clampedRes.status).toBe(200)
    const clampedBody = await clampedRes.json() as GeoResponse
    expect(clampedBody.plan.segments).toEqual([])
    expect(clampedBody.records).toEqual([])
  })
})

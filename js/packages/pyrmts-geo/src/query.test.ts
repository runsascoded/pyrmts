import { describe, expect, test } from 'vitest'
import {
  buildGeoQueryUrl,
  fetchPyramidGeoQuery,
  type PyramidGeoQueryResult,
} from './query.js'

const d = (iso: string): Date => new Date(iso)
const BBOX = { minLat: 40.70, minLng: -74.02, maxLat: 40.79, maxLng: -73.95 }

describe('buildGeoQueryUrl', () => {
  test('serializes range, bin_budget, bbox, cell_budget, and filter', () => {
    expect(buildGeoQueryUrl({
      url: 'https://serve.example/q',
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-02T00:00:00Z') },
      binBudget: 100,
      bbox: BBOX,
      cellBudget: 1000,
      filter: { rideable: 'classic' },
    })).toBe(
      'https://serve.example/q?from=2026-01-01T00%3A00%3A00.000Z'
      + '&to=2026-01-02T00%3A00%3A00.000Z'
      + '&bin_budget=100'
      + '&bbox=40.7%2C-74.02%2C40.79%2C-73.95'
      + '&cell_budget=1000'
      + '&rideable=classic',
    )
  })

  test('relative URL: preserves pathname, returns no origin', () => {
    expect(buildGeoQueryUrl({
      url: '/api/pyrmts/ctbk',
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-02T00:00:00Z') },
      binBudget: 100,
      bbox: BBOX,
      cellBudget: 1000,
    })).toBe('/api/pyrmts/ctbk?from=2026-01-01T00%3A00%3A00.000Z&to=2026-01-02T00%3A00%3A00.000Z&bin_budget=100&bbox=40.7%2C-74.02%2C40.79%2C-73.95&cell_budget=1000')
  })
})

describe('fetchPyramidGeoQuery', () => {
  const okBody: PyramidGeoQueryResult = {
    records: [
      { ts: 1735689600000, h3_cell: '892a100d2c7ffff', count: 42 },
    ],
    plan: {
      outputTier: 'h1',
      outputBin: '1h',
      outputRes: 9,
      outputCells: ['892a100d2c7ffff'],
      authoritativeEnd: null,
      segments: [{
        tier: 'h1',
        from: '2026-01-01T00:00:00.000Z',
        to: '2026-01-01T01:00:00.000Z',
        reaggregate: false,
        keys: ['trips/h1/2026-01.parquet'],
      }],
    },
  }

  test('resolves with response JSON on 2xx', async () => {
    const fetchImpl = async (): Promise<Response> => new Response(JSON.stringify(okBody), { status: 200 })
    const result = await fetchPyramidGeoQuery({
      url: 'https://serve.example/q',
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-01T01:00:00Z') },
      binBudget: 100,
      bbox: BBOX,
      cellBudget: 1000,
      fetchImpl,
    })
    expect(result).toEqual(okBody)
  })

  test('rejects with HTTP error details on 4xx', async () => {
    const fetchImpl = async (): Promise<Response> => new Response(JSON.stringify({ error: 'bad bbox' }), {
      status: 400,
      statusText: 'Bad Request',
    })
    await expect(fetchPyramidGeoQuery({
      url: 'https://serve.example/q',
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-01T01:00:00Z') },
      binBudget: 100,
      bbox: BBOX,
      cellBudget: 1000,
      fetchImpl,
    })).rejects.toThrow('fetchPyramidGeoQuery: 400 Bad Request — {"error":"bad bbox"}')
  })
})

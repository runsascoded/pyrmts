import { describe, expect, test } from 'vitest'
import { buildQueryUrl, fetchPyramidQuery, type PyramidQueryResult } from './query.js'

const d = (iso: string): Date => new Date(iso)

describe('buildQueryUrl', () => {
  test('absolute URL: serializes range, bin_budget, and filter as query params', () => {
    expect(buildQueryUrl({
      url: 'https://serve.example/query',
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-02T00:00:00Z') },
      binBudget: 256,
      filter: { device_id: 17617 },
    })).toBe(
      'https://serve.example/query?from=2026-01-01T00%3A00%3A00.000Z'
      + '&to=2026-01-02T00%3A00%3A00.000Z'
      + '&bin_budget=256'
      + '&device_id=17617',
    )
  })

  test('relative URL: preserves pathname, returns no origin', () => {
    expect(buildQueryUrl({
      url: '/api/pyrmts/awair',
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-02T00:00:00Z') },
      binBudget: 100,
    })).toBe('/api/pyrmts/awair?from=2026-01-01T00%3A00%3A00.000Z&to=2026-01-02T00%3A00%3A00.000Z&bin_budget=100')
  })

  test('omits filter params when not supplied', () => {
    expect(buildQueryUrl({
      url: 'https://x/q',
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-02T00:00:00Z') },
      binBudget: 100,
    })).toBe('https://x/q?from=2026-01-01T00%3A00%3A00.000Z&to=2026-01-02T00%3A00%3A00.000Z&bin_budget=100')
  })
})

describe('fetchPyramidQuery', () => {
  const okBody: PyramidQueryResult = {
    records: [
      { ts: 1735689600000, device_id: 17617, temp_n: 60, temp_sum: 1200, temp_sumsq: 24500 },
    ],
    plan: {
      outputTier: 'h1',
      outputBin: '1h',
      authoritativeEnd: null,
      segments: [{
        tier: 'h1',
        from: '2026-01-01T00:00:00.000Z',
        to: '2026-01-01T01:00:00.000Z',
        reaggregate: false,
        keys: ['awair-17617/h1/2026-01.parquet'],
      }],
    },
  }

  test('resolves with the response JSON on 2xx', async () => {
    const fetchImpl = async (): Promise<Response> => new Response(JSON.stringify(okBody), { status: 200 })
    const result = await fetchPyramidQuery({
      url: 'https://serve.example/q',
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-01T01:00:00Z') },
      binBudget: 100,
      filter: { device_id: 17617 },
      fetchImpl,
    })
    expect(result).toEqual(okBody)
  })

  test('rejects with HTTP error details on 4xx/5xx', async () => {
    const fetchImpl = async (): Promise<Response> => new Response(JSON.stringify({ error: 'bad input' }), {
      status: 400,
      statusText: 'Bad Request',
    })
    await expect(fetchPyramidQuery({
      url: 'https://serve.example/q',
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-01T01:00:00Z') },
      binBudget: 100,
      fetchImpl,
    })).rejects.toThrow('fetchPyramidQuery: 400 Bad Request — {"error":"bad input"}')
  })

  test('passes the URL with query params to fetch', async () => {
    let observedUrl: string | undefined
    const fetchImpl: typeof fetch = async (input) => {
      observedUrl = typeof input === 'string' ? input : input instanceof URL ? input.toString() : input.url
      return new Response(JSON.stringify(okBody), { status: 200 })
    }
    await fetchPyramidQuery({
      url: 'https://serve.example/q',
      range: { from: d('2026-01-01T00:00:00Z'), to: d('2026-01-01T01:00:00Z') },
      binBudget: 100,
      filter: { device_id: 17617 },
      fetchImpl,
    })
    expect(observedUrl).toBe(
      'https://serve.example/q?from=2026-01-01T00%3A00%3A00.000Z'
      + '&to=2026-01-01T01%3A00%3A00.000Z'
      + '&bin_budget=100'
      + '&device_id=17617',
    )
  })
})

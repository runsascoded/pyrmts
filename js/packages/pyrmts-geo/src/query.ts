// Client-side geo query fetcher. Pure async — no React, no global state.
// Mirrors `fetchPyramidQuery` in `pyrmts` but adds bbox + cell_budget params
// and exposes outputRes/outputCells in the response.

import type { Duration, Row, SmoothMode } from 'pyrmts'
import type { BBox } from './planner.js'

export interface FetchPyramidGeoQueryInput {
  url: string
  range: { from: Date; to: Date }
  binBudget: number
  bbox: BBox
  cellBudget: number
  filter?: Record<string, string | number>
  smoothing?: Duration | 'auto' | `auto${number}`
  smoothMode?: SmoothMode
  signal?: AbortSignal
  fetchImpl?: typeof fetch
}

export interface GeoPlanMeta {
  outputTier: string
  outputBin: string
  outputRes: number
  outputCells: string[]
  authoritativeEnd: string | null
  smoothing: {
    smoothBin: string
    smoothBinCount: number
    smoothMode: SmoothMode
    smoothSourceTier: string
  } | null
  segments: Array<{
    tier: string
    from: string
    to: string
    reaggregate: boolean
    keys: string[]
  }>
}

export interface PyramidGeoQueryResult {
  records: Row[]
  plan: GeoPlanMeta
}

export async function fetchPyramidGeoQuery(
  input: FetchPyramidGeoQueryInput,
): Promise<PyramidGeoQueryResult> {
  const url = buildGeoQueryUrl(input)
  const f = input.fetchImpl ?? fetch
  const res = await f(url, { signal: input.signal ?? null })
  if (!res.ok) {
    const body = await res.text()
    throw new Error(`fetchPyramidGeoQuery: ${res.status} ${res.statusText} — ${body}`)
  }
  return await res.json() as PyramidGeoQueryResult
}

export function buildGeoQueryUrl(
  input: Pick<FetchPyramidGeoQueryInput, 'url' | 'range' | 'binBudget' | 'bbox' | 'cellBudget' | 'filter' | 'smoothing' | 'smoothMode'>,
): string {
  const u = new URL(input.url, 'http://placeholder')
  u.searchParams.set('from', input.range.from.toISOString())
  u.searchParams.set('to', input.range.to.toISOString())
  u.searchParams.set('bin_budget', String(input.binBudget))
  u.searchParams.set('bbox', formatBBox(input.bbox))
  u.searchParams.set('cell_budget', String(input.cellBudget))
  if (input.filter) {
    for (const [name, value] of Object.entries(input.filter)) {
      u.searchParams.set(name, String(value))
    }
  }
  if (input.smoothing !== undefined) u.searchParams.set('smooth', input.smoothing)
  if (input.smoothMode !== undefined) u.searchParams.set('smooth_mode', input.smoothMode)
  return input.url.startsWith('http')
    ? u.toString()
    : `${u.pathname}${u.search}`
}

function formatBBox(b: BBox): string {
  return `${b.minLat},${b.minLng},${b.maxLat},${b.maxLng}`
}

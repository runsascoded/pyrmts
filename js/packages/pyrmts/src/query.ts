// Client-side query fetcher. Pure async function — no React, no global
// state. The React hook (`usePyramid`) is a thin wrapper around this.
//
// The response shape matches what `serveQuery` (in pyrmts-cfw) emits:
//   { records, plan: { outputTier, outputBin, authoritativeEnd, segments } }

import type { Row } from './monoids.js'
import type { SmoothMode } from './planner.js'
import type { Duration } from './types.js'

export interface FetchPyramidQueryInput {
  // CFW endpoint URL (with or without query string; existing params preserved).
  url: string
  range: { from: Date; to: Date }
  binBudget: number
  filter?: Record<string, string | number>
  // Server-side rolling-window smoothing. Either an explicit Duration (e.g.
  // `'4h'`), `'auto'` (server picks based on bin_budget), or `'auto<N>'`
  // (auto with explicit multiplier). See `specs/done/server-side-smoothing.md`.
  smoothing?: Duration | 'auto' | `auto${number}`
  smoothMode?: SmoothMode
  signal?: AbortSignal
  // Override the default `fetch` (used in tests).
  fetchImpl?: typeof fetch
}

export interface PlanMeta {
  outputTier: string
  outputBin: string
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

export interface PyramidQueryResult {
  records: Row[]
  plan: PlanMeta
}

export async function fetchPyramidQuery(
  input: FetchPyramidQueryInput,
): Promise<PyramidQueryResult> {
  const url = buildQueryUrl(input)
  const f = input.fetchImpl ?? fetch
  const res = await f(url, { signal: input.signal ?? null })
  if (!res.ok) {
    const body = await res.text()
    throw new Error(`fetchPyramidQuery: ${res.status} ${res.statusText} — ${body}`)
  }
  return await res.json() as PyramidQueryResult
}

export function buildQueryUrl(
  input: Pick<FetchPyramidQueryInput, 'url' | 'range' | 'binBudget' | 'filter' | 'smoothing' | 'smoothMode'>,
): string {
  const u = new URL(input.url, 'http://placeholder')   // base used only if `url` is relative
  u.searchParams.set('from', input.range.from.toISOString())
  u.searchParams.set('to', input.range.to.toISOString())
  u.searchParams.set('bin_budget', String(input.binBudget))
  if (input.filter) {
    for (const [name, value] of Object.entries(input.filter)) {
      u.searchParams.set(name, String(value))
    }
  }
  if (input.smoothing !== undefined) u.searchParams.set('smooth', input.smoothing)
  if (input.smoothMode !== undefined) u.searchParams.set('smooth_mode', input.smoothMode)
  // Strip the placeholder origin if the original URL was relative.
  return input.url.startsWith('http')
    ? u.toString()
    : `${u.pathname}${u.search}`
}

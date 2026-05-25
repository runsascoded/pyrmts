// React hook wrapping `fetchPyramidGeoQuery`. Re-fetches when serialized
// inputs change; cancels in-flight via AbortController on cleanup. For
// production caching, wrap with TanStack Query instead.

import { useEffect, useState } from 'react'
import type { Row } from 'pyrmts'
import {
  fetchPyramidGeoQuery,
  type FetchPyramidGeoQueryInput,
  type GeoPlanMeta,
} from './query.js'

export interface UsePyramidGeoInput
  extends Omit<FetchPyramidGeoQueryInput, 'signal' | 'fetchImpl'> {}

export interface UsePyramidGeoResult {
  records: Row[]
  plan: GeoPlanMeta | null
  isLoading: boolean
  error: Error | null
}

export function usePyramidGeo(input: UsePyramidGeoInput): UsePyramidGeoResult {
  const [state, setState] = useState<UsePyramidGeoResult>({
    records: [],
    plan: null,
    isLoading: true,
    error: null,
  })

  const depKey = JSON.stringify({
    url: input.url,
    from: input.range.from.toISOString(),
    to: input.range.to.toISOString(),
    binBudget: input.binBudget,
    bbox: input.bbox,
    cellBudget: input.cellBudget,
    filter: input.filter ?? {},
  })

  useEffect(() => {
    const ctrl = new AbortController()
    setState(s => ({ ...s, isLoading: true, error: null }))
    fetchPyramidGeoQuery({ ...input, signal: ctrl.signal })
      .then(({ records, plan }) => {
        if (!ctrl.signal.aborted) {
          setState({ records, plan, isLoading: false, error: null })
        }
      })
      .catch((err: unknown) => {
        if (ctrl.signal.aborted) return
        const e = err instanceof Error ? err : new Error(String(err))
        setState(s => ({ ...s, isLoading: false, error: e }))
      })
    return () => ctrl.abort()
    // eslint-disable-next-line react-hooks/exhaustive-deps -- depKey serializes the deps
  }, [depKey])

  return state
}

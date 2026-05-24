// React hook around `fetchPyramidQuery`. Re-fetches when range/binBudget/
// filter changes; cancels in-flight requests via AbortController on
// unmount or dependency change.
//
// For production, wrap with TanStack Query (`useQuery`) instead — this hook
// is intentionally minimal (no caching, no retry, no stale-while-revalidate).

import { useEffect, useState } from 'react'
import type { Row } from './monoids.js'
import {
  fetchPyramidQuery,
  type FetchPyramidQueryInput,
  type PlanMeta,
} from './query.js'

export interface UsePyramidInput
  extends Omit<FetchPyramidQueryInput, 'signal' | 'fetchImpl'> {}

export interface UsePyramidResult {
  records: Row[]
  plan: PlanMeta | null
  isLoading: boolean
  error: Error | null
}

export function usePyramid(input: UsePyramidInput): UsePyramidResult {
  const [state, setState] = useState<UsePyramidResult>({
    records: [],
    plan: null,
    isLoading: true,
    error: null,
  })

  // Stable-ish deps: serialize the inputs that determine the request.
  const depKey = JSON.stringify({
    url: input.url,
    from: input.range.from.toISOString(),
    to: input.range.to.toISOString(),
    binBudget: input.binBudget,
    filter: input.filter ?? {},
  })

  useEffect(() => {
    const ctrl = new AbortController()
    setState(s => ({ ...s, isLoading: true, error: null }))
    fetchPyramidQuery({ ...input, signal: ctrl.signal })
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

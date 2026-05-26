// React hook wrapping `fetchPyramidGeoQuery`. Re-fetches when serialized
// inputs change; cancels in-flight via AbortController on cleanup. For
// production caching, wrap with TanStack Query instead.
import { useEffect, useState } from 'react';
import { fetchPyramidGeoQuery, } from './query.js';
export function usePyramidGeo(input) {
    const [state, setState] = useState({
        records: [],
        plan: null,
        isLoading: true,
        error: null,
    });
    const depKey = JSON.stringify({
        url: input.url,
        from: input.range.from.toISOString(),
        to: input.range.to.toISOString(),
        binBudget: input.binBudget,
        bbox: input.bbox,
        cellBudget: input.cellBudget,
        filter: input.filter ?? {},
        smoothing: input.smoothing ?? null,
        smoothMode: input.smoothMode ?? null,
    });
    useEffect(() => {
        const ctrl = new AbortController();
        setState(s => ({ ...s, isLoading: true, error: null }));
        fetchPyramidGeoQuery({ ...input, signal: ctrl.signal })
            .then(({ records, plan }) => {
            if (!ctrl.signal.aborted) {
                setState({ records, plan, isLoading: false, error: null });
            }
        })
            .catch((err) => {
            if (ctrl.signal.aborted)
                return;
            const e = err instanceof Error ? err : new Error(String(err));
            setState(s => ({ ...s, isLoading: false, error: e }));
        });
        return () => ctrl.abort();
        // eslint-disable-next-line react-hooks/exhaustive-deps -- depKey serializes the deps
    }, [depKey]);
    return state;
}
//# sourceMappingURL=use-pyramid-geo.js.map
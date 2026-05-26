// Client-side query fetcher. Pure async function — no React, no global
// state. The React hook (`usePyramid`) is a thin wrapper around this.
//
// The response shape matches what `serveQuery` (in pyrmts-cfw) emits:
//   { records, plan: { outputTier, outputBin, authoritativeEnd, segments } }
export async function fetchPyramidQuery(input) {
    const url = buildQueryUrl(input);
    const f = input.fetchImpl ?? fetch;
    const res = await f(url, { signal: input.signal ?? null });
    if (!res.ok) {
        const body = await res.text();
        throw new Error(`fetchPyramidQuery: ${res.status} ${res.statusText} — ${body}`);
    }
    return await res.json();
}
export function buildQueryUrl(input) {
    const u = new URL(input.url, 'http://placeholder'); // base used only if `url` is relative
    u.searchParams.set('from', input.range.from.toISOString());
    u.searchParams.set('to', input.range.to.toISOString());
    u.searchParams.set('bin_budget', String(input.binBudget));
    if (input.filter) {
        for (const [name, value] of Object.entries(input.filter)) {
            u.searchParams.set(name, String(value));
        }
    }
    if (input.smoothing !== undefined)
        u.searchParams.set('smooth', input.smoothing);
    if (input.smoothMode !== undefined)
        u.searchParams.set('smooth_mode', input.smoothMode);
    // Strip the placeholder origin if the original URL was relative.
    return input.url.startsWith('http')
        ? u.toString()
        : `${u.pathname}${u.search}`;
}
//# sourceMappingURL=query.js.map
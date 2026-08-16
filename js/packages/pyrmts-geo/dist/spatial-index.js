// Pluggable spatial-index abstraction. The planner consumes this interface
// rather than calling a concrete backend directly, so backends drop in
// without changing planner/serve/query code. See
// `specs/done/pluggable-spatial-backend.md` for the architectural framing.
//
// The shipped backend is `s2Index` (`s2-index.ts`). `h3Index` still exists
// (`h3-index.ts`) but is **test-only** and deliberately unexported from the
// package index — it is the second implementation that keeps this interface
// honest in the conformance suite, and nothing more. Importing it drags
// ~195 KB (minified) of `h3-js` into the consumer's bundle, which is why
// no shipped code path may reference it.
// Resolve the `SpatialIndex` for a pyramid. The index must be set
// explicitly: there is deliberately no default backend.
//
// This used to fall back to `h3Index`, which forced every consumer's
// bundle to carry `h3-js` (the fallback made `h3Index` reachable from the
// package index, and `h3-js` declares no `sideEffects`, so it could never
// be tree-shaken). Both known consumers already pass `index: s2Index`
// explicitly, and H3 is no longer a supported serving backend — pyramids
// keyed by H3 cells can't do exact multi-resolution aggregation at all.
export function getSpatialIndex(pyramid) {
    if (pyramid.geo === undefined) {
        throw new Error('getSpatialIndex: pyramid has no `geo` config');
    }
    const { index } = pyramid.geo;
    if (index === undefined) {
        throw new Error('getSpatialIndex: pyramid `geo.index` is unset — set it explicitly ' +
            '(e.g. `index: s2Index`); there is no default backend');
    }
    return index;
}
//# sourceMappingURL=spatial-index.js.map
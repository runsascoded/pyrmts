// Pluggable spatial-index abstraction. The planner consumes this interface
// rather than calling h3-js directly, so backends drop in without changing
// planner/serve/query code. Concrete impls: `h3Index` (fixed-level legacy),
// `s2Index` (prod-ready multi-resolution). See
// `specs/done/pluggable-spatial-backend.md` for the architectural framing.
export {};
//# sourceMappingURL=spatial-index.js.map
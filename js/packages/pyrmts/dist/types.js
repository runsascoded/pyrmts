// Core type definitions for pyrmts. See ../../../../SPEC.md.
// Raised when a `putIfMatch` precondition fails (object changed since
// the etag was read, or existed when create-only was requested).
// Callers retry — see `invalidate` / `pruneSpent` for the pattern.
export class EtagConflict extends Error {
    constructor(message) {
        super(message);
        this.name = 'EtagConflict';
    }
}
// Raised by consumers that reach for optional `Storage` methods on a
// backend that doesn't implement them (e.g. `invalidate` on a read-only
// fetch-only backend). Kept distinct from `EtagConflict` so callers
// can distinguish "backend can't do this" from "you lost the race".
export class NotSupported extends Error {
    constructor(message) {
        super(message);
        this.name = 'NotSupported';
    }
}
// Raised when a plan would exceed a `PlanLimits` ceiling. Callers map this
// to 400/413 rather than discovering the problem as an OOM.
export class PlanLimitError extends Error {
    limit;
    requested;
    allowed;
    constructor(limit, requested, allowed) {
        super(`planQuery: ${limit} limit exceeded (${requested} > ${allowed})`);
        this.name = 'PlanLimitError';
        this.limit = limit;
        this.requested = requested;
        this.allowed = allowed;
    }
}
//# sourceMappingURL=types.js.map
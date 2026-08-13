// In-memory Storage adapter. Useful for tests and dev fixtures. Not for
// production use — the whole pyramid lives in a single Map.
//
// Implements the full `Storage` surface — including the optional
// `getWithEtag`/`putIfMatch`/`listWithMtime` primitives that
// `invalidation.ts` and other coordination helpers depend on. Etag is
// an md5 (stable per content), and mtime is stamped from an injectable
// clock so tests can advance time deterministically.
import { EtagConflict } from './types.js';
// Backwards-compatible signature: `memStorage()` or
// `memStorage(new Map())` still works; pass `{ clock }` for test-time
// mtime control.
export function memStorage(arg = new Map()) {
    const opts = arg instanceof Map ? { data: arg } : arg;
    const data = opts.data ?? new Map();
    const clock = opts.clock ?? (() => new Date());
    const mtimes = new Map();
    for (const k of data.keys())
        mtimes.set(k, clock());
    return {
        async head(key) {
            const bytes = data.get(key);
            if (bytes === undefined)
                return null;
            return { size: bytes.byteLength, etag: etagOf(bytes) };
        },
        async getRange(key, start, end) {
            const bytes = data.get(key);
            if (bytes === undefined) {
                throw new Error(`memStorage.getRange: not found: ${key}`);
            }
            if (end <= start) {
                throw new Error(`memStorage.getRange: empty range [${start}, ${end})`);
            }
            if (start < 0 || end > bytes.byteLength) {
                throw new Error(`memStorage.getRange: out of bounds [${start}, ${end}) of ${bytes.byteLength}-byte object ${key}`);
            }
            return bytes.subarray(start, end);
        },
        async get(key) {
            return data.get(key) ?? null;
        },
        async put(key, bytes) {
            data.set(key, bytes);
            mtimes.set(key, clock());
        },
        async getWithEtag(key) {
            const bytes = data.get(key);
            if (bytes === undefined)
                return [null, null];
            return [bytes, etagOf(bytes)];
        },
        async putIfMatch(key, bytes, etag) {
            const cur = data.get(key);
            if (etag === null) {
                if (cur !== undefined) {
                    throw new EtagConflict(`putIfMatch: ${key} already exists`);
                }
            }
            else if (cur === undefined || etagOf(cur) !== etag) {
                throw new EtagConflict(`putIfMatch: ${key} changed since read`);
            }
            data.set(key, bytes);
            mtimes.set(key, clock());
        },
        list(prefix) {
            return (async function* () {
                for (const k of [...data.keys()].sort()) {
                    if (k.startsWith(prefix))
                        yield k;
                }
            })();
        },
        listWithMtime(prefix) {
            return (async function* () {
                for (const k of [...data.keys()].sort()) {
                    if (k.startsWith(prefix))
                        yield [k, mtimes.get(k) ?? null];
                }
            })();
        },
    };
}
// md5-like content hash. We use FNV-1a rather than pulling in a crypto
// dep — for the in-memory tests it just needs to be stable per content.
function etagOf(bytes) {
    let h = 0x811c9dc5;
    for (let i = 0; i < bytes.length; i++) {
        h ^= bytes[i];
        h = Math.imul(h, 0x01000193);
    }
    return (h >>> 0).toString(16).padStart(8, '0');
}
//# sourceMappingURL=storage.js.map
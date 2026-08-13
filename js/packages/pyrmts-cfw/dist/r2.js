// R2 implementation of pyrmts' Storage interface. Thin shim around a
// Cloudflare R2 bucket binding (the `R2Bucket` global from
// `@cloudflare/workers-types`).
//
// Range semantics translated to R2's `{ offset, length }` form. `head` returns
// `null` for missing objects (R2 returns null). `list` is exposed as an async
// iterable that handles cursor pagination internally.
//
// Optional CAS + mtime primitives (`getWithEtag`, `putIfMatch`,
// `listWithMtime`) map to R2's conditional writes (`onlyIf: { etagMatches
// | etagDoesNotMatch }`) and `R2Object.uploaded`. Consumers use these
// via `pyrmts`' invalidation journal (`invalidation.ts`) — see the
// EtagConflict retry loop there.
import { EtagConflict } from 'pyrmts';
export function r2Storage(bucket) {
    return {
        async head(key) {
            const obj = await bucket.head(key);
            if (obj === null)
                return null;
            return { size: obj.size, etag: obj.etag };
        },
        async getRange(key, start, end) {
            const length = end - start;
            if (length <= 0) {
                throw new Error(`r2Storage.getRange: empty range [${start}, ${end})`);
            }
            const body = await bucket.get(key, { range: { offset: start, length } });
            if (body === null) {
                throw new Error(`r2Storage.getRange: object not found: ${key}`);
            }
            return new Uint8Array(await body.arrayBuffer());
        },
        async get(key) {
            const body = await bucket.get(key);
            if (body === null)
                return null;
            return new Uint8Array(await body.arrayBuffer());
        },
        async put(key, bytes) {
            await bucket.put(key, bytes);
        },
        async getWithEtag(key) {
            // Fetch body + etag in one round-trip. `bucket.get` returns an
            // R2ObjectBody with both.
            const body = await bucket.get(key);
            if (body === null)
                return [null, null];
            const bytes = new Uint8Array(await body.arrayBuffer());
            return [bytes, body.etag];
        },
        async putIfMatch(key, bytes, etag) {
            // R2's conditional-write knobs:
            //   `onlyIf: { etagMatches: <etag> }`        → If-Match
            //   `onlyIf: { etagDoesNotMatch: '*' }`      → If-None-Match:* (create-only)
            // On precondition failure, R2 returns `null` from `put`. We surface
            // that as `EtagConflict` — the retry contract pyrmts' invalidation
            // journal is built on.
            const onlyIf = etag === null
                ? { etagDoesNotMatch: '*' }
                : { etagMatches: etag };
            const result = await bucket.put(key, bytes, { onlyIf });
            if (result === null) {
                throw new EtagConflict(`putIfMatch: ${key}: ${etag === null ? 'already exists' : 'changed since read'}`);
            }
        },
        list(prefix) {
            return listPaginated(bucket, prefix);
        },
        listWithMtime(prefix) {
            return listPaginatedWithMtime(bucket, prefix);
        },
    };
}
async function* listPaginated(bucket, prefix) {
    let cursor;
    while (true) {
        const page = await bucket.list(cursor ? { prefix, cursor } : { prefix });
        for (const obj of page.objects)
            yield obj.key;
        if (!page.truncated)
            return;
        cursor = page.cursor;
    }
}
async function* listPaginatedWithMtime(bucket, prefix) {
    let cursor;
    while (true) {
        const page = await bucket.list(cursor ? { prefix, cursor } : { prefix });
        for (const obj of page.objects) {
            // R2 `uploaded` is a Date; forward as-is (`null` reserved for backends
            // that genuinely can't report mtimes).
            yield [obj.key, obj.uploaded ?? null];
        }
        if (!page.truncated)
            return;
        cursor = page.cursor;
    }
}
//# sourceMappingURL=r2.js.map
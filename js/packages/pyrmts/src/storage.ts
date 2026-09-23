// In-memory Storage adapter. Useful for tests and dev fixtures. Not for
// production use — the whole pyramid lives in a single Map.
//
// Implements the full `Storage` surface — including the optional
// `getWithEtag`/`putIfMatch`/`listWithMtime` primitives that
// `invalidation.ts` and other coordination helpers depend on. Etag is
// an md5 (stable per content), and mtime is stamped from an injectable
// clock so tests can advance time deterministically.

import type { Storage } from './types.js'
import { EtagConflict, NotSupported } from './types.js'

export interface MemStorageOptions {
  data?: Map<string, Uint8Array>
  // Clock for mtime stamping (`listWithMtime`). Defaults to `Date.now()`.
  clock?: () => Date
}

// Backwards-compatible signature: `memStorage()` or
// `memStorage(new Map())` still works; pass `{ clock }` for test-time
// mtime control.
export function memStorage(
  arg: Map<string, Uint8Array> | MemStorageOptions = new Map(),
): Storage {
  const opts: MemStorageOptions = arg instanceof Map ? { data: arg } : arg
  const data = opts.data ?? new Map<string, Uint8Array>()
  const clock = opts.clock ?? (() => new Date())
  const mtimes = new Map<string, Date>()
  for (const k of data.keys()) mtimes.set(k, clock())

  return {
    async head(key) {
      const bytes = data.get(key)
      if (bytes === undefined) return null
      return { size: bytes.byteLength, etag: etagOf(bytes) }
    },

    async getRange(key, start, end, opts) {
      const bytes = data.get(key)
      if (bytes === undefined) {
        throw new Error(`memStorage.getRange: not found: ${key}`)
      }
      if (opts?.ifMatch !== undefined && etagOf(bytes) !== opts.ifMatch) {
        throw new EtagConflict(`memStorage.getRange: etag mismatch for ${key} (If-Match ${opts.ifMatch})`)
      }
      if (end <= start) {
        throw new Error(`memStorage.getRange: empty range [${start}, ${end})`)
      }
      if (start < 0 || end > bytes.byteLength) {
        throw new Error(
          `memStorage.getRange: out of bounds [${start}, ${end}) of ${bytes.byteLength}-byte object ${key}`,
        )
      }
      return bytes.subarray(start, end)
    },

    async get(key) {
      return data.get(key) ?? null
    },

    async put(key, bytes) {
      data.set(key, bytes)
      mtimes.set(key, clock())
    },

    async getWithEtag(key) {
      const bytes = data.get(key)
      if (bytes === undefined) return [null, null]
      return [bytes, etagOf(bytes)]
    },

    async putIfMatch(key, bytes, etag) {
      const cur = data.get(key)
      if (etag === null) {
        if (cur !== undefined) {
          throw new EtagConflict(`putIfMatch: ${key} already exists`)
        }
      } else if (cur === undefined || etagOf(cur) !== etag) {
        throw new EtagConflict(`putIfMatch: ${key} changed since read`)
      }
      data.set(key, bytes)
      mtimes.set(key, clock())
    },

    list(prefix) {
      return (async function* () {
        for (const k of [...data.keys()].sort()) {
          if (k.startsWith(prefix)) yield k
        }
      })()
    },

    listWithMtime(prefix) {
      return (async function* () {
        for (const k of [...data.keys()].sort()) {
          if (k.startsWith(prefix)) yield [k, mtimes.get(k) ?? null] as [string, Date | null]
        }
      })()
    },
  }
}

// md5-like content hash. We use FNV-1a rather than pulling in a crypto
// dep — for the in-memory tests it just needs to be stable per content.
function etagOf(bytes: Uint8Array): string {
  let h = 0x811c9dc5
  for (let i = 0; i < bytes.length; i++) {
    h ^= bytes[i]!
    h = Math.imul(h, 0x01000193)
  }
  return (h >>> 0).toString(16).padStart(8, '0')
}

export interface HttpStorageOptions {
  /** Extra request headers (auth, etc.). */
  headers?: Record<string, string>
  /** `fetch` to use (default: global). */
  fetch?: typeof fetch
}

// Read-only `Storage` over plain HTTP: `head` → HEAD (Content-Length, ETag),
// `getRange` → GET with `Range` (+ `If-Match` when `ifMatch` is given; a 412
// is an `EtagConflict`), `get` → GET. Any origin that serves static files with
// range support works: a public R2 bucket, Pages, S3 website hosting, or
// `scripts/serve-range.mjs` locally. Writes and listing are `NotSupported`.
export function httpStorage(baseUrl: string, opts: HttpStorageOptions = {}): Storage {
  const base = baseUrl.endsWith('/') ? baseUrl : `${baseUrl}/`
  const doFetch = opts.fetch ?? fetch
  const url = (key: string) => new URL(key.split('/').map(encodeURIComponent).join('/'), base).toString()
  const hdrs = (extra: Record<string, string> = {}) => ({ ...(opts.headers ?? {}), ...extra })
  const unsupported = (op: string) => new NotSupported(`httpStorage.${op}: read-only backend`)
  return {
    async head(key) {
      const res = await doFetch(url(key), { method: 'HEAD', headers: hdrs() })
      if (res.status === 404) return null
      if (!res.ok) throw new Error(`httpStorage.head: ${key}: HTTP ${res.status}`)
      const len = res.headers.get('content-length')
      if (len === null) throw new Error(`httpStorage.head: ${key}: no Content-Length`)
      const etag = res.headers.get('etag')
      return { size: Number(len), ...(etag !== null ? { etag } : {}) }
    },
    async getRange(key, start, end, rangeOpts) {
      if (end <= start) throw new Error(`httpStorage.getRange: empty range [${start}, ${end})`)
      const res = await doFetch(url(key), {
        headers: hdrs({
          Range: `bytes=${start}-${end - 1}`,
          ...(rangeOpts?.ifMatch !== undefined ? { 'If-Match': rangeOpts.ifMatch } : {}),
        }),
      })
      if (res.status === 412) {
        throw new EtagConflict(`httpStorage.getRange: etag mismatch for ${key} (If-Match ${rangeOpts?.ifMatch})`)
      }
      if (res.status === 404) throw new Error(`httpStorage.getRange: object not found: ${key}`)
      if (res.status !== 206) throw new Error(`httpStorage.getRange: ${key}: expected 206, got HTTP ${res.status}`)
      const bytes = new Uint8Array(await res.arrayBuffer())
      if (bytes.byteLength !== end - start) {
        throw new Error(`httpStorage.getRange: ${key}: got ${bytes.byteLength} bytes for [${start}, ${end})`)
      }
      return bytes
    },
    async get(key) {
      const res = await doFetch(url(key), { headers: hdrs() })
      if (res.status === 404) return null
      if (!res.ok) throw new Error(`httpStorage.get: ${key}: HTTP ${res.status}`)
      return new Uint8Array(await res.arrayBuffer())
    },
    async put() { throw unsupported('put') },
    async *list() { throw unsupported('list') },
  }
}

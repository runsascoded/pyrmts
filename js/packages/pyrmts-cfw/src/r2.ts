// R2 implementation of pyrmts' Storage interface. Thin shim around a
// Cloudflare R2 bucket binding (the `R2Bucket` global from
// `@cloudflare/workers-types`).
//
// Range semantics translated to R2's `{ offset, length }` form. `head` returns
// `null` for missing objects (R2 returns null). `list` is exposed as an async
// iterable that handles cursor pagination internally.

import type { Storage } from 'pyrmts'

export function r2Storage(bucket: R2Bucket): Storage {
  return {
    async head(key) {
      const obj = await bucket.head(key)
      if (obj === null) return null
      return { size: obj.size, etag: obj.etag }
    },

    async getRange(key, start, end) {
      const length = end - start
      if (length <= 0) {
        throw new Error(`r2Storage.getRange: empty range [${start}, ${end})`)
      }
      const body = await bucket.get(key, { range: { offset: start, length } })
      if (body === null) {
        throw new Error(`r2Storage.getRange: object not found: ${key}`)
      }
      return new Uint8Array(await body.arrayBuffer())
    },

    async get(key) {
      const body = await bucket.get(key)
      if (body === null) return null
      return new Uint8Array(await body.arrayBuffer())
    },

    async put(key, bytes) {
      await bucket.put(key, bytes)
    },

    list(prefix) {
      return listPaginated(bucket, prefix)
    },
  }
}

async function* listPaginated(bucket: R2Bucket, prefix: string): AsyncIterable<string> {
  let cursor: string | undefined
  while (true) {
    const page: R2Objects = await bucket.list(cursor ? { prefix, cursor } : { prefix })
    for (const obj of page.objects) yield obj.key
    if (!page.truncated) return
    cursor = page.cursor
  }
}

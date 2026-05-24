// In-memory Storage adapter. Useful for tests and dev fixtures. Not for
// production use — the whole pyramid lives in a single Map.

import type { Storage } from './types.js'

export function memStorage(data: Map<string, Uint8Array> = new Map()): Storage {
  return {
    async head(key) {
      const bytes = data.get(key)
      if (bytes === undefined) return null
      return { size: bytes.byteLength }
    },

    async getRange(key, start, end) {
      const bytes = data.get(key)
      if (bytes === undefined) {
        throw new Error(`memStorage.getRange: not found: ${key}`)
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
    },

    list(prefix) {
      return (async function* () {
        for (const k of [...data.keys()].sort()) {
          if (k.startsWith(prefix)) yield k
        }
      })()
    },
  }
}

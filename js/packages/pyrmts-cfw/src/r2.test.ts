import { describe, expect, test } from 'vitest'
import { EtagConflict } from 'pyrmts'
import { r2Storage } from './r2.js'

// Minimal in-memory R2-compatible fake. Implements only what r2Storage uses.
interface FakeEntry {
  body: Uint8Array
  etag: string
  uploaded: Date
}

class FakeR2Bucket {
  private data = new Map<string, FakeEntry>()
  private putSeq = 0
  // Configure page size for pagination tests.
  pageSize = 1000
  // Injectable clock — R2's `uploaded` is a Date; the fake stamps it
  // from this. Tests advance it to exercise mtime semantics.
  clock: () => Date = () => new Date()

  async head(key: string): Promise<{ size: number; etag: string } | null> {
    const e = this.data.get(key)
    if (!e) return null
    return { size: e.body.byteLength, etag: e.etag }
  }

  async get(
    key: string,
    opts?: {
      range?: { offset: number; length: number }
    },
  ): Promise<{ arrayBuffer: () => Promise<ArrayBuffer>; etag: string } | null> {
    const e = this.data.get(key)
    if (!e) return null
    let slice = e.body
    if (opts?.range) {
      const { offset, length } = opts.range
      slice = e.body.subarray(offset, offset + length)
    }
    const buf = slice.slice().buffer
    return { arrayBuffer: async () => buf, etag: e.etag }
  }

  async put(
    key: string,
    value: Uint8Array | string,
    opts?: {
      onlyIf?: { etagMatches?: string; etagDoesNotMatch?: string }
    },
  ): Promise<{ key: string } | null> {
    const body = typeof value === 'string' ? new TextEncoder().encode(value) : value
    const cur = this.data.get(key)
    // R2 conditional-write semantics: return null when the precondition
    // fails (see `putIfMatch` in r2.ts).
    if (opts?.onlyIf?.etagDoesNotMatch === '*' && cur !== undefined) return null
    if (opts?.onlyIf?.etagMatches !== undefined && (cur === undefined || cur.etag !== opts.onlyIf.etagMatches)) return null
    const etag = `etag-${this.putSeq++}-${body.byteLength}`
    this.data.set(key, { body, etag, uploaded: this.clock() })
    return { key }
  }

  async list(opts: { prefix?: string; cursor?: string }): Promise<{
    objects: { key: string; size: number; etag: string; uploaded: Date }[]
    truncated: boolean
    cursor?: string
  }> {
    const prefix = opts.prefix ?? ''
    const all = [...this.data.entries()]
      .filter(([k]) => k.startsWith(prefix))
      .map(([k, e]) => ({ key: k, size: e.body.byteLength, etag: e.etag, uploaded: e.uploaded }))
      .sort((a, b) => (a.key < b.key ? -1 : 1))

    const startIdx = opts.cursor ? parseInt(opts.cursor, 10) : 0
    const pageEnd = Math.min(startIdx + this.pageSize, all.length)
    const objects = all.slice(startIdx, pageEnd)
    const truncated = pageEnd < all.length
    return truncated
      ? { objects, truncated, cursor: String(pageEnd) }
      : { objects, truncated }
  }
}

// Cast helper to satisfy the R2Bucket parameter type. The fake implements only
// the methods r2Storage exercises.
function asR2(b: FakeR2Bucket): R2Bucket {
  return b as unknown as R2Bucket
}

describe('r2Storage.head', () => {
  test('returns { size, etag } for an existing object', async () => {
    const b = new FakeR2Bucket()
    await b.put('foo.parquet', new Uint8Array([1, 2, 3, 4]))
    const s = r2Storage(asR2(b))
    expect(await s.head('foo.parquet')).toEqual({ size: 4, etag: 'etag-0-4' })
  })

  test('returns null for a missing object', async () => {
    const b = new FakeR2Bucket()
    const s = r2Storage(asR2(b))
    expect(await s.head('missing.parquet')).toBe(null)
  })
})

describe('r2Storage.getRange', () => {
  test('returns the half-open byte slice [start, end)', async () => {
    const b = new FakeR2Bucket()
    await b.put('foo', new Uint8Array([10, 20, 30, 40, 50]))
    const s = r2Storage(asR2(b))
    expect([...await s.getRange('foo', 1, 4)]).toEqual([20, 30, 40])
  })

  test('throws on empty range', async () => {
    const b = new FakeR2Bucket()
    await b.put('foo', new Uint8Array([1, 2, 3]))
    const s = r2Storage(asR2(b))
    await expect(s.getRange('foo', 1, 1)).rejects.toThrow('r2Storage.getRange: empty range [1, 1)')
  })

  test('throws on missing object', async () => {
    const b = new FakeR2Bucket()
    const s = r2Storage(asR2(b))
    await expect(s.getRange('missing', 0, 10))
      .rejects.toThrow('r2Storage.getRange: object not found: missing')
  })
})

describe('r2Storage.get and put', () => {
  test('round-trips bytes', async () => {
    const b = new FakeR2Bucket()
    const s = r2Storage(asR2(b))
    const bytes = new Uint8Array([1, 2, 3, 4, 5])
    await s.put('foo', bytes)
    expect([...await s.get('foo') ?? []]).toEqual([1, 2, 3, 4, 5])
  })

  test('get returns null for a missing object', async () => {
    const b = new FakeR2Bucket()
    const s = r2Storage(asR2(b))
    expect(await s.get('missing')).toBe(null)
  })
})

describe('r2Storage.list', () => {
  test('yields all keys with prefix', async () => {
    const b = new FakeR2Bucket()
    await b.put('awair-17/raw/2026-01.parquet', new Uint8Array([1]))
    await b.put('awair-17/raw/2026-02.parquet', new Uint8Array([2]))
    await b.put('awair-99/raw/2026-01.parquet', new Uint8Array([3]))
    const s = r2Storage(asR2(b))
    const keys: string[] = []
    for await (const k of s.list('awair-17/')) keys.push(k)
    expect(keys).toEqual([
      'awair-17/raw/2026-01.parquet',
      'awair-17/raw/2026-02.parquet',
    ])
  })

  test('paginates across multiple R2 list calls', async () => {
    const b = new FakeR2Bucket()
    b.pageSize = 2
    for (let i = 0; i < 5; i++) {
      await b.put(`a/${i}.parquet`, new Uint8Array([i]))
    }
    const s = r2Storage(asR2(b))
    const keys: string[] = []
    for await (const k of s.list('a/')) keys.push(k)
    expect(keys).toEqual([
      'a/0.parquet', 'a/1.parquet', 'a/2.parquet', 'a/3.parquet', 'a/4.parquet',
    ])
  })
})

// ---- CAS + mtime primitives (invalidation.ts contract) ----

describe('r2Storage.getWithEtag', () => {
  test('returns [bytes, etag] for an existing object', async () => {
    const b = new FakeR2Bucket()
    await b.put('foo', new Uint8Array([1, 2, 3]))
    const s = r2Storage(asR2(b))
    const [bytes, etag] = await s.getWithEtag!('foo')
    expect([...bytes!]).toEqual([1, 2, 3])
    expect(etag).toBe('etag-0-3')
  })

  test('returns [null, null] for a missing object', async () => {
    const b = new FakeR2Bucket()
    const s = r2Storage(asR2(b))
    expect(await s.getWithEtag!('missing')).toEqual([null, null])
  })
})

describe('r2Storage.putIfMatch', () => {
  test('create-only (etag=null) succeeds when key does not exist', async () => {
    const b = new FakeR2Bucket()
    const s = r2Storage(asR2(b))
    await s.putIfMatch!('foo', new Uint8Array([1]), null)
    expect([...(await s.get('foo'))!]).toEqual([1])
  })

  test('create-only (etag=null) throws EtagConflict when key exists', async () => {
    const b = new FakeR2Bucket()
    await b.put('foo', new Uint8Array([1]))
    const s = r2Storage(asR2(b))
    await expect(s.putIfMatch!('foo', new Uint8Array([2]), null))
      .rejects.toThrow(EtagConflict)
  })

  test('etag-match succeeds when the etag is current', async () => {
    const b = new FakeR2Bucket()
    const s = r2Storage(asR2(b))
    await s.put('foo', new Uint8Array([1]))
    const [, etag] = await s.getWithEtag!('foo')
    await s.putIfMatch!('foo', new Uint8Array([2]), etag)
    expect([...(await s.get('foo'))!]).toEqual([2])
  })

  test('etag-match throws EtagConflict when the etag is stale', async () => {
    const b = new FakeR2Bucket()
    const s = r2Storage(asR2(b))
    await s.put('foo', new Uint8Array([1]))
    const [, staleEtag] = await s.getWithEtag!('foo')
    // Concurrent writer replaces the object, rotating the etag.
    await s.put('foo', new Uint8Array([99]))
    await expect(s.putIfMatch!('foo', new Uint8Array([2]), staleEtag))
      .rejects.toThrow(EtagConflict)
    // Original write not applied.
    expect([...(await s.get('foo'))!]).toEqual([99])
  })
})

describe('r2Storage.listWithMtime', () => {
  test('yields [key, uploaded] pairs', async () => {
    const b = new FakeR2Bucket()
    const t1 = new Date('2026-01-01T00:00:00Z')
    const t2 = new Date('2026-01-02T00:00:00Z')
    b.clock = () => t1
    await b.put('a/1.parquet', new Uint8Array([1]))
    b.clock = () => t2
    await b.put('a/2.parquet', new Uint8Array([2]))
    const s = r2Storage(asR2(b))
    const out: [string, Date | null][] = []
    for await (const pair of s.listWithMtime!('a/')) out.push(pair)
    expect(out).toEqual([
      ['a/1.parquet', t1],
      ['a/2.parquet', t2],
    ])
  })

  test('paginates like list()', async () => {
    const b = new FakeR2Bucket()
    b.pageSize = 2
    const t = new Date('2026-01-01T00:00:00Z')
    b.clock = () => t
    for (let i = 0; i < 5; i++) {
      await b.put(`a/${i}.parquet`, new Uint8Array([i]))
    }
    const s = r2Storage(asR2(b))
    const out: [string, Date | null][] = []
    for await (const pair of s.listWithMtime!('a/')) out.push(pair)
    expect(out.map(p => p[0])).toEqual([
      'a/0.parquet', 'a/1.parquet', 'a/2.parquet', 'a/3.parquet', 'a/4.parquet',
    ])
    expect(out.every(p => p[1]!.getTime() === t.getTime())).toBe(true)
  })
})

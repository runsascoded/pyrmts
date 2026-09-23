// `httpStorage` against a real Node HTTP server with Range / ETag / If-Match
// (the same semantics `scripts/serve-range.mjs` implements).

import { createServer, type Server } from 'node:http'
import { afterAll, beforeAll, describe, expect, test } from 'vitest'
import { httpStorage } from './storage.js'
import { EtagConflict, NotSupported } from './types.js'

const files = new Map<string, Uint8Array>([['dir/obj.bin', new Uint8Array([10, 11, 12, 13, 14, 15])]])
const etagOf = (b: Uint8Array) => `"e${b.byteLength}-${b[0]}"`

let server: Server
let base: string

beforeAll(async () => {
  server = createServer((req, res) => {
    const key = decodeURIComponent(new URL(req.url ?? '/', 'http://x').pathname.slice(1))
    const body = files.get(key)
    if (body === undefined) {
      res.writeHead(404).end()
      return
    }
    const etag = etagOf(body)
    const ifMatch = req.headers['if-match']
    if (ifMatch !== undefined && ifMatch !== etag) {
      res.writeHead(412).end()
      return
    }
    res.setHeader('ETag', etag)
    res.setHeader('Accept-Ranges', 'bytes')
    const range = req.headers.range
    if (req.method === 'HEAD') {
      res.writeHead(200, { 'Content-Length': body.byteLength }).end()
      return
    }
    if (range !== undefined) {
      const m = /^bytes=(\d+)-(\d+)$/.exec(range)!
      const start = Number(m[1])
      const end = Number(m[2]) + 1
      res.writeHead(206, { 'Content-Length': end - start, 'Content-Range': `bytes ${start}-${end - 1}/${body.byteLength}` })
      res.end(Buffer.from(body.subarray(start, end)))
      return
    }
    res.writeHead(200, { 'Content-Length': body.byteLength }).end(Buffer.from(body))
  })
  await new Promise<void>(r => server.listen(0, '127.0.0.1', r))
  const addr = server.address() as { port: number }
  base = `http://127.0.0.1:${addr.port}/`
})

afterAll(async () => {
  await new Promise<void>(r => server.close(() => r()))
})

describe('httpStorage', () => {
  test('head, range reads, whole reads', async () => {
    const s = httpStorage(base)
    expect(await s.head('dir/obj.bin')).toEqual({ size: 6, etag: '"e6-10"' })
    expect(await s.head('missing')).toBeNull()
    expect(await s.getRange('dir/obj.bin', 1, 4)).toEqual(new Uint8Array([11, 12, 13]))
    expect(await s.get('dir/obj.bin')).toEqual(new Uint8Array([10, 11, 12, 13, 14, 15]))
    expect(await s.get('missing')).toBeNull()
  })

  test('If-Match: served on a match, EtagConflict on a mismatch', async () => {
    const s = httpStorage(base)
    expect(await s.getRange('dir/obj.bin', 0, 2, { ifMatch: '"e6-10"' })).toEqual(new Uint8Array([10, 11]))
    await expect(s.getRange('dir/obj.bin', 0, 2, { ifMatch: '"stale"' })).rejects.toBeInstanceOf(EtagConflict)
  })

  test('writes and listing are NotSupported', async () => {
    const s = httpStorage(base)
    await expect(s.put('k', new Uint8Array())).rejects.toBeInstanceOf(NotSupported)
    await expect((async () => { for await (const _ of s.list('')) void _ })()).rejects.toBeInstanceOf(NotSupported)
  })
})

#!/usr/bin/env node
// Static file server with Range / ETag / If-Match, for exercising `httpStorage`
// and `bench-walk.mjs` over a real HTTP path locally.
//
//   node scripts/serve-range.mjs --dir /path/to/files [--port 3729] [--latency 0]
//
// `--latency MS` sleeps before every response, to emulate a remote origin's
// round trip on the real code path (a simulation; the request count is what
// the walk measures, the latency per request is the knob).

import { createReadStream, statSync } from 'node:fs'
import { createServer } from 'node:http'
import { join, normalize } from 'node:path'
import { parseArgs } from 'node:util'

const { values } = parseArgs({
  options: {
    dir: { type: 'string', short: 'd' },
    port: { type: 'string', short: 'p', default: '3729' },
    latency: { type: 'string', short: 'l', default: '0' },
  },
})
if (!values.dir) {
  console.error('usage: serve-range.mjs --dir DIR [--port 3729] [--latency MS]')
  process.exit(2)
}
const root = values.dir
const latency = Number(values.latency)
const sleep = ms => new Promise(r => setTimeout(r, ms))

const server = createServer(async (req, res) => {
  if (latency > 0) await sleep(latency)
  const rel = decodeURIComponent(new URL(req.url ?? '/', 'http://x').pathname)
  const file = join(root, normalize(rel))
  if (!file.startsWith(root)) {
    res.writeHead(403).end()
    return
  }
  let st
  try {
    st = statSync(file)
  } catch {
    res.writeHead(404).end()
    return
  }
  if (!st.isFile()) {
    res.writeHead(404).end()
    return
  }
  const etag = `"${st.size.toString(16)}-${Math.floor(st.mtimeMs).toString(16)}"`
  const ifMatch = req.headers['if-match']
  if (ifMatch !== undefined && ifMatch !== etag) {
    res.writeHead(412).end()
    return
  }
  res.setHeader('ETag', etag)
  res.setHeader('Accept-Ranges', 'bytes')
  if (req.method === 'HEAD') {
    res.writeHead(200, { 'Content-Length': st.size }).end()
    return
  }
  const range = req.headers.range
  if (range !== undefined) {
    const m = /^bytes=(\d+)-(\d+)$/.exec(range)
    if (!m) {
      res.writeHead(416).end()
      return
    }
    const start = Number(m[1])
    const end = Math.min(Number(m[2]) + 1, st.size)
    res.writeHead(206, { 'Content-Length': end - start, 'Content-Range': `bytes ${start}-${end - 1}/${st.size}` })
    createReadStream(file, { start, end: end - 1 }).pipe(res)
    return
  }
  res.writeHead(200, { 'Content-Length': st.size })
  createReadStream(file).pipe(res)
})

server.listen(Number(values.port), '127.0.0.1', () => {
  console.error(`serve-range: ${root} on http://127.0.0.1:${values.port}/ (latency ${latency} ms)`)
})

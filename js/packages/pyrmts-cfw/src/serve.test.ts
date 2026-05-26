import { parquetWriteBuffer } from 'hyparquet-writer'
import { memStorage, type Pyramid } from 'pyrmts'
import { describe, expect, test } from 'vitest'
import { serveQuery } from './serve.js'

const ms = (iso: string): number => new Date(iso).getTime()

function awairH1Parquet(): Uint8Array {
  const buf = parquetWriteBuffer({
    columnData: [
      {
        name: 'ts',
        type: 'INT64',
        data: [
          BigInt(ms('2026-01-01T00:00:00Z')),
          BigInt(ms('2026-01-01T01:00:00Z')),
          BigInt(ms('2026-01-01T02:00:00Z')),
        ],
      },
      { name: 'device_id', type: 'INT32', data: [17617, 17617, 17617] },
      { name: 'temp_n', type: 'INT32', data: [60, 60, 60] },
      { name: 'temp_sum', type: 'DOUBLE', data: [1200, 1260, 1320] },
      { name: 'temp_sumsq', type: 'DOUBLE', data: [24500, 26800, 29200] },
    ],
  })
  return new Uint8Array(buf)
}

function awairRawParquet(): Uint8Array {
  // 60 minutes of raw rows at 2026-01-01T02:00 — one reading per minute,
  // each with temp_n=1, temp_sum=21, temp_sumsq=441. Reaggregated to h1
  // these become {n: 60, sum: 1260, sumsq: 26460}.
  const ts: bigint[] = []
  const deviceId: number[] = []
  const tempN: number[] = []
  const tempSum: number[] = []
  const tempSumsq: number[] = []
  const baseMs = ms('2026-01-01T02:00:00Z')
  for (let m = 0; m < 60; m++) {
    ts.push(BigInt(baseMs + m * 60_000))
    deviceId.push(17617)
    tempN.push(1)
    tempSum.push(21)
    tempSumsq.push(441)
  }
  const buf = parquetWriteBuffer({
    columnData: [
      { name: 'ts', type: 'INT64', data: ts },
      { name: 'device_id', type: 'INT32', data: deviceId },
      { name: 'temp_n', type: 'INT32', data: tempN },
      { name: 'temp_sum', type: 'DOUBLE', data: tempSum },
      { name: 'temp_sumsq', type: 'DOUBLE', data: tempSumsq },
    ],
  })
  return new Uint8Array(buf)
}

function buildPyramid(): Pyramid {
  const data = new Map<string, Uint8Array>()
  data.set('awair-17617/h1/2026-01.parquet', awairH1Parquet())
  data.set('awair-17617/raw/2026-01.parquet', awairRawParquet())
  return {
    storage: memStorage(data),
    keyTemplate: 'awair-{device_id}/{tier}/{period}.parquet',
    axis: 'time',
    binCol: 'ts',
    dims: [{ name: 'device_id', type: 'int' }],
    metrics: [{ name: 'temp', monoid: 'sum' }],
    tiers: [
      { name: 'raw', bin: '1min', shard: '1mo' },
      { name: 'h1', bin: '1h', shard: '1mo' },
      { name: 'd1', bin: '1d', shard: '1y' },
      { name: 'mo1', bin: '1mo', shard: '1y' },
    ],
  }
}

interface ResponseBody {
  records: Array<Record<string, unknown>>
  plan: {
    outputTier: string
    outputBin: string
    authoritativeEnd: string | null
    smoothing: {
      smoothBin: string
      smoothBinCount: number
      smoothMode: 'centered' | 'trailing'
      smoothSourceTier: string
    } | null
    segments: Array<{
      tier: string
      from: string
      to: string
      reaggregate: boolean
      keys: string[]
    }>
  }
}

describe('serveQuery', () => {
  test('happy path returns records + plan metadata', async () => {
    const pyramid = buildPyramid()
    const url = 'https://serve.example/?'
      + 'from=2026-01-01T00:00:00Z'
      + '&to=2026-01-01T03:00:00Z'
      + '&bin_budget=100'
      + '&device_id=17617'
    const res = await serveQuery({ pyramid, request: new Request(url) })
    expect(res.status).toBe(200)
    const body = await res.json() as ResponseBody
    expect(body.records).toEqual([
      { ts: ms('2026-01-01T00:00:00Z'), device_id: 17617, temp_n: 60, temp_sum: 1200, temp_sumsq: 24500 },
      { ts: ms('2026-01-01T01:00:00Z'), device_id: 17617, temp_n: 60, temp_sum: 1260, temp_sumsq: 26800 },
      { ts: ms('2026-01-01T02:00:00Z'), device_id: 17617, temp_n: 60, temp_sum: 1320, temp_sumsq: 29200 },
    ])
    expect(body.plan).toEqual({
      outputTier: 'h1',
      outputBin: '1h',
      authoritativeEnd: null,
      smoothing: null,
      segments: [{
        tier: 'h1',
        from: '2026-01-01T00:00:00.000Z',
        to: '2026-01-01T03:00:00.000Z',
        reaggregate: false,
        keys: ['awair-17617/h1/2026-01.parquet'],
      }],
    })
  })

  test('returns 400 on missing from/to', async () => {
    const res = await serveQuery({
      pyramid: buildPyramid(),
      request: new Request('https://serve.example/?to=2026-01-01T03:00:00Z'),
    })
    expect(res.status).toBe(400)
    expect(await res.json()).toEqual({ error: 'from and to query params required (ISO-8601)' })
  })

  test('returns 400 on invalid bin_budget', async () => {
    const res = await serveQuery({
      pyramid: buildPyramid(),
      request: new Request(
        'https://serve.example/?from=2026-01-01T00:00:00Z&to=2026-01-01T03:00:00Z&bin_budget=abc&device_id=17617',
      ),
    })
    expect(res.status).toBe(400)
    expect(await res.json()).toEqual({ error: "invalid bin_budget 'abc'" })
  })

  test('forwards planner errors as 400 (e.g. missing filter)', async () => {
    const res = await serveQuery({
      pyramid: buildPyramid(),
      request: new Request(
        'https://serve.example/?from=2026-01-01T00:00:00Z&to=2026-01-01T03:00:00Z',
      ),
    })
    expect(res.status).toBe(400)
    expect(await res.json()).toEqual({
      error: 'planQuery: missing key template value for {device_id}',
    })
  })

  test('adds CORS header when enabled', async () => {
    const res = await serveQuery({
      pyramid: buildPyramid(),
      request: new Request(
        'https://serve.example/?from=2026-01-01T00:00:00Z&to=2026-01-01T03:00:00Z&device_id=17617',
      ),
      cors: true,
    })
    expect(res.headers.get('access-control-allow-origin')).toBe('*')
  })

  test('tolerateMissingShards: returns empty bins for shards that do not exist', async () => {
    // Pyramid without the awair-99999 device's h1 shard for 2026-01.
    const data = new Map<string, Uint8Array>()
    data.set('awair-17617/h1/2026-01.parquet', awairH1Parquet())
    const pyramid: Pyramid = {
      ...buildPyramid(),
      storage: memStorage(data),
    }

    // Query for device 99999 — its h1 shard doesn't exist. Without
    // tolerateMissingShards we'd 400; with it we get an empty record set.
    const url = 'https://serve.example/?'
      + 'from=2026-01-01T00:00:00Z'
      + '&to=2026-01-01T03:00:00Z'
      + '&bin_budget=100'
      + '&device_id=99999'

    const errRes = await serveQuery({ pyramid, request: new Request(url) })
    expect(errRes.status).toBe(400)
    expect(await errRes.json()).toEqual({
      error: 'fetchShardData: object not found: awair-99999/h1/2026-01.parquet',
    })

    const okRes = await serveQuery({
      pyramid,
      request: new Request(url),
      tolerateMissingShards: true,
    })
    expect(okRes.status).toBe(200)
    const body = await okRes.json() as ResponseBody
    expect(body.records).toEqual([])
    expect(body.plan.segments).toEqual([{
      tier: 'h1',
      from: '2026-01-01T00:00:00.000Z',
      to: '2026-01-01T03:00:00.000Z',
      reaggregate: false,
      keys: ['awair-99999/h1/2026-01.parquet'],
    }])
  })

  test('earliestWatermarks clamps the segment so a pre-data shard is never planned', async () => {
    // Device 17617 raw data starts at 2026-01-01T02:00; query starts at
    // 2026-01-01T00:00. earliestWatermarks pushes the query's effective
    // start to 02:00, so the planner only emits one (h1) segment for the
    // tail rather than [00:00, 02:00) from a pre-data tier.
    const res = await serveQuery({
      pyramid: buildPyramid(),
      request: new Request(
        'https://serve.example/?'
        + 'from=2026-01-01T00:00:00Z'
        + '&to=2026-01-01T03:00:00Z'
        + '&bin_budget=100'
        + '&device_id=17617',
      ),
      earliestWatermarks: { raw: new Date('2026-01-01T02:00:00Z') },
    })
    expect(res.status).toBe(200)
    const body = await res.json() as ResponseBody
    expect(body.plan.segments).toEqual([{
      tier: 'h1',
      from: '2026-01-01T02:00:00.000Z',
      to: '2026-01-01T03:00:00.000Z',
      reaggregate: false,
      keys: ['awair-17617/h1/2026-01.parquet'],
    }])
    expect(body.records).toEqual([
      { ts: ms('2026-01-01T02:00:00Z'), device_id: 17617, temp_n: 60, temp_sum: 1320, temp_sumsq: 29200 },
    ])
  })

  test('earliestWatermarks accepts a callback', async () => {
    const res = await serveQuery({
      pyramid: buildPyramid(),
      request: new Request(
        'https://serve.example/?'
        + 'from=2026-01-01T00:00:00Z'
        + '&to=2026-01-01T03:00:00Z'
        + '&bin_budget=100'
        + '&device_id=17617',
      ),
      earliestWatermarks: () => ({ raw: new Date('2026-01-01T02:00:00Z') }),
    })
    expect(res.status).toBe(200)
    const body = await res.json() as ResponseBody
    expect(body.plan.segments[0]!.from).toBe('2026-01-01T02:00:00.000Z')
  })

  test('honors watermark callback (splits into h1 + reaggregated raw)', async () => {
    // h1 watermark at 02:00 → segment 0 reads h1 shard for [00:00, 02:00),
    // segment 1 reaggregates raw shard for [02:00, 03:00) into h1 bins.
    const res = await serveQuery({
      pyramid: buildPyramid(),
      request: new Request(
        'https://serve.example/?'
        + 'from=2026-01-01T00:00:00Z'
        + '&to=2026-01-01T03:00:00Z'
        + '&bin_budget=100'
        + '&device_id=17617',
      ),
      watermarks: () => ({ h1: new Date('2026-01-01T02:00:00Z') }),
    })
    expect(res.status).toBe(200)
    const body = await res.json() as ResponseBody
    expect(body.plan.segments).toEqual([
      {
        tier: 'h1',
        from: '2026-01-01T00:00:00.000Z',
        to: '2026-01-01T02:00:00.000Z',
        reaggregate: false,
        keys: ['awair-17617/h1/2026-01.parquet'],
      },
      {
        tier: 'raw',
        from: '2026-01-01T02:00:00.000Z',
        to: '2026-01-01T03:00:00.000Z',
        reaggregate: true,
        keys: ['awair-17617/raw/2026-01.parquet'],
      },
    ])
    expect(body.records).toEqual([
      { ts: ms('2026-01-01T00:00:00Z'), device_id: 17617, temp_n: 60, temp_sum: 1200, temp_sumsq: 24500 },
      { ts: ms('2026-01-01T01:00:00Z'), device_id: 17617, temp_n: 60, temp_sum: 1260, temp_sumsq: 26800 },
      { ts: ms('2026-01-01T02:00:00Z'), device_id: 17617, temp_n: 60, temp_sum: 1260, temp_sumsq: 26460 },
    ])
  })

  test('?smooth plumbs through to plan + emits _smooth_<state> columns', async () => {
    // 3 visible bins at h1 → cap floor(3/4)=0 → maxCount<1 → snaps to 1 (no-op).
    // So `?smooth=2h` snaps down to 1h (count=1, no-op). Smooth cols still
    // appear, equal to raw cols.
    const res = await serveQuery({
      pyramid: buildPyramid(),
      request: new Request(
        'https://serve.example/?'
        + 'from=2026-01-01T00:00:00Z'
        + '&to=2026-01-01T03:00:00Z'
        + '&bin_budget=100'
        + '&device_id=17617'
        + '&smooth=2h',
      ),
    })
    expect(res.status).toBe(200)
    const body = await res.json() as ResponseBody
    expect(body.plan.smoothing).toEqual({
      smoothBin: '1h',
      smoothBinCount: 1,
      smoothMode: 'centered',
      smoothSourceTier: 'h1',
    })
    // count=1 → smooth cols mirror raw cols.
    expect(body.records[0]).toMatchObject({
      ts: ms('2026-01-01T00:00:00Z'),
      temp_n: 60,
      temp_sum: 1200,
      temp_smooth_n: 60,
      temp_smooth_sum: 1200,
      temp_smooth_sumsq: 24500,
    })
  })

  test('?smooth_mode=trailing plumbs through to plan', async () => {
    const res = await serveQuery({
      pyramid: buildPyramid(),
      request: new Request(
        'https://serve.example/?'
        + 'from=2026-01-01T00:00:00Z'
        + '&to=2026-01-01T03:00:00Z'
        + '&bin_budget=100'
        + '&device_id=17617'
        + '&smooth=1h'
        + '&smooth_mode=trailing',
      ),
    })
    expect(res.status).toBe(200)
    const body = await res.json() as ResponseBody
    expect(body.plan.smoothing?.smoothMode).toBe('trailing')
  })

  test('invalid ?smooth returns 400', async () => {
    const res = await serveQuery({
      pyramid: buildPyramid(),
      request: new Request(
        'https://serve.example/?'
        + 'from=2026-01-01T00:00:00Z'
        + '&to=2026-01-01T03:00:00Z'
        + '&device_id=17617'
        + '&smooth=garbage',
      ),
    })
    expect(res.status).toBe(400)
    expect(await res.json()).toEqual({
      error: "invalid smooth 'garbage' (expected Duration like '4h' or 'auto[N]')",
    })
  })

  test('invalid ?smooth_mode returns 400', async () => {
    const res = await serveQuery({
      pyramid: buildPyramid(),
      request: new Request(
        'https://serve.example/?'
        + 'from=2026-01-01T00:00:00Z'
        + '&to=2026-01-01T03:00:00Z'
        + '&device_id=17617'
        + '&smooth_mode=sideways',
      ),
    })
    expect(res.status).toBe(400)
    expect(await res.json()).toEqual({
      error: "invalid smooth_mode 'sideways' (centered|trailing)",
    })
  })
})

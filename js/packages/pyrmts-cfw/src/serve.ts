// HTTP handler that ties planQuery + fetchSegmentRows + stitch behind a CFW
// fetch endpoint. Consumers wire it into their worker's `fetch` export.
//
// Query params:
//   from         ISO-8601 instant (required for time-axis pyramids)
//   to           ISO-8601 instant (required)
//   bin_budget   max output bins (default 1024)
//   <dim>=<v>    one per pyramid dim used in the key template
//
// Watermarks aren't a query param — they're consumer-supplied (this worker
// knows where its raw shards end). Pass a `watermarks` callback to compute
// them lazily on each request, or precompute and pass the map directly.

import {
  fetchSegmentRows,
  planQuery,
  stitch,
  type Pyramid,
} from 'pyrmts'

export interface ServeOptions {
  pyramid: Pyramid
  request: Request
  // Per-tier latest-complete-bin instants. Missing tiers default to "complete
  // through `to`" (see planQuery semantics).
  watermarks?:
    | Record<string, Date>
    | ((req: Request) => Promise<Record<string, Date>> | Record<string, Date>)
  // Add `access-control-allow-origin: *` to responses.
  cors?: boolean
}

export async function serveQuery(opts: ServeOptions): Promise<Response> {
  const { pyramid, request, cors } = opts
  const url = new URL(request.url)

  const from = parseInstant(url.searchParams.get('from'))
  const to = parseInstant(url.searchParams.get('to'))
  if (from === null || to === null) {
    return errorResponse(400, 'from and to query params required (ISO-8601)', cors)
  }

  const binBudgetRaw = url.searchParams.get('bin_budget')
  const binBudget = binBudgetRaw === null ? 1024 : Number.parseInt(binBudgetRaw, 10)
  if (!Number.isFinite(binBudget) || binBudget <= 0) {
    return errorResponse(400, `invalid bin_budget '${binBudgetRaw}'`, cors)
  }

  const filter: Record<string, string> = {}
  for (const dim of pyramid.dims) {
    const v = url.searchParams.get(dim.name)
    if (v !== null) filter[dim.name] = v
  }

  const watermarks = await resolveWatermarks(opts.watermarks, request)

  let result: { records: unknown[]; plan: unknown }
  try {
    const plan = planQuery(pyramid, { range: { from, to }, binBudget, watermarks, filter })
    const shardRows = await Promise.all(
      plan.segments.map(seg => fetchSegmentRows(pyramid.storage, seg.keys, {
        binCol: pyramid.binCol,
        range: { from: seg.from, to: seg.to },
      })),
    )
    const records = stitch({ pyramid, plan, shardRows })
    result = {
      records,
      plan: {
        outputTier: plan.outputTier.name,
        outputBin: plan.outputBin,
        authoritativeEnd: plan.authoritativeEnd?.toISOString() ?? null,
        segments: plan.segments.map(s => ({
          tier: s.shardTier.name,
          from: s.from.toISOString(),
          to: s.to.toISOString(),
          reaggregate: s.reaggregate,
          keys: s.keys,
        })),
      },
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err)
    return errorResponse(400, msg, cors)
  }

  return jsonResponse(200, result, cors)
}

function parseInstant(s: string | null): Date | null {
  if (s === null) return null
  const t = new Date(s)
  return Number.isNaN(t.getTime()) ? null : t
}

async function resolveWatermarks(
  src: ServeOptions['watermarks'],
  request: Request,
): Promise<Record<string, Date>> {
  if (src === undefined) return {}
  if (typeof src === 'function') {
    return await src(request)
  }
  return src
}

function jsonResponse(status: number, body: unknown, cors?: boolean): Response {
  const headers: Record<string, string> = { 'content-type': 'application/json' }
  if (cors) headers['access-control-allow-origin'] = '*'
  return new Response(JSON.stringify(body), { status, headers })
}

function errorResponse(status: number, message: string, cors?: boolean): Response {
  return jsonResponse(status, { error: message }, cors)
}

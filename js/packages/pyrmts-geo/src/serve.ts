// HTTP handler for geo pyramids. Mirrors `serveQuery` in pyrmts-cfw but
// adds bbox + cell-budget params and runs through `planGeoQuery` +
// `filterCellsAndRes` before `stitch`.
//
// Query params:
//   from         ISO-8601 instant (required)
//   to           ISO-8601 instant (required)
//   bin_budget   max output time bins (default 1024)
//   bbox         "minLat,minLng,maxLat,maxLng" (required)
//   cell_budget  max output cells (default 1024)
//   <dim>=<v>    one per pyramid dim used in the key template

import {
  fetchSegmentRows,
  stitch,
  type Pyramid,
  type QueryPlan,
} from 'pyrmts'
import { filterCellsAndRes, planGeoQuery, type BBox } from './planner.js'

export interface ServeGeoOptions {
  pyramid: Pyramid
  request: Request
  watermarks?:
    | Record<string, Date>
    | ((req: Request) => Promise<Record<string, Date>> | Record<string, Date>)
  // Per-tier earliest-available-bin instants. See pyrmts-cfw `ServeOptions`.
  earliestWatermarks?:
    | Record<string, Date>
    | ((req: Request) => Promise<Record<string, Date>> | Record<string, Date>)
  // Treat missing shard objects as empty instead of erroring. See pyrmts-cfw
  // `ServeOptions`.
  tolerateMissingShards?: boolean
  cors?: boolean
}

export async function serveGeoQuery(opts: ServeGeoOptions): Promise<Response> {
  const { pyramid, request, cors } = opts

  if (pyramid.geo === undefined) {
    return errorResponse(500, 'serveGeoQuery: pyramid has no `geo` config', cors)
  }

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

  const bbox = parseBBox(url.searchParams.get('bbox'))
  if (bbox === null) {
    return errorResponse(400, 'bbox query param required (minLat,minLng,maxLat,maxLng)', cors)
  }

  const cellBudgetRaw = url.searchParams.get('cell_budget')
  const cellBudget = cellBudgetRaw === null ? 1024 : Number.parseInt(cellBudgetRaw, 10)
  if (!Number.isFinite(cellBudget) || cellBudget <= 0) {
    return errorResponse(400, `invalid cell_budget '${cellBudgetRaw}'`, cors)
  }

  const filter: Record<string, string> = {}
  for (const dim of pyramid.dims) {
    const v = url.searchParams.get(dim.name)
    if (v !== null) filter[dim.name] = v
  }

  const watermarks = await resolveWatermarks(opts.watermarks, request)
  const earliestWatermarks = await resolveWatermarks(opts.earliestWatermarks, request)

  let result: { records: unknown[]; plan: unknown }
  try {
    const plan = planGeoQuery(pyramid, {
      range: { from, to },
      binBudget,
      bbox,
      cellBudget,
      watermarks,
      earliestWatermarks,
      filter,
    })

    const shardRows = await Promise.all(
      plan.segments.map(seg => fetchSegmentRows(pyramid.storage, seg.keys, {
        binCol: pyramid.binCol,
        range: { from: seg.from, to: seg.to },
        ...(opts.tolerateMissingShards !== undefined ? { tolerate404: opts.tolerateMissingShards } : {}),
      })),
    )
    const filteredRows = shardRows.map(rows =>
      filterCellsAndRes(rows, pyramid.geo!.cellCol, plan.outputRes, plan.outputCells),
    )

    // Project the geo plan down to a plain QueryPlan for stitch (which
    // lives in pyrmts core and doesn't know about geo fields).
    const timePlan: QueryPlan = {
      outputTier: plan.outputTier,
      outputBin: plan.outputBin,
      segments: plan.segments.map(s => ({
        from: s.from,
        to: s.to,
        shardTier: s.shardTier,
        keys: s.keys,
        reaggregate: s.reaggregate,
      })),
      authoritativeEnd: plan.authoritativeEnd,
    }
    const records = stitch({ pyramid, plan: timePlan, shardRows: filteredRows })

    result = {
      records,
      plan: {
        outputTier: plan.outputTier.name,
        outputBin: plan.outputBin,
        outputRes: plan.outputRes,
        outputCells: plan.outputCells,
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

function parseBBox(s: string | null): BBox | null {
  if (s === null) return null
  const parts = s.split(',').map(p => Number.parseFloat(p.trim()))
  if (parts.length !== 4 || parts.some(n => !Number.isFinite(n))) return null
  const [minLat, minLng, maxLat, maxLng] = parts as [number, number, number, number]
  if (minLat > maxLat || minLng > maxLng) return null
  return { minLat, minLng, maxLat, maxLng }
}

async function resolveWatermarks(
  src: ServeGeoOptions['watermarks'] | ServeGeoOptions['earliestWatermarks'],
  request: Request,
): Promise<Record<string, Date>> {
  if (src === undefined) return {}
  if (typeof src === 'function') return await src(request)
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

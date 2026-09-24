// Key templates: `{name}` placeholders plus the content-hash token
// (../../../../specs/content-addressed-shards.md). Twin of Python `pyrmts.keys`.
//
// `{hash}` expands to the shard payload's full md5 (32 hex chars); `{hash:N}`
// to its first N (1..32) — the hashed-asset-filename convention (webpack
// `[contenthash:8]`, Vite `[hash:8]`: `:N` truncates). A template with a hash
// token yields immutable keys: a slot's *current* key lives in the registry
// row, never in the template, so readers must plan from the inventory
// (`planQueryFromInventory`) rather than derive keys here.
//
// Vocabulary: a *slot* is `(tier, shard_dur, period[, filter dims])`; its
// *slot key* (`slotKey`) is the template with everything but the hash
// substituted (equal to the storage key for a hashless template).

import { formatPeriod, parseDuration } from './axis.js'
import type { Pyramid, Shard } from './types.js'

const PLACEHOLDER = /\{(\w+)(?::(\d+))?\}/g
export const HASH = 'hash'
export const MD5_HEX_LEN = 32

/** Config-time check: `:N` is only defined for `{hash}`, and N is 1..32. */
export function validateKeyTemplate(template: string): void {
  for (const m of template.matchAll(PLACEHOLDER)) {
    const [, name, width] = m
    if (width === undefined) continue
    if (name !== HASH) {
      throw new Error(`keyTemplate: \`:${width}\` is only defined for {hash}, not {${name}} ('${template}')`)
    }
    const n = Number(width)
    if (!(n >= 1 && n <= MD5_HEX_LEN)) {
      throw new Error(`keyTemplate: {hash:${width}} must be 1..${MD5_HEX_LEN} ('${template}')`)
    }
  }
}

export function templateHasHash(template: string): boolean {
  return [...template.matchAll(PLACEHOLDER)].some(m => m[1] === HASH)
}

/** Hex chars the template's hash token keeps (32 for bare `{hash}`), or null. */
export function hashWidth(template: string): number | null {
  for (const m of template.matchAll(PLACEHOLDER)) {
    if (m[1] === HASH) return m[2] !== undefined ? Number(m[2]) : MD5_HEX_LEN
  }
  return null
}

/** Expand every placeholder. `values.hash` must be the payload's md5 hex
 * when the template has a hash token (`{hash:N}` keeps its first N). */
export function substituteKey(
  template: string,
  values: Record<string, string | number>,
): string {
  validateKeyTemplate(template)
  return template.replace(PLACEHOLDER, (_, name: string, width?: string) => {
    if (!(name in values)) {
      throw new Error(`substituteKey: missing value for {${name}}`)
    }
    const value = String(values[name])
    if (name === HASH) {
      if (!/^[0-9a-f]{32}$/.test(value)) {
        throw new Error(`substituteKey: {hash} wants a 32-char md5 hex, got '${value}'`)
      }
      return width !== undefined ? value.slice(0, Number(width)) : value
    }
    return value
  })
}

/** The template with every placeholder but `{hash}` substituted: a slot's
 * stable identity (and, for a hashless template, its storage key). */
export function slotKey(template: string, values: Record<string, string | number>): string {
  validateKeyTemplate(template)
  return template.replace(PLACEHOLDER, (whole, name: string) => {
    if (name === HASH) return whole
    if (!(name in values)) throw new Error(`slotKey: missing value for {${name}}`)
    return String(values[name])
  })
}

function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

/** A regex matching keys the template can produce, one named group per
 * placeholder (`hash` fixed-width `[0-9a-f]{N}`, others one path segment).
 * The inverse of `substituteKey` for listings. */
export function keyPattern(template: string): RegExp {
  validateKeyTemplate(template)
  const parts: string[] = []
  let pos = 0
  const seen = new Set<string>()
  for (const m of template.matchAll(PLACEHOLDER)) {
    parts.push(escapeRegExp(template.slice(pos, m.index)))
    const [whole, name, width] = m
    if (seen.has(name!)) {
      parts.push(`\\k<${name}>`)
    } else {
      seen.add(name!)
      if (name === HASH) parts.push(`(?<${name}>[0-9a-f]{${width !== undefined ? Number(width) : MD5_HEX_LEN}})`)
      else parts.push(`(?<${name}>[^/]+)`)
    }
    pos = m.index! + whole.length
  }
  parts.push(escapeRegExp(template.slice(pos)))
  return new RegExp(`^${parts.join('')}$`)
}

/** Placeholder values a key encodes, or null if it doesn't match the template. */
export function parseKey(template: string, key: string): Record<string, string> | null {
  const m = keyPattern(template).exec(key)
  return m === null ? null : { ...m.groups }
}

/** The slot key a storage key belongs to, or null if it doesn't match. */
export function slotOf(template: string, key: string): string | null {
  const values = parseKey(template, key)
  if (values === null) return null
  const { [HASH]: _hash, ...rest } = values
  return slotKey(template, rest)
}

// The slot key for one shard: the pyramid's `keyTemplate` with tier / shard /
// period (and any `filter` dims) substituted, `{hash}` left in place. For a
// hashless template this is the storage key. Twin of Python
// `pyrmts.gap_discovery._make_expected`'s key.
export function shardKey(
  pyramid: Pyramid,
  tierName: string,
  shardDur: Shard,
  periodStart: Date,
  filter: Record<string, string | number> = {},
): string {
  const span = parseDuration(shardDur)
  return slotKey(pyramid.keyTemplate, {
    ...filter,
    tier: tierName,
    shard: shardDur,
    period: formatPeriod(periodStart, span),
  })
}

// A response ETag for a query served from a set of shard keys. With
// content-hashed keys a shard's bytes never change under its key, so the set
// of keys a response was built from identifies the response: serve with a
// short `max-age` + revalidation, and a rewrite (registry swap) changes the
// tag. Order-independent, and a version prefix so a format change
// invalidates every cached tag at once.
export function keysEtag(keys: Iterable<string>, version = 1): string {
  const sorted = [...keys].sort()
  let h = 0x811c9dc5
  for (const ch of `${version}\u0000${sorted.join('\u0000')}`) {
    h ^= ch.charCodeAt(0)
    h = Math.imul(h, 0x01000193) >>> 0
  }
  return `"v${version}-${sorted.length}-${h.toString(16).padStart(8, '0')}"`
}

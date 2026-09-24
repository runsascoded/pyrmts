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
import { formatPeriod, parseDuration } from './axis.js';
const PLACEHOLDER = /\{(\w+)(?::(\d+))?\}/g;
export const HASH = 'hash';
export const MD5_HEX_LEN = 32;
/** Config-time check: `:N` is only defined for `{hash}`, and N is 1..32. */
export function validateKeyTemplate(template) {
    for (const m of template.matchAll(PLACEHOLDER)) {
        const [, name, width] = m;
        if (width === undefined)
            continue;
        if (name !== HASH) {
            throw new Error(`keyTemplate: \`:${width}\` is only defined for {hash}, not {${name}} ('${template}')`);
        }
        const n = Number(width);
        if (!(n >= 1 && n <= MD5_HEX_LEN)) {
            throw new Error(`keyTemplate: {hash:${width}} must be 1..${MD5_HEX_LEN} ('${template}')`);
        }
    }
}
export function templateHasHash(template) {
    return [...template.matchAll(PLACEHOLDER)].some(m => m[1] === HASH);
}
/** Hex chars the template's hash token keeps (32 for bare `{hash}`), or null. */
export function hashWidth(template) {
    for (const m of template.matchAll(PLACEHOLDER)) {
        if (m[1] === HASH)
            return m[2] !== undefined ? Number(m[2]) : MD5_HEX_LEN;
    }
    return null;
}
/** Expand every placeholder. `values.hash` must be the payload's md5 hex
 * when the template has a hash token (`{hash:N}` keeps its first N). */
export function substituteKey(template, values) {
    validateKeyTemplate(template);
    return template.replace(PLACEHOLDER, (_, name, width) => {
        if (!(name in values)) {
            throw new Error(`substituteKey: missing value for {${name}}`);
        }
        const value = String(values[name]);
        if (name === HASH) {
            if (!/^[0-9a-f]{32}$/.test(value)) {
                throw new Error(`substituteKey: {hash} wants a 32-char md5 hex, got '${value}'`);
            }
            return width !== undefined ? value.slice(0, Number(width)) : value;
        }
        return value;
    });
}
/** The template with every placeholder but `{hash}` substituted: a slot's
 * stable identity (and, for a hashless template, its storage key). */
export function slotKey(template, values) {
    validateKeyTemplate(template);
    return template.replace(PLACEHOLDER, (whole, name) => {
        if (name === HASH)
            return whole;
        if (!(name in values))
            throw new Error(`slotKey: missing value for {${name}}`);
        return String(values[name]);
    });
}
function escapeRegExp(s) {
    return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}
/** A regex matching keys the template can produce, one named group per
 * placeholder (`hash` fixed-width `[0-9a-f]{N}`, others one path segment).
 * The inverse of `substituteKey` for listings. */
export function keyPattern(template) {
    validateKeyTemplate(template);
    const parts = [];
    let pos = 0;
    const seen = new Set();
    for (const m of template.matchAll(PLACEHOLDER)) {
        parts.push(escapeRegExp(template.slice(pos, m.index)));
        const [whole, name, width] = m;
        if (seen.has(name)) {
            parts.push(`\\k<${name}>`);
        }
        else {
            seen.add(name);
            if (name === HASH)
                parts.push(`(?<${name}>[0-9a-f]{${width !== undefined ? Number(width) : MD5_HEX_LEN}})`);
            else
                parts.push(`(?<${name}>[^/]+)`);
        }
        pos = m.index + whole.length;
    }
    parts.push(escapeRegExp(template.slice(pos)));
    return new RegExp(`^${parts.join('')}$`);
}
/** Placeholder values a key encodes, or null if it doesn't match the template. */
export function parseKey(template, key) {
    const m = keyPattern(template).exec(key);
    return m === null ? null : { ...m.groups };
}
/** The slot key a storage key belongs to, or null if it doesn't match. */
export function slotOf(template, key) {
    const values = parseKey(template, key);
    if (values === null)
        return null;
    const { [HASH]: _hash, ...rest } = values;
    return slotKey(template, rest);
}
// The slot key for one shard: the pyramid's `keyTemplate` with tier / shard /
// period (and any `filter` dims) substituted, `{hash}` left in place. For a
// hashless template this is the storage key. Twin of Python
// `pyrmts.gap_discovery._make_expected`'s key.
export function shardKey(pyramid, tierName, shardDur, periodStart, filter = {}) {
    const span = parseDuration(shardDur);
    return slotKey(pyramid.keyTemplate, {
        ...filter,
        tier: tierName,
        shard: shardDur,
        period: formatPeriod(periodStart, span),
    });
}
export function keysEtag(entries, version = 1) {
    const parts = [...entries].map(e => {
        if (typeof e === 'string')
            return e;
        const at = e.writtenAt instanceof Date ? e.writtenAt.getTime() : e.writtenAt;
        return `${e.key}#${e.md5 ?? ''}#${at ?? ''}`;
    }).sort();
    const text = `${version}\u0000${parts.join('\u0000')}`;
    let h1 = 0x811c9dc5;
    let h2 = 0x01000193 ^ 0x811c9dc5;
    for (const ch of text) {
        const c = ch.charCodeAt(0);
        h1 = Math.imul(h1 ^ c, 0x01000193) >>> 0;
        h2 = Math.imul(h2 ^ c, 0x01000193) >>> 0;
        h2 = ((h2 << 13) | (h2 >>> 19)) >>> 0;
    }
    return `"v${version}-${parts.length}-${h1.toString(16).padStart(8, '0')}${h2.toString(16).padStart(8, '0')}"`;
}
//# sourceMappingURL=keys.js.map
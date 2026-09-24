import type { Pyramid, Shard } from './types.js';
export declare const HASH = "hash";
export declare const MD5_HEX_LEN = 32;
/** Config-time check: `:N` is only defined for `{hash}`, and N is 1..32. */
export declare function validateKeyTemplate(template: string): void;
export declare function templateHasHash(template: string): boolean;
/** Hex chars the template's hash token keeps (32 for bare `{hash}`), or null. */
export declare function hashWidth(template: string): number | null;
/** Expand every placeholder. `values.hash` must be the payload's md5 hex
 * when the template has a hash token (`{hash:N}` keeps its first N). */
export declare function substituteKey(template: string, values: Record<string, string | number>): string;
/** The template with every placeholder but `{hash}` substituted: a slot's
 * stable identity (and, for a hashless template, its storage key). */
export declare function slotKey(template: string, values: Record<string, string | number>): string;
/** A regex matching keys the template can produce, one named group per
 * placeholder (`hash` fixed-width `[0-9a-f]{N}`, others one path segment).
 * The inverse of `substituteKey` for listings. */
export declare function keyPattern(template: string): RegExp;
/** Placeholder values a key encodes, or null if it doesn't match the template. */
export declare function parseKey(template: string, key: string): Record<string, string> | null;
/** The slot key a storage key belongs to, or null if it doesn't match. */
export declare function slotOf(template: string, key: string): string | null;
export declare function shardKey(pyramid: Pyramid, tierName: string, shardDur: Shard, periodStart: Date, filter?: Record<string, string | number>): string;
export interface EtagEntry {
    key: string;
    md5?: string;
    writtenAt?: Date | number;
}
export declare function keysEtag(entries: Iterable<string | EtagEntry>, version?: number): string;
//# sourceMappingURL=keys.d.ts.map
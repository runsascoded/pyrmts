// Shared conformance suite for any `ShardIndex` impl. Both
// `ManifestShardIndex` (in pyrmts core) and `D1ShardIndex` (in
// pyrmts-cfw) call this so they're guaranteed observationally
// indistinguishable.
//
// Usage:
//
//   import { assertShardIndexConformance } from 'pyrmts'
//   describe('MyShardIndex', () => {
//     assertShardIndexConformance(() => new MyShardIndex(...))
//   })
//
// The factory is invoked once per test; impls should return a fresh,
// empty index each time so scenarios are isolated.
import { describe, expect, test } from 'vitest';
export function assertShardIndexConformance(makeIndex, opts = {}) {
    const inventory = opts.inventory ?? true;
    describe('ShardIndex conformance', () => {
        test('getWatermarks on unknown pyramid returns empty Map', async () => {
            const idx = makeIndex();
            expect(await idx.getWatermarks('avail')).toEqual(new Map());
        });
        test('recordShard → watermark visible at `${tier}@${shardDur}` key', async () => {
            const idx = makeIndex();
            await idx.recordShard({
                pyramidName: 'avail',
                tier: '15m',
                shardDur: '1d',
                periodStart: new Date('2026-06-21T14:00:00Z'),
                periodEnd: new Date('2026-06-21T15:00:00Z'),
                key: 'avail/15m/1d/2026-06-21.parquet',
            });
            expect(await idx.getWatermarks('avail')).toEqual(new Map([
                ['15m@1d', new Date('2026-06-21T15:00:00Z')],
            ]));
        });
        test('multiple (tier, shardDur) rows coexist in one pyramid', async () => {
            const idx = makeIndex();
            const base = {
                pyramidName: 'avail',
                periodStart: new Date('2026-06-21T14:00:00Z'),
                periodEnd: new Date('2026-06-21T15:00:00Z'),
                key: 'k',
            };
            await idx.recordShard({ ...base, tier: '15m', shardDur: '1d' });
            await idx.recordShard({ ...base, tier: '15m', shardDur: '1h' });
            await idx.recordShard({ ...base, tier: '15m', shardDur: '30min' });
            await idx.recordShard({ ...base, tier: '1h', shardDur: '1d' });
            const got = await idx.getWatermarks('avail');
            expect(Array.from(got.keys()).sort()).toEqual([
                '15m@1d', '15m@1h', '15m@30min', '1h@1d',
            ]);
        });
        test('monotonic guard: older periodEnd does not regress the watermark', async () => {
            const idx = makeIndex();
            await idx.recordShard({
                pyramidName: 'avail',
                tier: '15m',
                shardDur: '1d',
                periodStart: new Date('2026-06-21T14:00:00Z'),
                periodEnd: new Date('2026-06-21T15:00:00Z'),
                key: 'k1',
            });
            // Rewrite an earlier period — typical recovery case. The watermark
            // should stay at the later value.
            await idx.recordShard({
                pyramidName: 'avail',
                tier: '15m',
                shardDur: '1d',
                periodStart: new Date('2026-06-21T13:00:00Z'),
                periodEnd: new Date('2026-06-21T14:00:00Z'),
                key: 'k0',
            });
            expect(await idx.getWatermarks('avail')).toEqual(new Map([
                ['15m@1d', new Date('2026-06-21T15:00:00Z')],
            ]));
        });
        test('idempotency: re-recording the same shard does not duplicate or regress', async () => {
            const idx = makeIndex();
            const input = {
                pyramidName: 'avail',
                tier: '15m',
                shardDur: '1h',
                periodStart: new Date('2026-06-21T14:00:00Z'),
                periodEnd: new Date('2026-06-21T15:00:00Z'),
                key: 'k',
            };
            await idx.recordShard(input);
            await idx.recordShard(input);
            await idx.recordShard(input);
            expect(await idx.getWatermarks('avail')).toEqual(new Map([
                ['15m@1h', new Date('2026-06-21T15:00:00Z')],
            ]));
        });
        test('newer periodEnd advances the watermark', async () => {
            const idx = makeIndex();
            await idx.recordShard({
                pyramidName: 'avail',
                tier: '15m',
                shardDur: '1d',
                periodStart: new Date('2026-06-21T14:00:00Z'),
                periodEnd: new Date('2026-06-21T15:00:00Z'),
                key: 'k1',
            });
            await idx.recordShard({
                pyramidName: 'avail',
                tier: '15m',
                shardDur: '1d',
                periodStart: new Date('2026-06-21T15:00:00Z'),
                periodEnd: new Date('2026-06-21T16:00:00Z'),
                key: 'k2',
            });
            expect(await idx.getWatermarks('avail')).toEqual(new Map([
                ['15m@1d', new Date('2026-06-21T16:00:00Z')],
            ]));
        });
        if (inventory) {
            test('listShards on unknown pyramid returns empty array', async () => {
                const idx = makeIndex();
                expect(await idx.listShards('avail')).toEqual([]);
            });
            test('listShards returns recorded inventory rows', async () => {
                const idx = makeIndex();
                const base = {
                    pyramidName: 'avail',
                    key: 'k',
                };
                await idx.recordShard({
                    ...base,
                    tier: '15m',
                    shardDur: '1h',
                    periodStart: new Date('2026-06-21T14:00:00Z'),
                    periodEnd: new Date('2026-06-21T15:00:00Z'),
                    key: 'avail/15m/1h/2026-06-21T14.parquet',
                });
                await idx.recordShard({
                    ...base,
                    tier: '15m',
                    shardDur: '1h',
                    periodStart: new Date('2026-06-21T15:00:00Z'),
                    periodEnd: new Date('2026-06-21T16:00:00Z'),
                    key: 'avail/15m/1h/2026-06-21T15.parquet',
                });
                await idx.recordShard({
                    ...base,
                    tier: '1h',
                    shardDur: '1d',
                    periodStart: new Date('2026-06-21T00:00:00Z'),
                    periodEnd: new Date('2026-06-22T00:00:00Z'),
                    key: 'avail/1h/1d/2026-06-21.parquet',
                });
                const got = await idx.listShards('avail');
                const sorted = [...got].sort((a, b) => {
                    if (a.tier !== b.tier)
                        return a.tier.localeCompare(b.tier);
                    if (a.shardDur !== b.shardDur)
                        return String(a.shardDur).localeCompare(String(b.shardDur));
                    return a.periodStart.getTime() - b.periodStart.getTime();
                });
                expect(sorted).toEqual([
                    {
                        tier: '15m', shardDur: '1h',
                        periodStart: new Date('2026-06-21T14:00:00Z'),
                        periodEnd: new Date('2026-06-21T15:00:00Z'),
                        key: 'avail/15m/1h/2026-06-21T14.parquet',
                    },
                    {
                        tier: '15m', shardDur: '1h',
                        periodStart: new Date('2026-06-21T15:00:00Z'),
                        periodEnd: new Date('2026-06-21T16:00:00Z'),
                        key: 'avail/15m/1h/2026-06-21T15.parquet',
                    },
                    {
                        tier: '1h', shardDur: '1d',
                        periodStart: new Date('2026-06-21T00:00:00Z'),
                        periodEnd: new Date('2026-06-22T00:00:00Z'),
                        key: 'avail/1h/1d/2026-06-21.parquet',
                    },
                ]);
            });
            test('listShards: re-recording overwrites in place (no duplicates)', async () => {
                const idx = makeIndex();
                const input = {
                    pyramidName: 'avail',
                    tier: '15m',
                    shardDur: '1h',
                    periodStart: new Date('2026-06-21T14:00:00Z'),
                    periodEnd: new Date('2026-06-21T15:00:00Z'),
                    key: 'k1',
                };
                await idx.recordShard(input);
                await idx.recordShard({ ...input, key: 'k2' });
                const got = await idx.listShards('avail');
                expect(got).toHaveLength(1);
                expect(got[0].key).toBe('k2');
            });
        }
        else {
            test('listShards throws when inventory is disabled', async () => {
                const idx = makeIndex();
                await idx.recordShard({
                    pyramidName: 'avail',
                    tier: '15m',
                    shardDur: '1h',
                    periodStart: new Date('2026-06-21T14:00:00Z'),
                    periodEnd: new Date('2026-06-21T15:00:00Z'),
                    key: 'k',
                });
                await expect(idx.listShards('avail')).rejects.toThrow(/inventory/);
            });
        }
    });
}
//# sourceMappingURL=shard-index-conformance.js.map
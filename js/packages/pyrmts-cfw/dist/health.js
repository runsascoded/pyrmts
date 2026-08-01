// Pyramid health SDK (`specs/pyrmts-ops-adoption.md` phase 4 — absorbed
// from ctbk `gbfs/api/src/health.ts`'s generic pieces).
//
// - `pyramidCover`: per-tier min-cover status of a registry pyramid —
//   which cover slots are present in the shard registry, which are
//   pending (just-closed, within the write-lag grace window), which are
//   missing, and how many registered shards fell out of the current
//   min-cover (stale — GC candidates). Feeds a /health timeline bar.
// - `BuildProgress` + `getBuildsHealth`: reader for the driver-written
//   build-progress docs (`pyrmts_ops.rebuild.BuildProgress` is the
//   writer — one JSON contract, defined by these types).
// - `readCachedSnapshot` / `computeAndStoreSnapshot`: the
//   cron-refreshed-snapshot pattern for expensive health computations.
//
// Consumers keep their pyramid registry (which pyramids to surface,
// genesis dates, key prefixes) and any app-specific health sections;
// the cover math, doc contract, and cache pattern live here.
import { floorToSpan, listExpectedShards, parseDuration, shardBuildableAt, } from 'pyrmts';
/** A cover slot whose period closed within this window and hasn't been
 *  registered yet counts as `pending`, not `missing`. */
export const PENDING_GRACE_MS = 10 * 60_000;
/** Min-cover status for one registry pyramid. `pyramid` needs only
 *  `tiers` + `keyTemplate` (all `listExpectedShards` reads). Returns
 *  null when the registry is unavailable or the pyramid has no rows yet
 *  (hidden rather than all-missing). */
export async function pyramidCover(db, pyramid, opts) {
    const { name, genesis, now = new Date(), pendingGraceMs = PENDING_GRACE_MS, tableName = 'pyramid_shards', shardCol = 'shard_dur', } = opts;
    const tiers = pyramid.tiers;
    const largestPerTier = Object.fromEntries(tiers.map((t) => [t.name, String(t.shards[t.shards.length - 1])]));
    const shardSql = `SELECT tier, ${shardCol} AS sd, period_start ` +
        `FROM ${tableName} WHERE pyramid = ?`;
    let shardRows = [];
    try {
        const s = await db.prepare(shardSql).bind(name).all();
        shardRows = s.results ?? [];
    }
    catch {
        return null; // registry unavailable
    }
    if (shardRows.length === 0)
        return null;
    // Translate legacy canonical sentinel rows: '' → tier's largest shard.
    const norm = (tier, sd) => sd === '' ? (largestPerTier[tier] ?? sd) : sd;
    const presentKey = (tier, sd, periodStartMs) => `${tier}\x00${sd}\x00${periodStartMs}`;
    const present = new Set();
    const presentByTier = {};
    for (const r of shardRows) {
        const sd = norm(r.tier, r.sd);
        present.add(presentKey(r.tier, sd, r.period_start));
        presentByTier[r.tier] = (presentByTier[r.tier] ?? 0) + 1;
    }
    const expected = listExpectedShards(pyramid, { from: genesis, to: now });
    const expectedByTier = new Map();
    for (const e of expected) {
        const list = expectedByTier.get(e.tier) ?? [];
        list.push(e);
        expectedByTier.set(e.tier, list);
    }
    const tierStatuses = [];
    let totalMissing = 0;
    let totalPending = 0;
    let totalStale = 0;
    for (const t of tiers) {
        const maxRung = String(t.shards[t.shards.length - 1]);
        const lastMaxBoundary = floorToSpan(now, parseDuration(maxRung));
        const cover = expectedByTier.get(t.name) ?? [];
        const rungOrder = [];
        const rungAgg = {};
        const segments = [];
        let firstMissingPeriod = null;
        let coverPresent = 0;
        let coverPending = 0;
        for (const slot of cover) {
            const sd = String(slot.shardDur);
            const role = sd === maxRung ? 'max' : 'dust';
            if (!(sd in rungAgg)) {
                rungOrder.push(sd);
                rungAgg[sd] = { expected: 0, present: 0, pending: 0, role };
            }
            const agg = rungAgg[sd];
            agg.expected += 1;
            const key = presentKey(t.name, sd, slot.periodStart.getTime());
            let status;
            let buildableAtIso;
            if (present.has(key)) {
                status = 'present';
                agg.present += 1;
                coverPresent += 1;
            }
            else {
                // Grace is measured from when a cron tick can first land the
                // shard — later than periodEnd when the source cover is still
                // open (structural lag; `buildableAt == periodEnd` otherwise).
                const buildableAt = shardBuildableAt(pyramid, t.name, slot.periodEnd);
                if (buildableAt.getTime() > slot.periodEnd.getTime()) {
                    buildableAtIso = buildableAt.toISOString();
                }
                if (buildableAt.getTime() > now.getTime() - pendingGraceMs) {
                    status = 'pending';
                    agg.pending += 1;
                    coverPending += 1;
                }
                else {
                    status = 'missing';
                    if (firstMissingPeriod === null) {
                        firstMissingPeriod = slot.periodStart.toISOString();
                    }
                }
            }
            // Clip the head tile to genesis so the bar's x-domain is exact.
            const segStart = slot.effectiveStart > slot.periodStart ? slot.effectiveStart : slot.periodStart;
            segments.push({
                start: segStart.toISOString(),
                end: slot.periodEnd.toISOString(),
                shardDur: sd,
                status,
                ...(status === 'present' ? { key: slot.key } : {}),
                ...(buildableAtIso !== undefined ? { buildableAt: buildableAtIso } : {}),
            });
        }
        const rungs = rungOrder.map((sd) => ({
            shardDur: sd,
            role: rungAgg[sd].role,
            expected: rungAgg[sd].expected,
            present: rungAgg[sd].present,
            pending: rungAgg[sd].pending,
        }));
        const totalExpected = cover.length;
        const tierMissing = totalExpected - coverPresent - coverPending;
        const staleShardCount = Math.max(0, (presentByTier[t.name] ?? 0) - coverPresent);
        totalMissing += tierMissing;
        totalPending += coverPending;
        totalStale += staleShardCount;
        tierStatuses.push({
            tier: t.name,
            bin: String(t.bin),
            maxRung,
            rungs,
            segments,
            totalExpected,
            totalPresent: coverPresent,
            totalPending: coverPending,
            complete: tierMissing === 0,
            firstMissingPeriod,
            lastMaxBoundary: lastMaxBoundary.toISOString(),
            dustAgeSec: Math.floor((now.getTime() - lastMaxBoundary.getTime()) / 1000),
            staleShardCount,
        });
    }
    return {
        name,
        genesis: genesis.toISOString(),
        now: now.toISOString(),
        tiers: tierStatuses,
        totalMissing,
        totalPending,
        totalStale,
        allComplete: totalMissing === 0,
    };
}
const decoder = new TextDecoder();
async function getJson(storage, key) {
    const bytes = await storage.get(key);
    if (bytes === null)
        return null;
    try {
        return JSON.parse(decoder.decode(bytes));
    }
    catch {
        return null; // malformed doc — skip
    }
}
/** Recent driver build-progress docs (running builds first, then most
 *  recently updated; anything idle past `maxIdleMs` is dropped). */
export async function getBuildsHealth(storage, opts) {
    const { prefix, limit = 20, maxIdleMs = 7 * 86_400_000, now = Date.now() } = opts;
    const keys = [];
    for await (const key of storage.list(prefix)) {
        keys.push(key);
        if (keys.length >= limit)
            break;
    }
    const out = [];
    for (const key of keys) {
        const doc = await getJson(storage, key);
        if (doc === null)
            continue;
        const updated = Date.parse(doc.updatedAt ?? doc.startedAt ?? '');
        if (Number.isFinite(updated) && now - updated < maxIdleMs)
            out.push(doc);
    }
    out.sort((a, b) => Number(b.status === 'running') - Number(a.status === 'running')
        || Date.parse(b.updatedAt ?? '0') - Date.parse(a.updatedAt ?? '0'));
    return out;
}
/** Read a cron-refreshed snapshot if it's fresher than `maxAgeS`; null →
 *  the caller computes live (and should persist the result for the next
 *  request via `computeAndStoreSnapshot`). */
export async function readCachedSnapshot(storage, key, maxAgeS, now = Date.now()) {
    const snapshot = await getJson(storage, key);
    if (snapshot === null)
        return null;
    const ageS = Math.floor(now / 1000) - (snapshot.generatedAt ?? 0);
    return ageS <= maxAgeS ? snapshot : null;
}
const encoder = new TextEncoder();
/** Compute a snapshot and persist it (the cron path). */
export async function computeAndStoreSnapshot(storage, key, compute) {
    const snapshot = await compute();
    await storage.put(key, encoder.encode(JSON.stringify(snapshot)));
    return snapshot;
}
//# sourceMappingURL=health.js.map
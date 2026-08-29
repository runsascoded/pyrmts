// `D1ShardIndex` — Cloudflare D1 (SQLite) implementation of pyrmts's
// `ShardIndex` interface.
//
// Schema (see `D1ShardIndex.schemaSql()`):
//
//   pyramid_watermarks(pyramid, tier, shard_dur, latest_period_end, updated_at)
//     PRIMARY KEY (pyramid, tier, shard_dur)
//
//   pyramid_shards(pyramid, tier, shard_dur, period_start, period_end, key, written_at)
//     PRIMARY KEY (pyramid, tier, shard_dur, period_start)
//
// One row per `(tier, shard_dur)` — no canonical/partial dichotomy. See
// `specs/done/unified-shard-ladder.md` §Watermark grid.
import { encodeWatermarkKey, } from 'pyrmts';
const DEFAULT_WATERMARKS_TABLE = 'pyramid_watermarks';
const DEFAULT_SHARDS_TABLE = 'pyramid_shards';
function quoteIdent(name) {
    return `"${name.replace(/"/g, '""')}"`;
}
export class D1ShardIndex {
    db;
    watermarksTable;
    shardsTable;
    skipInventory;
    now;
    constructor(db, opts = {}) {
        this.db = db;
        this.watermarksTable = opts.watermarksTable ?? DEFAULT_WATERMARKS_TABLE;
        this.shardsTable = opts.shardsTable ?? DEFAULT_SHARDS_TABLE;
        this.skipInventory = opts.skipInventory ?? false;
        this.now = opts.now ?? Date.now;
    }
    async getWatermarks(pyramidName) {
        const sql = `SELECT tier, shard_dur, latest_period_end FROM ${quoteIdent(this.watermarksTable)} WHERE pyramid = ?`;
        const res = await this.db.prepare(sql).bind(pyramidName).all();
        const out = new Map();
        for (const row of res.results) {
            out.set(encodeWatermarkKey(row.tier, row.shard_dur), new Date(row.latest_period_end));
        }
        return out;
    }
    async listShards(pyramidName, filter) {
        if (this.skipInventory) {
            throw new Error(`D1ShardIndex.listShards: skipInventory was enabled at construction; ` +
                `no per-shard inventory was recorded. Re-create with ` +
                `{ skipInventory: false } to use gap discovery.`);
        }
        const clauses = ['pyramid = ?'];
        const binds = [pyramidName];
        if (filter?.tier !== undefined) {
            clauses.push('tier = ?');
            binds.push(filter.tier);
        }
        if (filter?.range !== undefined) {
            // Intersect: period_end > range.from AND period_start < range.to.
            clauses.push('period_end > ?', 'period_start < ?');
            binds.push(filter.range.from.getTime(), filter.range.to.getTime());
        }
        const sql = `SELECT tier, shard_dur, period_start, period_end, key, written_at ` +
            `FROM ${quoteIdent(this.shardsTable)} WHERE ${clauses.join(' AND ')}`;
        const res = await this.db.prepare(sql).bind(...binds).all();
        return res.results.map(row => ({
            tier: row.tier,
            shardDur: row.shard_dur,
            periodStart: new Date(row.period_start),
            periodEnd: new Date(row.period_end),
            key: row.key,
            writtenAt: new Date(row.written_at),
        }));
    }
    async recordShard(input) {
        const now = this.now();
        const periodEndMs = input.periodEnd.getTime();
        const periodStartMs = input.periodStart.getTime();
        const shardDur = String(input.shardDur);
        const watermarkSql = `INSERT INTO ${quoteIdent(this.watermarksTable)} ` +
            `(pyramid, tier, shard_dur, latest_period_end, updated_at) VALUES (?, ?, ?, ?, ?) ` +
            `ON CONFLICT(pyramid, tier, shard_dur) DO UPDATE SET ` +
            `latest_period_end = MAX(excluded.latest_period_end, ${quoteIdent(this.watermarksTable)}.latest_period_end), ` +
            `updated_at = excluded.updated_at`;
        const watermarkStmt = this.db.prepare(watermarkSql).bind(input.pyramidName, input.tier, shardDur, periodEndMs, now);
        if (this.skipInventory) {
            await runStatement(watermarkStmt);
            return;
        }
        const shardsSql = `INSERT INTO ${quoteIdent(this.shardsTable)} ` +
            `(pyramid, tier, shard_dur, period_start, period_end, key, written_at) VALUES (?, ?, ?, ?, ?, ?, ?) ` +
            `ON CONFLICT(pyramid, tier, shard_dur, period_start) DO UPDATE SET ` +
            `period_end = excluded.period_end, key = excluded.key, written_at = excluded.written_at`;
        const shardsStmt = this.db.prepare(shardsSql).bind(input.pyramidName, input.tier, shardDur, periodStartMs, periodEndMs, input.key, now);
        // Prefer atomic batch when the D1 impl supports it; else fall back
        // to sequential run() (mock D1s in tests typically only do prepare/all).
        if (this.db.batch !== undefined) {
            await this.db.batch([watermarkStmt, shardsStmt]);
        }
        else {
            await runStatement(watermarkStmt);
            await runStatement(shardsStmt);
        }
    }
    // SQL DDL to create the backing tables. Consumers apply via wrangler
    // migrations or via `db.exec()` in a setup script for fixtures.
    //
    // Returns one statement per table in dependency order. WITHOUT ROWID
    // makes the tables key-only — saves space and guarantees strict
    // PK-driven uniqueness.
    //
    // The shards table also gets a `(pyramid, period_end)` secondary index:
    // a windowed `listShards` pins neither `tier` nor `shard_dur`, so the PK
    // can only seek to the pyramid partition and must scan all of it —
    // O(shards in pyramid) rows read per call, growing silently with history
    // (measured 857× amplification at 14.5K shards). `period_end > from` is
    // the selective predicate, and on a WITHOUT ROWID table the index entries
    // carry the PK columns, so the index is near-covering. Cost: one extra
    // index entry per `recordShard` — D1 bills writes per row, so a hot
    // ingest path pays 1 additional row-write per shard recorded.
    //
    // All statements are `IF NOT EXISTS`, so re-running `schemaSql()` is the
    // migration path for deployments provisioned before the index existed.
    static schemaSql(opts = {}) {
        return D1ShardIndex.schemaObjects(opts).map(o => o.sql);
    }
    // The objects the index needs, each with the columns that make it correct.
    // `schemaSql()` is this, projected; `verifySchema()` diffs it against a
    // live database. Python's twin is `pyrmts.d1.schema_objects()`.
    static schemaObjects(opts = {}) {
        const watermarksTable = opts.watermarksTable ?? DEFAULT_WATERMARKS_TABLE;
        const shardsTable = opts.shardsTable ?? DEFAULT_SHARDS_TABLE;
        const w = quoteIdent(watermarksTable);
        const s = quoteIdent(shardsTable);
        const out = [
            {
                name: watermarksTable,
                kind: 'table',
                sql: `CREATE TABLE IF NOT EXISTS ${w} (\n` +
                    `  pyramid TEXT NOT NULL,\n` +
                    `  tier TEXT NOT NULL,\n` +
                    `  shard_dur TEXT NOT NULL,\n` +
                    `  latest_period_end INTEGER NOT NULL,\n` +
                    `  updated_at INTEGER NOT NULL,\n` +
                    `  PRIMARY KEY (pyramid, tier, shard_dur)\n` +
                    `) WITHOUT ROWID`,
                columns: ['pyramid', 'tier', 'shard_dur', 'latest_period_end', 'updated_at'],
            },
        ];
        if (!(opts.skipInventory ?? false)) {
            out.push({
                name: shardsTable,
                kind: 'table',
                sql: `CREATE TABLE IF NOT EXISTS ${s} (\n` +
                    `  pyramid TEXT NOT NULL,\n` +
                    `  tier TEXT NOT NULL,\n` +
                    `  shard_dur TEXT NOT NULL,\n` +
                    `  period_start INTEGER NOT NULL,\n` +
                    `  period_end INTEGER NOT NULL,\n` +
                    `  key TEXT NOT NULL,\n` +
                    `  written_at INTEGER NOT NULL,\n` +
                    `  PRIMARY KEY (pyramid, tier, shard_dur, period_start)\n` +
                    `) WITHOUT ROWID`,
                columns: [
                    'pyramid', 'tier', 'shard_dur', 'period_start', 'period_end',
                    'key', 'written_at',
                ],
            }, {
                name: `${shardsTable}_period`,
                kind: 'index',
                sql: `CREATE INDEX IF NOT EXISTS ${quoteIdent(`${shardsTable}_period`)} ` +
                    `ON ${s} (pyramid, period_end)`,
                columns: ['pyramid', 'period_end'],
            });
        }
        return out;
    }
    // Diff a live database against `schemaObjects()`, read-only.
    //
    // Cheap enough to surface from a health endpoint, which is the point: the
    // DDL is library-owned but applied by the consumer, so without a check
    // there is nothing that notices a deployment provisioned before a schema
    // change (both consumers were missing `pyramid_shards_period` for exactly
    // this reason). `sqlite_master` and `PRAGMA table_info`/`index_info` are
    // all supported by D1.
    static async verifySchema(db, opts = {}) {
        const expected = D1ShardIndex.schemaObjects(opts);
        const placeholders = expected.map(() => '?').join(', ');
        const found = await db
            .prepare(`SELECT type, name FROM sqlite_master WHERE name IN (${placeholders})`)
            .bind(...expected.map(o => o.name))
            .all();
        const live = new Set(found.results.map(r => r.name));
        const missing = [];
        const mismatched = [];
        for (const o of expected) {
            if (!live.has(o.name)) {
                missing.push(o.name);
                continue;
            }
            const pragma = o.kind === 'table' ? 'table_info' : 'index_info';
            const info = await db
                .prepare(`PRAGMA ${pragma}(${quoteIdent(o.name)})`)
                .bind()
                .all();
            const actual = info.results.map(r => r.name);
            if (o.kind === 'table') {
                // Column order is not load-bearing for a table (SELECTs name their
                // columns); an index's order is.
                const a = [...actual].sort();
                const e = [...o.columns].sort();
                if (a.length !== e.length || a.some((c, k) => c !== e[k])) {
                    mismatched.push(`${o.name}: expected=${JSON.stringify(e)} actual=${JSON.stringify(a)}`);
                }
            }
            else if (actual.length !== o.columns.length || actual.some((c, k) => c !== o.columns[k])) {
                mismatched.push(`${o.name}: expected=${JSON.stringify(o.columns)} actual=${JSON.stringify(actual)}`);
            }
        }
        return { ok: missing.length === 0 && mismatched.length === 0, missing, mismatched };
    }
}
async function runStatement(stmt) {
    if (stmt.run === undefined) {
        // Impls that lack run() (some test mocks) accept all() as a no-op
        // for non-result statements. Cast through never to bypass the
        // result-type check.
        await stmt.all();
        return;
    }
    await stmt.run();
}
//# sourceMappingURL=shard-index.js.map
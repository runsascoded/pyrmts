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

import {
  type RecordShardInput,
  type RecordedShard,
  type Shard,
  type ShardIndex,
  encodeWatermarkKey,
} from 'pyrmts'
import type { D1Like, D1PreparedStatement } from './d1.js'

export interface D1ShardIndexOptions {
  // Override table names if the consumer wants a different schema layout
  // (e.g. multi-tenant `tenant_pyramid_watermarks`).
  watermarksTable?: string  // default 'pyramid_watermarks'
  shardsTable?: string      // default 'pyramid_shards'
  // Skip writes to the inventory table. Default false (record both).
  // Use when storage cost matters more than per-shard auditability.
  skipInventory?: boolean
  // Injectable clock (epoch ms) for `updated_at` / `written_at`.
  // Default `Date.now`.
  now?: () => number
}

const DEFAULT_WATERMARKS_TABLE = 'pyramid_watermarks'
const DEFAULT_SHARDS_TABLE = 'pyramid_shards'

function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`
}

export class D1ShardIndex implements ShardIndex {
  private readonly watermarksTable: string
  private readonly shardsTable: string
  private readonly skipInventory: boolean
  private readonly now: () => number

  constructor(private readonly db: D1Like, opts: D1ShardIndexOptions = {}) {
    this.watermarksTable = opts.watermarksTable ?? DEFAULT_WATERMARKS_TABLE
    this.shardsTable = opts.shardsTable ?? DEFAULT_SHARDS_TABLE
    this.skipInventory = opts.skipInventory ?? false
    this.now = opts.now ?? Date.now
  }

  async getWatermarks(pyramidName: string): Promise<Map<string, Date>> {
    const sql = `SELECT tier, shard_dur, latest_period_end FROM ${quoteIdent(this.watermarksTable)} WHERE pyramid = ?`
    const res = await this.db.prepare(sql).bind(pyramidName).all<{
      tier: string
      shard_dur: string
      latest_period_end: number
    }>()
    const out = new Map<string, Date>()
    for (const row of res.results) {
      out.set(encodeWatermarkKey(row.tier, row.shard_dur as Shard), new Date(row.latest_period_end))
    }
    return out
  }

  async listShards(pyramidName: string): Promise<RecordedShard[]> {
    if (this.skipInventory) {
      throw new Error(
        `D1ShardIndex.listShards: skipInventory was enabled at construction; ` +
        `no per-shard inventory was recorded. Re-create with ` +
        `{ skipInventory: false } to use gap discovery.`,
      )
    }
    const sql =
      `SELECT tier, shard_dur, period_start, period_end, key ` +
      `FROM ${quoteIdent(this.shardsTable)} WHERE pyramid = ?`
    const res = await this.db.prepare(sql).bind(pyramidName).all<{
      tier: string
      shard_dur: string
      period_start: number
      period_end: number
      key: string
    }>()
    return res.results.map(row => ({
      tier: row.tier,
      shardDur: row.shard_dur as Shard,
      periodStart: new Date(row.period_start),
      periodEnd: new Date(row.period_end),
      key: row.key,
    }))
  }

  async recordShard(input: RecordShardInput): Promise<void> {
    const now = this.now()
    const periodEndMs = input.periodEnd.getTime()
    const periodStartMs = input.periodStart.getTime()
    const shardDur = String(input.shardDur)

    const watermarkSql =
      `INSERT INTO ${quoteIdent(this.watermarksTable)} ` +
      `(pyramid, tier, shard_dur, latest_period_end, updated_at) VALUES (?, ?, ?, ?, ?) ` +
      `ON CONFLICT(pyramid, tier, shard_dur) DO UPDATE SET ` +
      `latest_period_end = MAX(excluded.latest_period_end, ${quoteIdent(this.watermarksTable)}.latest_period_end), ` +
      `updated_at = excluded.updated_at`
    const watermarkStmt = this.db.prepare(watermarkSql).bind(
      input.pyramidName, input.tier, shardDur, periodEndMs, now,
    )

    if (this.skipInventory) {
      await runStatement(watermarkStmt)
      return
    }

    const shardsSql =
      `INSERT INTO ${quoteIdent(this.shardsTable)} ` +
      `(pyramid, tier, shard_dur, period_start, period_end, key, written_at) VALUES (?, ?, ?, ?, ?, ?, ?) ` +
      `ON CONFLICT(pyramid, tier, shard_dur, period_start) DO UPDATE SET ` +
      `period_end = excluded.period_end, key = excluded.key, written_at = excluded.written_at`
    const shardsStmt = this.db.prepare(shardsSql).bind(
      input.pyramidName, input.tier, shardDur,
      periodStartMs, periodEndMs, input.key, now,
    )

    // Prefer atomic batch when the D1 impl supports it; else fall back
    // to sequential run() (mock D1s in tests typically only do prepare/all).
    if (this.db.batch !== undefined) {
      await this.db.batch([watermarkStmt, shardsStmt])
    } else {
      await runStatement(watermarkStmt)
      await runStatement(shardsStmt)
    }
  }

  // SQL DDL to create the backing tables. Consumers apply via wrangler
  // migrations or via `db.exec()` in a setup script for fixtures.
  //
  // Returns one statement per table in dependency order. WITHOUT ROWID
  // makes the tables key-only — saves space and guarantees strict
  // PK-driven uniqueness.
  static schemaSql(opts: D1ShardIndexOptions = {}): string[] {
    const w = quoteIdent(opts.watermarksTable ?? DEFAULT_WATERMARKS_TABLE)
    const s = quoteIdent(opts.shardsTable ?? DEFAULT_SHARDS_TABLE)
    const out = [
      `CREATE TABLE IF NOT EXISTS ${w} (\n` +
      `  pyramid TEXT NOT NULL,\n` +
      `  tier TEXT NOT NULL,\n` +
      `  shard_dur TEXT NOT NULL,\n` +
      `  latest_period_end INTEGER NOT NULL,\n` +
      `  updated_at INTEGER NOT NULL,\n` +
      `  PRIMARY KEY (pyramid, tier, shard_dur)\n` +
      `) WITHOUT ROWID`,
    ]
    if (!(opts.skipInventory ?? false)) {
      out.push(
        `CREATE TABLE IF NOT EXISTS ${s} (\n` +
        `  pyramid TEXT NOT NULL,\n` +
        `  tier TEXT NOT NULL,\n` +
        `  shard_dur TEXT NOT NULL,\n` +
        `  period_start INTEGER NOT NULL,\n` +
        `  period_end INTEGER NOT NULL,\n` +
        `  key TEXT NOT NULL,\n` +
        `  written_at INTEGER NOT NULL,\n` +
        `  PRIMARY KEY (pyramid, tier, shard_dur, period_start)\n` +
        `) WITHOUT ROWID`,
      )
    }
    return out
  }
}

async function runStatement(stmt: D1PreparedStatement): Promise<void> {
  if (stmt.run === undefined) {
    // Impls that lack run() (some test mocks) accept all() as a no-op
    // for non-result statements. Cast through never to bypass the
    // result-type check.
    await (stmt.all as () => Promise<unknown>)()
    return
  }
  await stmt.run()
}

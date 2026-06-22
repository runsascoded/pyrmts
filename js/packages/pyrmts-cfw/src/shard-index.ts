// `D1ShardIndex` — Cloudflare D1 (SQLite) implementation of pyrmts's
// `ShardIndex` interface.
//
// Schema (see `D1ShardIndex.schemaSql()`):
//
//   pyramid_watermarks(pyramid, tier, cadence, latest_period_end, updated_at)
//     PRIMARY KEY (pyramid, tier, cadence)
//
//   pyramid_shards(pyramid, tier, cadence, period_start, period_end, key, written_at)
//     PRIMARY KEY (pyramid, tier, cadence, period_start)
//
// Sentinel: SQLite allows NULL in PRIMARY KEY columns without enforcing
// uniqueness (a long-standing footgun documented in SQLite's own
// guidance), so this impl uses `cadence = ''` (empty string) as the
// canonical-shard sentinel and translates at the interface boundary —
// `cadence: null` ↔ `''`. The exported `ShardIndex` contract still
// surfaces `Duration | null`; callers don't see the empty-string SQL
// detail. See `specs/partial-shards.md` §Watermark index for the
// schema/contract rationale.

import {
  type Duration,
  type RecordShardInput,
  type ShardIndex,
  encodeWatermarkKey,
} from 'pyrmts'
import type { D1Like, D1PreparedStatement } from './d1.js'

const CANONICAL_SENTINEL = ''

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
    const sql = `SELECT tier, cadence, latest_period_end FROM ${quoteIdent(this.watermarksTable)} WHERE pyramid = ?`
    const res = await this.db.prepare(sql).bind(pyramidName).all<{
      tier: string
      cadence: string
      latest_period_end: number
    }>()
    const out = new Map<string, Date>()
    for (const row of res.results) {
      const cadence: Duration | null = row.cadence === CANONICAL_SENTINEL ? null : (row.cadence as Duration)
      out.set(encodeWatermarkKey(row.tier, cadence), new Date(row.latest_period_end))
    }
    return out
  }

  async recordShard(input: RecordShardInput): Promise<void> {
    const cadence = input.cadence ?? CANONICAL_SENTINEL
    const now = this.now()
    const periodEndMs = input.periodEnd.getTime()
    const periodStartMs = input.periodStart.getTime()

    const watermarkSql =
      `INSERT INTO ${quoteIdent(this.watermarksTable)} ` +
      `(pyramid, tier, cadence, latest_period_end, updated_at) VALUES (?, ?, ?, ?, ?) ` +
      `ON CONFLICT(pyramid, tier, cadence) DO UPDATE SET ` +
      `latest_period_end = MAX(excluded.latest_period_end, ${quoteIdent(this.watermarksTable)}.latest_period_end), ` +
      `updated_at = excluded.updated_at`
    const watermarkStmt = this.db.prepare(watermarkSql).bind(
      input.pyramidName, input.tier, cadence, periodEndMs, now,
    )

    if (this.skipInventory) {
      await runStatement(watermarkStmt)
      return
    }

    const shardsSql =
      `INSERT INTO ${quoteIdent(this.shardsTable)} ` +
      `(pyramid, tier, cadence, period_start, period_end, key, written_at) VALUES (?, ?, ?, ?, ?, ?, ?) ` +
      `ON CONFLICT(pyramid, tier, cadence, period_start) DO UPDATE SET ` +
      `period_end = excluded.period_end, key = excluded.key, written_at = excluded.written_at`
    const shardsStmt = this.db.prepare(shardsSql).bind(
      input.pyramidName, input.tier, cadence,
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
  // migrations (`wrangler d1 migrations create …` + the returned strings),
  // or via `db.exec()` in a setup script for fixtures.
  //
  // Returns one statement per table in dependency order. WITHOUT ROWID
  // makes the tables key-only — saves space and guarantees strict
  // PK-driven uniqueness (with the empty-string sentinel preventing the
  // SQLite NULL-in-PK footgun).
  static schemaSql(opts: D1ShardIndexOptions = {}): string[] {
    const w = quoteIdent(opts.watermarksTable ?? DEFAULT_WATERMARKS_TABLE)
    const s = quoteIdent(opts.shardsTable ?? DEFAULT_SHARDS_TABLE)
    const out = [
      `CREATE TABLE IF NOT EXISTS ${w} (\n` +
      `  pyramid TEXT NOT NULL,\n` +
      `  tier TEXT NOT NULL,\n` +
      `  cadence TEXT NOT NULL DEFAULT '',\n` +
      `  latest_period_end INTEGER NOT NULL,\n` +
      `  updated_at INTEGER NOT NULL,\n` +
      `  PRIMARY KEY (pyramid, tier, cadence)\n` +
      `) WITHOUT ROWID`,
    ]
    if (!(opts.skipInventory ?? false)) {
      out.push(
        `CREATE TABLE IF NOT EXISTS ${s} (\n` +
        `  pyramid TEXT NOT NULL,\n` +
        `  tier TEXT NOT NULL,\n` +
        `  cadence TEXT NOT NULL DEFAULT '',\n` +
        `  period_start INTEGER NOT NULL,\n` +
        `  period_end INTEGER NOT NULL,\n` +
        `  key TEXT NOT NULL,\n` +
        `  written_at INTEGER NOT NULL,\n` +
        `  PRIMARY KEY (pyramid, tier, cadence, period_start)\n` +
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

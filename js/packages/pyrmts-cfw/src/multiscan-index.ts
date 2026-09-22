// D1 (Cloudflare SQLite) reader for the multi-scan routing manifest —
// `pyramid_multiscans`. pyrmts owns the DDL + row shape + this read query; the
// consumer owns the D1 instance + migration + the WRITE (via its own CF-D1 HTTP
// machinery). Returns `MultiScanIndexEntry[]`, which `resolveScan` /
// `seriesAcrossGroups` (from `pyrmts`) route over — the same entries the JSONL
// `parseMultiScanIndex` yields, so a JSONL-backed and a D1-backed reader are
// interchangeable.
//
// Row shape (mirrors `pyrmts_engine.multiscan_index.multiscan_d1_row`): `scans`
// and `digests` are JSON text (D1 has no array/object type); PK `(dataset,
// key)` — `key` is the archive path, unique per sealed capped-K group.

import type { MultiScanEncoder, MultiScanIndexEntry } from 'pyrmts'
import type { D1Like } from './d1.js'

const DEFAULT_TABLE = 'pyramid_multiscans'

function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`
}

/** The `CREATE TABLE` a consumer runs to provision the manifest (byte-for-byte
 * the `pyrmts_engine.multiscan_index.multiscan_d1_ddl` shape). */
export function multiScanDdl(table = DEFAULT_TABLE): string {
  return (
    `CREATE TABLE IF NOT EXISTS ${quoteIdent(table)} (\n` +
    '  dataset TEXT NOT NULL,\n' +
    '  tier TEXT NOT NULL,\n' +
    '  shard_dur TEXT NOT NULL,\n' +
    '  period_start INTEGER NOT NULL,\n' +
    '  period_end INTEGER NOT NULL,\n' +
    '  key TEXT NOT NULL,\n' +
    '  scans TEXT NOT NULL,\n' +
    '  encoder TEXT NOT NULL,\n' +
    '  digests TEXT,\n' +
    '  written_at INTEGER NOT NULL,\n' +
    '  PRIMARY KEY (dataset, key)\n' +
    ')'
  )
}

interface D1MultiScanRow {
  dataset: string
  tier: string
  shard_dur: string
  period_start: number
  period_end: number
  key: string
  scans: string
  encoder: string
  digests: string | null
  written_at: number
}

/** A `pyramid_multiscans` row → a `MultiScanIndexEntry` (parsing the JSON
 * `scans` / `digests` text). */
export function multiScanEntryFromRow(r: D1MultiScanRow): MultiScanIndexEntry {
  const digests = r.digests ? (JSON.parse(r.digests) as Record<string, string>) : undefined
  return {
    dataset: r.dataset,
    tier: r.tier,
    shardDur: r.shard_dur,
    periodStart: r.period_start,
    periodEnd: r.period_end,
    key: r.key,
    scans: JSON.parse(r.scans) as string[],
    encoder: r.encoder as MultiScanEncoder,
    writtenAt: r.written_at,
    ...(digests ? { digests } : {}),
  }
}

export interface MultiScanD1IndexOptions {
  table?: string // default 'pyramid_multiscans'
}

export interface ListMultiScansFilter {
  tier?: string
  // Intersect: period_end > range.from AND period_start < range.to (epoch ms).
  range?: { from: number; to: number }
}

/** Reads the multi-scan routing manifest from D1. Read-only — the consumer
 * writes rows through its own CF-D1 path (`multiScanDdl` + the row shape). */
export class MultiScanD1Index {
  private readonly table: string

  constructor(private readonly db: D1Like, opts: MultiScanD1IndexOptions = {}) {
    this.table = opts.table ?? DEFAULT_TABLE
  }

  async listMultiScans(dataset: string, filter?: ListMultiScansFilter): Promise<MultiScanIndexEntry[]> {
    const clauses = ['dataset = ?']
    const binds: (string | number)[] = [dataset]
    if (filter?.tier !== undefined) {
      clauses.push('tier = ?')
      binds.push(filter.tier)
    }
    if (filter?.range !== undefined) {
      clauses.push('period_end > ?', 'period_start < ?')
      binds.push(filter.range.from, filter.range.to)
    }
    const sql =
      `SELECT dataset, tier, shard_dur, period_start, period_end, key, scans, encoder, digests, written_at ` +
      `FROM ${quoteIdent(this.table)} WHERE ${clauses.join(' AND ')}`
    const res = await this.db.prepare(sql).bind(...binds).all<D1MultiScanRow>()
    return res.results.map(multiScanEntryFromRow)
  }
}

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
const DEFAULT_TABLE = 'pyramid_multiscans';
function quoteIdent(name) {
    return `"${name.replace(/"/g, '""')}"`;
}
/** The `CREATE TABLE` a consumer runs to provision the manifest (byte-for-byte
 * the `pyrmts_engine.multiscan_index.multiscan_d1_ddl` shape). */
export function multiScanDdl(table = DEFAULT_TABLE) {
    return (`CREATE TABLE IF NOT EXISTS ${quoteIdent(table)} (\n` +
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
        ')');
}
/** A `pyramid_multiscans` row → a `MultiScanIndexEntry` (parsing the JSON
 * `scans` / `digests` text). */
export function multiScanEntryFromRow(r) {
    const digests = r.digests ? JSON.parse(r.digests) : undefined;
    return {
        dataset: r.dataset,
        tier: r.tier,
        shardDur: r.shard_dur,
        periodStart: r.period_start,
        periodEnd: r.period_end,
        key: r.key,
        scans: JSON.parse(r.scans),
        encoder: r.encoder,
        writtenAt: r.written_at,
        ...(digests ? { digests } : {}),
    };
}
/** Reads the multi-scan routing manifest from D1. Read-only — the consumer
 * writes rows through its own CF-D1 path (`multiScanDdl` + the row shape). */
export class MultiScanD1Index {
    db;
    table;
    constructor(db, opts = {}) {
        this.db = db;
        this.table = opts.table ?? DEFAULT_TABLE;
    }
    async listMultiScans(dataset, filter) {
        const clauses = ['dataset = ?'];
        const binds = [dataset];
        if (filter?.tier !== undefined) {
            clauses.push('tier = ?');
            binds.push(filter.tier);
        }
        if (filter?.range !== undefined) {
            clauses.push('period_end > ?', 'period_start < ?');
            binds.push(filter.range.from, filter.range.to);
        }
        const sql = `SELECT dataset, tier, shard_dur, period_start, period_end, key, scans, encoder, digests, written_at ` +
            `FROM ${quoteIdent(this.table)} WHERE ${clauses.join(' AND ')}`;
        const res = await this.db.prepare(sql).bind(...binds).all();
        return res.results.map(multiScanEntryFromRow);
    }
}
//# sourceMappingURL=multiscan-index.js.map
"""Content-equality validation for pyramid builds
(`specs/pyrmts-ops-adoption.md` phase 3 — absorbed from ctbk
`pyramid_cascade/engine_check.py`'s generic harness).

Compares shards from two builds of the same pyramid — e.g. an engine
scratch build vs the incumbent writer's output — on **parsed-content
equality** (long form, sorted), not byte equality: hist-JSON key order
and parquet row-group sizing legitimately differ across writers.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from functools import partial
from io import BytesIO
from pathlib import Path
from typing import Callable

import polars as pl
import pyarrow.parquet as pq

from pyrmts import Pyramid, shard_periods_covering, substitute_key

from .longform import wide_to_long

err = partial(print, file=sys.stderr, flush=True)


def aligned_range(
    dur: str,
    n: int,
    genesis: datetime,
    now: datetime | None = None,
) -> tuple[datetime, datetime]:
    """First `dur`-aligned boundary ≥ `genesis`, spanning `n` periods —
    a fully-aligned interior range (no genesis clip), so smoke-run
    shards are directly comparable to a full build's tiles."""
    now = now or datetime.now(timezone.utc)
    periods = shard_periods_covering(genesis, now, dur)
    if periods[0].start < genesis:
        periods = periods[1:]
    if len(periods) < n:
        raise ValueError(f"only {len(periods)} full {dur} periods since genesis; wanted {n}")
    return periods[0].start, periods[n - 1].end


def canonical_long(
    blob: bytes,
    pyramid: Pyramid,
    bin_range: tuple[int, int] | None = None,
) -> pl.DataFrame:
    """Parse a wide shard to sorted long form — the comparison basis.
    `bin_range` pushes a `[start_ms, end_ms)` filter into the parquet
    read (RG pruning) — without it a coarse covering tile would
    materialize whole."""
    filters = None
    if bin_range is not None:
        s, e = bin_range
        filters = [(pyramid.binCol, '>=', s), (pyramid.binCol, '<', e)]
    wide = pl.from_arrow(pq.read_table(BytesIO(blob), filters=filters))
    return wide_to_long(wide, pyramid).sort(by=pl.all())


def compare_streaming(
    a_blob: bytes,
    b_blob: bytes,
    pyramid: Pyramid,
    chunk_rows: int = 1 << 18,
) -> tuple[str, str]:
    """Compare two wide shards in aligned streaming chunks — peak memory
    is one chunk per side (long-expanded), never the whole pair. Valid
    because both writers total-sort by the same unique row key, so equal
    content ⇒ identical row order; the first divergent chunk proves
    inequality. Returns `(verdict, detail)`, verdict ∈
    {'equal', 'empty_both', 'diff'}."""
    pf_a = pq.ParquetFile(BytesIO(a_blob))
    pf_b = pq.ParquetFile(BytesIO(b_blob))
    n_a, n_b = pf_a.metadata.num_rows, pf_b.metadata.num_rows
    if n_a == 0 and n_b == 0:
        return 'empty_both', ''
    if n_a != n_b:
        return 'diff', f'row counts: {n_a:,} vs {n_b:,}'

    def frames(pf):
        for batch in pf.iter_batches(batch_size=chunk_rows):
            yield pl.from_arrow(batch)

    def fill(buf, it):
        while buf is None or buf.height < chunk_rows:
            nxt = next(it, None)
            if nxt is None:
                break
            buf = nxt if buf is None else pl.concat([buf, nxt])
        return buf

    it_a, it_b = frames(pf_a), frames(pf_b)
    buf_a = buf_b = None
    offset = 0
    while True:
        buf_a = fill(buf_a, it_a)
        buf_b = fill(buf_b, it_b)
        if buf_a is None or buf_a.height == 0:
            # Total row counts are equal, so both sides exhaust together.
            return 'equal', ''
        if set(buf_a.columns) != set(buf_b.columns):
            return 'diff', f'column sets differ: {buf_a.columns} vs {buf_b.columns}'
        n = min(buf_a.height, buf_b.height)
        a, buf_a = buf_a.head(n), buf_a.slice(n)
        b, buf_b = buf_b.head(n), buf_b.slice(n)
        b = b.select(a.columns)
        if not a.equals(b):
            # Wide bytes differ — normalize (hist-JSON key order varies
            # across writers) before declaring a real diff.
            la = wide_to_long(a, pyramid).sort(by=pl.all())
            lb = wide_to_long(b, pyramid).sort(by=pl.all())
            if not la.equals(lb):
                return 'diff', f'content diverges in rows [{offset}, {offset + n})'
        offset += n


def covering_shard(
    pyramid: Pyramid,
    tier: str,
    shard_dur: str,
    start_ms: int,
    end_ms: int,
) -> tuple[str | None, bytes | None]:
    """(key, blob) of a shard at `tier` (any other rung) whose single
    period contains [start_ms, end_ms) — shard content is per-bin, so
    its rows restricted to that window must equal the target shard's
    exactly."""
    tier_obj = pyramid.tier(tier)
    start = datetime.fromtimestamp(start_ms / 1000, timezone.utc)
    end = datetime.fromtimestamp(end_ms / 1000, timezone.utc)
    for dur in (d for d in tier_obj.shards if d != shard_dur):
        periods = shard_periods_covering(start, end, dur)
        if len(periods) != 1:
            continue
        key = substitute_key(
            pyramid.keyTemplate,
            {'tier': tier, 'shard': dur, 'period': periods[0].label},
        )
        blob = pyramid.storage.get(key)
        if blob is not None:
            return key, blob
    return None, None


def compare_manifest(
    manifest: str | Path,
    tgt: Pyramid,
    ref: Pyramid,
    *,
    key_map: Callable[[str], str] = lambda k: k,
    limit: int | None = None,
    detail: bool = False,
) -> dict[str, list[str]]:
    """For every manifest record: fetch the target build's shard and the
    reference build's shard at `key_map(key)`, require canonical-long
    equality. When no reference shard exists at the mapped key (short
    smoke ranges produce sub-tile covers), fall back to a coarser
    reference shard covering the period, filtered to it. Buckets:
    equal / equal_via_cover / diff / missing / empty_both."""
    entries = []
    seen = set()
    for line in Path(manifest).read_text().splitlines():
        rec = json.loads(line)
        if rec['key'] not in seen:
            seen.add(rec['key'])
            entries.append(rec)
    if limit is not None:
        entries = entries[:limit]
    buckets: dict[str, list[str]] = {
        'equal': [], 'equal_via_cover': [], 'diff': [], 'missing': [], 'empty_both': [],
    }
    for i, rec in enumerate(entries):
        key = rec['key']
        ref_key = key_map(key)
        tgt_blob = tgt.storage.get(key)
        ref_blob = ref.storage.get(ref_key)
        if tgt_blob is None:
            raise RuntimeError(f"manifest key missing from target storage: {key}")
        if ref_blob is not None:
            verdict, why = compare_streaming(tgt_blob, ref_blob, ref)
            if verdict == 'diff':
                buckets['diff'].append(ref_key)
                if detail:
                    err(f"  DIFF {key}: {why}")
            else:
                buckets[verdict].append(ref_key)
            continue
        ref_key, ref_blob = covering_shard(
            ref, rec['tier'], rec['shard_dur'], rec['period_start'], rec['period_end'])
        if ref_blob is None:
            buckets['missing'].append(key)
            if detail:
                err(f"  MISSING {key} (no exact or covering reference shard)")
            continue
        bin_range = (rec['period_start'], rec['period_end'])
        a = canonical_long(tgt_blob, tgt)
        b = canonical_long(ref_blob, ref, bin_range=bin_range)
        del ref_blob
        if a.height == 0 and b.height == 0:
            buckets['empty_both'].append(ref_key)
        elif a.equals(b):
            buckets['equal_via_cover'].append(ref_key)
            if detail:
                err(f"  EQUAL {key} vs {ref_key} (filtered)")
        else:
            buckets['diff'].append(ref_key)
            if detail:
                extra = a.join(b, on=a.columns, how='anti').height
                short = b.join(a, on=a.columns, how='anti').height
                err(f"  DIFF {key} vs {ref_key}: {a.height:,} vs {b.height:,} rows; "
                    f"{extra:,} target-only, {short:,} reference-only")
        if (i + 1) % 25 == 0:
            err(f"  … {i + 1}/{len(entries)}: "
                + ', '.join(f'{k}={len(v)}' for k, v in buckets.items() if v))
    return buckets

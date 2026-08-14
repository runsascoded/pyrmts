# Streaming-tip writer: move `invalidate` to `pyrmts` core; optional `TipWriter` SDK helper

Source: awair session, 2026-08-14 (spec written by awair session at pyrmts's request; ~/c/awair). Companion: `calendar-rung-consolidation.md` (unblocks the "1d tip + 1mo consolidation" layout this helper targets).

## TL;DR

Awair's Lambda is the reference streaming-tip producer: every minute, grow the base-tier tip parquet in place, then append a shard-invalidation entry so cascade re-derives downstream tiers on its next tick. It works today — but the invalidate-append had to be **reimplemented** in `awair/src/awair/pyramid/invalidate.py` (40 lines) because `pyrmts_engine.invalidation.invalidate` lives in the heavy engine package, which pulls polars (+40 MB, blows Lambda's 50 MB upload cap and its 250 MB unzipped cap even before adding pyarrow duplication).

**Primary ask** — move the write-side of `invalidation.py` (`invalidate`, `journal_key`, `load_invalidations`, `Invalidation`) to `pyrmts.invalidation`. The functions have zero engine dependencies today (`from pyrmts import ExpectedShard, Pyramid; from pyrmts.storage import EtagConflict; from stdlib`). Awair drops its reimplementation and imports from `pyrmts.invalidation` directly. Backwards-compatible re-exports from `pyrmts_engine.invalidation` keep existing callers unchanged.

**Optional follow-on** — a `pyrmts.tip_writer.TipWriter` SDK helper that packages the whole streaming-tip pattern (derive tip key from tier+time; read shard; merge new rows; write back; invalidate). Small, ~60 lines, replaces awair's `write_pyrmts_raw_shard` boilerplate. Not blocking; the primary ask alone eliminates the duplication that matters.

## Motivation

Two producer models coexist in the pyrmts world today:

1. **Bulk pull** — engine reads external raw archives via `TiledSource`, writes tiles into the pyramid, cascades. `specs/done/engine-raw-ingest.md` (`DailyStatusSource(TiledSource)` for ctbk avail-v6). Full-lifecycle engine control.
2. **Streaming push** — an external writer (Lambda, CFW cron, long-running daemon) grows the base-tier tip file in place, and cascades pick up from there. **Awair Lambda is the canonical example.** No engine involvement in the write path — the producer owns the tip.

The bulk-pull path is well-abstracted (`TiledSource` chassis + `Source` protocol). The streaming-push path isn't — every streaming producer has to hand-roll:
- Derive the tip shard's key from `(tier, rung, at)` and the pyramid's `keyTemplate`.
- Read the current tip shard (or start empty).
- Merge new rows (dedupe/sort per the pyramid's contract).
- Atomically write back.
- Append an invalidation entry covering the touched interval so cascade re-derives downstream.

Steps 1, 2, 4 already have pyrmts primitives (`shard_key` derivation, `Storage.get`/`put`). Step 5 has a primitive (`pyrmts_engine.invalidation.invalidate`) that's **packaged wrong for streaming producers** — behind the engine's polars dep. Step 3 is app-specific (dedupe on `ts`? merge monoids? per-column overwrite?) — no candidate for factoring.

## Why the invalidate move matters concretely

Awair Lambda's package budget:
- Lambda unzipped cap: 250 MB
- Pandas + pyarrow (via AWS Pandas layer): ~120 MB
- pyrmts + boto3 + requests + click + utz: ~30 MB
- `pyrmts_engine` (polars): would add ~40 MB → **over the cap**, before even bundling awair's own code

So `awair/src/awair/lmbda/deploy.py` installs pyrmts with `--no-deps` and awair reimplements `invalidate` from scratch (`src/awair/pyramid/invalidate.py`) — a byte-for-byte identical journal appender, existing solely because the upstream one is packaged with polars.

The 40 lines of duplication are the acute pain, but the real cost is architectural: **any future streaming-producer project** (a Cloudflare Worker cron writing raw shards to R2, a Fly.io machine, a long-lived container) hits the same problem. The write-side of the journal must be a lightweight primitive next to `Pyramid` and `Storage`, not gated behind the engine's build-tool dep tree.

## Design

### Primary — move `invalidation.py` (write-side) to `pyrmts.invalidation`

New module `pyrmts/invalidation.py` with:

```python
JOURNAL_BASENAME = '_invalidations.json'
CAS_ATTEMPTS = 5

@dataclass(frozen=True)
class Invalidation:
    start: datetime
    end: datetime
    requested_at: datetime

def journal_key(pyramid: Pyramid) -> str: ...
def load_invalidations(pyramid: Pyramid) -> tuple[list[Invalidation], str | None]: ...
def invalidate(
    pyramid: Pyramid,
    interval: tuple[datetime, datetime],
    *,
    now: datetime | None = None,
) -> int: ...
```

Zero new dependencies — the current `pyrmts_engine/invalidation.py` already only imports from `pyrmts` and stdlib (verified 2026-08-14: `from pyrmts import ExpectedShard, Pyramid`, `from pyrmts.storage import EtagConflict`, stdlib only). Move is mechanical.

`pyrmts_engine.invalidation` keeps the reader-side (`overlaps`, `stale_keys_for`, `prune_spent` — these are used by `discovery.py` and `consolidate.py`, i.e. engine-only callers) and **re-exports** the write-side names from `pyrmts.invalidation` for back-compat:

```python
# pyrmts_engine/invalidation.py
from pyrmts.invalidation import (
    Invalidation, JOURNAL_BASENAME, CAS_ATTEMPTS,
    journal_key, load_invalidations, invalidate,
)
# ... rest of reader-side code unchanged
```

Existing engine consumers (`consolidate.run_extension_fill`, CLI `invalidate` command) don't change. Streaming producers get a lightweight import path.

### Alternative considered: split pyrmts_engine into core+full

Instead of moving code across packages, split `pyrmts_engine` itself into `pyrmts_engine_core` (no polars/pyarrow) + `pyrmts_engine` (adds polars/pyarrow, re-exports core). Rejected: (a) more surface-area churn than the one-file move needs, (b) `pyrmts_engine.invalidation` isn't really an "engine" concept — the journal is next to the shards in storage, appended by any producer, read by the engine. It belongs next to `Pyramid` and `Storage`, not next to `consolidate`. The current placement is historical (it was written by the engine session, so it landed there).

### Optional follow-on — `pyrmts.tip_writer.TipWriter`

Replaces awair's `write_pyrmts_raw_shard` boilerplate (~40 lines) with:

```python
from pyrmts.tip_writer import TipWriter

with TipWriter(pyramid, tier='raw', at=now, dims={'device_id': 17617}) as tip:
    tip.append(new_rows)  # pyarrow.Table or polars.DataFrame or list-of-dict
    # on __exit__:
    #   - dedupe (per-row on the pyramid's dim+bin key set)
    #   - sort by (dim_cols, bin_col)
    #   - atomic write via pyramid.storage
    #   - append Invalidation(covering min-max touched ts)
```

Key design questions:
- **Merge semantics**: dedupe on `(dim_cols, bin_col)`, keep-first vs keep-last (default: keep-last so streaming producers can correct earlier writes)? Or: fail on collision? Configurable via kwarg.
- **Row shape input**: accept `pa.Table` (native), `pl.DataFrame` (convert), `list[dict]` (build via pa.Table.from_pylist)? Start with `pa.Table` only, add helpers as needed.
- **Rung selection**: `at` + `tier` gives us the target rung as `tier.shards[0]` (finest rung — the tip). Multi-rung tiers: writer always targets the finest rung; consolidation to coarser rungs is cascade's job (per `calendar-rung-consolidation.md`).
- **Locking**: this helper does NOT provide concurrency control across writers. Callers wanting single-writer semantics use CFW cron singletons, Lambda reserved-concurrency=1, or their own coordination. The Storage-layer `put` is atomic; concurrent writers just race and last-write-wins for the tip shard content (they don't corrupt each other's bytes).

Awair's Lambda would shrink to:
```python
from pyrmts.tip_writer import TipWriter

with TipWriter(config, tier='raw', at=now, dims={'device_id': device_id}) as tip:
    tip.append(new_rows_arrow_table)
    # __exit__ handles read-merge-write + invalidate
```

Marginal — the current hand-rolled version works. Real value: any future streaming producer gets the same API without reinventing.

### Not in scope

- **Same-tier consolidation of the tip.** Covered by `calendar-rung-consolidation.md` — the writer produces the finest rung; cascade consolidates. `TipWriter` targets only the finest rung.
- **A pull-side chassis for streaming producers** (e.g. "watch a Kafka topic and write tip shards"). Way out of scope; producers pull from their own sources.
- **`Storage` conditional writes for the tip file itself.** The tip's atomic overwrite semantics (via `Storage.put`) are already correct for last-write-wins; adding CAS to the tip write would be a producer-side change if concurrent writers land in scope, not a pyrmts change.

## Acceptance

### Primary (module move)

1. `pyrmts/invalidation.py` exists with `Invalidation`, `journal_key`, `load_invalidations`, `invalidate`, `JOURNAL_BASENAME`, `CAS_ATTEMPTS`.
2. `pyrmts_engine.invalidation` re-exports the moved names; existing engine callers (`consolidate.run_extension_fill`, `cli.invalidate`) unchanged.
3. Existing `test_invalidation.py` test suite green (whether hosted in `pyrmts_engine` or duplicated in `pyrmts`; recommend moving the write-side tests to `pyrmts/tests` too).
4. New test: import from `pyrmts.invalidation` in a synthetic streaming-producer fixture (no engine imports); write two intervals to a `MemStorage`-backed pyramid; assert both journal entries appear + CAS-retry-preserves-concurrent-append.
5. Awair-side follow-up (separate; awair repo): delete `awair/src/awair/pyramid/invalidate.py`, replace usages with `from pyrmts.invalidation import invalidate`. Lambda deploy still under cap (no new deps).

### Optional (TipWriter helper)

6. `pyrmts/tip_writer.py` with `TipWriter` context manager.
7. Round-trip test: fixture pyramid, write 60 tip appends over "an hour" of simulated time, assert final tip content = concat-sorted-dedupe of all appends, assert journal has 60 entries with correct `[start, end)`.
8. Awair-side follow-up: `write_pyrmts_raw_shard` shrinks to `~5 lines` using `TipWriter`.

## Rollout

Small, additive. Primary move can land in a single PR:
1. `git mv pyrmts_engine/invalidation.py pyrmts/invalidation.py` (keep git history).
2. Split: leave reader-side (`overlaps`, `stale_keys_for`, `prune_spent`) in `pyrmts_engine/invalidation.py` as a slim module that re-exports the writer-side. Move the write-side tests to `pyrmts/tests/`.
3. Awair repins pyrmts, drops its `invalidate.py`, redeploys Lambda — one less thing to keep in sync when the wire format evolves.

Optional TipWriter is a follow-on PR; not gating.

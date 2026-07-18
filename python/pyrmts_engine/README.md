# `pyrmts-engine`

Fused long-form pyramid build engine. See [`specs/pyramid-build-engine.md`](../../specs/pyramid-build-engine.md).

- **Long form** `(*dims, bin, metric, state, count)` is the canonical build-internal representation; wide hist-JSON is materialized exactly once per output shard.
- **Windowed streaming local executor**: one pass over the base tier's source, every other tier piggybacks via divisibility-predecessor re-binning; per-tier WIP buffers flush shards as their periods close (no scaffold shards, single PUT per shard).
- Outputs are exactly `pyrmts.list_expected_shards` (min-cover, genesis-clipped).
- `ShardIndex` protocol records each shard right after its PUT (no-op / JSONL manifest / D1 REST impls).

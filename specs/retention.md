# History horizon: time-based eviction for shards the min-cover still expects

Status: **open, premise rewritten 2026-08-28.** The original draft of this spec (never committed) was written against a pyramid model that no longer exists, and its core motivation is now false. Rewritten here with the corrected framing; **not implemented**, and arguably not yet needed — see "Is this wanted?".

## What the original draft got wrong

It opened with:

> Today every `(tier, shard_dur)` rung in a pyramid is kept forever. For busy pyramids (e.g. ctbk avail-v3 with 7 rungs × 15 tiers, /5m cron writing the smallest rung every tick) the smaller rungs accumulate indefinitely.

That was true when written and is not true now. The unified shard ladder made `list_expected_shards` a **minimal** cover: closed history is tiled by max-shard tiles only, and smaller rungs appear *solely* in the trailing partial-max window near `to`. Verified on the fixture pyramid (tier `q` with `shards=('6h','1d')`, range 2026-01-02 → 01-08):

```
cover by (tier, shard): {('q','1d'): 6, ('h','4d'): 2, ('d','4d'): 2, ('h','1d'): 1}
q-tier 6h shards in cover: []
```

Zero fine-rung shards in closed history. And `pyrmts_ops.gc.gc_sweep` computes `eligible = registered − expected-min-cover − raw prefixes`, so those old fine-rung shards are *already* GC-eligible and are deleted once past grace with a same-tier covering parent verified on storage. **The accumulation problem the spec existed to solve is solved**, by two pieces of work that landed after it was drafted.

Two further defects in the original design, worth recording because they'd have bitten an implementer:

- **It proposed a TypeScript API** (`listGcEligible` / `gcShards` exported from `pyrmts`), but the GC that exists is Python (`pyrmts_ops.gc`), and that is where consumers actually run it — ctbk's cascade Lambda behind `GC_ENABLED`, on the hour's first firing. A TS-side deletion path would be a second, divergent GC.
- **"No interaction with the planner … GC + planner are decoupled" is half right.** It holds for the *planner*, which reads the ShardIndex for what exists and falls through to a coarser rung. It does **not** hold for fill: `pyrmts_engine.discovery` and `build_local(fill=True)` diff the expected cover against what exists and build the difference. Delete a shard the cover still expects and the next fill **rebuilds it** — forever, every tick. A pure delete API would have fought the fill loop, and the symptom (a cron that rebuilds the same shards nightly) is exactly the kind of thing that looks like a bug in the engine rather than in the retention policy.

## What is actually still uncovered

`gc_sweep` never deletes a shard that is *in* the expected cover, no matter how old — so a pyramid's **coarsest rung grows without bound**, forever. That is correct default behavior (it's the data), but there is no way to say "keep two years" or to satisfy a deletion request.

So the real capability is a **history horizon**, not per-rung retention: a floor that moves forward with wall-clock, below which a tier's history is no longer expected *or* retained.

## Design sketch

The insight that makes this small: **retention belongs in the cover, not in a deleter.** Clip the expected cover, and eviction mostly falls out of machinery that already exists.

1. **Config** — a per-*tier* `keep` (not per-rung; rungs below the max are already evicted by min-cover + `gc_sweep`), parsed by the existing Duration parser, defaulting to forever. Back-compat is free: absent `keep` = today's behavior.
2. **Cover** — `list_expected_shards` clips each tier's effective `from_` to `max(from_, now − keep)`, the same shape as the existing `genesis` floor. This is the load-bearing half: once the old shards aren't expected, fill stops wanting to rebuild them, and `gc_sweep` classes them eligible with no new eligibility logic. It also needs the JS twin (`gap-discovery.ts`) to keep the planner and the engine agreeing.
3. **Eviction** — one new branch in `gc_sweep`. Expired shards are at a tier's *max* rung by construction, so `covering_parent` returns `None` and today's code would skip them as `no-covering-parent`. A shard below the retention floor is deletable without a parent — that is the point — so the branch must be explicit and narrow, gated on the configured floor rather than on age generally.
4. **Non-goal, unchanged from the original:** no soft-delete or grace beyond the existing `grace_ms`; a consumer wanting a grace configures a longer `keep`.

The dangerous part is (3): a bug there deletes history that no coarser rung covers, and unlike every other `gc_sweep` branch it cannot be justified by "a parent still has the data". It should refuse to run without an explicit configured floor, and `dry_run` should be the documented first step.

## Is this wanted?

Not urgently, and no consumer has asked. The original draft said as much — *"about giving consumers a clean knob before those constraints bite, not solving an urgent problem"* — and the three conditions it named for when retention starts to matter are all still unmet: no pyramid is at hundreds of GB, LIST-page counts aren't a cost driver (that was the `pyramid_shards` index problem, since fixed), and nobody has a deletion requirement.

Recommendation: **leave open, unimplemented.** The value of this document now is the corrected premise and the fill-loop hazard — so that whoever picks it up builds a cover clip rather than the delete API the original specced. If a compliance/deletion requirement or a genuinely large pyramid shows up, the design above is ~a day's work.

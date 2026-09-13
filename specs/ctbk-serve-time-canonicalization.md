# Serve-time station canonicalization: raw-id leaves + id-map rollup

Status: proposed (2026-09-13). From the ctbk session. Companion to ctbk's
`specs/deversion-clean-slate.md` and the harmonize co-activity fix
(`ctbk/stations/harmonize.py`, 2026-09-13). Grounded in `ctbk/pyramid_cascade/
rides_source.py` + `configs/pyramids/station-vocab.json` (ctbk) and
`js/packages/pyrmts-geo` (pyrmts).

## The problem

ctbk keys its rides/avail pyramids by **canonical** station identity, applied at
**ingest**: `rides_source.py` maps each ride's reported station id → its
canonical (via `station-id-map.json`, baked into `station-vocab.json`) before
emitting `s:<canonical>` cells to the engine. Two consequences, both bad:

1. **Merges are baked into built tiles.** A station-identity error in the
   id-map (the union-find false-merges just fixed — `5216.04` "Barclay St" and
   `5288.08` "Park Pl", distinct stations folded into one `s:5216.06`) can only
   be corrected by **rebuilding the pyramid** on Batch (genesis→now). Every
   future harmonize refinement pays that cost.
2. **The pyramid cannot show a merged pair separated.** A merge-audit view
   (does `HB106` hand off to `HB609`, or do they co-operate?) needs the two ids'
   series side by side — which the canonicalized pyramid has already summed away.
   The only un-merged source today is the raw ride parquets (no serving path).

The identity decision is a **view**, not ground truth, but it's committed at
build time. It should move to serve time.

## Proposal: two levels, id-map as a reactive dependency

- **Leaf level — raw reported id.** `rides_source.py` stops applying the
  canonical map; it emits `s:<raw_id>` (the id as reported on the ride). Tiles at
  this level are **id-map-independent** — they never need rebuilding when the
  id-map changes. Each ride carries exactly one reported id, so `{raw_id}` is the
  right key; `{raw_id, month}` is redundant (month is the bin axis, and the
  id-map is a single global `alias→canonical`, not era-specific).
- **Canonical level — materialized rollup.** For each canonical id, sum its
  constituent `s:<raw_id>` leaves per the id-map. **Materialized** (owner
  decision: rollups over fan-outs — serving must be fast; pre-compute + extra
  storage are fine, and this project's data is small). Its dependencies are
  `{leaf tiles, station-id-map.json}` — so an id-map change dirties **only the
  canonical tiles**, which regenerate from the leaves (no raw re-ingest),
  exactly the DVX dirty→regen reactivity ctbk already invests in.
- **Serving reads the canonical level** by default; `/merge-review` (and any
  audit) reads the leaf level to show ids un-merged. Both are normal
  bin-responsive pyrmts queries.

Net: the deferred "rebuild pyramids to fix the 25 merges" becomes a **one-time
re-key** (build the leaf level once); after that, every id-map correction —
including a human override from the review page — is a cheap canonical-rollup
regen, never a Batch rebuild. The false-merge bug *class* is designed out.

## Does pyrmts need new features?

The identity keying + vocab live in **ctbk** (`rides_source.py`, `vocab.py`,
`station-vocab.json`) — so dropping ingest-time canonicalization and emitting raw
ids is a ctbk change. The genuinely-new capability is the **canonical rollup**,
and there are two shapes; **this is the pyrmts design call**:

- **(A) Mapping-driven rollup (new engine transform).** pyrmts materializes a
  coarser identity level from finer leaves + a mapping table (`raw_id →
  canonical`) — a sum-monoid group-by on the identity dim keyed by a lookup,
  analogous to the existing geo-cover/time-bin rollups but over an arbitrary
  (non-geometric) partition. The mapping is a declared pyramid input (→ DVX dep).
  Most general; reusable for any alias/vocab rollup.
- **(B) Custom vocab hierarchy (extend the ragged vocab).** Model the id-map as
  parent/child edges in the `s:` vocab (canonical `s:` cell = parent of its alias
  `s:` cells), so the **existing** minimal-cover / rollup machinery materializes
  the canonical level with no new transform. Cheaper if `pyrmts-geo`'s ragged
  vocab can already carry non-S2 parent/child edges; the question is whether it
  can, or whether that's as much work as (A).

Recommendation: **(A)** unless the ragged vocab trivially supports (B) — (A) is a
clean, reusable primitive and keeps the id-map an explicit, inspectable input
rather than smuggling it into vocab geometry. Either way pyrmts owns this half.

## ctbk-side work (this session's project)

- `rides_source.py`: emit `s:<raw_id>`, drop the canonical map from ingest.
- `vocab.py` / `station-vocab.json`: raw-id vocab (~3,900 ids vs ~canonical).
- Config: declare the canonical-rollup level with `station-id-map.json` as a dep.
- **api worker** (`gbfs/api/src/rides_v1.ts`, `avail_geo.ts`): serve the canonical
  level by default; expose the leaf level for the audit view.
- **One-time re-key**: rebuild rides-v5 (start/end) + avail (v6/v5) keyed by raw
  id on Batch — the last mandatory genesis→now run for this class of change.
- **`/merge-review` page**: for each flagged pair, a Leaflet map (both stations)
  + a rides plot of each id's counts over time with shared-active months shaded
  (clean hand-off vs concurrent), reading the leaf level.
- Same pattern applies to **avail** (also `s:`-keyed).

## Open questions

- (A) vs (B) — pyrmts's call; drives the engine effort estimate.
- Rollup granularity: materialize canonical at **every** tier/bin, or only the
  serve-hot tiers and derive the rest? (Storage vs regen-time; owner leans
  "store extra within reason".)
- Cost of the one-time re-key (rides-v5 ≈ a few $; avail v5+v6 the larger share)
  — bounded, one-time.
- Does the leaf level change per-request query cost meaningfully vs. today
  (extra vocab entries), or is it invisible once the canonical level is the
  default serve target?

## pyrmts review (2026-09-13) — recommendation: one-vocab, not two levels

Read from the pyrmts session against the actual code (`pyrmts-geo/src/vocab-cover.ts`, `python/pyrmts/src/pyrmts/{cascade,monoids}.py`). **Held pending a ctbk round-trip** (merge-vetting thoroughness + the ctbk-side work below); recording the recommendation, not rewriting the body.

**Prefer a single vocab over a separate leaf stack + canonical rollup.** Put raw + canonical + s2 all as vocab members inside each time-bounded shard, rather than a separate leaf level (the original A/B framing) or a sidecar. The canonical row is written at build time as an **id-map-keyed monoid rollup of its raw constituents** — so the parent/child (canonical→raw) edge is *supplied by the id-map*, applied at write time. This sidesteps `buildVocabGraph`'s geometry-driven parent walk (which can't express a non-geometric identity edge — that was the real blocker for option (B)); `vocabCover`'s DP is geometry-free and serves canonical-by-default and raw-for-audit off the *same* shard.

Why this beats the two-level / sidecar framing in the body:
- **One stack, one file set, no cross-file mixed covers.** A minimal cover already mixes s2-cell ids and `s:` identity keys resolved against one shard's rows; splitting canonical off into separate parquets would turn every mixed cover into a cross-file gather. One vocab keeps today's query model intact.
- **Row bloat is only for merged clusters** (+1 canonical row per cluster per shard). An unmerged station *is* its own canonical (no duplicate row) — so "store extra within reason" holds; the extra is ~25 clusters' worth.
- Fits `cascade_tiers`' existing relabel-and-monoid-combine; the new capability is "given the id-map, emit a summed canonical row per canonical class present in the shard."

**The materialization ↔ remap-cost tradeoff (resolved).** No design here re-*ingests*: raw rows are id-map-independent, s2 rollups are id-map-independent (they sum physical activity, identity-agnostic), so a map change touches **only the canonical `s:` rows**, re-derived from raw rows already stored — no source access, no Batch. Because we chose materialization (for serve speed), a map fix *does* rewrite canonical rows, but the blast radius scales with the change if the id-map is a declared DVX dep with shard-scoped invalidation: fixing 2 stations dirties only shards where those ids have activity; a *wholesale* remap rewrites everything. The only way a remap is truly free is to **not** materialize (fan out and sum at serve) — explicitly ruled out on serve-speed grounds. So materialization ↔ remap cost is a fundamental trade, not a wart; having picked fast serves, a map fix = cheap, local, shard-scoped canonical re-derive.

**Orthogonal gap, do not fold into the merge fix — bin-responsive geometry.** A station's coordinate can change over its life (a move is often *why* it got a new id). Poly/rect→station-set resolution and "which stations are drawn over a time range" need a coordinate *per time-bin*, not a single representative coord. This is a pre-existing issue that canonicalization only makes visible; it wants its own treatment (per-bin station→s2-cell / coord), separate from the identity rollup.

**For the ctbk round-trip:** how thoroughly were the ~25 merges vetted? The one-vocab design keeps raw rows in every shard, so re-splitting a bad merge stays a cheap canonical re-derive regardless — which lowers the stakes on getting all 25 right up front. Also confirm the ctbk-side plan (`rides_source.py` emitting raw ids, `vocab.py`/`station-vocab.json`, api-worker default-canonical + audit-leaf serving).

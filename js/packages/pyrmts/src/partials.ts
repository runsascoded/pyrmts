// Validation + alignment computation for sub-shard cadences.
// See `specs/partial-shards.md`.

import { fixedDurationMs, parseDuration } from './axis.js'
import type { Duration, Pyramid, Shard, Tier } from './types.js'

export interface ValidatedPartials {
  // Cadences in ascending ms order. Same set as `pyramid.partials`, just sorted.
  cadences: Duration[]
  // Per-tier list of cadences that are alignment-valid for that tier
  // (`cadence % tier.bin == 0` AND `cadence` strictly finer than the tier's
  // canonical shard). Ascending ms; absent tiers (no valid cadence) map to `[]`.
  perTier: Record<string, Duration[]>
}

// Validate `pyramid.partials` + `pyramid.partialKey` and compute the per-tier
// alignment-valid cadences. Returns `null` when `pyramid.partials` is unset or
// empty (no validation needed; planner behaves as today). Throws on:
//   - calendar cadences (`mo` / `y`)
//   - cadence-ladder non-divisibility (each cadence must divide all coarser)
//   - missing `partialKey` when `partials` is non-empty
//   - calendar tier bins (sub-shards require fixed-width tier bins)
//   - no `(tier, cadence)` pair alignment-valid (the entire ladder is dead)
export function validatePartials(pyramid: Pyramid): ValidatedPartials | null {
  const partials = pyramid.partials
  if (!partials || partials.length === 0) return null

  if (typeof pyramid.partialKey !== 'string' || pyramid.partialKey.length === 0) {
    throw new Error(
      `validatePartials: pyramid.partials is non-empty but pyramid.partialKey is unset`,
    )
  }

  const cadencesMs: Array<{ cadence: Duration; ms: number }> = []
  for (const c of partials) {
    const parsed = parseDuration(c)
    if (parsed.unit === 'mo' || parsed.unit === 'y') {
      throw new Error(
        `validatePartials: cadence '${c}' is calendar-variable; sub-shard cadences must be fixed-duration (min/h/d)`,
      )
    }
    cadencesMs.push({ cadence: c, ms: fixedDurationMs(c) })
  }
  cadencesMs.sort((a, b) => a.ms - b.ms)

  // Ladder divisibility: each cadence divides all strictly-coarser cadences.
  // (Equivalently, the ladder forms a divisibility chain when sorted ascending.)
  for (let i = 0; i < cadencesMs.length; i++) {
    for (let j = i + 1; j < cadencesMs.length; j++) {
      if (cadencesMs[j]!.ms % cadencesMs[i]!.ms !== 0) {
        throw new Error(
          `validatePartials: cadence ladder not divisibility-chained ` +
          `(${cadencesMs[j]!.cadence} not a multiple of ${cadencesMs[i]!.cadence}); ` +
          `required so a /finest cron can skip coarser cadences when unaligned`,
        )
      }
    }
  }

  const perTier: Record<string, Duration[]> = {}
  let anyValid = false
  for (const tier of pyramid.tiers) {
    const valid = alignmentValidCadences(tier, cadencesMs)
    perTier[tier.name] = valid
    if (valid.length > 0) anyValid = true
  }
  if (!anyValid) {
    throw new Error(
      `validatePartials: no (tier, cadence) pair is alignment-valid ` +
      `(cadences=[${partials.join(', ')}]); each cadence must satisfy ` +
      `cadence % tier.bin == 0 AND cadence < tier.shard for some tier`,
    )
  }

  return {
    cadences: cadencesMs.map(c => c.cadence),
    perTier,
  }
}

// Filter `cadences` to those alignment-valid for `tier`:
//   - `cadence % tier.bin == 0` (whole-bin sub-shards)
//   - `cadence < tier.shard` for fixed-duration shards; auto-true for
//     calendar (mo/y) and 'all' shards (sub-shard cadence is always
//     strictly finer than a calendar / unbounded canonical shard).
// Calendar tier bins (`mo`/`y`) reject all sub-shard cadences — sub-shards
// exist for fresh-data fall-through within a fixed-width tier.
function alignmentValidCadences(
  tier: Tier,
  cadences: Array<{ cadence: Duration; ms: number }>,
): Duration[] {
  const parsedBin = parseDuration(tier.bin as Duration)
  if (parsedBin.unit === 'mo' || parsedBin.unit === 'y') return []
  const binMs = fixedDurationMs(tier.bin as Duration)
  const shardMaxMs = canonicalShardMs(tier.shard)
  const out: Duration[] = []
  for (const { cadence, ms } of cadences) {
    if (ms % binMs !== 0) continue
    if (shardMaxMs !== null && ms >= shardMaxMs) continue
    out.push(cadence)
  }
  return out
}

// `null` = treat the canonical shard as unbounded for the sub-shard
// comparison (calendar `mo`/`y` are variable-width; `'all'` is unbounded;
// `'1run'` is step-axis and out of scope here).
function canonicalShardMs(shard: Shard): number | null {
  if (shard === 'all') return null
  if (shard === '1run') {
    throw new Error(`validatePartials: tier with shard='1run' (step-axis) is not supported`)
  }
  const parsed = parseDuration(shard)
  if (parsed.unit === 'mo' || parsed.unit === 'y') return null
  return fixedDurationMs(shard)
}

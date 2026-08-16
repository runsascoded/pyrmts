// Guard on the package's public export surface.
//
// The load-bearing assertion is a negative one that's easy to regress:
// `h3Index` must NOT be reachable from the package index. `h3-js` declares
// no `sideEffects`, so a single re-export makes it un-tree-shakeable and
// silently adds ~195 KB (minified) to every consumer bundle — which is
// exactly what the old `getSpatialIndex` H3 fallback did. Asserting the
// whole export list (rather than just `'h3Index' in pkg === false`) also
// catches unintended additions and removals.

import { describe, expect, test } from 'vitest'
import * as pkg from './index.js'

describe('pyrmts-geo public exports', () => {
  test('exports exactly the intended surface (no `h3Index`)', () => {
    expect(Object.keys(pkg).sort()).toEqual([
      'VERSION',
      'buildGeoQueryUrl',
      'buildVocabGraph',
      'fetchPyramidGeoQuery',
      'filterCellsAndRes',
      'filterCellsByCover',
      'getSpatialIndex',
      'isCellInCover',
      'minimalCover',
      'planGeoQuery',
      'planGeoQueryFromInventory',
      's2Index',
      'serveGeoQuery',
      'vocabCover',
    ])
  })
})

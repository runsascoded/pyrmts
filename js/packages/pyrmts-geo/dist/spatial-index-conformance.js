// Reusable conformance suite for the `SpatialIndex` interface. Backends
// (h3, s2, h13-deferred, t4-deferred) pass their concrete impl + known-
// good sample inputs; vitest's `describe`/`test` are imported here so
// callers don't need to thread them.
//
// Kept out of any `.test.ts` file so that importing this helper doesn't
// re-register its tests in the importing file's vitest context. (Bad
// previous setup: helper lived in `spatial-index.test.ts`, and every
// `import { assertSpatialIndex }` triggered the h3 suite to run twice.)
import { describe, expect, test } from 'vitest';
// The interface contract: every method on a SpatialIndex exercised with
// known inputs. Any new backend must pass this suite.
export function assertSpatialIndex(index, opts) {
    const { samplePoint, sampleBBox, sampleLevel, coarserLevel } = opts;
    test('name + maxLevel', () => {
        expect(typeof index.name).toBe('string');
        expect(index.name.length).toBeGreaterThan(0);
        expect(index.maxLevel).toBeGreaterThanOrEqual(sampleLevel);
    });
    test('latLngToCell + cellLevel round-trip', () => {
        const cell = index.latLngToCell(samplePoint.lat, samplePoint.lng, sampleLevel);
        expect(typeof cell).toBe('string');
        expect(index.cellLevel(cell)).toBe(sampleLevel);
    });
    test('cellToParent returns a coarser-level cell', () => {
        const cell = index.latLngToCell(samplePoint.lat, samplePoint.lng, sampleLevel);
        const parent = index.cellToParent(cell);
        expect(index.cellLevel(parent)).toBe(sampleLevel - 1);
    });
    test('cellToParent honors explicit target level', () => {
        const cell = index.latLngToCell(samplePoint.lat, samplePoint.lng, sampleLevel);
        const parent = index.cellToParent(cell, coarserLevel);
        expect(index.cellLevel(parent)).toBe(coarserLevel);
    });
    test('bboxToCells returns cells at the requested level', () => {
        const cells = index.bboxToCells(sampleBBox, sampleLevel);
        expect(cells.length).toBeGreaterThan(0);
        for (const c of cells) {
            expect(index.cellLevel(c)).toBe(sampleLevel);
        }
    });
    test('cellInSet: cell-in-include → true', () => {
        const cell = index.latLngToCell(samplePoint.lat, samplePoint.lng, sampleLevel);
        expect(index.cellInSet(cell, sampleLevel, { include: [cell], exclude: [] })).toBe(true);
    });
    test('cellInSet: cell-not-in-include → false', () => {
        const cell = index.latLngToCell(samplePoint.lat, samplePoint.lng, sampleLevel);
        expect(index.cellInSet(cell, sampleLevel, { include: [], exclude: [] })).toBe(false);
    });
    test('cellInSet: cell-in-exclude → false (overrides include)', () => {
        const cell = index.latLngToCell(samplePoint.lat, samplePoint.lng, sampleLevel);
        expect(index.cellInSet(cell, sampleLevel, { include: [cell], exclude: [cell] })).toBe(false);
    });
    test('cellInSet: wrong level → false (drops other-resolution rows)', () => {
        const cell = index.latLngToCell(samplePoint.lat, samplePoint.lng, sampleLevel);
        expect(index.cellInSet(cell, coarserLevel, { include: [cell], exclude: [] })).toBe(false);
    });
    // Lineage-aware membership for mixed-resolution sets (`minimalCover`
    // output is at varying levels; row filters check membership against it
    // via `cellInSet`).
    test('cellInSet: parent in include covers descendant (lineage walk up)', () => {
        const cell = index.latLngToCell(samplePoint.lat, samplePoint.lng, sampleLevel);
        const parent = index.cellToParent(cell);
        expect(index.cellInSet(cell, sampleLevel, { include: [parent], exclude: [] })).toBe(true);
    });
    test('cellInSet: cell in exclude beats parent in include', () => {
        const cell = index.latLngToCell(samplePoint.lat, samplePoint.lng, sampleLevel);
        const parent = index.cellToParent(cell);
        expect(index.cellInSet(cell, sampleLevel, { include: [parent], exclude: [cell] })).toBe(false);
    });
    test('cellInSet: no ancestor in include → false (default)', () => {
        const cell = index.latLngToCell(samplePoint.lat, samplePoint.lng, sampleLevel);
        const otherParent = index.cellToParent(index.latLngToCell(samplePoint.lat + 5, samplePoint.lng + 5, sampleLevel));
        expect(index.cellInSet(cell, sampleLevel, { include: [otherParent], exclude: [] })).toBe(false);
    });
}
//# sourceMappingURL=spatial-index-conformance.js.map
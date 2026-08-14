import type { GeoSpec, Pyramid } from 'pyrmts';
export interface BBox {
    minLat: number;
    maxLat: number;
    minLng: number;
    maxLng: number;
}
export interface SpatialSet<C extends string = string> {
    include: C[];
    exclude: C[];
}
export interface MinimalCoverOpts {
    allowSubtraction?: boolean;
    /** Stop the bottom-up roll-up at this level. Cover output won't contain
     *  cells coarser than `coarsestLevel`. Pass the shallowest materialized
     *  level of the consuming pyramid — coarser cells have no shards to
     *  query. Undefined → walk to backend root. */
    coarsestLevel?: number;
}
export interface SpatialIndex<C extends string = string> {
    readonly name: string;
    readonly maxLevel: number;
    latLngToCell(lat: number, lng: number, level: number): C;
    cellLevel(cell: C): number;
    cellToParent(cell: C, level?: number): C;
    bboxToCells(bbox: BBox, level: number): C[];
    cellInSet(cell: C, level: number, set: SpatialSet<C>): boolean;
    minimalCover(include: C[], system: C[], opts?: MinimalCoverOpts): SpatialSet<C>;
}
export type GeoSpecWithIndex = GeoSpec & {
    index?: SpatialIndex;
};
export type GeoPyramid = Omit<Pyramid, 'geo'> & {
    geo?: GeoSpecWithIndex;
};
//# sourceMappingURL=spatial-index.d.ts.map
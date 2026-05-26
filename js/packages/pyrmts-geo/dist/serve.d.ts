import { type Pyramid } from 'pyrmts';
export interface ServeGeoOptions {
    pyramid: Pyramid;
    request: Request;
    watermarks?: Record<string, Date> | ((req: Request) => Promise<Record<string, Date>> | Record<string, Date>);
    earliestWatermarks?: Record<string, Date> | ((req: Request) => Promise<Record<string, Date>> | Record<string, Date>);
    tolerateMissingShards?: boolean;
    cors?: boolean;
}
export declare function serveGeoQuery(opts: ServeGeoOptions): Promise<Response>;
//# sourceMappingURL=serve.d.ts.map
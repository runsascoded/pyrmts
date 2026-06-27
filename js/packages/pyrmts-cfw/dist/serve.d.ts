import { type Pyramid } from 'pyrmts';
export interface ServeOptions {
    pyramid: Pyramid;
    request: Request;
    watermarks?: Record<string, Date> | ((req: Request) => Promise<Record<string, Date>> | Record<string, Date>);
    earliestWatermarks?: Record<string, Date> | ((req: Request) => Promise<Record<string, Date>> | Record<string, Date>);
    earliestPerCadence?: Record<string, Date> | ((req: Request) => Promise<Record<string, Date>> | Record<string, Date>);
    tolerateMissingShards?: boolean;
    cors?: boolean;
}
export declare function serveQuery(opts: ServeOptions): Promise<Response>;
//# sourceMappingURL=serve.d.ts.map
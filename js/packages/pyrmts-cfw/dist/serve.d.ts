import { type Pyramid, type ShardIndex } from 'pyrmts';
export interface ServeOptions {
    pyramid: Pyramid;
    request: Request;
    pyramidName?: string;
    shardIndex?: ShardIndex;
    watermarks?: Record<string, Date> | ((req: Request) => Promise<Record<string, Date>> | Record<string, Date>);
    earliestWatermarks?: Record<string, Date> | ((req: Request) => Promise<Record<string, Date>> | Record<string, Date>);
    earliestPerShard?: Record<string, Date> | ((req: Request) => Promise<Record<string, Date>> | Record<string, Date>);
    tolerateMissingShards?: boolean;
    cors?: boolean;
}
export declare function serveQuery(opts: ServeOptions): Promise<Response>;
//# sourceMappingURL=serve.d.ts.map
import type { Storage } from './types.js';
export interface MemStorageOptions {
    data?: Map<string, Uint8Array>;
    clock?: () => Date;
}
export declare function memStorage(arg?: Map<string, Uint8Array> | MemStorageOptions): Storage;
export interface HttpStorageOptions {
    /** Extra request headers (auth, etc.). */
    headers?: Record<string, string>;
    /** `fetch` to use (default: global). */
    fetch?: typeof fetch;
}
export declare function httpStorage(baseUrl: string, opts?: HttpStorageOptions): Storage;
//# sourceMappingURL=storage.d.ts.map
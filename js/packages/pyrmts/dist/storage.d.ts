import type { Storage } from './types.js';
export interface MemStorageOptions {
    data?: Map<string, Uint8Array>;
    clock?: () => Date;
}
export declare function memStorage(arg?: Map<string, Uint8Array> | MemStorageOptions): Storage;
//# sourceMappingURL=storage.d.ts.map
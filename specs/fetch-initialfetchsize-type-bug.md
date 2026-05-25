# `initialFetchSize` type error in `fetch.ts`

`packages/pyrmts/src/fetch.ts:53` passes `initialFetchSize` to
`parquetReadObjects`, but hyparquet's `ParquetReadOptions` type doesn't
include that property — it's a `parquetMetadataAsync` option (line 57 uses
it correctly).

```ts
// Line 53 — TS error:
const rows = await parquetReadObjects({ file, initialFetchSize })
//                                            ^^^^^^^^^^^^^^^^
// Object literal may only specify known properties, and 'initialFetchSize'
// does not exist in type 'Omit<ParquetReadOptions, "onComplete">'.

// Line 57 — fine, parquetMetadataAsync does accept it:
const metadata = await parquetMetadataAsync(file, { initialFetchSize })
```

Doesn't break runtime (esbuild strips the type and hyparquet silently
ignores unknown props at runtime — but `initialFetchSize` is *also* never
respected inside `parquetReadObjects`, so the only effect is dead code).
Breaks `tsc --noEmit` for downstream consumers that include pyrmts'
source in their typecheck (e.g. awair's `cfw/serve` until pyrmts ships a
built `dist`).

Surfaced when wiring awair against `89c1e8f` — for now we just skip
typecheck and rely on `wrangler deploy` (esbuild) to bundle.

Likely fix: drop the param from the no-filter branch (it was probably
copy-pasted from the metadata branch).

```ts
if (opts?.binCol === undefined || opts.range === undefined) {
  const rows = await parquetReadObjects({ file })  // ← drop initialFetchSize
  return rows.map(normalizeRow)
}
```

If the intent was to also bound footer fetch in the no-filter path, the
call needs to be split into `parquetMetadataAsync({ file, initialFetchSize })`
+ `parquetReadObjects({ file, metadata })`.

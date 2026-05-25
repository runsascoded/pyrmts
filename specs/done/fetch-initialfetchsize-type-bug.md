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

## Resolution

Took the "preserve footer-fetch bound" path — the file-level comment on
`initialFetchSize` says that's the intent. Hoisted `parquetMetadataAsync`
out of the filter branch, and pass `metadata` to `parquetReadObjects` in
both branches.

```ts
const metadata = await parquetMetadataAsync(file, { initialFetchSize })

if (opts?.binCol === undefined || opts.range === undefined) {
  const rows = await parquetReadObjects({ file, metadata })
  return rows.map(normalizeRow)
}

const runs = selectRowGroupRuns(metadata, opts.binCol, opts.range)
// ...
```

One extra metadata-fetch in the unfiltered path vs letting hyparquet do
it internally, but it's now bounded by `initialFetchSize` instead of
hyparquet's 512KB default. `tsc --noEmit` passes across the workspace;
all 118 tests still green.

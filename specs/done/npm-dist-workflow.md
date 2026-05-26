# `npm-dist` workflow for pyrmts packages

> Status: **done** (2026-05-25). Workflow at `.github/workflows/build-dist.yml`,
> packaging tweaks at `js/packages/{pyrmts,pyrmts-cfw,pyrmts-geo}/package.json`,
> in-workspace vitest aliases at `js/vitest.config.ts`.
>
> Diverged from the spec in two notable ways:
>
> 1. **Single `dist` branch with subdirs, not per-pkg branches.** Push produces
>    one `dist` branch with `js/packages/{pyrmts,pyrmts-cfw,pyrmts-geo}/`,
>    consumed via pnpm's `&path:` syntax (which pds already supports —
>    `switch.ts:140`). The spec's draft consumer ref
>    `github:runsascoded/pyrmts#dist-<sha>/js/packages/pyrmts` was wrong syntax;
>    correct form is `https://github.com/runsascoded/pyrmts#<sha>&path:/js/packages/pyrmts`.
>    `pds init <local-path>` auto-detects the subdir.
> 2. **Bypassed `npm-dist`'s `pkgs:` monorepo mode** and rolled the equivalent
>    ~80-line bash directly into the workflow. Reason: `pnpm pack` rewrites
>    `workspace:*` cross-deps to the literal version (`"0.0.0"`), which won't
>    resolve at consumer install time since these packages aren't on npm.
>    `npm-dist`'s monorepo script doesn't have a hook for cross-dep rewriting,
>    so it was simpler to inline. Cross-refs use branch name (`#dist&path:/...`)
>    — consumer lockfiles pin the resolved SHA, so the only race window is
>    initial install. Could be upstreamed to `npm-dist` later as a feature.
>
> Open question from the spec — "one workflow → many dist branches, or one
> per package?" — resolved as one branch with subdirs after empirically
> confirming `&path:` works (tested against `runsascoded/shapes` from `apvd`'s
> `package.json:29`).

## Goal

Awair's `cfw/serve` currently consumes `pyrmts` + `pyrmts-cfw` via
`file:../../../pyrmts/js/packages/pyrmts*` references — fragile (only works
when pyrmts is checked out at that exact relative path) and breaks any clean
CI build. The proper fix, per the `npm-dist` workflow Ryan uses across his
JS projects (CLAUDE.md):

- Run [`runsascoded/npm-dist`](https://github.com/runsascoded/npm-dist) GHA
  on push, which builds each package + publishes the built artifacts to a
  `dist-<sha>` branch.
- Downstream consumers depend on `git+https://github.com/runsascoded/pyrmts.git#dist-<sha>`
  (preferred) or `…#dist-<branch>` via `pds gh pyrmts` / `pds gh pyrmts-cfw`.

## What awair will switch to (once this lands)

```bash
cd cfw/serve
pds gh pyrmts pyrmts-cfw
```

producing in `cfw/serve/package.json`:

```json
"dependencies": {
  "pyrmts": "github:runsascoded/pyrmts#dist-<sha>/js/packages/pyrmts",
  "pyrmts-cfw": "github:runsascoded/pyrmts#dist-<sha>/js/packages/pyrmts-cfw"
}
```

(`pds` handles the subpath syntax. If pds doesn't, the workflow can also be
configured to push **per-package** branches like `dist-pyrmts-<sha>` /
`dist-pyrmts-cfw-<sha>` — npm-dist supports both modes.)

## Workflow setup (rough)

1. Add a `build` script to each publishable package's `package.json`:
   ```json
   "scripts": {
     "build": "tsc --build"
   }
   ```
   Currently `main`/`types` point at `./src/index.ts` (raw TS) — that's why
   consumers' `tsc` chokes on pyrmts internals (e.g. the `initialFetchSize`
   bug surfaced in awair's `tsc --noEmit` until fixed upstream). The build
   should emit `dist/index.js` + `dist/index.d.ts`, and `package.json`
   should switch `main`/`types` to the built files (or use `exports`).

2. Add `.github/workflows/build-dist.yml`:
   ```yaml
   name: Build dist branches
   on:
     push:
       branches: [main]
   jobs:
     dist:
       runs-on: ubuntu-latest
       steps:
         - uses: actions/checkout@v4
         - uses: pnpm/action-setup@v4
         - uses: actions/setup-node@v4
           with: { node-version: 22, cache: pnpm }
         - run: pnpm install --frozen-lockfile
         - run: pnpm -r run build
         - uses: runsascoded/npm-dist@v1
           with:
             # Build each workspace package into its own dist branch
             packages: js/packages/pyrmts,js/packages/pyrmts-cfw,js/packages/pyrmts-geo
             # ...see npm-dist README for exact inputs
   ```

3. First run produces `dist-<sha>` branches. Awair switches via `pds gh`.

## Side benefits

- **No more `pyrmts` type leakage**: built `.d.ts` files don't expose
  internal `.ts` files to downstream `tsc`. The `initialFetchSize` bug
  reproduced in awair was a symptom of this.
- **Pyrmts can be `pnpm add`'d** by anyone (other consumers: ctbk, tomat,
  crashes) without a local checkout.
- **Stable refs** by SHA — `pds gh` defaults to pinning by commit, so
  downstream builds are reproducible.

## Open questions

- **One workflow → many dist branches, or one per package?** `npm-dist`
  supports both. One-per-package keeps consumer `package.json` clean
  (each dep is its own ref). One combined keeps GH branch count low. Pick
  whichever your other projects (ctbk, etc.) use.
- **Workflow on tags or just `main` pushes?** Probably just main initially;
  add tag-triggered later if you want versioned releases.
- **Geo package**: `pyrmts-geo` has the same pattern. Include in this
  workflow.

## Awair-side follow-up (after this lands)

```bash
cd ~/c/awair/cfw/serve
pds init pyrmts
pds init pyrmts-cfw
pds gh pyrmts pyrmts-cfw   # or whatever pds's exact CLI is
# Remove the `pnpm.overrides` workaround in package.json — no longer needed
# Bump cfw/serve, redeploy worker
```

import { resolve } from 'node:path'
import { defineConfig } from 'vitest/config'

// Source aliases keep tests resolving against `src/` even though each
// package's `main`/`types` now point at `dist/` (for published consumers).
const pkg = (name: string): string => resolve(__dirname, 'packages', name, 'src/index.ts')

export default defineConfig({
  test: {
    include: ['packages/*/src/**/*.test.ts'],
    globals: false,
  },
  resolve: {
    alias: {
      // Sub-path aliases first so they take precedence over the bare pyrmts alias.
      'pyrmts/test-utils': resolve(__dirname, 'packages', 'pyrmts', 'src/shard-index-conformance.ts'),
      'pyrmts-react/kbd': resolve(__dirname, 'packages', 'pyrmts-react', 'src/kbd.ts'),
      pyrmts: pkg('pyrmts'),
      'pyrmts-cfw': pkg('pyrmts-cfw'),
      'pyrmts-geo': pkg('pyrmts-geo'),
      'pyrmts-react': pkg('pyrmts-react'),
    },
  },
})

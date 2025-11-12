import { defineConfig } from 'tsdown';

export default defineConfig({
  entry: ['src/index.ts'],
  format: ['esm'],
  dts: true, // Generate declaration files
  clean: true, // Clean output directory before build
  sourcemap: true, // Generate source maps
  treeshake: true, // Enable tree-shaking
  external: [
    '@aws-sdk/client-sqs', // Keep as external dependency
  ],
  outExtensions: ({ format }) => ({
    js: '.js', // Use .js instead of .mjs for ESM
  }),
});


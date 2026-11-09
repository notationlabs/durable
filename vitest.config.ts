import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // Tests start Node child processes that import the workspace packages,
    // which are TypeScript source. tsx lets those processes load it.
    env: { NODE_OPTIONS: "--import tsx" },
  },
});

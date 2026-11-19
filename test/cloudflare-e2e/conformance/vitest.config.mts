import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [cloudflareTest({ wrangler: { configPath: "./wrangler.jsonc" } })],
  test: {
    // Not *.test.ts, and .mts so that neither the root vitest run nor `tsc -p test` picks it up.
    include: ["suites.workerd.mts"],
  },
});

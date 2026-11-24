import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

export default defineConfig({
  resolve: {
    alias: {
      // Only workerd provides this module. The suites in test/cloudflare-e2e
      // run there with their own configs and load the real one.
      "cloudflare:workers": fileURLToPath(
        new URL("./test/cloudflare-workers-stub.ts", import.meta.url)
      ),
    },
  },
});

import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

const file = (path: string) => fileURLToPath(new URL(path, import.meta.url));

export default defineConfig({
  resolve: {
    alias: {
      // Only workerd provides this module. The suites in test/cloudflare-e2e
      // run there with their own configs and load the real one.
      "cloudflare:workers": file("./test/cloudflare-workers-stub.ts"),
      // Runs every delivery that a suite makes through the test invoker the
      // way a Cloudflare object runs it in another instance.
      ...(process.env.DURABLE_TEST_DELIVERIES === "isolated" && {
        "@durable/test-invoker": file("./test/isolated-test-invoker.ts"),
      }),
    },
  },
});

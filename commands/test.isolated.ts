import { defineCommand } from "@pokit/core";

export const command = defineCommand({
  label: "Run tests with each delivery run as a Cloudflare object runs it in another instance",
  run: async (r) => {
    await r.exec(
      "env DURABLE_TEST_DELIVERIES=isolated bun --bun ./node_modules/vitest/vitest.mjs run"
    );
  },
});

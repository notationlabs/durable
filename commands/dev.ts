import { defineCommand } from "@pokit/core";

export const command = defineCommand({
  label: "Watch types and tests",
  run: async (r) => {
    await r.exec(
      "pnpm exec concurrently -r \"pnpm exec tsc -p . --watch --preserveWatchOutput\" \"pnpm exec tsc -p test --watch --preserveWatchOutput\" \"bun --bun ./node_modules/vitest/vitest.mjs\"",
    );
  },
});

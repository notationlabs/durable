import { defineCommand } from "@pokit/core";

export const command = defineCommand({
  label: "Type-check the workspace",
  run: async (r) => {
    await r.exec("pnpm exec tsc -p .");
  },
});

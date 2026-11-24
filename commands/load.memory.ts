import { defineCommand } from "@pokit/core";

export const command = defineCommand({
  label: "Run the load suite against the in-memory runtime",
  run: async (r) => {
    await r.exec("bun test/load/memory.ts");
  },
});

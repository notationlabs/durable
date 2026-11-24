import { defineCommand } from "@pokit/core";

export const command = defineCommand({
  label: "Run the load suite against SQLite on Node",
  run: async (r) => {
    await r.exec("bun test/load/sqlite.ts node");
  },
});

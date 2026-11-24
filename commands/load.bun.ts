import { defineCommand } from "@pokit/core";

export const command = defineCommand({
  label: "Run the load suite against SQLite on Bun",
  run: async (r) => {
    await r.exec("bun test/load/sqlite.ts bun");
  },
});

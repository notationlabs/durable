import { defineCommand } from "@pokit/core";

// `pok test all` runs every suite in sequence.
export const command = defineCommand({
  label: "Run tests",
  enableRunAllChildren: "sequential",
});

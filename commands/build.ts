import { defineCommand } from "@pokit/core";

export const command = defineCommand({
  label: "Build the published packages",
  run: async (r) => {
    await r.exec('pnpm --filter "./packages/*" run build');
    // The examples import the published packages, so they are checked after the build.
    await r.exec("pnpm exec tsc -p examples");
  },
});

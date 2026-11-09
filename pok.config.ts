import { defineConfig } from "@pokit/core";
import { release } from "pok-plugins";

/** Builds both published packages in dependency order; identical across packages, so it runs once. */
const BUILD = 'pnpm --filter "./packages/*" run build';

const published = (dir: string) => ({ file: `packages/${dir}/package.json`, build: BUILD });
const versionOnly = (file: string) => ({ file, publish: false as const });

export default defineConfig({
  commandsDir: "./commands",
  appName: "durable",
  plugins: [
    release({
      // Lockstep: every manifest moves to the same version.
      packages: [
        versionOnly("package.json"),
        published("durable"),
        published("durable-conformance"),
        versionOnly("packages/core/package.json"),
        versionOnly("packages/http-server/package.json"),
        versionOnly("packages/runtime-conformance/package.json"),
        versionOnly("packages/sdk/package.json"),
        versionOnly("packages/sqlite-runtime/package.json"),
        versionOnly("packages/worker-invoker/package.json"),
        versionOnly("packages/test-invoker/package.json"),
        versionOnly("packages/test-runtime/package.json"),
        versionOnly("packages/test-utils/package.json"),
        versionOnly("examples/package.json"),
        versionOnly("test/package.json"),
      ],
      preid: "alpha",
    }),
  ],
});

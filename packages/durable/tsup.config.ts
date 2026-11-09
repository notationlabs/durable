import { resolve } from "node:path";
import { defineConfig } from "tsup";

const here = import.meta.dirname;
const source = (path: string) => resolve(here, "..", path);

export default defineConfig({
  entry: {
    index: source("sdk/src/index.ts"),
    runtime: source("core/src/index.ts"),
    http: source("http-server/src/index.ts"),
    worker: source("worker-invoker/src/index.ts"),
    sqlite: source("sqlite-runtime/src/index.ts"),
    "sqlite-bun": source("sqlite-runtime/src/bun.ts"),
    "sqlite-node": source("sqlite-runtime/src/node.ts"),
  },
  format: "esm",
  platform: "node",
  target: "node22",
  dts: true,
  splitting: true,
  sourcemap: true,
  clean: true,
  noExternal: [/^@durable\//],
  external: ["bun:sqlite"],
  // node:sqlite has no unprefixed form, so the prefix must survive the build.
  removeNodeProtocol: false,
  esbuildOptions(options) {
    options.alias = {
      "@durable/core": source("core/src/index.ts"),
      "@durable/sdk": source("sdk/src/index.ts"),
      "@durable/http-server": source("http-server/src/index.ts"),
      "@durable/worker-invoker": source("worker-invoker/src/index.ts"),
      "@durable/sqlite-runtime": source("sqlite-runtime/src/index.ts"),
    };
  },
});

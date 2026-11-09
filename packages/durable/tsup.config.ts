import { resolve } from "node:path";
import { defineConfig } from "tsup";
import { declaredImportsOnly } from "../../scripts/declared-imports";

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
  clean: true,
  external: ["bun:sqlite"],
  // node:sqlite has no unprefixed form, so the prefix must survive the build.
  removeNodeProtocol: false,
  esbuildPlugins: [declaredImportsOnly(here)],
});

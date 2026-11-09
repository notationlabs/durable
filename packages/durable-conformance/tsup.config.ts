import { resolve } from "node:path";
import { defineConfig } from "tsup";

const here = import.meta.dirname;

export default defineConfig({
  entry: { index: resolve(here, "../runtime-conformance/src/index.ts") },
  format: "esm",
  platform: "node",
  target: "node22",
  dts: true,
  sourcemap: true,
  clean: true,
  esbuildOptions(options) {
    options.alias = { "@durable/core": resolve(here, "src/runtime.ts") };
  },
});

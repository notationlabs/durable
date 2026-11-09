import { resolve } from "node:path";
import { defineConfig } from "tsup";
import { declaredImportsOnly } from "../../scripts/declared-imports";

const here = import.meta.dirname;

export default defineConfig({
  entry: { index: resolve(here, "../runtime-conformance/src/index.ts") },
  format: "esm",
  platform: "node",
  target: "node22",
  dts: true,
  clean: true,
  esbuildPlugins: [declaredImportsOnly(here)],
});

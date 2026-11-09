import { readFileSync } from "node:fs";
import { builtinModules } from "node:module";
import { join } from "node:path";
import type { Plugin } from "esbuild";

const builtins = new Set(builtinModules);

function packageName(specifier: string) {
  const parts = specifier.split("/");
  return specifier.startsWith("@") ? parts.slice(0, 2).join("/") : parts[0];
}

// Fails the build when bundled code imports a third-party package that the
// published package.json does not declare. esbuild would otherwise bundle it
// into the published output.
export function declaredImportsOnly(packageRoot: string): Plugin {
  const manifest = JSON.parse(readFileSync(join(packageRoot, "package.json"), "utf8"));
  const declared = new Set([
    ...Object.keys(manifest.dependencies ?? {}),
    ...Object.keys(manifest.peerDependencies ?? {}),
  ]);
  return {
    name: "declared-imports-only",
    setup(build) {
      build.onResolve({ filter: /^[^./]/ }, (args) => {
        if (args.path.startsWith("node:") || args.path.startsWith("bun:")) return undefined;
        const name = packageName(args.path);
        if (builtins.has(name) || declared.has(name) || name.startsWith("@durable/")) return undefined;
        return {
          errors: [{ text: `${args.importer} imports ${name}, which ${manifest.name} does not declare` }],
        };
      });
    },
  };
}

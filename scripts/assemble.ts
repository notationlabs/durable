import { builtinModules } from "node:module";
import { cpSync, existsSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";

// The published packages contain no code of their own. Each one is assembled
// from the built output of private workspace packages. Every import of a
// private package is rewritten either to a relative path inside the published
// package or to a public import of another published package.
type Target = {
  // Private packages copied into dist/<folder>, keyed by package name.
  bundled: Record<string, string>;
  // Private packages that are imported from another published package instead.
  external: Record<string, string>;
};

const targets: Record<string, Target> = {
  "packages/durable": {
    bundled: {
      "@durable/sdk": "sdk",
      "@durable/core": "core",
      "@durable/http-server": "http-server",
      "@durable/worker-invoker": "worker-invoker",
      "@durable/sqlite-runtime": "sqlite-runtime",
    },
    external: {},
  },
  "packages/durable-conformance": {
    bundled: { "@durable/runtime-conformance": "runtime-conformance" },
    // The runtime under test must share these classes with the suite, so the
    // suite imports them from the user's copy of @notation/durable.
    external: { "@durable/core": "@notation/durable/runtime" },
  },
};

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const builtins = new Set(builtinModules);
const specifierPattern = /((?:\bfrom|\bimport)\s*\(?\s*)(["'])([^"'\s]+)\2/g;

function walk(directory: string): string[] {
  return readdirSync(directory).flatMap((entry) => {
    const path = join(directory, entry);
    return statSync(path).isDirectory() ? walk(path) : [path];
  });
}

function isShipped(path: string) {
  if (path.endsWith(".test.d.ts")) return false;
  return path.endsWith(".js") || path.endsWith(".d.ts");
}

function packageName(specifier: string) {
  const parts = specifier.split("/");
  return specifier.startsWith("@") ? parts.slice(0, 2).join("/") : parts[0];
}

function assemble(targetDirectory: string) {
  const target = targets[targetDirectory];
  if (!target) throw new Error(`No assembly target for ${targetDirectory}`);

  const packageRoot = join(root, targetDirectory);
  const manifest = JSON.parse(readFileSync(join(packageRoot, "package.json"), "utf8"));
  const declared = new Set([
    ...Object.keys(manifest.dependencies ?? {}),
    ...Object.keys(manifest.peerDependencies ?? {}),
  ]);
  const dist = join(packageRoot, "dist");
  rmSync(dist, { recursive: true, force: true });

  for (const folder of Object.values(target.bundled)) {
    const source = join(root, "packages", folder, "dist");
    if (!existsSync(source)) throw new Error(`${source} does not exist. Build the workspace packages first.`);
    cpSync(source, join(dist, folder), {
      recursive: true,
      filter: (path) => statSync(path).isDirectory() || isShipped(path),
    });
  }

  const problems: string[] = [];

  for (const file of walk(dist)) {
    const text = readFileSync(file, "utf8");
    const rewritten = text.replace(specifierPattern, (match, prefix, quote, specifier: string) => {
      // The sources import relative paths without extensions, which consumers
      // using nodenext resolution cannot follow in the declarations.
      if (specifier.startsWith(".")) {
        if (!file.endsWith(".d.ts") || /\.[cm]?js$/.test(specifier)) return match;
        const base = join(dirname(file), specifier);
        if (existsSync(`${base}.d.ts`)) return `${prefix}${quote}${specifier}.js${quote}`;
        if (existsSync(join(base, "index.d.ts"))) return `${prefix}${quote}${specifier}/index.js${quote}`;
        problems.push(`${file}: ${specifier} has no declaration file`);
        return match;
      }

      const name = packageName(specifier);
      const subpath = specifier.slice(name.length + 1);

      const folder = target.bundled[name];
      if (folder) {
        const entry = join(dist, folder, `${subpath || "index"}.js`);
        if (!existsSync(entry)) problems.push(`${file}: ${specifier} has no file at ${entry}`);
        let path = relative(dirname(file), entry);
        if (!path.startsWith(".")) path = `./${path}`;
        return `${prefix}${quote}${path}${quote}`;
      }

      const replacement = target.external[name];
      if (replacement) {
        if (subpath) problems.push(`${file}: ${specifier} uses a subpath that has no public equivalent`);
        return `${prefix}${quote}${replacement}${quote}`;
      }

      return match;
    });

    for (const [, , , specifier] of rewritten.matchAll(specifierPattern)) {
      if (specifier.startsWith(".")) continue;
      if (specifier.startsWith("node:") || specifier.startsWith("bun:")) continue;
      const name = packageName(specifier);
      if (builtins.has(name)) continue;
      if (!declared.has(name)) problems.push(`${file}: imports ${name}, which ${targetDirectory}/package.json does not declare`);
    }

    if (rewritten !== text) writeFileSync(file, rewritten);
  }

  if (problems.length > 0) {
    throw new Error(`Assembly of ${targetDirectory} failed:\n${problems.join("\n")}`);
  }
}

const requested = process.argv.slice(2);
for (const targetDirectory of requested.length > 0 ? requested : Object.keys(targets)) {
  assemble(targetDirectory);
}

import { mkdtemp, readdir, rm, writeFile } from "node:fs/promises";
import { builtinModules } from "node:module";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const directory = await mkdtemp(join(tmpdir(), "durable-conformance-"));

function run(command: string[], cwd = root) {
  const result = Bun.spawnSync(command, {
    cwd,
    env: process.env,
    stderr: "inherit",
    stdout: "inherit",
  });
  if (result.exitCode !== 0) {
    throw new Error(`${command.join(" ")} exited with ${result.exitCode}`);
  }
}

async function tarball(prefix: string) {
  const name = (await readdir(directory)).find(
    (file) => file.startsWith(prefix) && /^\d/.test(file.slice(prefix.length))
  );
  if (!name) throw new Error(`No tarball starting with ${prefix} in ${directory}`);
  return `file:./${name}`;
}

// Installs the packed tarballs into an empty project, so the suites resolve
// without the private workspace packages.
try {
  for (const packageDirectory of ["durable", "durable-conformance"]) {
    run([
      "pnpm",
      "--dir",
      join(root, "packages", packageDirectory),
      "pack",
      "--pack-destination",
      directory,
    ]);
  }

  const durable = await tarball("notation-durable-");
  const conformance = await tarball("notation-durable-conformance-");

  await writeFile(
    join(directory, "package.json"),
    JSON.stringify({
      private: true,
      type: "module",
      devDependencies: {
        "@notation/durable": durable,
        "@notation/durable-conformance": conformance,
        "@types/node": "^22.18.0",
        typescript: "^5.9.2",
        vitest: "^4.1.12",
      },
    })
  );
  await writeFile(
    join(directory, "conformance.test.ts"),
    `import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  registerCancellationClientConformance,
  registerEffectClientConformance,
  registerExecutionClientConformance,
  registerJoinClientConformance,
  registerStoreClientConformance,
} from "@notation/durable-conformance";
import { SqliteRuntime, createSqliteDb } from "@notation/durable/sqlite/node";

function sqlite(options?: { clock: () => number }) {
  const dir = mkdtempSync(join(tmpdir(), "conformance-"));
  const path = join(dir, "durable.sqlite");
  const dbs = [createSqliteDb({ path })];

  const open = () => {
    const db = createSqliteDb({ path });
    dbs.push(db);
    return new SqliteRuntime(db, options);
  };

  return {
    runtime: new SqliteRuntime(dbs[0]!, options),
    createPeer: open,
    restart: open,
    dispose() {
      for (const db of dbs) db.close();
      rmSync(dir, { recursive: true, force: true });
    },
  };
}

registerStoreClientConformance({ name: "sqlite", create: () => sqlite() });
registerExecutionClientConformance({ name: "sqlite", create: sqlite });
registerEffectClientConformance({ name: "sqlite", create: sqlite });
registerJoinClientConformance({ name: "sqlite", create: sqlite });
registerCancellationClientConformance({ name: "sqlite", create: sqlite });
`
  );
  await writeFile(
    join(directory, "testing.test.ts"),
    `import { expect, test } from "vitest";
import { workflow } from "@notation/durable";
import { createTestSdk } from "@notation/durable/testing";

const greet = workflow<{ name: string }, string>(async function* (step, event) {
  return yield* step.run(() => \`Hello, \${event.params.name}\`);
});

test("the testing entry point runs a workflow in memory", async () => {
  const sdk = createTestSdk({ greet });
  await expect(
    sdk.triggerAndWait({ workflowId: "greet", params: { name: "World" } })
  ).resolves.toBe("Hello, World");
});
`
  );
  await writeFile(
    join(directory, "pnpm-workspace.yaml"),
    `allowBuilds:
  esbuild: true
`
  );

  run(["pnpm", "install"], directory);
  run(["pnpm", "exec", "tsc", "--noEmit", "--strict", "--module", "nodenext", "--moduleResolution", "nodenext", "--skipLibCheck", "false", "--types", "node", "conformance.test.ts", "testing.test.ts"], directory);
  run(["pnpm", "exec", "vitest", "run", "conformance.test.ts", "testing.test.ts"], directory);

  // Bun's browser target replaces Node built-ins with polyfills, so the
  // output does not show them. The plugin records them as they resolve.
  const nodeImports = new Set<string>();
  const client = await Bun.build({
    entrypoints: [join(directory, "node_modules/@notation/durable/dist/client.js")],
    target: "browser",
    plugins: [
      {
        name: "record-node-builtins",
        setup(build) {
          build.onResolve({ filter: /.*/ }, (args) => {
            if (args.path.startsWith("node:") || builtinModules.includes(args.path.split("/")[0])) {
              nodeImports.add(args.path);
            }
            return undefined;
          });
        },
      },
    ],
  });
  if (!client.success) {
    throw new AggregateError(client.logs, "@notation/durable/client does not bundle for the browser");
  }
  if (nodeImports.size > 0) {
    throw new Error(`@notation/durable/client imports ${[...nodeImports].join(", ")}`);
  }
} finally {
  await rm(directory, { recursive: true, force: true });
}

import { mkdtemp, readdir, rm, writeFile } from "node:fs/promises";
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
        vitest: "3.2.7",
      },
    })
  );
  await writeFile(
    join(directory, "conformance.test.ts"),
    `import {
  registerCancellationClientConformance,
  registerEffectClientConformance,
  registerExecutionClientConformance,
  registerJoinClientConformance,
  registerStoreClientConformance,
} from "@notation/durable-conformance";
import { SqliteRuntime, createSqliteDb } from "@notation/durable/sqlite/node";

function sqlite(options?: ConstructorParameters<typeof SqliteRuntime>[1]) {
  const db = createSqliteDb({ path: ":memory:", wal: false });
  return {
    runtime: new SqliteRuntime(db, options),
    createPeer: () => new SqliteRuntime(db, options),
    restart: () => new SqliteRuntime(db, options),
    dispose: () => db.close(),
  };
}

registerStoreClientConformance({ name: "installed-sqlite", create: () => sqlite() });
registerExecutionClientConformance({ name: "installed-sqlite", create: sqlite });
registerEffectClientConformance({ name: "installed-sqlite", create: sqlite });
registerJoinClientConformance({ name: "installed-sqlite", create: sqlite });
registerCancellationClientConformance({ name: "installed-sqlite", create: sqlite });
`
  );
  await writeFile(
    join(directory, "pnpm-workspace.yaml"),
    `allowBuilds:
  esbuild: true
`
  );

  run(["pnpm", "install"], directory);
  run(["pnpm", "exec", "tsc", "--noEmit", "--strict", "--module", "nodenext", "--moduleResolution", "nodenext", "--skipLibCheck", "false", "--types", "node", "conformance.test.ts"], directory);
  run(["pnpm", "exec", "vitest", "run", "conformance.test.ts"], directory);
} finally {
  await rm(directory, { recursive: true, force: true });
}

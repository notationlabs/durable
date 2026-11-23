import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { createHttpSdkFactory } from "@durable/sdk";
import { runLoad, type LoadRouter } from "@durable/runtime-load";
import { finish, loadOptions } from "./options";

// Usage: bun test/load/sqlite.ts bun|node
const runtime = process.argv[2];
if (runtime !== "bun" && runtime !== "node") {
  throw new Error("Usage: bun test/load/sqlite.ts bun|node");
}

const dir = mkdtempSync(join(tmpdir(), "durable-load-"));
const env: Record<string, string | undefined> = { ...process.env, LOAD_DB: join(dir, "load.sqlite") };
let command: string[];
if (runtime === "bun") {
  command = ["bun", join(import.meta.dirname, "sqlite-server.ts")];
} else {
  // Node runs the server and every worker from a bundle, so that no process
  // loads TypeScript source at start-up. The Bun driver stays out of the
  // bundle, because Node cannot load `bun:sqlite`.
  const build = await Bun.build({
    entrypoints: ["sqlite-server.ts", "sqlite-worker.ts"].map((file) => join(import.meta.dirname, file)),
    outdir: join(dir, "build"),
    target: "node",
    external: ["@durable/sqlite-runtime/bun"],
  });
  if (!build.success) throw new AggregateError(build.logs, "Bundling the Node server failed");
  command = ["node", join(dir, "build", "sqlite-server.js")];
  env.LOAD_WORKER_PATH = join(dir, "build", "sqlite-worker.js");
}
const server = spawn(command[0], command.slice(1), { env, stdio: ["ignore", "pipe", "inherit"] });

try {
  const port = await new Promise<number>((resolve, reject) => {
    server.once("exit", (code) => reject(new Error(`Server exited with code ${code}`)));
    createInterface({ input: server.stdout! }).once("line", (line) => resolve(JSON.parse(line).port));
  });
  const base = `http://127.0.0.1:${port}`;
  const sdk = createHttpSdkFactory<LoadRouter>()({ url: base });
  const report = await runLoad(
    {
      sdk,
      probe: async () => (await fetch(`${base}/load/probe`)).json() as Promise<Record<string, number>>,
    },
    loadOptions()
  );
  finish(`sqlite-${runtime}`, report);
} finally {
  server.kill();
  rmSync(dir, { recursive: true, force: true });
}

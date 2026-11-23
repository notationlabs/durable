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
const serverPath = join(import.meta.dirname, "sqlite-server.ts");
// The server and its worker processes load the private packages' source, which
// Node reads through tsx.
const server = spawn(
  runtime === "bun" ? "bun" : "node",
  runtime === "bun" ? [serverPath] : ["--import", "tsx", serverPath],
  {
    env: { ...process.env, LOAD_DB: join(dir, "load.sqlite") },
    stdio: ["ignore", "pipe", "inherit"],
  }
);

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

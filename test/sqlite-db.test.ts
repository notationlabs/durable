import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, test } from "vitest";

const openDb = fileURLToPath(new URL("./fixtures/sqlite/open-db.mjs", import.meta.url));

function open(path: string) {
  return new Promise<{ code: number | null; stderr: string }>((resolve) => {
    const child = spawn(process.execPath, [openDb, path], { stdio: ["ignore", "ignore", "pipe"] });
    let stderr = "";
    child.stderr.on("data", (chunk) => (stderr += chunk));
    child.on("exit", (code) => resolve({ code, stderr }));
  });
}

test("processes that open a new database file at the same time all start", async () => {
  const dir = mkdtempSync(join(tmpdir(), "durable-sqlite-db-"));
  try {
    const path = join(dir, "durable.sqlite");
    const results = await Promise.all(Array.from({ length: 8 }, () => open(path)));
    expect(results).toEqual(results.map(() => ({ code: 0, stderr: "" })));
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}, 30_000);

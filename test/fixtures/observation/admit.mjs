import { createInterface } from "node:readline";
import { SqliteRuntime } from "@yieldstar/sqlite-runtime";

const [dbPath, request] = process.argv.slice(2);
const { createSqliteDb } =
  "Bun" in globalThis
    ? await import("@yieldstar/sqlite-runtime/bun")
    : await import("@yieldstar/sqlite-runtime/node");

const runtime = new SqliteRuntime(createSqliteDb({ path: dbPath, wal: true }));
const { context, ...event } = JSON.parse(request);

// The test writes "go" to every process once all of them print "ready", so
// the admissions start together.
const lines = createInterface({ input: process.stdin });
process.stdout.write("ready\n");
for await (const line of lines) {
  if (line !== "go") continue;
  const admission = await runtime.executions.admit({
    ...event,
    context: new Map(Object.entries(context ?? {})),
  });
  process.stdout.write(`${JSON.stringify(admission)}\n`);
  process.exit(0);
}

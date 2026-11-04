import { ExecutionDispatcher, WorkflowRunner } from "@yieldstar/core";
import { SqliteRuntime } from "@yieldstar/sqlite-runtime";
import { createWorkflowInvoker } from "@yieldstar/test-invoker";
import { workflow } from "yieldstar";

const [dbPath] = process.argv.slice(2);
const { createSqliteDb } =
  "Bun" in globalThis
    ? await import("@yieldstar/sqlite-runtime/bun")
    : await import("@yieldstar/sqlite-runtime/node");

const router = {
  "charge-order": workflow(async function* (step, event) {
    yield* step.delay("cancellation-window", event.params.delay ?? 0);
    return yield* step.run("charge", () => ({ receipt: `r-${event.params.orderId}` }));
  }),
};

const logger = { info() {}, error() {}, warn() {}, debug() {} };
const runtime = new SqliteRuntime(createSqliteDb({ path: dbPath, wal: true }));
const runner = new WorkflowRunner(runtime, { router, logger });
const invoker = createWorkflowInvoker({ runner, logger });
new ExecutionDispatcher(runtime, { invoker, logger, pollInterval: 5 }).start();

import { ExecutionDispatcher, WorkflowRunner } from "@durable/core";
import { SqliteRuntime } from "@durable/sqlite-runtime";
import { createWorkflowInvoker } from "@durable/test-invoker";
import { workflow } from "@durable/sdk";

const [dbPath] = process.argv.slice(2);
const { createSqliteDb } =
  "Bun" in globalThis
    ? await import("@durable/sqlite-runtime/bun")
    : await import("@durable/sqlite-runtime/node");

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

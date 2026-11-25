import pino from "pino";
import { expect, test } from "vitest";
import { ExecutionDispatcher, WorkflowRunner } from "@durable/core";
import { createLocalSdk, createWorkflow, defineStore } from "@durable/sdk";
import { SqliteRuntime } from "@durable/sqlite-runtime";
import { createWorkflowInvoker } from "@durable/test-invoker";
import { testSchema } from "@durable/test-utils";

const isBun = "Bun" in globalThis;
const { createSqliteDb } = isBun
  ? await import("@durable/sqlite-runtime/bun")
  : await import("@durable/sqlite-runtime/node");

type SqliteDb = ReturnType<typeof createSqliteDb>;

const Gate = defineStore("gate", testSchema<{ open: boolean }>(), {
  initial: () => ({ open: false }),
});

const child = createWorkflow(async function* (step, event: { params: { n: number } }) {
  yield* step.delay("pause", 5);
  return yield* step.run("double", () => event.params.n * 2);
});

// Reaches every table the runtime writes for a workflow without effects: step
// records, a start request, a join waiter, a store, a store waiter and an
// applied store step.
const parent = createWorkflow(async function* (step, event: { params: { gate: string } }) {
  const first = yield* step.run("first", () => 1);
  const doubled = yield* step.call<typeof child>("child", {
    workflowId: "child",
    params: { n: first },
  });
  const gate = step.store(Gate, { id: event.params.gate });
  yield* gate.when("opened", (state) => (state.open ? true : undefined));
  yield* gate.update("close", (draft) => {
    draft.open = false;
  });
  return doubled + 1;
});

const router = { parent, child };

async function runParent(db: SqliteDb) {
  const logger = pino({ level: "silent" });
  const runtime = new SqliteRuntime(db);
  const invoker = createWorkflowInvoker({
    runner: new WorkflowRunner(runtime, { router, logger }),
    logger,
  });
  const dispatcher = new ExecutionDispatcher(runtime, {
    invoker,
    logger,
    pollInterval: 1,
    keepAlive: false,
  });
  const sdk = createLocalSdk<typeof router>(runtime, { invoker, pollInterval: 1 });
  dispatcher.start();
  try {
    const { executionId } = await sdk.trigger({ workflowId: "parent", params: { gate: "g" } });
    await expect.poll(async () => (await sdk.poll(executionId))?.status).toBe("suspended");
    await expect
      .poll(() => db.query(`SELECT * FROM store_waiters`).all().length, { timeout: 5_000 })
      .toBe(1);
    await runtime.stores.store(Gate, "g").update((draft) => {
      draft.open = true;
    });
    return await sdk.wait(executionId, { timeout: 5_000 });
  } finally {
    await dispatcher.stop();
  }
}

test("an update of an execution sets only the columns whose value changes", async () => {
  const db = createSqliteDb({ path: ":memory:", wal: false });
  try {
    new SqliteRuntime(db);
    // SQLite writes every index on a column that an UPDATE sets, and a
    // trigger `OF` a column fires for the same updates. Each trigger records
    // an update that set its column to the value the column had.
    db.run(`CREATE TEMP TABLE repeated (column_name TEXT NOT NULL)`);
    const columns = db.query<{ name: string }>(`PRAGMA table_info(executions)`).all();
    expect(columns.length).toBeGreaterThan(20);
    for (const { name } of columns) {
      db.run(`
        CREATE TEMP TRIGGER repeated_${name} BEFORE UPDATE OF ${name} ON executions
        WHEN NEW.${name} IS OLD.${name}
        BEGIN INSERT INTO repeated VALUES ('${name}'); END
      `);
    }

    expect(await runParent(db)).toMatchObject({ status: "completed", result: 3 });
    expect(db.query(`SELECT DISTINCT column_name FROM repeated`).all()).toEqual([]);
  } finally {
    db.close();
  }
});

test("a database whose tables an earlier version created runs a workflow", async () => {
  const db = createSqliteDb({ path: ":memory:", wal: false });
  try {
    // The tables as they were created before they became WITHOUT ROWID.
    db.run(`
      CREATE TABLE step_responses (
        execution_id TEXT NOT NULL,
        step_key TEXT NOT NULL,
        step_attempt INTEGER NOT NULL,
        step_done BOOL NOT NULL,
        step_response TEXT NOT NULL,
        PRIMARY KEY (execution_id, step_key, step_attempt)
      );
      CREATE INDEX idx_execution_step_attempt
      ON step_responses(execution_id, step_key, step_attempt DESC);
      CREATE TABLE join_waiters (
        target_execution_id TEXT NOT NULL,
        execution_id TEXT NOT NULL,
        step_key TEXT NOT NULL,
        PRIMARY KEY (target_execution_id, execution_id, step_key)
      );
      CREATE TABLE effect_waiters (
        effect_id TEXT NOT NULL,
        execution_id TEXT NOT NULL,
        step_key TEXT NOT NULL,
        PRIMARY KEY (effect_id, execution_id, step_key)
      );
      CREATE TABLE store_waiters (
        workflow_id TEXT NOT NULL,
        execution_id TEXT NOT NULL,
        step_key TEXT NOT NULL,
        store_name TEXT NOT NULL,
        store_id TEXT NOT NULL,
        since_version INTEGER NOT NULL,
        read_paths TEXT NOT NULL,
        PRIMARY KEY (store_name, store_id, execution_id, step_key)
      );
      CREATE TABLE start_requests (
        caller_execution_id TEXT NOT NULL,
        step_key TEXT NOT NULL,
        workflow_id TEXT NOT NULL,
        params TEXT,
        execution_id TEXT NOT NULL,
        PRIMARY KEY (caller_execution_id, step_key)
      );
      CREATE TABLE store_applied_steps (
        store_name TEXT NOT NULL,
        store_id TEXT NOT NULL,
        execution_id TEXT NOT NULL,
        step_key TEXT NOT NULL,
        result TEXT NOT NULL,
        PRIMARY KEY (store_name, store_id, execution_id, step_key)
      );
    `);

    expect(await runParent(db)).toMatchObject({ status: "completed", result: 3 });
    // The runtime wrote to the tables it found, and did not replace them.
    const withRowid = db
      .query<{ count: number }>(`SELECT COUNT(rowid) AS count FROM step_responses`)
      .get();
    expect(Number(withRowid?.count)).toBeGreaterThan(5);
  } finally {
    db.close();
  }
});

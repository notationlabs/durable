import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { Logger } from "pino";
import { afterEach, describe, expect, test, vi } from "vitest";
import {
  ExecutionDispatcher,
  defineStore,
  type ExecutionLease,
  type RetentionOptions,
  type Runtime,
  type WorkflowInvoker,
} from "@yieldstar/core";
import { MemoryRuntime } from "@yieldstar/test-runtime";
import { SqliteRuntime, type SqliteDriver } from "@yieldstar/sqlite-runtime";
import { testSchema } from "@yieldstar/test-utils";

const { createSqliteDb } =
  "Bun" in globalThis
    ? await import("@yieldstar/sqlite-runtime/bun")
    : await import("@yieldstar/sqlite-runtime/node");

const DAY = 86_400_000;
const START = 1_000 * DAY;
const ORDER = "order-42";
const RETAIN: RetentionOptions = { default: { completed: 7 * DAY, failed: 30 * DAY } };

const OrderStore = defineStore("orders", testSchema<{ paid?: boolean }>(), {
  initial: () => ({}),
});
const LedgerStore = defineStore("ledger", testSchema<{ total?: number }>(), {
  initial: () => ({}),
});

type Backend = {
  clock: { now: number };
  open(retain?: RetentionOptions): Runtime;
  /** The SQLite connection, for checks of rows the client API does not expose. */
  db?: SqliteDriver;
};

const cleanups: (() => void)[] = [];
afterEach(() => {
  for (const fn of cleanups.splice(0).reverse()) fn();
});

function memoryBackend(): Backend {
  const clock = { now: START };
  let runtime: MemoryRuntime | undefined;
  return {
    clock,
    open(retain = RETAIN) {
      // A memory runtime cannot be reopened, so later opens share the first
      // one and replace its policy, as a restart with new options would.
      runtime ??= new MemoryRuntime({ clock: () => clock.now, retain });
      (runtime as { retain?: RetentionOptions }).retain = retain;
      return runtime;
    },
  };
}

function sqliteBackend(): Backend {
  const clock = { now: START };
  const dir = mkdtempSync(join(tmpdir(), "yieldstar-retention-"));
  const path = join(dir, "retention.db");
  const dbs: SqliteDriver[] = [];
  const connect = () => {
    const db = createSqliteDb({ path, wal: true });
    dbs.push(db);
    return db;
  };
  const db = connect();
  cleanups.push(() => {
    for (const each of dbs) each.close();
    rmSync(dir, { recursive: true, force: true });
  });
  return {
    clock,
    db,
    open: (retain = RETAIN) => new SqliteRuntime(connect(), { clock: () => clock.now, retain }),
  };
}

const backends: [string, () => Backend][] = [
  ["memory", memoryBackend],
  ["sqlite", sqliteBackend],
];

async function admit(runtime: Runtime, executionId: string, workflowId = "place-order") {
  const admitted = await runtime.executions.admit({
    executionId,
    workflowId,
    params: { orderId: 42 },
    context: new Map(),
  });
  expect(admitted.status).toBe("accepted");
}

async function claim(runtime: Runtime, executionId: string): Promise<ExecutionLease> {
  const [lease] = await runtime.executions.claimDue({
    limit: 1,
    duration: 30_000,
    filter: { executionIds: [executionId] },
  });
  if (!lease) throw new Error(`${executionId} is not due`);
  return lease;
}

async function complete(runtime: Runtime, lease: ExecutionLease) {
  await runtime.executions.acknowledge({
    lease,
    outcome: { status: "completed", result: "shipped" },
  });
}

/**
 * Runs order-42 to completion with a step record, a store receipt on a
 * shared store and on its own store, a started child and an effect waiter.
 */
async function runOrder(runtime: Runtime) {
  await admit(runtime, ORDER);
  const lease = await claim(runtime, ORDER);
  await runtime.heap.writeStep({
    lease,
    stepKey: "reserve",
    stepAttempt: 1,
    done: true,
    stepResponseJson: "{}",
    stepKind: "run",
  });
  await runtime.stores.updateStore({
    definition: LedgerStore,
    id: "shared",
    updater: (draft) => {
      draft.total = 1;
    },
    stepId: { executionId: ORDER, stepKey: "ledger" },
  });
  await runtime.stores.updateStore({
    definition: OrderStore,
    id: ORDER,
    updater: (draft) => {
      draft.paid = true;
    },
    stepId: { executionId: ORDER, stepKey: "pay" },
  });
  const started = await runtime.executions.start({
    requestId: { executionId: ORDER, stepKey: "start:ship" },
    workflowId: "ship-order",
  });
  if (started.status !== "accepted") throw new Error(started.message);
  await runtime.effects.begin({
    effectId: "charge-42",
    operation: "charge",
    input: { amount: 1 },
    executionId: ORDER,
    stepKey: "charge",
  });
  await runtime.effects.watch({ effectId: "charge-42", executionId: ORDER, stepKey: "charge" });
  await complete(runtime, lease);
  return { child: started.executionId };
}

describe.each(backends)("retention on %s", (_name, create) => {
  test("an expired execution is pruned, and its tombstone answers every client", async () => {
    const backend = create();
    const runtime = backend.open();
    const { child } = await runOrder(runtime);

    backend.clock.now += 7 * DAY - 1;
    expect(await runtime.prune()).toEqual({ pruned: 0, cleaned: 0 });
    backend.clock.now += 1;
    expect(await runtime.prune()).toEqual({ pruned: 1, cleaned: 1 });
    const prunedAt = backend.clock.now;

    const tombstone = {
      status: "pruned",
      executionId: ORDER,
      workflowId: "place-order",
      terminalStatus: "completed",
      prunedAt,
    };
    expect(await runtime.executions.get(ORDER)).toEqual(tombstone);
    expect(await runtime.heap.listSteps({ executionId: ORDER })).toEqual([]);
    expect(await runtime.heap.readStep({ executionId: ORDER, stepKey: "reserve" })).toBeNull();
    expect(await runtime.executions.listPendingPrunes({ limit: 10 })).toEqual([]);

    // The child, the shared store, the execution's own store and the effect stay.
    expect(await runtime.executions.get(child)).toMatchObject({
      status: "admitted",
      parent: { executionId: ORDER, stepKey: "start:ship", detached: false },
    });
    expect(
      (await runtime.stores.getStore({ definition: LedgerStore, id: "shared" })).state
    ).toEqual({ total: 1 });
    expect((await runtime.stores.getStore({ definition: OrderStore, id: ORDER })).state).toEqual({
      paid: true,
    });
    expect(await runtime.effects.getEffect("charge-42")).toMatchObject({ status: "pending" });

    // A repeated receipt is gone, so the update applies again.
    const repeated = await runtime.stores.updateStore({
      definition: LedgerStore,
      id: "shared",
      updater: (draft) => {
        draft.total = 2;
      },
      stepId: { executionId: ORDER, stepKey: "ledger" },
    });
    expect(repeated.state).toEqual({ total: 2 });

    // Settling the effect wakes nobody: the waiter went with the cleanup.
    await runtime.effects.settle({
      effectId: "charge-42",
      operation: "charge",
      status: "settled",
      result: 1,
    });
    expect(
      await runtime.executions.claimDue({
        limit: 10,
        duration: 30_000,
        filter: { executionIds: [ORDER] },
      })
    ).toEqual([]);

    const listed = (await runtime.executions.list()).executions;
    expect(listed.map((execution) => execution.executionId)).toEqual([child]);
    expect(await runtime.executions.list({ status: "completed" })).toEqual({ executions: [] });
    expect(await runtime.executions.list({ status: "pruned" })).toEqual({
      executions: [tombstone],
    });

    const request = {
      executionId: ORDER,
      workflowId: "place-order",
      params: { orderId: 42 },
      context: new Map(),
    };
    expect(await runtime.executions.admit(request)).toMatchObject({
      status: "rejected",
      code: "EXECUTION_ID_CONFLICT",
    });
    expect(await runtime.executions.admit({ ...request, idempotent: true })).toMatchObject({
      status: "rejected",
      code: "EXECUTION_PRUNED",
    });
    expect(await runtime.executions.cancel({ executionId: ORDER, force: true })).toEqual(
      tombstone
    );
    expect(await runtime.executions.clearDivergence(ORDER)).toEqual({ status: "not-diverged" });
    expect(await runtime.executions.requestWake({ executionId: ORDER, wakeAt: 0 })).toEqual({
      status: "ignored",
    });
    expect(
      await runtime.joins.join({ targetExecutionId: ORDER, executionId: child, stepKey: "join" })
    ).toEqual({ status: "pruned" });
    expect(await runtime.joins.getWaiters(ORDER)).toEqual([]);
    expect(await runtime.executions.get(ORDER)).toEqual(tombstone);

    if (backend.db) {
      const count = (table: string, where: string) =>
        backend.db!.query<{ n: number }>(`SELECT COUNT(*) AS n FROM ${table} WHERE ${where}`).get()!
          .n;
      expect(count("start_requests", "caller_execution_id = 'order-42'")).toBe(0);
      expect(count("store_applied_steps", "execution_id = 'order-42' AND step_key = 'pay'")).toBe(0);
      expect(count("effect_waiters", "execution_id = 'order-42'")).toBe(0);
      expect(count("executions", "execution_id = 'order-42' AND params = ''")).toBe(1);
    }
  });

  test("pruneExecutionStores deletes the stores whose id is the execution ID", async () => {
    const backend = create();
    const runtime = backend.open({ default: { completed: 0, pruneExecutionStores: true } });
    await runOrder(runtime);
    expect(await runtime.prune()).toEqual({ pruned: 1, cleaned: 1 });
    expect(await runtime.stores.listStores(OrderStore)).toEqual([]);
    expect(await runtime.stores.listStores(LedgerStore)).toEqual(["shared"]);
  });

  test("non-terminal executions, outcomes without a period and runtimes without a policy are kept", async () => {
    const backend = create();
    const runtime = backend.open({
      default: { failed: 0 },
      workflows: { "charge-card": { completed: 0 } },
    });
    await admit(runtime, "admitted-42");
    await admit(runtime, "suspended-42");
    const lease = await claim(runtime, "suspended-42");
    await runtime.executions.acknowledge({ lease, outcome: { status: "suspended" } });
    await admit(runtime, "completed-42");
    await complete(runtime, await claim(runtime, "completed-42"));
    await admit(runtime, "charge-42", "charge-card");
    await complete(runtime, await claim(runtime, "charge-42"));

    backend.clock.now += 1_000 * DAY;
    expect(await runtime.prune()).toEqual({ pruned: 1, cleaned: 1 });
    expect(await runtime.executions.get("charge-42")).toMatchObject({ status: "pruned" });
    for (const id of ["admitted-42", "suspended-42", "completed-42"]) {
      expect(await runtime.executions.get(id)).not.toMatchObject({ status: "pruned" });
    }

    const unconfigured = backend.open({});
    expect(await unconfigured.prune()).toEqual({ pruned: 0, cleaned: 0 });
  });

  test("a target is kept while a non-terminal joiner waits, and pruned after the join is recorded", async () => {
    const backend = create();
    const runtime = backend.open({ default: { completed: 0 } });
    await admit(runtime, "ship-42", "ship-order");
    await admit(runtime, "charge-42", "charge-order");
    const joiner = await claim(runtime, "charge-42");
    const waiter = { targetExecutionId: "ship-42", executionId: "charge-42", stepKey: "join" };
    expect(await runtime.joins.join(waiter)).toEqual({ status: "waiting" });
    await runtime.executions.acknowledge({ lease: joiner, outcome: { status: "suspended" } });
    await complete(runtime, await claim(runtime, "ship-42"));

    // The joiner is woken but not delivered.
    expect(await runtime.prune()).toEqual({ pruned: 0, cleaned: 0 });
    expect(await runtime.executions.get("ship-42")).toMatchObject({ status: "completed" });

    const delivery = await claim(runtime, "charge-42");
    await runtime.heap.writeStep({
      lease: delivery,
      stepKey: "join",
      stepAttempt: 1,
      done: true,
      stepResponseJson: "{}",
    });
    await runtime.executions.acknowledge({ lease: delivery, outcome: { status: "suspended" } });
    expect(await runtime.prune()).toEqual({ pruned: 1, cleaned: 1 });
    expect(await runtime.executions.get("ship-42")).toMatchObject({ status: "pruned" });
  });

  test("a waiter left by a terminal joiner does not keep the target", async () => {
    const backend = create();
    const runtime = backend.open({ default: { completed: 0 } });
    await admit(runtime, "ship-42", "ship-order");
    await admit(runtime, "charge-42", "charge-order");
    await runtime.joins.join({
      targetExecutionId: "ship-42",
      executionId: "charge-42",
      stepKey: "join",
    });
    await complete(runtime, await claim(runtime, "charge-42"));
    await complete(runtime, await claim(runtime, "ship-42"));
    expect(await runtime.prune()).toEqual({ pruned: 2, cleaned: 2 });
  });

  test("a pass that stops after the tombstone is finished by the next one", async () => {
    const backend = create();
    const runtime = backend.open();
    await runOrder(runtime);
    backend.clock.now += 7 * DAY;

    const [tombstone] = await runtime.executions.prune({ retain: RETAIN, limit: 10 });
    expect(tombstone).toMatchObject({ executionId: ORDER, status: "pruned" });
    expect(await runtime.executions.get(ORDER)).toEqual(tombstone);
    expect(await runtime.executions.listPendingPrunes({ limit: 10 })).toEqual([
      { executionId: ORDER, deleteExecutionStore: false },
    ]);
    expect(await runtime.heap.listSteps({ executionId: ORDER })).toHaveLength(1);

    expect(await runtime.prune()).toEqual({ pruned: 0, cleaned: 1 });
    expect(await runtime.heap.listSteps({ executionId: ORDER })).toEqual([]);
    expect(await runtime.executions.get(ORDER)).toEqual(tombstone);
  });

  test("a shortened policy applies to executions that are already terminal", async () => {
    const backend = create();
    await runOrder(backend.open());
    backend.clock.now += DAY;
    expect(await backend.open().prune()).toEqual({ pruned: 0, cleaned: 0 });
    expect(await backend.open({ default: { completed: DAY } }).prune()).toEqual({
      pruned: 1,
      cleaned: 1,
    });
  });

  test("the dispatcher prunes on its interval when the runtime has a policy", async () => {
    const backend = create();
    const runtime = backend.open({ default: { completed: 0 } });
    await admit(runtime, ORDER);
    await complete(runtime, await claim(runtime, ORDER));
    const dispatcher = new ExecutionDispatcher(runtime, {
      invoker: {
        deliver: vi.fn(),
        workflowEndEmitter: { emit() {} },
      } as unknown as WorkflowInvoker,
      logger: { info() {}, warn() {}, error() {} } as unknown as Logger,
      pollInterval: 5,
      pruneInterval: 5,
    });
    dispatcher.start();
    try {
      await vi.waitFor(async () => {
        expect(await runtime.executions.get(ORDER)).toMatchObject({ status: "pruned" });
      });
    } finally {
      await dispatcher.stop();
    }
  });
});

describe("retention on SQLite", () => {
  test("two processes pruning one file reach the same end state", async () => {
    const backend = sqliteBackend();
    const runtime = backend.open({ default: { completed: 0 } });
    const ids = Array.from({ length: 20 }, (_, i) => `order-${i}`);
    for (const id of ids) {
      await admit(runtime, id);
      const lease = await claim(runtime, id);
      await runtime.heap.writeStep({
        lease,
        stepKey: "a",
        stepAttempt: 1,
        done: true,
        stepResponseJson: "{}",
      });
      await complete(runtime, lease);
    }
    const peers = [0, 1].map(() => backend.open({ default: { completed: 0 } }));
    const results = await Promise.all(peers.map((peer) => peer.prune({ limit: 7 })));
    while ((await peers[0].prune({ limit: 7 })).pruned > 0);
    expect(results.reduce((sum, result) => sum + result.pruned, 0)).toBeLessThanOrEqual(ids.length);
    const page = await runtime.executions.list({ status: "pruned", limit: 100 });
    const prunedIds = page.executions.map((execution) => execution.executionId);
    expect(prunedIds.sort()).toEqual([...ids].sort());
    expect(await runtime.executions.listPendingPrunes({ limit: 100 })).toEqual([]);
    for (const id of ids) {
      expect(await runtime.heap.listSteps({ executionId: id })).toEqual([]);
    }
  });

  test("a database from before retention reads its rows as not pruned and prunes them", async () => {
    const db = createSqliteDb({ path: ":memory:", wal: false });
    cleanups.push(() => db.close());
    db.run(`
      CREATE TABLE executions (
        execution_id TEXT PRIMARY KEY,
        workflow_id TEXT NOT NULL,
        params TEXT NOT NULL,
        context TEXT NOT NULL,
        status TEXT NOT NULL,
        outcome TEXT,
        admitted_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        due_at INTEGER,
        claimed_due_at INTEGER,
        lease_token INTEGER NOT NULL DEFAULT 0,
        lease_expires_at INTEGER,
        unacknowledged_delivery_count INTEGER NOT NULL DEFAULT 0,
        redeliver_at INTEGER,
        next_claim_at INTEGER,
        parent_execution_id TEXT,
        parent_step_key TEXT,
        admission_sequence INTEGER,
        parent_detached INTEGER NOT NULL DEFAULT 0,
        cancellation_reason TEXT,
        cancellation_source TEXT,
        cancellation_forced INTEGER,
        cancellation_requested_at INTEGER,
        cancellation_delivered_at INTEGER,
        workflow_version INTEGER,
        divergence_code TEXT,
        divergence_step_key TEXT,
        divergence_workflow_version INTEGER,
        divergence_detected_at INTEGER
      );
      INSERT INTO executions (
        execution_id, workflow_id, params, context, status, outcome,
        admitted_at, updated_at, admission_sequence
      ) VALUES (
        'order-42', 'place-order', '{}', '[]', 'completed',
        '{"result":"shipped","status":"completed"}', 10, 10, 1
      );
    `);
    const clock = { now: 10 };
    const runtime = new SqliteRuntime(db, {
      clock: () => clock.now,
      retain: { default: { completed: 5 } },
    });
    expect(await runtime.executions.get("order-42")).toMatchObject({ status: "completed" });
    expect(await new SqliteRuntime(db).executions.list()).toMatchObject({
      executions: [{ executionId: "order-42", status: "completed" }],
    });
    clock.now = 15;
    expect(await runtime.prune()).toEqual({ pruned: 1, cleaned: 1 });
    expect(await runtime.executions.get("order-42")).toEqual({
      status: "pruned",
      executionId: "order-42",
      workflowId: "place-order",
      terminalStatus: "completed",
      prunedAt: 15,
    });
  });

  test("invalid retention periods throw a TypeError from the constructor", () => {
    const db = createSqliteDb({ path: ":memory:", wal: false });
    cleanups.push(() => db.close());
    expect(() => new SqliteRuntime(db, { retain: { default: { completed: -1 } } })).toThrow(
      TypeError
    );
    expect(() => new MemoryRuntime({ retain: { default: { failed: Infinity } } })).toThrow(
      TypeError
    );
  });
});

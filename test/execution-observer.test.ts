import { EventEmitter } from "node:events";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, test } from "vitest";
import {
  ExecutionListRequestError,
  createExecutionObserver,
  type Runtime,
} from "@yieldstar/core";
import { MemoryRuntime } from "@yieldstar/test-runtime";
import { SqliteRuntime } from "@yieldstar/sqlite-runtime";

const isBun = "Bun" in globalThis;
const { createSqliteDb } = isBun
  ? await import("@yieldstar/sqlite-runtime/bun")
  : await import("@yieldstar/sqlite-runtime/node");

const LEASE = 30_000;

const cleanups: (() => void)[] = [];
afterEach(() => {
  for (const cleanup of cleanups.splice(0)) cleanup();
});

/** Two runtimes on one database file, as two processes would open it. */
function openSharedFile() {
  const dir = mkdtempSync(join(tmpdir(), "yieldstar-observer-"));
  const path = join(dir, "executions.db");
  const dbs: ReturnType<typeof createSqliteDb>[] = [];
  const open = (options?: { clock?: () => number }) => {
    const db = createSqliteDb({ path, wal: true });
    dbs.push(db);
    return new SqliteRuntime(db, options);
  };
  cleanups.push(() => {
    for (const db of dbs) db.close();
    rmSync(dir, { recursive: true, force: true });
  });
  return open;
}

const backends: [string, () => { runtime: Runtime; peer: Runtime }][] = [
  [
    "memory",
    () => {
      const runtime = new MemoryRuntime();
      return { runtime, peer: runtime };
    },
  ],
  [
    "sqlite file",
    () => {
      const open = openSharedFile();
      return { runtime: open(), peer: open() };
    },
  ],
];

async function admit(runtime: Runtime, executionId: string) {
  const admitted = await runtime.executions.admit({
    executionId,
    workflowId: "charge-order",
    params: { orderId: 42 },
    context: new Map([["token", "secret"]]),
  });
  expect(admitted).toEqual({ status: "accepted", executionId });
}

async function claim(runtime: Runtime, executionId: string) {
  const leases = await runtime.executions.claimDue({
    limit: 1,
    duration: LEASE,
    filter: { executionIds: [executionId] },
  });
  expect(leases).toHaveLength(1);
  return leases[0];
}

async function complete(runtime: Runtime, executionId: string, result: unknown) {
  await runtime.executions.acknowledge({
    lease: await claim(runtime, executionId),
    outcome: { status: "completed", result },
  });
}

async function fail(runtime: Runtime, executionId: string, error: Error) {
  await runtime.executions.acknowledge({
    lease: await claim(runtime, executionId),
    outcome: { status: "failed", error },
  });
}

async function elapsed<T>(fn: () => Promise<T>) {
  const start = Date.now();
  const value = await fn();
  return { value, ms: Date.now() - start };
}

for (const [name, create] of backends) {
  describe(`execution observer on ${name}`, () => {
    test("poll returns a view of each status, with an outcome only when terminal", async () => {
      const { runtime } = create();
      const observer = createExecutionObserver(runtime.executions);

      await admit(runtime, "admitted");
      await admit(runtime, "running");
      await claim(runtime, "running");
      await admit(runtime, "suspended");
      await runtime.executions.acknowledge({
        lease: await claim(runtime, "suspended"),
        outcome: { status: "suspended" },
      });
      await admit(runtime, "completed");
      await complete(runtime, "completed", { receipt: "r-1" });
      await admit(runtime, "failed");
      await fail(runtime, "failed", new Error("card declined"));

      for (const status of ["admitted", "running", "suspended"] as const) {
        const record = (await runtime.executions.get(status))!;
        expect(await observer.poll(status)).toEqual({
          executionId: status,
          workflowId: "charge-order",
          status,
          admittedAt: record.admittedAt,
          updatedAt: record.updatedAt,
        });
      }
      const completed = await observer.poll("completed");
      expect(completed).toMatchObject({ status: "completed", result: { receipt: "r-1" } });
      const failed = await observer.poll("failed");
      expect(failed).toMatchObject({ status: "failed" });
      expect(failed!.status === "failed" && failed!.error).toBeInstanceOf(Error);
      expect(failed!.status === "failed" && failed!.error.message).toBe("card declined");

      for (const view of [completed, failed]) {
        expect(Object.keys(view!).sort()).toEqual(
          [
            "admittedAt",
            "executionId",
            "status",
            "updatedAt",
            "workflowId",
            view!.status === "completed" ? "result" : "error",
          ].sort()
        );
      }
    });

    test("poll returns undefined for an unknown ID", async () => {
      const { runtime } = create();
      expect(await createExecutionObserver(runtime.executions).poll("unknown")).toBeUndefined();
    });

    test("wait returns an outcome recorded before the call at once", async () => {
      const { runtime } = create();
      const observer = createExecutionObserver(runtime.executions, { pollInterval: 60_000 });
      await admit(runtime, "done");
      await complete(runtime, "done", "receipt");
      const { value, ms } = await elapsed(() => observer.wait("done"));
      expect(value).toMatchObject({ status: "completed", result: "receipt" });
      expect(ms).toBeLessThan(1_000);
    });

    test("wait returns an outcome recorded after the call by another client", async () => {
      const { runtime, peer } = create();
      const observer = createExecutionObserver(runtime.executions, { pollInterval: 5 });
      await admit(runtime, "later");
      const waiting = observer.wait("later");
      await new Promise((resolve) => setTimeout(resolve, 20));
      await complete(peer, "later", "receipt");
      expect(await waiting).toMatchObject({
        executionId: "later",
        status: "completed",
        result: "receipt",
      });
    });

    test("wait returns undefined at once for an unknown ID", async () => {
      const { runtime } = create();
      const observer = createExecutionObserver(runtime.executions, { pollInterval: 60_000 });
      const { value, ms } = await elapsed(() => observer.wait("unknown"));
      expect(value).toBeUndefined();
      expect(ms).toBeLessThan(1_000);
    });

    test("wait returns timed-out with the current view when the timeout passes first", async () => {
      const { runtime } = create();
      const observer = createExecutionObserver(runtime.executions, { pollInterval: 5 });
      await admit(runtime, "slow");
      await claim(runtime, "slow");

      const { value, ms } = await elapsed(() => observer.wait("slow", { timeout: 50 }));
      expect(value).toEqual({
        status: "timed-out",
        execution: await observer.poll("slow"),
      });
      expect(value!.status === "timed-out" && value!.execution.status).toBe("running");
      expect(ms).toBeGreaterThanOrEqual(45);

      expect(await observer.wait("slow", { timeout: 0 })).toMatchObject({
        status: "timed-out",
        execution: { status: "running" },
      });
    });

    test("wait returns the same error on every call for a failed execution", async () => {
      const { runtime } = create();
      const observer = createExecutionObserver(runtime.executions);
      class CardDeclinedError extends Error {
        name = "CardDeclinedError";
      }
      await admit(runtime, "failed");
      await fail(runtime, "failed", new CardDeclinedError("card declined"));
      const errors = [];
      for (let i = 0; i < 3; i++) {
        const result = await observer.wait("failed");
        if (result?.status !== "failed") throw new Error("expected a failed outcome");
        errors.push({ name: result.error.name, message: result.error.message });
      }
      expect(errors).toEqual(
        Array.from({ length: 3 }, () => ({ name: "CardDeclinedError", message: "card declined" }))
      );
    });

    test("wait rejects a negative timeout", async () => {
      const { runtime } = create();
      const observer = createExecutionObserver(runtime.executions);
      await expect(observer.wait("any", { timeout: -1 })).rejects.toThrow(RangeError);
    });

    test("list returns views newest first and passes the cursor through", async () => {
      const { runtime } = create();
      const observer = createExecutionObserver(runtime.executions);
      for (const id of ["a", "b", "c"]) await admit(runtime, id);
      await complete(runtime, "b", "receipt");

      const first = await observer.list({ limit: 2 });
      expect(first.executions.map((view) => view.executionId)).toEqual(["c", "b"]);
      expect(first.executions[1]).toMatchObject({ status: "completed", result: "receipt" });
      for (const view of first.executions) expect(view).not.toHaveProperty("params");
      expect(typeof first.cursor).toBe("string");

      const rest = await observer.list({ limit: 2, cursor: first.cursor });
      expect(rest).toEqual({ executions: [await observer.poll("a")] });
      expect(await observer.list({ status: "completed" })).toEqual({
        executions: [await observer.poll("b")],
      });
      await expect(observer.list({ limit: 0 })).rejects.toThrow(ExecutionListRequestError);
    });
  });
}

test("wait returns an outcome recorded after a restart of the process that ran the execution", async () => {
  const open = openSharedFile();
  const worker = open();
  const observer = createExecutionObserver(open().executions, { pollInterval: 5 });
  await admit(worker, "order-42");
  await claim(worker, "order-42");
  const waiting = observer.wait("order-42");

  // The worker restarts. Its clock runs ahead, so the lease it held has
  // expired and the redelivery delay has passed.
  const restarted = open({ clock: () => Date.now() + LEASE + 1_000 });
  await complete(restarted, "order-42", "receipt");
  expect(await waiting).toMatchObject({ status: "completed", result: "receipt" });
});

test("a notifier makes wait read the record early", async () => {
  const runtime = new MemoryRuntime();
  const notifier = new EventEmitter();
  const observer = createExecutionObserver(runtime.executions, {
    pollInterval: 60_000,
    notifier,
  });
  await admit(runtime, "nudged");
  const waiting = elapsed(() => observer.wait("nudged"));
  await new Promise((resolve) => setTimeout(resolve, 10));
  await complete(runtime, "nudged", "receipt");
  notifier.emit("nudged");
  const { value, ms } = await waiting;
  expect(value).toMatchObject({ status: "completed", result: "receipt" });
  expect(ms).toBeLessThan(1_000);
  expect(notifier.listenerCount("nudged")).toBe(0);
});

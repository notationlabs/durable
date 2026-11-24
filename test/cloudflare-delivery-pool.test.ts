import { expect, test } from "vitest";
import { SqliteRuntime } from "@durable/sqlite-runtime";
import { createSqliteDb } from "@durable/cloudflare";
import { encodeThrownValue, type ExecutionLease, type WorkflowEvent } from "@durable/core";
import { createIsolatedDeliver, type DeliveryReply } from "../packages/cloudflare/src/delivery";
import { createFakeStorage } from "./durable-object-storage";

const suspended: DeliveryReply = { status: "outcome", outcome: { status: "suspended" } };

// Records the calls each slot's instance receives, and answers a delivery
// with what `run` returns.
function createPool(run: (slot: number, executionId: string) => Promise<DeliveryReply>) {
  const fake = createFakeStorage();
  const calls: string[] = [];
  const deliver = createIsolatedDeliver({
    runtime: new SqliteRuntime(createSqliteDb(fake.storage)),
    getRunner: (slot) => ({
      async durableRunDelivery(request) {
        calls.push(`run ${slot} ${request.event.executionId}`);
        return run(slot, request.event.executionId);
      },
      async durableStopDelivery() {
        calls.push(`stop ${slot}`);
      },
    }),
  });
  const start = (executionId: string, signal = new AbortController().signal) =>
    deliver({ workflowId: "w", executionId, params: undefined, context: new Map() } as WorkflowEvent, {
      lease: { executionId, token: 1, expiresAt: Date.now() + 30_000 } as ExecutionLease,
      signal,
    });
  return { calls, start, close: () => fake.close() };
}

test("a slot is stopped before its first delivery, and not before the next", async () => {
  const pool = createPool(async () => suspended);

  await pool.start("a");
  await pool.start("b");

  expect(pool.calls).toEqual(["stop 0", "run 0 a", "run 0 b"]);
  pool.close();
});

test("deliveries that run at once use different slots, and a free slot is used again", async () => {
  const gates = new Map<string, () => void>();
  const pool = createPool(
    (_, executionId) =>
      new Promise((resolve) => gates.set(executionId, () => resolve(suspended)))
  );

  const a = pool.start("a");
  const b = pool.start("b");
  await expect.poll(() => gates.size).toBe(2);
  gates.get("a")!();
  await a;
  const c = pool.start("c");
  await expect.poll(() => gates.size).toBe(3);
  gates.get("b")!();
  gates.get("c")!();
  await Promise.all([b, c]);

  expect(pool.calls.filter((call) => call.startsWith("run"))).toEqual([
    "run 0 a",
    "run 1 b",
    "run 0 c",
  ]);
  pool.close();
});

test("a slot whose instance stopped before replying is stopped before it is used again", async () => {
  let resets = 1;
  const pool = createPool(async () => {
    if (resets-- > 0) throw new Error("Durable Object exceeded its CPU time limit and was reset.");
    return suspended;
  });

  await expect(pool.start("a")).rejects.toMatchObject({
    name: "WorkerExitedError",
    message: expect.stringContaining("exceeded its CPU time limit"),
  });
  await pool.start("b");

  expect(pool.calls).toEqual(["stop 0", "run 0 a", "stop 0", "run 0 b"]);
  pool.close();
});

test("an aborted delivery rejects at once, and its slot is held until the instance has stopped", async () => {
  const pool = createPool(() => new Promise(() => {}));
  const controller = new AbortController();

  const a = pool.start("a", controller.signal);
  await expect.poll(() => pool.calls).toEqual(["stop 0", "run 0 a"]);
  controller.abort(new Error("lease lost"));

  await expect(a).rejects.toMatchObject({ name: "InfrastructureError", message: "lease lost" });
  expect(pool.calls).toEqual(["stop 0", "run 0 a", "stop 0"]);
  pool.close();
});

test("an infrastructure error in the instance rejects without a WorkerExitedError", async () => {
  const pool = createPool(async () => ({
    status: "error",
    error: encodeThrownValue(new TypeError("heap write failed")),
  }));

  // The dispatcher releases the lease after any other rejection.
  await expect(pool.start("a")).rejects.toMatchObject({
    name: "TypeError",
    message: "heap write failed",
  });
  expect(pool.calls).toEqual(["stop 0", "run 0 a"]);
  pool.close();
});

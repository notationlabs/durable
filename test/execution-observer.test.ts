import { EventEmitter } from "node:events";
import { afterEach, describe, expect, test } from "vitest";
import { ExecutionListRequestError, createExecutionObserver } from "@yieldstar/core";
import { MemoryRuntime } from "@yieldstar/test-runtime";
import { createLocalSdk } from "yieldstar";
import {
  LEASE,
  admit,
  backendKinds,
  claim,
  cleanup,
  complete,
  elapsed,
  fail,
  openBackend,
  sleep,
  suspend,
} from "./observation-harness";

afterEach(cleanup);

for (const backendKind of backendKinds) {
  describe(`execution observer on ${backendKind}`, () => {
    const create = () => openBackend(backendKind);

    test("poll returns a view of each status, with an outcome only when terminal", async () => {
      const { runtime } = create();
      const observer = createExecutionObserver(runtime.executions);

      await admit(runtime, "admitted");
      await admit(runtime, "running");
      await claim(runtime, "running");
      await admit(runtime, "suspended");
      await suspend(runtime, "suspended");
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
      await sleep(20);
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
  const { open } = openBackend("sqlite");
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
  await sleep(10);
  await complete(runtime, "nudged", "receipt");
  notifier.emit("nudged");
  const { value, ms } = await waiting;
  expect(value).toMatchObject({ status: "completed", result: "receipt" });
  expect(ms).toBeLessThan(1_000);
  expect(notifier.listenerCount("nudged")).toBe(0);
});

test("aborting a wait's signal stops its reads and rejects with the signal's reason", async () => {
  const runtime = new MemoryRuntime();
  let reads = 0;
  const observer = createExecutionObserver(
    {
      get(executionId) {
        reads++;
        return runtime.executions.get(executionId);
      },
      list: (request) => runtime.executions.list(request),
    },
    { pollInterval: 5 }
  );
  await admit(runtime, "slow");
  const controller = new AbortController();
  const waiting = observer.wait("slow", { signal: controller.signal });
  await sleep(30);
  const reason = new Error("caller left");
  controller.abort(reason);
  await expect(waiting).rejects.toBe(reason);
  const readsAtAbort = reads;
  expect(readsAtAbort).toBeGreaterThan(1);
  await sleep(30);
  expect(reads).toBe(readsAtAbort);

  reads = 0;
  await expect(observer.wait("slow", { signal: controller.signal })).rejects.toBe(reason);
  expect(reads).toBe(0);
});

test("the observer and the local SDK reject a pollInterval that is not greater than zero", () => {
  const runtime = new MemoryRuntime();
  for (const pollInterval of [-1, 0, NaN]) {
    const error = new RangeError("pollInterval must be a positive number of milliseconds");
    expect(() => createExecutionObserver(runtime.executions, { pollInterval })).toThrow(error);
    expect(() => createLocalSdk(runtime, { pollInterval })).toThrow(error);
  }
});

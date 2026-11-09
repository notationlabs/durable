import { EventEmitter } from "node:events";
import { afterEach, expect, test } from "vitest";
import { createExecutionObserver } from "@durable/core";
import { MemoryRuntime } from "@durable/test-runtime";
import { createLocalSdk } from "@durable/sdk";
import { admit, cleanup, complete, elapsed, sleep } from "./observation-harness";

afterEach(cleanup);

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

test("a notification that arrives during a read makes wait read the record again at once", async () => {
  const runtime = new MemoryRuntime();
  const notifier = new EventEmitter();
  let reads = 0;
  const observer = createExecutionObserver(
    {
      async get(executionId) {
        const record = await runtime.executions.get(executionId);
        if (++reads === 1) {
          await complete(runtime, executionId, "receipt");
          notifier.emit(executionId);
        }
        return record;
      },
      list: (request) => runtime.executions.list(request),
    },
    { pollInterval: 60_000, notifier }
  );
  await admit(runtime, "nudged");
  const { value, ms } = await elapsed(() => observer.wait("nudged"));
  expect(value).toMatchObject({ status: "completed", result: "receipt" });
  expect(reads).toBe(2);
  expect(ms).toBeLessThan(1_000);
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

test("the observer and the local SDK reject a pollInterval that is not a positive, finite number", () => {
  const runtime = new MemoryRuntime();
  for (const pollInterval of [-1, 0, NaN, Infinity]) {
    const error = new RangeError("pollInterval must be a positive, finite number of milliseconds");
    expect(() => createExecutionObserver(runtime.executions, { pollInterval })).toThrow(error);
    expect(() => createLocalSdk(runtime, { pollInterval })).toThrow(error);
  }
});

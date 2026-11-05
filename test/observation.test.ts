import { isPrunedExecutionView } from "@yieldstar/core";
import { EventEmitter } from "node:events";
import { afterEach, describe, expect, test, vi } from "vitest";
import { MemoryRuntime } from "@yieldstar/test-runtime";
import { createRoutes } from "@yieldstar/http-server";
import {
  ExecutionListRequestError,
  HttpRouteError,
  createHttpSdkFactory,
  workflow,
} from "yieldstar";
import {
  CardDeclinedError,
  LEASE,
  admit,
  backendKinds,
  claim,
  cleanup,
  complete,
  createTestSdk,
  elapsed,
  fail,
  logger,
  openBackend,
  sdkKinds,
  serveRoutes,
  sleep,
  spawnDispatcher,
  startDispatcher,
  suspend,
} from "./observation-harness";
import { liveRecord } from "@yieldstar/runtime-conformance";

afterEach(cleanup);

const router = {
  "charge-order": workflow(async function* (step, event) {
    const { orderId } = event.params as { orderId: number };
    return yield* step.run("charge", () => ({ receipt: `r-${orderId}` }));
  }),
  declined: workflow(async function* () {
    throw new CardDeclinedError("card declined");
  }),
};

for (const sdkKind of sdkKinds) {
  for (const backendKind of backendKinds) {
    describe(`${sdkKind} SDK on ${backendKind}`, () => {
      const setup = (options?: Parameters<typeof createTestSdk>[2]) => {
        const backend = openBackend(backendKind);
        const sdk = createTestSdk(sdkKind, backend.runtime, options);
        return { ...backend, sdk };
      };

      test("poll, wait and list carry a started child's parent, and a triggered execution has none", async () => {
        const { runtime, sdk } = setup();
        await admit(runtime, "caller");
        const started = await runtime.executions.start({
          requestId: { executionId: "caller", stepKey: "ship" },
          workflowId: "charge-order",
          params: { orderId: 43 },
        });
        if (started.status !== "accepted") throw new Error("expected an accepted start");
        const child = started.executionId;
        const parent = { executionId: "caller", stepKey: "ship" };

        expect(await sdk.poll(child)).toMatchObject({ status: "admitted", parent });
        expect(await sdk.wait(child, { timeout: 0 })).toMatchObject({
          status: "timed-out",
          execution: { parent },
        });
        await complete(runtime, child, { receipt: "r-43" });
        expect(await sdk.wait(child)).toMatchObject({ status: "completed", parent });
        const { executions } = await sdk.list();
        expect(executions.find((view) => view.executionId === child)).toMatchObject({ parent });
        expect(executions.find((view) => view.executionId === "caller")).not.toHaveProperty(
          "parent"
        );
        expect(await sdk.poll("caller")).not.toHaveProperty("parent");
      });

      test("poll returns each status, with an outcome only when terminal, and no internal fields", async () => {
        const { runtime, sdk } = setup();
        await admit(runtime, "admitted");
        await admit(runtime, "running");
        await claim(runtime, "running");
        await admit(runtime, "suspended");
        await suspend(runtime, "suspended");
        await admit(runtime, "completed");
        await complete(runtime, "completed", { receipt: "r-42" });
        await admit(runtime, "failed");
        await fail(runtime, "failed", new CardDeclinedError("card declined"));

        for (const status of ["admitted", "running", "suspended"] as const) {
          const record = await liveRecord(runtime.executions, status);
          expect(await sdk.poll(status)).toEqual({
            executionId: status,
            workflowId: "charge-order",
            status,
            admittedAt: record.admittedAt,
            updatedAt: record.updatedAt,
          });
        }
        const completed = await sdk.poll("completed");
        expect(completed).toMatchObject({ status: "completed", result: { receipt: "r-42" } });
        const failed = await sdk.poll("failed");
        if (failed?.status !== "failed") throw new Error("expected a failed view");
        expect(failed.error).toBeInstanceOf(Error);
        expect(failed.error).toMatchObject({ name: "CardDeclinedError", message: "card declined" });

        const base = ["admittedAt", "executionId", "status", "updatedAt", "workflowId"];
        expect(Object.keys(completed!).sort()).toEqual([...base, "result"].sort());
        expect(Object.keys(failed).sort()).toEqual([...base, "error"].sort());
        const page = await sdk.list();
        for (const view of page.executions) {
          for (const key of ["params", "context", "lease", "token", "wakeAt", "unacknowledgedDeliveryCount"]) {
            expect(view).not.toHaveProperty(key);
          }
        }
      });

      test("poll returns undefined for an unknown ID", async () => {
        const { sdk } = setup();
        expect(await sdk.poll("unknown")).toBeUndefined();
        if (sdk.kind === "http") {
          expect(sdk.responses).toEqual([{ route: "poll", status: 404 }]);
        }
      });

      test("wait returns an outcome recorded before the call at once", async () => {
        const { runtime, sdk } = setup();
        await admit(runtime, "done");
        await complete(runtime, "done", "receipt");
        const { value, ms } = await elapsed(() => sdk.wait("done"));
        expect(value).toMatchObject({ executionId: "done", status: "completed", result: "receipt" });
        expect(ms).toBeLessThan(500);
      });

      test("wait returns once an outcome is recorded after the call", async () => {
        const { runtime, peer, sdk } = setup();
        await admit(runtime, "later");
        const waiting = sdk.wait("later");
        await sleep(30);
        await complete(peer, "later", "receipt");
        expect(await waiting).toMatchObject({ status: "completed", result: "receipt" });
      });

      test("wait returns timed-out with the current status when the timeout passes first", async () => {
        const { runtime, sdk } = setup();
        await admit(runtime, "slow");
        await claim(runtime, "slow");
        const { value, ms } = await elapsed(() => sdk.wait("slow", { timeout: 60 }));
        expect(value).toEqual({ status: "timed-out", execution: await sdk.poll("slow") });
        expect(value?.status === "timed-out" && value.execution.status).toBe("running");
        expect(ms).toBeGreaterThanOrEqual(55);
        await expect(sdk.wait("slow", { timeout: -1 })).rejects.toThrow(RangeError);
      });

      test("wait returns undefined at once for an unknown ID", async () => {
        const { sdk } = setup();
        const { value, ms } = await elapsed(() => sdk.wait("unknown"));
        expect(value).toBeUndefined();
        expect(ms).toBeLessThan(500);
      });

      test("wait on a failed execution returns the same error every time", async () => {
        const { runtime, sdk } = setup();
        await admit(runtime, "failed");
        await fail(runtime, "failed", new CardDeclinedError("card declined"));
        const errors = [];
        for (let i = 0; i < 3; i++) {
          const result = await sdk.wait("failed");
          if (result?.status !== "failed") throw new Error("expected a failed outcome");
          errors.push({ name: result.error.name, message: result.error.message });
        }
        expect(errors).toEqual(
          Array.from({ length: 3 }, () => ({ name: "CardDeclinedError", message: "card declined" }))
        );
      });

      test("list pages without repeats while new executions are admitted", async () => {
        const { runtime, peer, sdk } = setup();
        const earlier = Array.from({ length: 7 }, (_, i) => `earlier-${i}`);
        for (const id of earlier) await admit(runtime, id);

        const seen: string[] = [];
        let page = await sdk.list({ limit: 3 });
        let added = 0;
        while (true) {
          seen.push(...page.executions.map((view) => view.executionId));
          await admit(peer, `later-${added++}`);
          if (!page.cursor) break;
          page = await sdk.list({ limit: 3, cursor: page.cursor });
        }
        expect(new Set(seen).size).toBe(seen.length);
        expect(seen.filter((id) => id.startsWith("earlier-"))).toEqual([...earlier].reverse());
      });

      test("list filters by workflowId or status", async () => {
        const { runtime, sdk } = setup();
        await admit(runtime, "a", "charge-order");
        await admit(runtime, "b", "refund-order");
        await admit(runtime, "c", "charge-order");
        await complete(runtime, "c", "receipt");

        const ids = async (request: Parameters<typeof sdk.list>[0]) =>
          (await sdk.list(request)).executions.map((view) => view.executionId);
        expect(await ids({ workflowId: "charge-order" })).toEqual(["c", "a"]);
        expect(await ids({ status: "admitted" })).toEqual(["b", "a"]);
        expect(await ids({ workflowId: "charge-order", status: "completed" })).toEqual(["c"]);
        expect((await sdk.list({ status: "completed" })).executions[0]).toMatchObject({
          status: "completed",
          result: "receipt",
        });
        await expect(sdk.list({ limit: 0 })).rejects.toThrow(ExecutionListRequestError);
        await expect(sdk.list({ cursor: "not-a-cursor" })).rejects.toThrow(ExecutionListRequestError);
      });

      test("triggerAndWait returns the result without a workflowEndEmitter listener", async () => {
        const { runtime, sdk } = setup();
        const { invoker } = startDispatcher(runtime, router);
        const emitter = invoker.workflowEndEmitter;
        const emit = vi.spyOn(emitter, "emit");
        // An EventEmitter emits "newListener" for every listener added by any
        // method, so this records a listener even if it was removed before
        // the wait returned.
        const added: (string | symbol)[] = [];
        emitter.on("newListener", (event: string | symbol) => added.push(event));

        const result = await sdk.triggerAndWait({
          workflowId: "charge-order",
          executionId: "order-42",
          params: { orderId: 42 },
        });
        expect(result).toEqual({ receipt: "r-42" });
        expect(emit).toHaveBeenCalledWith("order-42", { receipt: "r-42" });
        expect(added).toEqual([]);

        const error = await sdk
          .triggerAndWait({ workflowId: "declined", executionId: "order-43", params: {} })
          .catch((err) => err);
        expect(error).toMatchObject({ name: "CardDeclinedError", message: "card declined" });
      });

      test("an invoker's workflowEndEmitter is only a fast path", async () => {
        const notifier = new EventEmitter();
        const { runtime, peer, sdk } = setup({ invoker: { workflowEndEmitter: notifier } });
        await admit(runtime, "nudged");
        const waiting = sdk.wait("nudged");
        await sleep(20);
        await complete(peer, "nudged", "receipt");
        // No event is emitted, so the wait can only end by reading the record.
        expect(await waiting).toMatchObject({ status: "completed", result: "receipt" });
      });
    });
  }

  describe(`${sdkKind} SDK across processes`, () => {
    test(
      "wait returns an outcome recorded by another process",
      { timeout: 15_000 },
      async () => {
        const backend = openBackend("sqlite");
        const sdk = createTestSdk(sdkKind, backend.runtime);
        const { executionId } = await sdk.trigger({
          workflowId: "charge-order",
          executionId: "order-42",
          params: { orderId: 42 },
        });
        const waiting = sdk.wait(executionId);
        spawnDispatcher(backend.path!);
        expect(await waiting).toMatchObject({
          executionId,
          status: "completed",
          result: { receipt: "r-42" },
        });
      }
    );

    test(
      "wait returns an outcome recorded after the process that ran the execution restarts",
      { timeout: 15_000 },
      async () => {
        const backend = openBackend("sqlite");
        const sdk = createTestSdk(sdkKind, backend.runtime);
        const { executionId } = await sdk.trigger({
          workflowId: "charge-order",
          executionId: "order-42",
          params: { orderId: 42, delay: 1_000 },
        });
        const waiting = sdk.wait(executionId);

        const first = spawnDispatcher(backend.path!);
        await vi.waitFor(
          async () => expect(await sdk.poll(executionId)).toMatchObject({ status: "suspended" }),
          { timeout: 10_000, interval: 20 }
        );
        first.kill("SIGKILL");
        await new Promise((resolve) => first.once("exit", resolve));
        expect(await sdk.wait(executionId, { timeout: 0 })).toMatchObject({
          status: "timed-out",
          execution: { status: "suspended" },
        });

        spawnDispatcher(backend.path!);
        expect(await waiting).toMatchObject({
          status: "completed",
          result: { receipt: "r-42" },
        });
      }
    );

    test("wait returns an outcome recorded by a restarted runtime with a later clock", async () => {
      const backend = openBackend("sqlite");
      const sdk = createTestSdk(sdkKind, backend.runtime);
      await admit(backend.runtime, "order-42");
      await claim(backend.peer, "order-42");
      const waiting = sdk.wait("order-42");

      // The worker restarts with a clock past its old lease and the redelivery delay.
      const restarted = backend.open({ clock: () => Date.now() + LEASE + 1_000 });
      await complete(restarted, "order-42", "receipt");
      expect(await waiting).toMatchObject({ status: "completed", result: "receipt" });
    });

    test("list returns executions from processes with skewed clocks in admission order", async () => {
      const backend = openBackend("sqlite");
      const ahead = backend.open({ clock: () => Date.now() + 3_600_000 });
      const behind = backend.open({ clock: () => Date.now() - 3_600_000 });
      const sdks = [createTestSdk(sdkKind, ahead), createTestSdk(sdkKind, behind)];
      const order: string[] = [];
      for (let i = 0; i < 6; i++) {
        const executionId = `execution-${i}`;
        await sdks[i % 2].trigger({ workflowId: "charge-order", executionId, params: {} });
        order.push(executionId);
      }
      for (const sdk of sdks) {
        const executions = (await sdk.list()).executions;
        expect(executions.map((view) => view.executionId)).toEqual([...order].reverse());
        const [newest, next] = executions;
        if (isPrunedExecutionView(newest) || isPrunedExecutionView(next)) {
          throw new Error("expected live views");
        }
        expect(newest.admittedAt).toBeLessThan(next.admittedAt);
      }
    });
  });
}

describe("HTTP maxWait", () => {
  test("the server caps a /wait request at 8 seconds by default and returns timed-out", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    const runtime = new MemoryRuntime();
    const routes = createRoutes({ runtime, logger });
    await admit(runtime, "slow");
    await claim(runtime, "slow");

    let settled = false;
    const response = Promise.resolve(
      routes["/wait"].POST(
        new Request("http://yieldstar.test/wait", {
          method: "POST",
          body: JSON.stringify({ executionId: "slow", timeout: 60_000 }),
        })
      )
    ).finally(() => {
      settled = true;
    });
    await vi.advanceTimersByTimeAsync(8_000 - 50);
    expect(settled).toBe(false);
    await vi.advanceTimersByTimeAsync(100);
    expect(settled).toBe(true);
    const res = await response;
    expect(res.status).toBe(200);
    expect(await res.json()).toMatchObject({
      status: "timed-out",
      execution: { executionId: "slow", status: "running" },
    });
  });

  test("a /wait request longer than the cap returns timed-out at the cap", async () => {
    const runtime = new MemoryRuntime();
    const { url } = serveRoutes(runtime, { maxWait: 100 });
    await admit(runtime, "slow");
    const { value: res, ms } = await elapsed(() =>
      fetch(`${url}/wait`, {
        method: "POST",
        body: JSON.stringify({ executionId: "slow", timeout: 60_000 }),
      })
    );
    expect(await res.json()).toMatchObject({
      status: "timed-out",
      execution: { status: "admitted" },
    });
    expect(ms).toBeGreaterThanOrEqual(95);
    expect(ms).toBeLessThan(1_000);
  });

  test("the HTTP SDK's wait without a timeout repeats capped requests until the outcome", async () => {
    const runtime = new MemoryRuntime();
    const sdk = createTestSdk("http", runtime, { maxWait: 50 });
    await admit(runtime, "slow");
    const waiting = sdk.wait("slow");
    await sleep(300);
    await complete(runtime, "slow", "receipt");
    expect(await waiting).toMatchObject({ status: "completed", result: "receipt" });
    const waits = sdk.responses.filter((response) => response.route === "wait");
    expect(waits.length).toBeGreaterThanOrEqual(3);
    expect(waits.every((response) => response.status === 200)).toBe(true);
  });

  test("the HTTP SDK's wait with a timeout longer than the cap waits for the whole timeout", async () => {
    const runtime = new MemoryRuntime();
    const sdk = createTestSdk("http", runtime, { maxWait: 50 });
    await admit(runtime, "slow");
    const { value, ms } = await elapsed(() => sdk.wait("slow", { timeout: 250 }));
    expect(value).toMatchObject({ status: "timed-out", execution: { status: "admitted" } });
    expect(ms).toBeGreaterThanOrEqual(245);
    expect(sdk.responses.filter((response) => response.route === "wait").length).toBeGreaterThanOrEqual(3);
  });

  test("waitForResult outlasts the cap and throws the recorded error", async () => {
    const runtime = new MemoryRuntime();
    const sdk = createTestSdk("http", runtime, { maxWait: 50 });
    const waiting = sdk.triggerAndWait({ workflowId: "charge-order", executionId: "fresh", params: {} });
    const failing = waiting.catch((err) => err);
    await sleep(150);
    await fail(runtime, "fresh", new CardDeclinedError("card declined"));
    const error = await failing;
    expect(error).toBeInstanceOf(CardDeclinedError);
    expect(error).toMatchObject({ code: "CARD_DECLINED", message: "card declined" });
  });
});

describe("HTTP SDK error responses", () => {
  test("a 404 without EXECUTION_NOT_FOUND, such as from a wrong base path, throws HttpRouteError", async () => {
    const runtime = new MemoryRuntime();
    const { url } = serveRoutes(runtime);
    await admit(runtime, "order-42");
    const sdk = createHttpSdkFactory<any>()({ url: `${url}/wrong-base` });
    for (const call of [
      () => sdk.poll("order-42"),
      () => sdk.wait("order-42", { timeout: 0 }),
      () => sdk.waitForResult({ executionId: "order-42" }),
    ]) {
      const error = await call().catch((err: unknown) => err);
      expect(error).toBeInstanceOf(HttpRouteError);
      expect((error as HttpRouteError).status).toBe(404);
      expect((error as HttpRouteError).code).toBeUndefined();
    }
  });
});

test("aborting the HTTP SDK's wait aborts its request and sends no more", async () => {
  const runtime = new MemoryRuntime();
  const sdk = createTestSdk("http", runtime, { maxWait: 20 });
  await admit(runtime, "slow");
  const controller = new AbortController();
  const waiting = sdk.wait("slow", { signal: controller.signal });
  await sleep(70);
  const reason = new Error("caller left");
  controller.abort(reason);
  await expect(waiting).rejects.toBe(reason);
  await sleep(10);
  const requestsAtAbort = sdk.responses.length;
  await sleep(60);
  expect(sdk.responses.length).toBe(requestsAtAbort);
});

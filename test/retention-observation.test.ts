import { afterEach, describe, expect, test } from "vitest";
import {
  durableValueCodec,
  encodeThrownValue,
  type RetentionOptions,
  type Runtime,
} from "@yieldstar/core";
import type { ObservationMiddlewareFunction, ObservationRequest } from "@yieldstar/http-server";
import {
  ExecutionAdmissionError,
  ExecutionPrunedError,
  createHttpSdkFactory,
} from "yieldstar";
import {
  admit,
  backendKinds,
  cleanup,
  complete,
  createTestSdk,
  fail,
  CardDeclinedError,
  openBackend,
  sdkKinds,
  serveRoutes,
} from "./observation-harness";

afterEach(cleanup);

const ORDER = "order-42";
const RETAIN: RetentionOptions = {
  workflows: { "charge-order": { completed: 0, failed: 0, cancelled: 0 } },
};

const post = (url: string, route: string, body: unknown) =>
  fetch(`${url}/${route}`, { method: "POST", body: JSON.stringify(body) });

async function expectError(response: Response, status: number, code: string) {
  expect(response.status).toBe(status);
  expect(await response.json()).toEqual({ code, message: expect.any(String) });
}

async function decode(response: Response) {
  const decoding = durableValueCodec.decode(await response.text());
  if (decoding.status !== "decoded") throw new Error(decoding.message);
  return decoding.value;
}

/** Completes order-42 and prunes it. Resolves with its tombstone. */
async function pruneOrder(runtime: Runtime, executionId = ORDER) {
  await admit(runtime, executionId);
  await complete(runtime, executionId, "receipt");
  expect(await runtime.prune()).toMatchObject({ pruned: 1 });
  const tombstone = await runtime.executions.get(executionId);
  expect(tombstone).toEqual({
    status: "pruned",
    executionId,
    workflowId: "charge-order",
    terminalStatus: "completed",
    prunedAt: expect.any(Number),
  });
  return tombstone!;
}

for (const backendKind of backendKinds) {
  describe(`retention routes on ${backendKind}`, () => {
    test("/poll and /wait respond 200 with the pruned view, and 404 for an ID never admitted", async () => {
      const { runtime } = openBackend(backendKind, RETAIN);
      const { url } = serveRoutes(runtime);
      const tombstone = await pruneOrder(runtime);

      for (const route of ["poll", "wait"]) {
        const response = await post(url, route, { executionId: ORDER });
        expect(response.status).toBe(200);
        expect(await decode(response)).toEqual(tombstone);
        await expectError(
          await post(url, route, { executionId: "order-43" }),
          404,
          "EXECUTION_NOT_FOUND"
        );
      }
    });

    test("/events responds 410 EXECUTION_PRUNED for a pruned execution", async () => {
      const { runtime } = openBackend(backendKind, RETAIN);
      const { url } = serveRoutes(runtime);
      await pruneOrder(runtime);
      await expectError(await post(url, "events", { executionId: ORDER }), 410, "EXECUTION_PRUNED");
      await expectError(
        await post(url, "events", { executionId: "order-43" }),
        404,
        "EXECUTION_NOT_FOUND"
      );
    });

    test("/list leaves pruned executions out unless status is pruned", async () => {
      const { runtime } = openBackend(backendKind, RETAIN);
      const { url } = serveRoutes(runtime);
      const tombstone = await pruneOrder(runtime);
      await admit(runtime, "order-43");

      const all = await decode(await post(url, "list", {}));
      expect(all).toEqual({ executions: [expect.objectContaining({ executionId: "order-43" })] });
      const completed = await decode(await post(url, "list", { status: "completed" }));
      expect(completed).toEqual({ executions: [] });
      const pruned = await decode(await post(url, "list", { status: "pruned" }));
      expect(pruned).toEqual({ executions: [tombstone] });
    });

    test("/trigger with a pruned ID responds 409 EXECUTION_ID_CONFLICT, or 409 EXECUTION_PRUNED when idempotent", async () => {
      const { runtime } = openBackend(backendKind, RETAIN);
      const { url } = serveRoutes(runtime);
      const tombstone = await pruneOrder(runtime);
      const event = { workflowId: "charge-order", executionId: ORDER, params: { orderId: 42 } };

      await expectError(await post(url, "trigger", event), 409, "EXECUTION_ID_CONFLICT");
      await expectError(
        await post(url, "trigger", { ...event, idempotent: true }),
        409,
        "EXECUTION_PRUNED"
      );
      expect(await runtime.executions.get(ORDER)).toEqual(tombstone);
    });

    test("/cancel responds 200 with the pruned view and changes nothing; /clear-divergence responds not-diverged", async () => {
      const { runtime } = openBackend(backendKind, RETAIN);
      const { url } = serveRoutes(runtime);
      const tombstone = await pruneOrder(runtime);

      for (const force of [false, true]) {
        const response = await post(url, "cancel", { executionId: ORDER, reason: "late", force });
        expect(response.status).toBe(200);
        expect(await decode(response)).toEqual(tombstone);
      }
      const cleared = await post(url, "clear-divergence", { executionId: ORDER });
      expect(cleared.status).toBe(200);
      expect(await decode(cleared)).toEqual({ status: "not-diverged" });
      expect(await runtime.executions.get(ORDER)).toEqual(tombstone);
    });

    test("observation middleware loads the tombstone, and a list request carries the pruned filter", async () => {
      const { runtime } = openBackend(backendKind, RETAIN);
      const tombstone = await pruneOrder(runtime);
      const loaded: [string, unknown][] = [];
      const filters: unknown[] = [];
      const middleware: ObservationMiddlewareFunction = async (_req, request: ObservationRequest) => {
        if (request.operation === "list") filters.push(request.filter);
        else loaded.push([request.operation, await request.load()]);
        return { allow: true };
      };
      const { url } = serveRoutes(runtime, { observationMiddleware: [middleware] });

      for (const route of ["poll", "wait", "events", "cancel", "clear-divergence"]) {
        await post(url, route, { executionId: ORDER });
      }
      await post(url, "list", { status: "pruned" });

      expect(loaded).toEqual(
        ["poll", "wait", "events", "cancel", "clear-divergence"].map((operation) => [
          operation,
          tombstone,
        ])
      );
      expect(filters).toEqual([{ status: "pruned" }]);
    });
  });

  for (const sdkKind of sdkKinds) {
    describe(`${sdkKind} SDK retention on ${backendKind}`, () => {
      const setup = () => {
        const backend = openBackend(backendKind, RETAIN);
        const sdk = createTestSdk(sdkKind, backend.runtime);
        return { ...backend, sdk };
      };

      test("poll and wait return the pruned view, and undefined for an ID never admitted", async () => {
        const { runtime, sdk } = setup();
        const tombstone = await pruneOrder(runtime);
        expect(await sdk.poll(ORDER)).toEqual(tombstone);
        expect(await sdk.wait(ORDER)).toEqual(tombstone);
        expect(await sdk.wait(ORDER, { timeout: 0 })).toEqual(tombstone);
        expect(await sdk.poll("order-43")).toBeUndefined();
        expect(await sdk.wait("order-43")).toBeUndefined();
      });

      test("list leaves pruned executions out unless status is pruned", async () => {
        const { runtime, sdk } = setup();
        const tombstone = await pruneOrder(runtime);
        await admit(runtime, "order-43");
        expect((await sdk.list()).executions.map((view) => view.executionId)).toEqual([
          "order-43",
        ]);
        expect(await sdk.list({ status: "pruned" })).toEqual({ executions: [tombstone] });
      });

      test("trigger with a pruned ID throws EXECUTION_ID_CONFLICT, or EXECUTION_PRUNED when idempotent", async () => {
        const { runtime, sdk } = setup();
        const tombstone = await pruneOrder(runtime);
        const event = { workflowId: "charge-order", executionId: ORDER, params: { orderId: 42 } };

        const conflict = await sdk.trigger(event).catch((err: unknown) => err);
        expect(conflict).toBeInstanceOf(ExecutionAdmissionError);
        expect(conflict).toMatchObject({ code: "EXECUTION_ID_CONFLICT" });
        const pruned = await sdk.trigger({ ...event, idempotent: true }).catch((err: unknown) => err);
        expect(pruned).toBeInstanceOf(ExecutionAdmissionError);
        expect(pruned).toMatchObject({ code: "EXECUTION_PRUNED" });
        expect(sdk.responses.map(({ status }) => status)).toEqual(
          sdkKind === "http" ? [409, 409] : []
        );
        expect(await runtime.executions.get(ORDER)).toEqual(tombstone);
      });

      test("cancel returns the pruned view and changes nothing; clearDivergence returns not-diverged", async () => {
        const { runtime, sdk } = setup();
        const tombstone = await pruneOrder(runtime);
        expect(await sdk.cancel(ORDER, { reason: "late" })).toEqual(tombstone);
        expect(await sdk.cancel(ORDER, { force: true })).toEqual(tombstone);
        expect(await sdk.clearDivergence(ORDER)).toEqual({ status: "not-diverged" });
        expect(await sdk.poll(ORDER)).toEqual(tombstone);
      });

      test("triggerAndWait throws ExecutionPrunedError when the execution is pruned before the wait reads it", async () => {
        const { runtime, sdk } = setup();
        const admitExecution = runtime.executions.admit.bind(runtime.executions);
        runtime.executions.admit = async (request) => {
          const admission = await admitExecution(request);
          await fail(runtime, request.executionId!, new CardDeclinedError("card declined"));
          await runtime.prune();
          return admission;
        };

        const error = await sdk
          .triggerAndWait({ workflowId: "charge-order", executionId: ORDER })
          .catch((err: unknown) => err);
        expect(error).toBeInstanceOf(ExecutionPrunedError);
        expect(error).toMatchObject({
          name: "ExecutionPrunedError",
          code: "EXECUTION_PRUNED",
          executionId: ORDER,
          workflowId: "charge-order",
          terminalStatus: "failed",
          prunedAt: expect.any(Number),
        });
      });
    });
  }

  test(`the HTTP SDK's waitForResult throws ExecutionPrunedError for a pruned execution on ${backendKind}`, async () => {
    const { runtime } = openBackend(backendKind, RETAIN);
    const { url } = serveRoutes(runtime);
    const tombstone = await pruneOrder(runtime);
    const sdk = createHttpSdkFactory<any>()({ url });

    const error = await sdk.waitForResult({ executionId: ORDER }).catch((err: unknown) => err);
    expect(error).toBeInstanceOf(ExecutionPrunedError);
    const { status: _status, ...fields } = tombstone as typeof tombstone & { status: string };
    expect(error).toMatchObject({ code: "EXECUTION_PRUNED", ...fields });
  });
}

test("ExecutionPrunedError is revived as its class by the durable value codec", () => {
  const error = new ExecutionPrunedError({
    executionId: ORDER,
    workflowId: "charge-order",
    terminalStatus: "cancelled",
    prunedAt: 1_000,
  });
  const decoding = durableValueCodec.decode(encodeThrownValue(error));
  if (decoding.status !== "decoded") throw new Error(decoding.message);
  expect(decoding.value).toBeInstanceOf(ExecutionPrunedError);
  expect(decoding.value).toMatchObject({
    message: error.message,
    code: "EXECUTION_PRUNED",
    executionId: ORDER,
    terminalStatus: "cancelled",
    prunedAt: 1_000,
  });
});

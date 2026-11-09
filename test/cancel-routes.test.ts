import { afterEach, describe, expect, test, vi } from "vitest";
import { WorkflowCancelledError, durableValueCodec } from "@durable/core";
import type { ObservationMiddlewareFunction, ObservationRequest } from "@durable/http-server";
import {
  admit,
  backendKinds,
  cleanup,
  complete,
  openBackend,
  serveRoutes,
} from "./observation-harness";

afterEach(cleanup);

const post = (url: string, route: string, body: unknown) =>
  fetch(`${url}/${route}`, {
    method: "POST",
    body: typeof body === "string" ? body : JSON.stringify(body),
  });

async function decoded(response: Response) {
  const decoding = durableValueCodec.decode(await response.text());
  if (decoding.status !== "decoded") throw new Error(decoding.message);
  return decoding.value;
}

async function expectError(response: Response, status: number, code: string) {
  expect(response.status).toBe(status);
  const body = await response.json();
  expect(body).toEqual({ code, message: expect.any(String) });
  return body;
}

const deny =
  (status: 401 | 403): ObservationMiddlewareFunction =>
  async () => ({ allow: false, status });

for (const backendKind of backendKinds) {
  describe(`/cancel on ${backendKind}`, () => {
    test("an allowed cancel of an active execution responds 202 with requested and the view", async () => {
      const { runtime } = openBackend(backendKind);
      const { url } = serveRoutes(runtime, {
        observationMiddleware: [async () => ({ allow: true })],
      });
      await admit(runtime, "order-42");

      const response = await post(url, "cancel", {
        executionId: "order-42",
        reason: "abandoned",
      });
      expect(response.status).toBe(202);
      const body = await decoded(response);
      expect(body).toEqual({
        status: "requested",
        execution: {
          executionId: "order-42",
          workflowId: "charge-order",
          status: "admitted",
          admittedAt: expect.any(Number),
          updatedAt: expect.any(Number),
          cancellation: { reason: "abandoned", forced: false, requestedAt: expect.any(Number) },
        },
      });
      expect(await runtime.executions.get("order-42")).toMatchObject({
        status: "admitted",
        cancellation: { reason: "abandoned", forced: false },
      });
    });

    test("an allowed cancel of a terminal execution responds 200 with its view and changes nothing", async () => {
      const { runtime } = openBackend(backendKind);
      const { url } = serveRoutes(runtime);
      await admit(runtime, "order-42");
      await complete(runtime, "order-42", "receipt");

      const response = await post(url, "cancel", { executionId: "order-42", force: true });
      expect(response.status).toBe(200);
      const body = await decoded(response);
      expect(body).toMatchObject({ executionId: "order-42", status: "completed", result: "receipt" });
      expect(body).not.toHaveProperty("cancellation");
      expect(await runtime.executions.get("order-42")).not.toHaveProperty("cancellation");
    });

    test("a forced cancel responds 200 with the cancelled view, and poll, wait and list encode it", async () => {
      const { runtime } = openBackend(backendKind);
      const { url } = serveRoutes(runtime);
      await admit(runtime, "order-42");

      const response = await post(url, "cancel", {
        executionId: "order-42",
        reason: "abandoned",
        force: true,
      });
      expect(response.status).toBe(200);
      const view = {
        executionId: "order-42",
        workflowId: "charge-order",
        status: "cancelled",
        reason: "abandoned",
        admittedAt: expect.any(Number),
        updatedAt: expect.any(Number),
        cancellation: { reason: "abandoned", forced: true, requestedAt: expect.any(Number) },
      };
      expect(await decoded(response)).toEqual(view);
      expect(await decoded(await post(url, "poll", { executionId: "order-42" }))).toEqual(view);
      expect(await decoded(await post(url, "wait", { executionId: "order-42" }))).toEqual(view);
      expect(await decoded(await post(url, "list", { status: "cancelled" }))).toEqual({
        executions: [view],
      });
    });

    test("/events responds 500 with a WorkflowCancelledError for a cancelled execution", async () => {
      const { runtime } = openBackend(backendKind);
      const { url } = serveRoutes(runtime);
      await admit(runtime, "order-42");
      await post(url, "cancel", { executionId: "order-42", reason: "abandoned", force: true });

      const response = await post(url, "events", { executionId: "order-42" });
      expect(response.status).toBe(500);
      const error = await decoded(response);
      expect(error).toBeInstanceOf(WorkflowCancelledError);
      expect(error).toMatchObject({ reason: "abandoned" });
    });

    for (const status of [401, 403] as const) {
      test(`a middleware that denies with ${status} responds the same for a known and an unknown ID, and records nothing`, async () => {
        const { runtime } = openBackend(backendKind);
        await admit(runtime, "known");
        const cancel = vi.spyOn(runtime.executions, "cancel");
        const get = vi.spyOn(runtime.executions, "get");
        const { url } = serveRoutes(runtime, { observationMiddleware: [deny(status)] });
        const code = status === 401 ? "UNAUTHORIZED" : "FORBIDDEN";

        const known = await expectError(
          await post(url, "cancel", { executionId: "known", force: true }),
          status,
          code
        );
        const unknown = await expectError(
          await post(url, "cancel", { executionId: "unknown", force: true }),
          status,
          code
        );
        expect(known).toEqual(unknown);
        expect(cancel).not.toHaveBeenCalled();
        expect(get).not.toHaveBeenCalled();
        expect(await runtime.executions.get("known")).toMatchObject({ status: "admitted" });
        expect(await runtime.executions.get("known")).not.toHaveProperty("cancellation");
      });
    }

    test("an allowed cancel of an unknown ID responds 404 EXECUTION_NOT_FOUND", async () => {
      const { runtime } = openBackend(backendKind);
      const { url } = serveRoutes(runtime, {
        observationMiddleware: [async () => ({ allow: true })],
      });
      await expectError(
        await post(url, "cancel", { executionId: "unknown" }),
        404,
        "EXECUTION_NOT_FOUND"
      );
    });

    test("a malformed body responds 400 INVALID_CANCEL_REQUEST before middleware runs", async () => {
      const { runtime } = openBackend(backendKind);
      const middleware = vi.fn<ObservationMiddlewareFunction>(async () => ({ allow: true }));
      const { url } = serveRoutes(runtime, { observationMiddleware: [middleware] });
      for (const body of [
        "{",
        [],
        "null",
        {},
        { executionId: "" },
        { executionId: 42 },
        { executionId: "order-42", reason: 7 },
        { executionId: "order-42", force: "yes" },
      ]) {
        await expectError(await post(url, "cancel", body), 400, "INVALID_CANCEL_REQUEST");
      }
      expect(middleware).not.toHaveBeenCalled();
    });

    test("middleware receives the cancel operation with its executionId and force, before any read", async () => {
      const { runtime } = openBackend(backendKind);
      await admit(runtime, "known");
      const events: string[] = [];
      const get = runtime.executions.get.bind(runtime.executions);
      vi.spyOn(runtime.executions, "get").mockImplementation(async (executionId) => {
        events.push("read");
        return get(executionId);
      });
      const cancel = runtime.executions.cancel.bind(runtime.executions);
      vi.spyOn(runtime.executions, "cancel").mockImplementation(async (request) => {
        events.push("cancel");
        return cancel(request);
      });
      const requests: ObservationRequest[] = [];
      const { url } = serveRoutes(runtime, {
        observationMiddleware: [
          async (_req, request) => {
            events.push("middleware");
            requests.push(request);
            return { allow: true };
          },
        ],
      });

      expect((await post(url, "cancel", { executionId: "known" })).status).toBe(202);
      expect(events).toEqual(["middleware", "cancel", "read"]);
      expect((await post(url, "cancel", { executionId: "known", force: true })).status).toBe(200);
      expect(requests).toEqual([
        { operation: "cancel", executionId: "known", force: false, load: expect.any(Function) },
        { operation: "cancel", executionId: "known", force: true, load: expect.any(Function) },
      ]);
    });

    test("load returns the record, or undefined for an unknown ID", async () => {
      const { runtime } = openBackend(backendKind);
      await admit(runtime, "known");
      const loaded: unknown[] = [];
      const { url } = serveRoutes(runtime, {
        observationMiddleware: [
          async (_req, request) => {
            if (request.operation === "cancel") loaded.push(await request.load());
            return { allow: true };
          },
        ],
      });
      const before = await runtime.executions.get("known");
      expect((await post(url, "cancel", { executionId: "known" })).status).toBe(202);
      expect((await post(url, "cancel", { executionId: "unknown" })).status).toBe(404);
      expect(loaded).toEqual([before, undefined]);
    });

    test("a policy can allow reads and deny cancel by checking operation", async () => {
      const { runtime } = openBackend(backendKind);
      await admit(runtime, "known");
      const { url } = serveRoutes(runtime, {
        observationMiddleware: [
          async (_req, request) =>
            request.operation === "cancel" ? { allow: false, status: 403 } : { allow: true },
        ],
      });
      expect((await post(url, "poll", { executionId: "known" })).status).toBe(200);
      await expectError(await post(url, "cancel", { executionId: "known" }), 403, "FORBIDDEN");
    });

    test("trigger middleware does not apply to /cancel", async () => {
      const { runtime } = openBackend(backendKind);
      await admit(runtime, "known");
      const { url } = serveRoutes(runtime, {
        middleware: [async () => new Response("Unauthorized", { status: 401 })],
      });
      expect((await post(url, "cancel", { executionId: "known" })).status).toBe(202);
    });
  });
}

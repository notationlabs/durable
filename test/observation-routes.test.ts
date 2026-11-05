import { EventEmitter } from "node:events";
import { afterEach, describe, expect, test, vi } from "vitest";
import { durableValueCodec, type ExecutionRecord } from "@yieldstar/core";
import {
  createRoutes,
  type ObservationMiddlewareFunction,
  type ObservationRequest,
} from "@yieldstar/http-server";
import { MemoryRuntime } from "@yieldstar/test-runtime";
import {
  CardDeclinedError,
  admit,
  backendKinds,
  cleanup,
  complete,
  elapsed,
  fail,
  logger,
  openBackend,
  serveRoutes,
  sleep,
} from "./observation-harness";

afterEach(cleanup);

const post = (url: string, route: string, body: unknown) =>
  fetch(`${url}/${route}`, {
    method: "POST",
    body: typeof body === "string" ? body : JSON.stringify(body),
  });

async function expectError(response: Response, status: number, code: string) {
  expect(response.status).toBe(status);
  const body = await response.json();
  expect(body).toEqual({ code, message: expect.any(String) });
  return body;
}

const executionRoutes = ["poll", "wait", "events"] as const;

for (const backendKind of backendKinds) {
  describe(`observation routes on ${backendKind}`, () => {
    test("/trigger responds 202 when accepted and 200 when an idempotent repeat is existing", async () => {
      const { runtime } = openBackend(backendKind);
      const { url } = serveRoutes(runtime);
      const event = { workflowId: "charge-order", executionId: "order-42", params: {}, idempotent: true };
      const accepted = await post(url, "trigger", event);
      expect(accepted.status).toBe(202);
      expect(await accepted.json()).toEqual({ executionId: "order-42", status: "accepted" });
      const existing = await post(url, "trigger", event);
      expect(existing.status).toBe(200);
      expect(await existing.json()).toEqual({ executionId: "order-42", status: "existing" });

      await expectError(
        await post(url, "trigger", { ...event, params: { amount: 45 } }),
        409,
        "EXECUTION_REQUEST_MISMATCH"
      );
      await expectError(
        await post(url, "trigger", { ...event, idempotent: undefined }),
        409,
        "EXECUTION_ID_CONFLICT"
      );
      await expectError(
        await post(url, "trigger", { ...event, executionId: undefined }),
        400,
        "IDEMPOTENT_TRIGGER_REQUIRES_ID"
      );
      await expectError(
        await post(url, "trigger", { ...event, executionId: "order-43", idempotent: "yes" }),
        400,
        "INVALID_EXECUTION_EVENT"
      );
      await expectError(await post(url, "trigger", "{"), 400, "INVALID_EXECUTION_EVENT");
    });

    test("/poll and /wait respond 200 with a view, and 404 for an unknown ID", async () => {
      const { runtime } = openBackend(backendKind);
      const { url } = serveRoutes(runtime);
      await admit(runtime, "order-42");
      await fail(runtime, "order-42", new CardDeclinedError("card declined"));

      for (const route of ["poll", "wait"]) {
        const response = await post(url, route, { executionId: "order-42" });
        expect(response.status).toBe(200);
        const decoding = durableValueCodec.decode(await response.text());
        if (decoding.status !== "decoded") throw new Error(decoding.message);
        expect(decoding.value).toMatchObject({ executionId: "order-42", status: "failed" });
        expect((decoding.value as { error: Error }).error).toBeInstanceOf(CardDeclinedError);
      }
      for (const route of executionRoutes) {
        await expectError(
          await post(url, route, { executionId: "unknown" }),
          404,
          "EXECUTION_NOT_FOUND"
        );
      }
    });

    test("malformed bodies and negative timeouts respond 400 INVALID_OBSERVATION_REQUEST", async () => {
      const { runtime } = openBackend(backendKind);
      const { url } = serveRoutes(runtime);
      for (const route of [...executionRoutes, "list"]) {
        await expectError(await post(url, route, "{"), 400, "INVALID_OBSERVATION_REQUEST");
        await expectError(await post(url, route, []), 400, "INVALID_OBSERVATION_REQUEST");
        await expectError(await post(url, route, "null"), 400, "INVALID_OBSERVATION_REQUEST");
      }
      for (const route of executionRoutes) {
        await expectError(await post(url, route, {}), 400, "INVALID_OBSERVATION_REQUEST");
        await expectError(
          await post(url, route, { executionId: 42 }),
          400,
          "INVALID_OBSERVATION_REQUEST"
        );
      }
      for (const timeout of [-1, "10"]) {
        await expectError(
          await post(url, "wait", { executionId: "order-42", timeout }),
          400,
          "INVALID_OBSERVATION_REQUEST"
        );
      }
    });

    test("an invalid list limit, status or cursor responds 400 INVALID_LIST_REQUEST", async () => {
      const { runtime } = openBackend(backendKind);
      const { url } = serveRoutes(runtime);
      for (const body of [
        { limit: 0 },
        { limit: 1.5 },
        { limit: "10" },
        { status: "done" },
        { cursor: "not-a-cursor" },
        { cursor: 7 },
        { workflowId: 42 },
      ]) {
        await expectError(await post(url, "list", body), 400, "INVALID_LIST_REQUEST");
      }
      const clamped = await post(url, "list", { limit: 5_000 });
      expect(clamped.status).toBe(200);
      expect(await clamped.json()).toEqual({ executions: [] });
    });

    test("/events responds 200 with the raw result, 500 with the serialized error, and 404 for an unknown ID", async () => {
      const { runtime } = openBackend(backendKind);
      const { url } = serveRoutes(runtime);
      await admit(runtime, "completed");
      await complete(runtime, "completed", { receipt: "r-42" });
      await admit(runtime, "failed");
      await fail(runtime, "failed", new CardDeclinedError("card declined"));

      const completed = await post(url, "events", { executionId: "completed" });
      expect(completed.status).toBe(200);
      expect(await completed.json()).toEqual({ receipt: "r-42" });

      const failed = await post(url, "events", { executionId: "failed" });
      expect(failed.status).toBe(500);
      const decoding = durableValueCodec.decode(await failed.text());
      if (decoding.status !== "decoded") throw new Error(decoding.message);
      expect(decoding.value).toBeInstanceOf(CardDeclinedError);
      expect(decoding.value).toMatchObject({ code: "CARD_DECLINED", message: "card declined" });

      await expectError(
        await post(url, "events", { executionId: "unknown" }),
        404,
        "EXECUTION_NOT_FOUND"
      );
    });

    test("/events waits for an outcome recorded later, past the /wait cap", async () => {
      const { runtime, peer } = openBackend(backendKind);
      const { url } = serveRoutes(runtime, { waitCap: 20 });
      await admit(runtime, "later");
      let settled = false;
      const response = post(url, "events", { executionId: "later" }).finally(() => {
        settled = true;
      });
      await sleep(150);
      expect(settled).toBe(false);
      await complete(peer, "later", "receipt");
      const res = await response;
      expect(res.status).toBe(200);
      expect(await res.json()).toBe("receipt");
    });

    describe("observation middleware", () => {
      const deny =
        (status: 401 | 403): ObservationMiddlewareFunction =>
        async () => ({ allow: false, status });

      test("a denial responds the same for a known and an unknown ID", async () => {
        const { runtime } = openBackend(backendKind);
        await admit(runtime, "known");
        for (const [status, code] of [
          [401, "UNAUTHORIZED"],
          [403, "FORBIDDEN"],
        ] as const) {
          const { url } = serveRoutes(runtime, { observationMiddleware: [deny(status)] });
          for (const route of executionRoutes) {
            const known = await expectError(
              await post(url, route, { executionId: "known" }),
              status,
              code
            );
            const unknown = await expectError(
              await post(url, route, { executionId: "unknown" }),
              status,
              code
            );
            expect(known).toEqual(unknown);
          }
          await expectError(await post(url, "list", {}), status, code);
        }
      });

      test("an allowing middleware that does not call load runs before the route reads the record", async () => {
        const { runtime } = openBackend(backendKind);
        await admit(runtime, "known");
        await complete(runtime, "known", "receipt");
        const reads: string[] = [];
        const events: string[] = [];
        const get = runtime.executions.get.bind(runtime.executions);
        vi.spyOn(runtime.executions, "get").mockImplementation(async (executionId) => {
          events.push("read");
          reads.push(executionId);
          return get(executionId);
        });
        const { url } = serveRoutes(runtime, {
          observationMiddleware: [
            async () => {
              events.push("middleware");
              return { allow: true };
            },
          ],
        });
        for (const route of executionRoutes) {
          events.length = 0;
          const response = await post(url, route, { executionId: "known" });
          expect(response.status).toBe(200);
          expect(events[0]).toBe("middleware");
          expect(events).toContain("read");
        }
        expect(reads.every((id) => id === "known")).toBe(true);
      });

      test("load returns the record, or undefined for an unknown ID", async () => {
        const { runtime } = openBackend(backendKind);
        await admit(runtime, "known");
        const loaded: (ExecutionRecord | undefined)[] = [];
        const requests: ObservationRequest[] = [];
        const { url } = serveRoutes(runtime, {
          observationMiddleware: [
            async (_req, request) => {
              requests.push(request);
              if (request.operation !== "list") loaded.push((await request.load()) as ExecutionRecord | undefined);
              return { allow: true };
            },
          ],
        });
        expect((await post(url, "poll", { executionId: "known" })).status).toBe(200);
        expect((await post(url, "wait", { executionId: "known", timeout: 0 })).status).toBe(200);
        expect((await post(url, "poll", { executionId: "unknown" })).status).toBe(404);
        expect(loaded).toEqual([
          await runtime.executions.get("known"),
          await runtime.executions.get("known"),
          undefined,
        ]);
        expect(loaded[0]).toMatchObject({ params: { orderId: 42 } });
        expect(requests.map((request) => request.operation)).toEqual(["poll", "wait", "poll"]);
        expect(requests[0]).toMatchObject({ executionId: "known" });
      });

      test("a list request carries its filter and no load", async () => {
        const { runtime } = openBackend(backendKind);
        const requests: ObservationRequest[] = [];
        const { url } = serveRoutes(runtime, {
          observationMiddleware: [
            async (_req, request) => {
              requests.push(request);
              return { allow: true };
            },
          ],
        });
        await post(url, "list", { workflowId: "charge-order", status: "failed", limit: 5 });
        await post(url, "list", {});
        expect(requests).toEqual([
          { operation: "list", filter: { workflowId: "charge-order", status: "failed" } },
          { operation: "list", filter: {} },
        ]);
        expect(requests[0]).not.toHaveProperty("load");
      });

      test("/list treats null fields as absent", async () => {
        const { runtime } = openBackend(backendKind);
        await admit(runtime, "a");
        await admit(runtime, "b");
        const requests: ObservationRequest[] = [];
        const { url } = serveRoutes(runtime, {
          observationMiddleware: [
            async (_req, request) => {
              requests.push(request);
              return { allow: true };
            },
          ],
        });
        const response = await post(url, "list", {
          workflowId: null,
          status: null,
          cursor: null,
          limit: null,
        });
        expect(response.status).toBe(200);
        const page = (await response.json()) as { executions: { executionId: string }[] };
        expect(page.executions.map((view) => view.executionId)).toEqual(["b", "a"]);
        expect(requests).toEqual([{ operation: "list", filter: {} }]);
      });

      test("middleware runs in array order, and the first denial wins", async () => {
        const { runtime } = openBackend(backendKind);
        const calls: string[] = [];
        const record =
          (name: string, decision: Awaited<ReturnType<ObservationMiddlewareFunction>>) =>
          async () => {
            calls.push(name);
            return decision;
          };
        const { url } = serveRoutes(runtime, {
          observationMiddleware: [
            record("first", { allow: true }),
            record("second", { allow: false, status: 401 }),
            record("third", { allow: false, status: 403 }),
          ],
        });
        await expectError(await post(url, "poll", { executionId: "any" }), 401, "UNAUTHORIZED");
        expect(calls).toEqual(["first", "second"]);
      });

      test("a middleware that throws responds 500 INTERNAL_ERROR with a fixed message and logs the error", async () => {
        const { runtime } = openBackend(backendKind);
        const logged = vi.spyOn(logger, "error");
        const policyError = new Error("policy store unavailable");
        const authError = new Error("auth service unavailable");
        const { url } = serveRoutes(runtime, {
          observationMiddleware: [
            async () => {
              throw policyError;
            },
          ],
          middleware: [
            async () => {
              throw authError;
            },
          ],
        });
        const bodies = [];
        for (const route of executionRoutes) {
          bodies.push(
            await expectError(await post(url, route, { executionId: "any" }), 500, "INTERNAL_ERROR")
          );
        }
        bodies.push(await expectError(await post(url, "list", {}), 500, "INTERNAL_ERROR"));
        bodies.push(
          await expectError(
            await post(url, "trigger", { workflowId: "charge-order" }),
            500,
            "INTERNAL_ERROR"
          )
        );
        for (const body of bodies) {
          expect(body).toEqual({ code: "INTERNAL_ERROR", message: "Internal error" });
        }
        expect(logged.mock.calls.map(([err]) => err)).toEqual([
          ...executionRoutes.map(() => policyError),
          policyError,
          authError,
        ]);
      });

      test("a runtime that throws responds 500 INTERNAL_ERROR without the runtime's message", async () => {
        const { runtime } = openBackend(backendKind);
        const logged = vi.spyOn(logger, "error");
        const dbError = new Error("SQLITE_IOERR: disk I/O error at /var/lib/app/executions.db");
        vi.spyOn(runtime.executions, "get").mockRejectedValue(dbError);
        vi.spyOn(runtime.executions, "list").mockRejectedValue(dbError);
        vi.spyOn(runtime.executions, "admit").mockRejectedValue(dbError);
        const { url } = serveRoutes(runtime);
        const responses = [
          ...executionRoutes.map((route) => post(url, route, { executionId: "any" })),
          post(url, "list", {}),
          post(url, "trigger", { workflowId: "charge-order" }),
        ];
        for (const response of await Promise.all(responses)) {
          expect(await expectError(response, 500, "INTERNAL_ERROR")).toEqual({
            code: "INTERNAL_ERROR",
            message: "Internal error",
          });
        }
        expect(logged).toHaveBeenCalledTimes(responses.length);
        expect(logged).toHaveBeenCalledWith(dbError);
      });

      test("trigger middleware that denies every request leaves observation routes unaffected", async () => {
        const { runtime } = openBackend(backendKind);
        await admit(runtime, "known");
        await complete(runtime, "known", "receipt");
        const { url } = serveRoutes(runtime, {
          middleware: [async () => new Response("Unauthorized", { status: 401 })],
        });
        expect((await post(url, "trigger", { workflowId: "charge-order" })).status).toBe(401);
        expect((await post(url, "poll", { executionId: "known" })).status).toBe(200);
        expect((await post(url, "wait", { executionId: "known", timeout: 0 })).status).toBe(200);
        expect((await post(url, "list", {})).status).toBe(200);
        const events = await post(url, "events", { executionId: "known" });
        expect(events.status).toBe(200);
        expect(await events.json()).toBe("receipt");
      });

      test("observation middleware does not apply to /trigger", async () => {
        const { runtime } = openBackend(backendKind);
        const { url } = serveRoutes(runtime, { observationMiddleware: [deny(403)] });
        expect((await post(url, "trigger", { workflowId: "charge-order" })).status).toBe(202);
      });
    });
  });
}

test("many open /wait and /events requests on one ID share one workflowEndEmitter listener", async () => {
  const runtime = new MemoryRuntime();
  const workflowEndEmitter = new EventEmitter();
  const warnings: Error[] = [];
  const onWarning = (warning: Error) => warnings.push(warning);
  process.on("warning", onWarning);
  try {
    const { url } = serveRoutes(runtime, {
      invoker: { workflowEndEmitter },
      pollInterval: 60_000,
    });
    await admit(runtime, "popular");
    const responses = [
      ...Array.from({ length: 12 }, () => post(url, "wait", { executionId: "popular" })),
      ...Array.from({ length: 12 }, () => post(url, "events", { executionId: "popular" })),
    ];
    await sleep(20);
    expect(workflowEndEmitter.listenerCount("popular")).toBe(1);

    await complete(runtime, "popular", "receipt");
    workflowEndEmitter.emit("popular", "receipt");
    for (const response of await Promise.all(responses)) {
      expect(response.status).toBe(200);
    }
    expect(workflowEndEmitter.listenerCount("popular")).toBe(0);
    await sleep(10);
    expect(warnings.filter((warning) => warning.name === "MaxListenersExceededWarning")).toEqual(
      []
    );
  } finally {
    process.off("warning", onWarning);
  }
});

test("pollInterval sets how often /wait reads the record", async () => {
  const runtime = new MemoryRuntime();
  const { url } = serveRoutes(runtime, { pollInterval: 60_000 });
  await admit(runtime, "later");
  const response = elapsed(() => post(url, "wait", { executionId: "later", timeout: 200 }));
  await sleep(20);
  await complete(runtime, "later", "receipt");
  const { value, ms } = await response;
  // With no notifier, the wait reads the record again only at its timeout.
  expect(await value.json()).toMatchObject({ status: "completed", result: "receipt" });
  expect(ms).toBeGreaterThanOrEqual(190);

  expect(() => createRoutes({ runtime, logger, pollInterval: -1 })).toThrow(RangeError);
});

test("basePath prefixes every route", () => {
  const routes = createRoutes({
    runtime: new MemoryRuntime(),
    logger,
    basePath: "/api/workflows",
  });
  expect(Object.keys(routes).sort()).toEqual(
    ["cancel", "clear-divergence", "events", "list", "poll", "trigger", "wait"].map((route) => `/api/workflows/${route}`)
  );
});

import { afterEach, describe, expect, test, vi } from "vitest";
import { durableValueCodec, type Runtime } from "@yieldstar/core";
import type { ObservationMiddlewareFunction, ObservationRequest } from "@yieldstar/http-server";
import {
  admit,
  backendKinds,
  claim,
  cleanup,
  createTestSdk,
  openBackend,
  sdkKinds,
  serveRoutes,
} from "./observation-harness";
import { findLiveRecord } from "@yieldstar/runtime-conformance";

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

/** Parks the execution as a diverged delivery would. */
async function diverge(runtime: Runtime, executionId: string) {
  await runtime.executions.acknowledge({
    lease: await claim(runtime, executionId),
    outcome: {
      status: "diverged",
      code: "NONDETERMINISTIC_REPLAY",
      stepKey: "charge-v3",
      workflowVersion: 2,
    },
  });
}

const divergence = {
  code: "NONDETERMINISTIC_REPLAY",
  stepKey: "charge-v3",
  workflowVersion: 2,
  detectedAt: expect.any(Number),
};

for (const backendKind of backendKinds) {
  describe(`/clear-divergence on ${backendKind}`, () => {
    test("an allowed clear of a diverged execution responds 200 cleared and makes it due", async () => {
      const { runtime } = openBackend(backendKind);
      const { url } = serveRoutes(runtime, {
        observationMiddleware: [async () => ({ allow: true })],
      });
      await admit(runtime, "order-42");
      await diverge(runtime, "order-42");
      expect(
        await runtime.executions.claimDue({ limit: 1, duration: 30_000 })
      ).toEqual([]);

      const response = await post(url, "clear-divergence", { executionId: "order-42" });
      expect(response.status).toBe(200);
      expect(await decoded(response)).toEqual({ status: "cleared" });
      expect(await runtime.executions.get("order-42")).not.toHaveProperty("divergence");
      expect(await runtime.executions.claimDue({ limit: 1, duration: 30_000 })).toHaveLength(1);
    });

    test("an allowed clear of a healthy execution responds 200 not-diverged", async () => {
      const { runtime } = openBackend(backendKind);
      const { url } = serveRoutes(runtime);
      await admit(runtime, "order-42");

      const response = await post(url, "clear-divergence", { executionId: "order-42" });
      expect(response.status).toBe(200);
      expect(await decoded(response)).toEqual({ status: "not-diverged" });
    });

    test("an allowed clear of an unknown ID responds 404 EXECUTION_NOT_FOUND", async () => {
      const { runtime } = openBackend(backendKind);
      const { url } = serveRoutes(runtime);
      await expectError(
        await post(url, "clear-divergence", { executionId: "unknown" }),
        404,
        "EXECUTION_NOT_FOUND"
      );
    });

    for (const status of [401, 403] as const) {
      test(`a middleware that denies with ${status} responds the same for a known and an unknown ID, and clears nothing`, async () => {
        const { runtime } = openBackend(backendKind);
        await admit(runtime, "known");
        await diverge(runtime, "known");
        const clear = vi.spyOn(runtime.executions, "clearDivergence");
        const get = vi.spyOn(runtime.executions, "get");
        const { url } = serveRoutes(runtime, {
          observationMiddleware: [async () => ({ allow: false, status })],
        });
        const code = status === 401 ? "UNAUTHORIZED" : "FORBIDDEN";

        const known = await expectError(
          await post(url, "clear-divergence", { executionId: "known" }),
          status,
          code
        );
        const unknown = await expectError(
          await post(url, "clear-divergence", { executionId: "unknown" }),
          status,
          code
        );
        expect(known).toEqual(unknown);
        expect(clear).not.toHaveBeenCalled();
        expect(get).not.toHaveBeenCalled();
        get.mockRestore();
        expect((await findLiveRecord(runtime.executions, "known"))?.divergence).toEqual(divergence);
      });
    }

    test("a body without an executionId responds 400 INVALID_OBSERVATION_REQUEST before middleware runs", async () => {
      const { runtime } = openBackend(backendKind);
      const middleware = vi.fn<ObservationMiddlewareFunction>(async () => ({ allow: true }));
      const { url } = serveRoutes(runtime, { observationMiddleware: [middleware] });
      for (const body of ["{", [], {}, { executionId: "" }, { executionId: 42 }]) {
        await expectError(
          await post(url, "clear-divergence", body),
          400,
          "INVALID_OBSERVATION_REQUEST"
        );
      }
      expect(middleware).not.toHaveBeenCalled();
    });

    test("middleware receives the clear-divergence operation with its executionId, before any read", async () => {
      const { runtime } = openBackend(backendKind);
      await admit(runtime, "known");
      const requests: ObservationRequest[] = [];
      const clear = vi.spyOn(runtime.executions, "clearDivergence");
      const { url } = serveRoutes(runtime, {
        observationMiddleware: [
          async (_req, request) => {
            requests.push(request);
            expect(clear).not.toHaveBeenCalled();
            return { allow: true };
          },
        ],
      });
      await post(url, "clear-divergence", { executionId: "known" });
      expect(requests).toEqual([
        { operation: "clear-divergence", executionId: "known", load: expect.any(Function) },
      ]);
      expect(await (requests[0] as { load(): Promise<unknown> }).load()).toMatchObject({
        executionId: "known",
      });
    });

    test("poll and list show workflowVersion and divergence, and list filters by diverged", async () => {
      const { runtime } = openBackend(backendKind);
      const { url } = serveRoutes(runtime);
      await admit(runtime, "order-42");
      await admit(runtime, "order-43");
      await diverge(runtime, "order-42");

      const view = await decoded(await post(url, "poll", { executionId: "order-42" }));
      expect(view).toMatchObject({ status: "admitted", workflowVersion: 2, divergence });
      const listed = (await decoded(await post(url, "list", { diverged: true }))) as {
        executions: { executionId: string }[];
      };
      expect(listed.executions.map(({ executionId }) => executionId)).toEqual(["order-42"]);
      await expectError(await post(url, "list", { diverged: "yes" }), 400, "INVALID_LIST_REQUEST");
    });

    test("list middleware receives the diverged filter", async () => {
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
      await post(url, "list", { diverged: true });
      expect(requests).toEqual([{ operation: "list", filter: { diverged: true } }]);
    });
  });
}

for (const sdkKind of sdkKinds) {
  for (const backendKind of backendKinds) {
    describe(`${sdkKind} SDK clearDivergence on ${backendKind}`, () => {
      const setup = () => {
        const backend = openBackend(backendKind);
        const sdk = createTestSdk(sdkKind, backend.runtime);
        return { ...backend, sdk };
      };

      test("clearDivergence returns cleared, then not-diverged, and undefined for an unknown ID", async () => {
        const { runtime, sdk } = setup();
        await admit(runtime, "order-42");
        await diverge(runtime, "order-42");
        expect(await sdk.poll("order-42")).toMatchObject({ divergence });
        expect((await sdk.list({ diverged: true })).executions).toHaveLength(1);

        expect(await sdk.clearDivergence("order-42")).toEqual({ status: "cleared" });
        expect(await sdk.poll("order-42")).not.toHaveProperty("divergence");
        expect(await sdk.clearDivergence("order-42")).toEqual({ status: "not-diverged" });
        expect(await sdk.clearDivergence("unknown")).toBeUndefined();
      });

      test("clearDivergence rejects an empty ID without a request", async () => {
        const { sdk } = setup();
        await expect(sdk.clearDivergence("")).rejects.toThrow(TypeError);
        expect(sdk.responses).toEqual([]);
      });
    });
  }
}

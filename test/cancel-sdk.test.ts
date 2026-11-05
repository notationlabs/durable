import { afterEach, describe, expect, test } from "vitest";
import { ExecutionCancelRequestError, WorkflowCancelledError } from "yieldstar";
import {
  admit,
  backendKinds,
  cleanup,
  complete,
  createTestSdk,
  openBackend,
  sdkKinds,
  suspend,
} from "./observation-harness";

afterEach(cleanup);

for (const sdkKind of sdkKinds) {
  for (const backendKind of backendKinds) {
    describe(`${sdkKind} SDK cancel on ${backendKind}`, () => {
      const setup = () => {
        const backend = openBackend(backendKind);
        const sdk = createTestSdk(sdkKind, backend.runtime);
        return { ...backend, sdk };
      };

      test("cancel of an active execution returns requested with the view, and poll shows the request", async () => {
        const { runtime, sdk } = setup();
        await admit(runtime, "order-42");
        await suspend(runtime, "order-42");

        const result = await sdk.cancel("order-42", { reason: "abandoned" });
        const cancellation = {
          reason: "abandoned",
          forced: false,
          requestedAt: expect.any(Number),
        };
        expect(result).toEqual({
          status: "requested",
          execution: expect.objectContaining({
            executionId: "order-42",
            status: "suspended",
            cancellation,
          }),
        });
        expect(await sdk.poll("order-42")).toMatchObject({ status: "suspended", cancellation });
        expect(await sdk.wait("order-42", { timeout: 0 })).toMatchObject({
          status: "timed-out",
          execution: { cancellation },
        });
      });

      test("a repeated cancel keeps the first reason", async () => {
        const { runtime, sdk } = setup();
        await admit(runtime, "order-42");
        await sdk.cancel("order-42", { reason: "abandoned" });
        expect(await sdk.cancel("order-42", { reason: "duplicate" })).toMatchObject({
          status: "requested",
          execution: { cancellation: { reason: "abandoned" } },
        });
      });

      test("cancel of a terminal execution returns its view and changes nothing", async () => {
        const { runtime, sdk } = setup();
        await admit(runtime, "order-42");
        await complete(runtime, "order-42", "receipt");

        const result = await sdk.cancel("order-42", { reason: "abandoned", force: true });
        expect(result).toMatchObject({ status: "completed", result: "receipt" });
        expect(result).not.toHaveProperty("cancellation");
        expect(await sdk.poll("order-42")).not.toHaveProperty("cancellation");
      });

      test("cancel of an unknown ID returns undefined", async () => {
        const { sdk } = setup();
        expect(await sdk.cancel("unknown")).toBeUndefined();
        expect(await sdk.cancel("unknown", { force: true })).toBeUndefined();
      });

      test("a forced cancel returns the cancelled view, and wait and list show it", async () => {
        const { runtime, sdk } = setup();
        await admit(runtime, "order-42");
        await admit(runtime, "order-43");

        const view = {
          executionId: "order-42",
          workflowId: "charge-order",
          status: "cancelled",
          reason: "abandoned",
          admittedAt: expect.any(Number),
          updatedAt: expect.any(Number),
          cancellation: { reason: "abandoned", forced: true, requestedAt: expect.any(Number) },
        };
        expect(await sdk.cancel("order-42", { reason: "abandoned", force: true })).toEqual(view);
        expect(await sdk.poll("order-42")).toEqual(view);
        expect(await sdk.wait("order-42")).toEqual(view);
        expect(await sdk.list({ status: "cancelled" })).toEqual({ executions: [view] });
        expect((await sdk.list({ status: "admitted" })).executions).toEqual([
          expect.objectContaining({ executionId: "order-43" }),
        ]);
      });

      test("a forced cancel after a graceful one returns the cancelled view with forced set", async () => {
        const { runtime, sdk } = setup();
        await admit(runtime, "order-42");
        await sdk.cancel("order-42", { reason: "abandoned" });
        expect(await sdk.cancel("order-42", { force: true })).toMatchObject({
          status: "cancelled",
          reason: "abandoned",
          cancellation: { reason: "abandoned", forced: true },
        });
      });

      test("wait on a child cancelled through its parent carries the source", async () => {
        const { runtime, sdk } = setup();
        await admit(runtime, "caller");
        const started = await runtime.executions.start({
          requestId: { executionId: "caller", stepKey: "ship" },
          workflowId: "charge-order",
          params: { orderId: 43 },
        });
        if (started.status !== "accepted") throw new Error("expected an accepted start");

        await sdk.cancel("caller", { reason: "abandoned", force: true });
        expect(await sdk.wait(started.executionId)).toMatchObject({
          status: "cancelled",
          reason: "abandoned",
          source: "caller",
        });
      });

      test("triggerAndWait rejects with WorkflowCancelledError when the execution is cancelled", async () => {
        const { sdk } = setup();
        const waiting = sdk
          .triggerAndWait({ workflowId: "charge-order", executionId: "order-42", params: {} })
          .catch((err) => err);
        while (!(await sdk.poll("order-42"))) await new Promise((r) => setTimeout(r, 5));
        await sdk.cancel("order-42", { reason: "abandoned", force: true });

        const error = (await waiting) as WorkflowCancelledError;
        expect(error).toBeInstanceOf(WorkflowCancelledError);
        expect(error).toMatchObject({ reason: "abandoned" });
        expect(error.source).toBeUndefined();
      });

      test("an idempotent trigger that matches a cancelled record returns existing", async () => {
        const { sdk } = setup();
        const event = {
          workflowId: "charge-order",
          executionId: "order-42",
          params: { orderId: 42 },
          idempotent: true,
        };
        expect(await sdk.trigger(event)).toEqual({ executionId: "order-42", status: "accepted" });
        await sdk.cancel("order-42", { force: true });
        expect(await sdk.trigger(event)).toEqual({ executionId: "order-42", status: "existing" });
        expect(await sdk.poll("order-42")).toMatchObject({ status: "cancelled" });
      });

      test("cancel rejects an empty ID, or a reason or force of the wrong type, without a request", async () => {
        const { sdk } = setup();
        await expect(sdk.cancel("")).rejects.toThrow(ExecutionCancelRequestError);
        await expect(sdk.cancel("order-42", { reason: 7 as never })).rejects.toThrow(
          ExecutionCancelRequestError
        );
        await expect(sdk.cancel("order-42", { force: "yes" as never })).rejects.toThrow(
          ExecutionCancelRequestError
        );
        expect(sdk.responses).toEqual([]);
      });
    });
  }
}

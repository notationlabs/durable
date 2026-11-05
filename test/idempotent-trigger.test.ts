import { afterEach, describe, expect, test } from "vitest";
import type { EventContext } from "@yieldstar/core";
import { MemoryRuntime } from "@yieldstar/test-runtime";
import { ExecutionAdmissionError, createLocalSdk, workflow } from "yieldstar";
import {
  backendKinds,
  cleanup,
  complete,
  createTestSdk,
  openBackend,
  sdkKinds,
} from "./observation-harness";

afterEach(cleanup);

const order = {
  workflowId: "charge-order",
  executionId: "order-42",
  params: { orderId: 42, amount: 30 },
  idempotent: true,
};

for (const sdkKind of sdkKinds) {
  for (const backendKind of backendKinds) {
    describe(`idempotent trigger through the ${sdkKind} SDK on ${backendKind}`, () => {
      const setup = () => {
        const backend = openBackend(backendKind);
        const sdk = createTestSdk(sdkKind, backend.runtime);
        const statuses = () => sdk.responses.map((response) => response.status);
        const count = async () => (await sdk.list()).executions.length;
        return { ...backend, sdk, statuses, count };
      };

      const rejection = (promise: Promise<unknown>) =>
        promise.then(
          () => {
            throw new Error("expected the trigger to be rejected");
          },
          (err) => {
            expect(err).toBeInstanceOf(ExecutionAdmissionError);
            return err as ExecutionAdmissionError;
          }
        );

      test("a first trigger is accepted, and a repeat of the same request is existing", async () => {
        const { runtime, sdk, statuses, count } = setup();
        expect(await sdk.trigger(order)).toEqual({ executionId: "order-42", status: "accepted" });
        const record = await runtime.executions.get("order-42");

        expect(await sdk.trigger(order)).toEqual({ executionId: "order-42", status: "existing" });
        expect(await runtime.executions.get("order-42")).toEqual(record);
        expect(await count()).toBe(1);
        if (sdkKind === "http") expect(statuses()).toEqual([202, 200, 200]);
      });

      test("a repeat with different params or workflowId is a mismatch, and the record is unchanged", async () => {
        const { runtime, sdk, statuses } = setup();
        await sdk.trigger(order);
        const record = await runtime.executions.get("order-42");

        const differentParams = await rejection(
          sdk.trigger({ ...order, params: { orderId: 42, amount: 45 } })
        );
        expect(differentParams.code).toBe("EXECUTION_REQUEST_MISMATCH");
        const differentWorkflow = await rejection(
          sdk.trigger({ ...order, workflowId: "refund-order" })
        );
        expect(differentWorkflow.code).toBe("EXECUTION_REQUEST_MISMATCH");
        const missingParams = await rejection(sdk.trigger({ ...order, params: undefined }));
        expect(missingParams.code).toBe("EXECUTION_REQUEST_MISMATCH");

        expect(await runtime.executions.get("order-42")).toEqual(record);
        if (sdkKind === "http") expect(statuses()).toEqual([202, 409, 409, 409]);
      });

      test("a repeat that differs only in context, or in key order, is existing", async () => {
        const { sdk } = setup();
        await sdk.trigger({ ...order, context: { token: "first" } });
        expect(await sdk.trigger({ ...order, context: { token: "fresh" } })).toMatchObject({
          status: "existing",
        });
        expect(
          await sdk.trigger({ ...order, params: { amount: 30, orderId: 42 } })
        ).toMatchObject({ status: "existing" });
      });

      test("an idempotent repeat of an execution admitted without idempotent mode is existing", async () => {
        const { sdk } = setup();
        await sdk.trigger({ ...order, idempotent: undefined });
        expect(await sdk.trigger(order)).toEqual({ executionId: "order-42", status: "existing" });
      });

      test("an idempotent repeat of a terminal execution is existing", async () => {
        const { runtime, sdk } = setup();
        await sdk.trigger(order);
        await complete(runtime, "order-42", { receipt: "r-42" });
        expect(await sdk.trigger(order)).toEqual({ executionId: "order-42", status: "existing" });
        expect(await sdk.poll("order-42")).toMatchObject({ status: "completed" });
      });

      test("two concurrent idempotent triggers with the same request admit one execution", async () => {
        const { runtime, peer, sdk, count } = setup();
        const other = createTestSdk(sdkKind, peer);
        const results = await Promise.all([sdk.trigger(order), other.trigger(order)]);
        expect(results.map((result) => result.status).sort()).toEqual(["accepted", "existing"]);
        expect(results.map((result) => result.executionId)).toEqual(["order-42", "order-42"]);
        expect(await count()).toBe(1);
        expect(await runtime.executions.get("order-42")).toBeDefined();
      });

      test("an idempotent trigger without an ID is rejected", async () => {
        const { sdk, statuses, count } = setup();
        const error = await rejection(sdk.trigger({ ...order, executionId: undefined }));
        expect(error.code).toBe("IDEMPOTENT_TRIGGER_REQUIRES_ID");
        expect(await count()).toBe(0);
        if (sdkKind === "http") expect(statuses()).toEqual([400, 200]);
      });

      test("a trigger without idempotent mode and a used ID is a conflict", async () => {
        const { sdk, statuses } = setup();
        await sdk.trigger(order);
        const error = await rejection(sdk.trigger({ ...order, idempotent: undefined }));
        expect(error.code).toBe("EXECUTION_ID_CONFLICT");
        const explicit = await rejection(sdk.trigger({ ...order, idempotent: false }));
        expect(explicit.code).toBe("EXECUTION_ID_CONFLICT");
        if (sdkKind === "http") expect(statuses()).toEqual([202, 409, 409]);
      });
    });
  }
}

test("idempotent: true without an executionId is a type error, and admission still rejects it", async () => {
  const router = {
    "charge-order": workflow<{ orderId: number }, string, EventContext>(async function* () {
      return "receipt";
    }),
  };
  const sdk = createLocalSdk<typeof router>(new MemoryRuntime());
  const ack = await sdk.trigger({
    workflowId: "charge-order",
    executionId: "order-42",
    params: { orderId: 42 },
    idempotent: true,
  });
  expect(ack).toEqual({ executionId: "order-42", status: "accepted" });

  const untyped = sdk.trigger(
    // @ts-expect-error idempotent: true requires executionId
    { workflowId: "charge-order", params: { orderId: 42 }, idempotent: true }
  );
  await expect(untyped).rejects.toMatchObject({ code: "IDEMPOTENT_TRIGGER_REQUIRES_ID" });
});

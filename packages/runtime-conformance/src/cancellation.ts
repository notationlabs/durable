import {
  DEFAULT_LEASE_DURATION,
  defineStore,
  redeliveryDelay,
  type ExecutionCancelRequest,
  type ExecutionLease,
  type Runtime,
  type StandardSchemaV1,
  type StoreWaiter,
} from "@yieldstar/core";
import { describe, expect, test } from "vitest";

export type CancellationConformanceHarnessOptions = {
  /** The suite's clock. Construct the runtime with it. */
  clock: () => number;
};

/** A clean runtime under test, built on the suite's clock. */
export type CancellationConformanceHarness = {
  runtime: Runtime;
  /**
   * Creates another runtime sharing the same backend, as another process
   * would. Cases that need one fall back to `runtime` when it is absent.
   */
  createPeer?(): Runtime | Promise<Runtime>;
  /** Opens the same durable backend in a new runtime, as after a restart. */
  restart?(): Runtime | Promise<Runtime>;
  /**
   * Makes writes to `executionId`'s record fail before they commit, until the
   * returned function is called. Without it, the propagation-failure case is skipped.
   */
  failExecutionWrites?(
    executionId: string
  ): (() => void | Promise<void>) | Promise<() => void | Promise<void>>;
  dispose?(): void | Promise<void>;
};

export type CancellationConformanceOptions = {
  name: string;
  create(
    options: CancellationConformanceHarnessOptions
  ): CancellationConformanceHarness | Promise<CancellationConformanceHarness>;
};

const START = 1_000_000;

const LEASE = DEFAULT_LEASE_DURATION;

/** How long after a claim an unacknowledged lease's execution is claimable again. */
const REDELIVER_AFTER = LEASE + redeliveryDelay(1);

type TestContext = {
  runtime: Runtime;
  peer: Runtime;
  harness: CancellationConformanceHarness;
  clock: { now: number };
  advance(ms: number): void;
};

type PaymentsState = { paid: boolean };

function schema<T>(): StandardSchemaV1<unknown, T> {
  return {
    "~standard": {
      version: 1,
      vendor: "yieldstar-cancellation-conformance",
      validate(value) {
        return { value: value as T };
      },
    },
  };
}

/**
 * Registers the runtime-adapter suite for `ExecutionClient.cancel`, its
 * propagation to descendants, the `detached` start option, and
 * `HeapClient.recordCancellation`.
 */
export function registerCancellationClientConformance({
  name,
  create,
}: CancellationConformanceOptions): void {
  const Payments = defineStore(`${name}-cancellation-conformance`, schema<PaymentsState>(), {
    initial: () => ({ paid: false }),
  });

  async function setup(): Promise<TestContext> {
    const clock = { now: START };
    const harness = await create({ clock: () => clock.now });
    const peer = (await harness.createPeer?.()) ?? harness.runtime;
    return {
      runtime: harness.runtime,
      peer,
      harness,
      clock,
      advance(ms) {
        clock.now += ms;
      },
    };
  }

  function it(
    title: string,
    fn: (ctx: TestContext, testContext: { skip(): void }) => Promise<void>
  ) {
    test(title, async (testContext) => {
      const ctx = await setup();
      try {
        await fn(ctx, testContext);
      } finally {
        await ctx.harness.dispose?.();
      }
    });
  }

  /** Like `it`, for adapters whose backend can be reopened after a crash. */
  function itAfterRestart(
    title: string,
    fn: (ctx: TestContext & { restart(): Promise<Runtime> }) => Promise<void>
  ) {
    test(title, async (testContext) => {
      const ctx = await setup();
      try {
        if (!ctx.harness.restart) {
          testContext.skip();
          return;
        }
        await fn({ ...ctx, restart: async () => ctx.harness.restart!() });
      } finally {
        await ctx.harness.dispose?.();
      }
    });
  }

  function claim(runtime: Runtime) {
    return runtime.executions.claimDue({ limit: 100, duration: LEASE });
  }

  async function claimOne(runtime: Runtime, executionId: string) {
    const leases = await claim(runtime);
    expect(leases.map((lease) => lease.executionId)).toEqual([executionId]);
    return leases[0];
  }

  async function claimedIds(runtime: Runtime) {
    return (await claim(runtime)).map((lease) => lease.executionId).sort();
  }

  async function admit(runtime: Runtime, executionId: string) {
    const admitted = await runtime.executions.admit({
      executionId,
      workflowId: "place-order",
      params: { executionId },
      context: new Map(),
    });
    expect(admitted).toEqual({ status: "accepted", executionId });
    return executionId;
  }

  async function admitSuspended(runtime: Runtime, executionId: string) {
    await admit(runtime, executionId);
    await suspend(runtime, await claimOne(runtime, executionId));
    return executionId;
  }

  async function suspend(runtime: Runtime, lease: ExecutionLease) {
    expect(
      await runtime.executions.acknowledge({ lease, outcome: { status: "suspended" } })
    ).toEqual({ status: "suspended" });
  }

  async function lease(runtime: Runtime, executionId: string) {
    await runtime.executions.requestWake({ executionId, wakeAt: START });
    return claimOne(runtime, executionId);
  }

  async function complete(runtime: Runtime, executionId: string, result: unknown) {
    return runtime.executions.acknowledge({
      lease: await lease(runtime, executionId),
      outcome: { status: "completed", result },
    });
  }

  /**
   * Starts a child from `caller`, then delivers the child once and suspends
   * it, so it is waiting rather than due. The caller's own state does not
   * change.
   */
  async function startChild(
    runtime: Runtime,
    caller: string,
    stepKey: string,
    options: { detached?: boolean; deliver?: boolean } = {}
  ) {
    const started = await runtime.executions.start({
      requestId: { executionId: caller, stepKey },
      workflowId: "fulfil-order",
      params: { stepKey },
      ...(options.detached !== undefined ? { detached: options.detached } : {}),
    });
    if (started.status !== "accepted") throw new Error(started.message);
    if (options.deliver !== false) {
      const leases = await runtime.executions.claimDue({
        limit: 1,
        duration: LEASE,
        filter: { executionIds: [started.executionId] },
      });
      if (leases.length === 1) await suspend(runtime, leases[0]);
    }
    return started.executionId;
  }

  async function cancellationOf(runtime: Runtime, executionId: string) {
    return (await runtime.executions.get(executionId))?.cancellation;
  }

  function cancel(runtime: Runtime, request: ExecutionCancelRequest) {
    return runtime.executions.cancel(request);
  }

  function storeWaiter(
    executionId: string,
    stepKey: string,
    snapshot: { instanceId: string; version: number }
  ): StoreWaiter {
    return {
      workflowId: "place-order",
      executionId,
      stepKey,
      storeName: Payments.name,
      storeId: executionId,
      instanceId: snapshot.instanceId,
      sinceVersion: snapshot.version,
      readPaths: [["paid"]],
    };
  }

  async function waitOnPayment(runtime: Runtime, executionId: string, stepKey: string) {
    const snapshot = await runtime.stores.getStore({ definition: Payments, id: executionId });
    await runtime.stores.registerWaiter(storeWaiter(executionId, stepKey, snapshot));
  }

  function markPaid(runtime: Runtime, executionId: string) {
    return runtime.stores.updateStore({
      definition: Payments,
      id: executionId,
      updater(draft) {
        draft.paid = true;
      },
    });
  }

  async function watchEffect(
    runtime: Runtime,
    effectId: string,
    executionId: string,
    stepKey: string
  ) {
    expect(
      await runtime.effects.begin({
        effectId,
        operation: "charge-card",
        input: { amount: 4999 },
        executionId,
        stepKey,
      })
    ).toEqual({ status: "new" });
    await runtime.effects.watch({ effectId, executionId, stepKey });
  }

  function settleEffect(runtime: Runtime, effectId: string) {
    return runtime.effects.settle({
      effectId,
      operation: "charge-card",
      status: "settled",
      result: { chargeId: "ch_1" },
    });
  }

  function join(runtime: Runtime, executionId: string, stepKey: string, target: string) {
    return runtime.joins.join({ targetExecutionId: target, executionId, stepKey });
  }

  function cancelledStep(stepKey: string, stepAttempt = 1) {
    return {
      stepKey,
      stepAttempt,
      stepResponseJson: JSON.stringify({ cancelled: stepKey }),
    };
  }

  describe(`${name} cancellation client conformance`, () => {
    describe("requests", () => {
      it("records a graceful request and makes a suspended execution due", async ({
        runtime,
        peer,
        clock,
      }) => {
        await admitSuspended(runtime, "order-42");
        expect(await claim(runtime)).toEqual([]);

        expect(await cancel(peer, { executionId: "order-42", reason: "abandoned" })).toEqual({
          status: "requested",
        });
        const record = await runtime.executions.get("order-42");
        expect(record).toMatchObject({ status: "suspended", updatedAt: clock.now });
        expect(record!.cancellation).toEqual({
          reason: "abandoned",
          forced: false,
          requestedAt: clock.now,
        });
        expect(await claimedIds(runtime)).toEqual(["order-42"]);
      });

      it("records a request without a reason on an admitted execution that never ran", async ({
        runtime,
        clock,
      }) => {
        await admit(runtime, "order-42");
        expect(await cancel(runtime, { executionId: "order-42" })).toEqual({
          status: "requested",
        });
        expect(await cancellationOf(runtime, "order-42")).toEqual({
          forced: false,
          requestedAt: clock.now,
        });
        expect(await claimedIds(runtime)).toEqual(["order-42"]);
      });

      it("records a graceful request during a live lease without ending it", async ({
        runtime,
        peer,
      }) => {
        await admitSuspended(runtime, "order-42");
        const holder = await lease(runtime, "order-42");

        expect(await cancel(peer, { executionId: "order-42", reason: "abandoned" })).toEqual({
          status: "requested",
        });
        expect(await runtime.executions.get("order-42")).toMatchObject({ status: "running" });
        expect(
          await runtime.heap.writeStep({
            lease: holder,
            stepKey: "reserve-stock",
            stepAttempt: 1,
            done: true,
            stepResponseJson: "{}",
          })
        ).toMatchObject({ meta: { attempt: 1, done: true } });
        expect(await claim(runtime)).toEqual([]);

        // The request's wake is due once the holder suspends.
        await suspend(runtime, holder);
        expect(await claimedIds(runtime)).toEqual(["order-42"]);
      });

      it("keeps the first reason and requestedAt when a request is repeated", async ({
        runtime,
        advance,
        clock,
      }) => {
        await admitSuspended(runtime, "order-42");
        await cancel(runtime, { executionId: "order-42", reason: "abandoned" });
        const requestedAt = clock.now;
        advance(1_000);
        expect(await cancel(runtime, { executionId: "order-42", reason: "fraud" })).toEqual({
          status: "requested",
        });
        expect(await cancellationOf(runtime, "order-42")).toEqual({
          reason: "abandoned",
          forced: false,
          requestedAt,
        });
      });

      it("leaves a terminal execution unchanged and returns its outcome", async ({
        runtime,
        advance,
      }) => {
        await admitSuspended(runtime, "completed-42");
        await complete(runtime, "completed-42", { shipped: true });
        await admit(runtime, "failed-42");
        await runtime.executions.acknowledge({
          lease: await claimOne(runtime, "failed-42"),
          outcome: { status: "failed", error: new Error("card declined") },
        });
        const before = {
          completed: await runtime.executions.get("completed-42"),
          failed: await runtime.executions.get("failed-42"),
        };
        advance(1_000);

        for (const force of [false, true]) {
          expect(
            await cancel(runtime, { executionId: "completed-42", reason: "late", force })
          ).toEqual({ status: "completed", result: { shipped: true } });
          const failed = await cancel(runtime, { executionId: "failed-42", force });
          expect(failed).toMatchObject({ status: "failed" });
          expect((failed as { error: Error }).error.message).toBe("card declined");
        }
        expect(await runtime.executions.get("completed-42")).toEqual(before.completed);
        expect(await runtime.executions.get("failed-42")).toEqual(before.failed);
        expect(await claim(runtime)).toEqual([]);
      });

      it("returns the cancelled outcome when a cancelled execution is cancelled again", async ({
        runtime,
      }) => {
        await admitSuspended(runtime, "order-42");
        await cancel(runtime, { executionId: "order-42", reason: "abandoned", force: true });
        const before = await runtime.executions.get("order-42");
        expect(
          await cancel(runtime, { executionId: "order-42", reason: "fraud", force: true })
        ).toEqual({ status: "cancelled", reason: "abandoned" });
        expect(await runtime.executions.get("order-42")).toEqual(before);
      });

      it("returns EXECUTION_NOT_FOUND for an unknown execution", async ({ runtime }) => {
        for (const force of [false, true]) {
          const result = await cancel(runtime, { executionId: "order-unknown", force });
          expect(result).toMatchObject({ status: "rejected", code: "EXECUTION_NOT_FOUND" });
          expect(typeof (result as { message: string }).message).toBe("string");
        }
        expect(await runtime.executions.get("order-unknown")).toBeUndefined();
      });

      const invalidRequests: [string, unknown][] = [
        ["an empty execution ID", { executionId: "" }],
        ["a non-string execution ID", { executionId: 42 }],
        ["a non-string reason", { executionId: "order-42", reason: 7 }],
        ["a non-boolean force", { executionId: "order-42", force: "yes" }],
      ];
      for (const [label, request] of invalidRequests) {
        it(`throws a TypeError and changes nothing for ${label}`, async ({ runtime }) => {
          await admitSuspended(runtime, "order-42");
          await expect(
            cancel(runtime, request as ExecutionCancelRequest)
          ).rejects.toBeInstanceOf(TypeError);
          expect(await runtime.executions.get("order-42")).not.toHaveProperty("cancellation");
          expect(await claim(runtime)).toEqual([]);
        });
      }
    });

    describe("forced cancellation", () => {
      it("records cancelled at once and fences out a live lease's holder", async ({
        runtime,
        peer,
        clock,
      }) => {
        await admitSuspended(runtime, "order-42");
        const holder = await lease(runtime, "order-42");

        expect(
          await cancel(peer, { executionId: "order-42", reason: "stuck", force: true })
        ).toEqual({ status: "cancelled", reason: "stuck" });
        const record = await runtime.executions.get("order-42");
        expect(record).toMatchObject({
          status: "cancelled",
          outcome: { status: "cancelled", reason: "stuck" },
          updatedAt: clock.now,
        });
        expect(record!.cancellation).toEqual({
          reason: "stuck",
          forced: true,
          requestedAt: clock.now,
        });

        expect(
          await runtime.heap.writeStep({
            lease: holder,
            stepKey: "release-stock",
            stepAttempt: 1,
            done: true,
            stepResponseJson: "{}",
          })
        ).toEqual({ status: "lost" });
        expect(
          await runtime.heap.recordCancellation({
            lease: holder,
            steps: [cancelledStep("wait-payment")],
          })
        ).toEqual({ status: "lost" });
        expect(await runtime.executions.renew({ lease: holder, duration: LEASE })).toEqual({
          status: "lost",
        });
        expect(await runtime.executions.release({ lease: holder })).toEqual({ status: "lost" });
        expect(
          await runtime.executions.acknowledge({
            lease: holder,
            outcome: { status: "completed", result: null },
          })
        ).toEqual({ status: "lost" });
        expect(
          await runtime.heap.readStep({ executionId: "order-42", stepKey: "release-stock" })
        ).toBeNull();
        expect(await runtime.executions.get("order-42")).toMatchObject({ status: "cancelled" });
      });

      it("clears the pending wake and ignores later wakes", async ({ runtime, advance }) => {
        await admitSuspended(runtime, "order-42");
        await runtime.executions.requestWake({ executionId: "order-42", wakeAt: START + 5_000 });
        await cancel(runtime, { executionId: "order-42", force: true });
        expect(
          await runtime.executions.requestWake({ executionId: "order-42", wakeAt: START })
        ).toEqual({ status: "ignored" });
        advance(REDELIVER_AFTER);
        expect(await claim(runtime)).toEqual([]);
      });

      it("accepts a forced request after a graceful one and keeps the first reason", async ({
        runtime,
        advance,
        clock,
      }) => {
        await admitSuspended(runtime, "order-42");
        await cancel(runtime, { executionId: "order-42", reason: "abandoned" });
        const requestedAt = clock.now;
        advance(1_000);

        expect(
          await cancel(runtime, { executionId: "order-42", reason: "stuck", force: true })
        ).toEqual({ status: "cancelled", reason: "abandoned" });
        const record = await runtime.executions.get("order-42");
        expect(record).toMatchObject({ status: "cancelled", updatedAt: clock.now });
        expect(record!.cancellation).toEqual({
          reason: "abandoned",
          forced: true,
          requestedAt,
        });
        expect(await claim(runtime)).toEqual([]);
      });

      it("wakes the join waiters on the cancelled execution, and a join returns the cancelled outcome", async ({
        runtime,
        peer,
      }) => {
        await admitSuspended(runtime, "fulfil-42");
        await admitSuspended(runtime, "order-42");
        await admitSuspended(runtime, "audit-42");
        await join(runtime, "order-42", "join:fulfil", "fulfil-42");
        await join(peer, "audit-42", "join:fulfil", "fulfil-42");

        await cancel(peer, { executionId: "fulfil-42", reason: "courier", force: true });
        expect(await claimedIds(runtime)).toEqual(["audit-42", "order-42"]);
        expect(await join(runtime, "order-42", "join:fulfil", "fulfil-42")).toEqual({
          status: "terminal",
          outcome: { status: "cancelled", reason: "courier" },
        });
      });

      it("deletes the store, effect and join waiters the cancelled execution holds", async ({
        runtime,
      }) => {
        await admitSuspended(runtime, "fulfil-42");
        await admitSuspended(runtime, "order-42");
        await waitOnPayment(runtime, "order-42", "wait-payment");
        await watchEffect(runtime, "charge:42", "order-42", "charge");
        await join(runtime, "order-42", "join:fulfil", "fulfil-42");

        await cancel(runtime, { executionId: "order-42", force: true });

        await markPaid(runtime, "order-42");
        await settleEffect(runtime, "charge:42");
        await complete(runtime, "fulfil-42", null);
        expect(await claim(runtime)).toEqual([]);
        // The effect record itself is not cancelled.
        expect(await runtime.effects.getEffect("charge:42")).toMatchObject({
          status: "settled",
        });
      });

      it("records the forced outcome and its join wakes together, or neither", async (
        { runtime, harness },
        testContext
      ) => {
        if (!harness.failExecutionWrites) {
          testContext.skip();
          return;
        }
        await admitSuspended(runtime, "fulfil-42");
        await admitSuspended(runtime, "order-42");
        await join(runtime, "order-42", "join:fulfil", "fulfil-42");

        const restore = await harness.failExecutionWrites("order-42");
        try {
          await expect(
            cancel(runtime, { executionId: "fulfil-42", force: true })
          ).rejects.toThrow();
        } finally {
          await restore();
        }
        expect(await runtime.executions.get("fulfil-42")).toMatchObject({
          status: "suspended",
        });
        expect(await runtime.executions.get("fulfil-42")).not.toHaveProperty("cancellation");
        expect(await claim(runtime)).toEqual([]);
      });
    });

    describe("propagation", () => {
      it("records a request on every non-terminal child and grandchild, with the root as source", async ({
        runtime,
        peer,
        clock,
      }) => {
        await admitSuspended(runtime, "order-42");
        const child = await startChild(runtime, "order-42", "start:fulfil");
        const sibling = await startChild(runtime, "order-42", "start:invoice");
        const grandchild = await startChild(runtime, child, "start:courier");
        expect(await claim(runtime)).toEqual([]);

        expect(await cancel(peer, { executionId: "order-42", reason: "abandoned" })).toEqual({
          status: "requested",
        });
        for (const descendant of [child, sibling, grandchild]) {
          expect(await cancellationOf(runtime, descendant)).toEqual({
            reason: "abandoned",
            source: "order-42",
            forced: false,
            requestedAt: clock.now,
          });
        }
        expect(await cancellationOf(runtime, "order-42")).not.toHaveProperty("source");
        expect(await claimedIds(runtime)).toEqual(
          ["order-42", child, sibling, grandchild].sort()
        );
      });

      it("force-cancels every non-terminal, non-detached descendant", async ({
        runtime,
        clock,
      }) => {
        await admitSuspended(runtime, "order-42");
        const child = await startChild(runtime, "order-42", "start:fulfil");
        const grandchild = await startChild(runtime, child, "start:courier");
        const childHolder = await lease(runtime, child);

        await cancel(runtime, { executionId: "order-42", reason: "stuck", force: true });
        for (const descendant of [child, grandchild]) {
          const record = await runtime.executions.get(descendant);
          expect(record).toMatchObject({
            status: "cancelled",
            outcome: { status: "cancelled", reason: "stuck", source: "order-42" },
          });
          expect(record!.cancellation).toEqual({
            reason: "stuck",
            source: "order-42",
            forced: true,
            requestedAt: clock.now,
          });
        }
        expect(
          await runtime.executions.acknowledge({
            lease: childHolder,
            outcome: { status: "suspended" },
          })
        ).toEqual({ status: "lost" });
        expect(await claim(runtime)).toEqual([]);
      });

      it("makes descendants forced when a graceful request becomes forced", async ({
        runtime,
      }) => {
        await admitSuspended(runtime, "order-42");
        const child = await startChild(runtime, "order-42", "start:fulfil");
        const grandchild = await startChild(runtime, child, "start:courier");
        await cancel(runtime, { executionId: "order-42", reason: "abandoned" });
        await cancel(runtime, { executionId: "order-42", force: true });

        for (const descendant of [child, grandchild]) {
          expect(await runtime.executions.get(descendant)).toMatchObject({
            status: "cancelled",
            outcome: { status: "cancelled", reason: "abandoned", source: "order-42" },
            cancellation: { reason: "abandoned", source: "order-42", forced: true },
          });
        }
      });

      it("records a detached child's parent as detached and sends it nothing", async ({
        runtime,
        peer,
      }) => {
        await admitSuspended(runtime, "order-42");
        const receipt = await startChild(runtime, "order-42", "start:receipt", {
          detached: true,
        });
        const attached = await startChild(runtime, "order-42", "start:fulfil", {
          detached: false,
        });
        const underReceipt = await startChild(runtime, receipt, "start:email");

        expect((await peer.executions.get(receipt))!.parent).toEqual({
          executionId: "order-42",
          stepKey: "start:receipt",
          detached: true,
        });
        expect((await peer.executions.get(attached))!.parent).toEqual({
          executionId: "order-42",
          stepKey: "start:fulfil",
          detached: false,
        });

        await cancel(runtime, { executionId: "order-42", reason: "abandoned" });
        await cancel(runtime, { executionId: "order-42", force: true });
        for (const executionId of [receipt, underReceipt]) {
          const record = await runtime.executions.get(executionId);
          expect(record).toMatchObject({ status: "suspended" });
          expect(record).not.toHaveProperty("cancellation");
        }
        expect(await runtime.executions.get(attached)).toMatchObject({ status: "cancelled" });
      });

      it("keeps a descendant's first reason and source, and still makes it forced", async ({
        runtime,
        advance,
      }) => {
        await admitSuspended(runtime, "order-42");
        const child = await startChild(runtime, "order-42", "start:fulfil");
        const grandchild = await startChild(runtime, child, "start:courier");
        await cancel(runtime, { executionId: child, reason: "courier unavailable" });
        const childRequestedAt = (await cancellationOf(runtime, child))!.requestedAt;
        advance(1_000);

        await cancel(runtime, { executionId: "order-42", reason: "abandoned" });
        expect(await cancellationOf(runtime, child)).toEqual({
          reason: "courier unavailable",
          forced: false,
          requestedAt: childRequestedAt,
        });
        expect(await cancellationOf(runtime, grandchild)).toEqual({
          reason: "courier unavailable",
          source: child,
          forced: false,
          requestedAt: childRequestedAt,
        });

        await cancel(runtime, { executionId: "order-42", force: true });
        expect(await runtime.executions.get(child)).toMatchObject({
          status: "cancelled",
          outcome: { status: "cancelled", reason: "courier unavailable" },
          cancellation: { reason: "courier unavailable", forced: true },
        });
        expect(await runtime.executions.get(grandchild)).toMatchObject({
          status: "cancelled",
          outcome: { status: "cancelled", reason: "courier unavailable", source: child },
          cancellation: { reason: "courier unavailable", source: child, forced: true },
        });
      });

      it("leaves terminal descendants unchanged and does not propagate through them", async ({
        runtime,
      }) => {
        await admitSuspended(runtime, "order-42");
        const child = await startChild(runtime, "order-42", "start:fulfil");
        const grandchild = await startChild(runtime, child, "start:courier");
        await complete(runtime, child, "fulfilled");
        const before = await runtime.executions.get(child);

        await cancel(runtime, { executionId: "order-42", force: true });
        expect(await runtime.executions.get(child)).toEqual(before);
        expect(await runtime.executions.get(grandchild)).not.toHaveProperty("cancellation");
      });

      it("records a request on a child started after the parent's request but before delivery", async ({
        runtime,
        clock,
      }) => {
        await admitSuspended(runtime, "order-42");
        await cancel(runtime, { executionId: "order-42", reason: "abandoned" });
        const holder = await claimOne(runtime, "order-42");

        const child = await startChild(runtime, "order-42", "start:fulfil", { deliver: false });
        const detached = await startChild(runtime, "order-42", "start:receipt", {
          deliver: false,
          detached: true,
        });
        expect(await runtime.executions.get(child)).toMatchObject({ status: "admitted" });
        expect(await cancellationOf(runtime, child)).toEqual({
          reason: "abandoned",
          source: "order-42",
          forced: false,
          requestedAt: clock.now,
        });
        expect(await runtime.executions.get(detached)).not.toHaveProperty("cancellation");
        await suspend(runtime, holder);
      });

      it("admits a child of a force-cancelled caller as cancelled", async ({ runtime }) => {
        await admitSuspended(runtime, "order-42");
        await cancel(runtime, { executionId: "order-42", reason: "stuck", force: true });

        // A start is not fenced, so a stale holder can still start a child.
        const child = await startChild(runtime, "order-42", "start:fulfil", { deliver: false });
        expect(await runtime.executions.get(child)).toMatchObject({
          status: "cancelled",
          outcome: { status: "cancelled", reason: "stuck", source: "order-42" },
          cancellation: { reason: "stuck", source: "order-42", forced: true },
        });
        expect(await claim(runtime)).toEqual([]);
      });

      it("records no request on a child started after the cancellation is delivered", async ({
        runtime,
      }) => {
        await admitSuspended(runtime, "order-42");
        await cancel(runtime, { executionId: "order-42", reason: "abandoned" });
        const holder = await claimOne(runtime, "order-42");
        await runtime.heap.recordCancellation({
          lease: holder,
          steps: [cancelledStep("wait-payment")],
        });

        const cleanup = await startChild(runtime, "order-42", "start:refund", {
          deliver: false,
        });
        expect(await runtime.executions.get(cleanup)).not.toHaveProperty("cancellation");
        await suspend(runtime, holder);
      });

      it("records the request on every descendant, or on none", async (
        { runtime, harness },
        testContext
      ) => {
        if (!harness.failExecutionWrites) {
          testContext.skip();
          return;
        }
        await admitSuspended(runtime, "order-42");
        const child = await startChild(runtime, "order-42", "start:fulfil");
        const grandchild = await startChild(runtime, child, "start:courier");

        const restore = await harness.failExecutionWrites(grandchild);
        try {
          await expect(
            cancel(runtime, { executionId: "order-42", reason: "abandoned" })
          ).rejects.toThrow();
        } finally {
          await restore();
        }
        for (const executionId of ["order-42", child, grandchild]) {
          expect(await runtime.executions.get(executionId)).not.toHaveProperty("cancellation");
        }
        expect(await claim(runtime)).toEqual([]);

        await cancel(runtime, { executionId: "order-42", reason: "abandoned" });
        expect(await cancellationOf(runtime, grandchild)).toMatchObject({
          source: "order-42",
        });
      });
    });

    describe("detached start requests", () => {
      it("compares detached on a repeated start request", async ({ runtime, peer }) => {
        await admitSuspended(runtime, "order-42");
        const request = {
          requestId: { executionId: "order-42", stepKey: "start:receipt" },
          workflowId: "send-receipt",
          params: { orderId: 42 },
        };
        const first = await runtime.executions.start({ ...request, detached: true });
        expect(first.status).toBe("accepted");
        expect(await peer.executions.start({ ...request, detached: true })).toEqual(first);
        for (const detached of [false, undefined]) {
          expect(await peer.executions.start({ ...request, detached })).toMatchObject({
            status: "rejected",
            code: "START_REQUEST_MISMATCH",
          });
        }

        const attached = { ...request, requestId: { ...request.requestId, stepKey: "start:fulfil" } };
        const second = await runtime.executions.start(attached);
        expect(await runtime.executions.start({ ...attached, detached: false })).toEqual(second);
        expect(await runtime.executions.start({ ...attached, detached: true })).toMatchObject({
          status: "rejected",
          code: "START_REQUEST_MISMATCH",
        });
      });

      it("rejects a non-boolean detached option", async ({ runtime }) => {
        await admitSuspended(runtime, "order-42");
        expect(
          await runtime.executions.start({
            requestId: { executionId: "order-42", stepKey: "start:receipt" },
            workflowId: "send-receipt",
            detached: "yes" as unknown as boolean,
          })
        ).toMatchObject({ status: "rejected" });
        expect(await claim(runtime)).toEqual([]);
      });
    });

    describe("recordCancellation", () => {
      it("writes each step's done record, returns them as stored, and sets deliveredAt", async ({
        runtime,
        peer,
        advance,
        clock,
      }) => {
        await admitSuspended(runtime, "order-42");
        await cancel(runtime, { executionId: "order-42", reason: "abandoned" });
        const requestedAt = clock.now;
        const holder = await claimOne(runtime, "order-42");
        // An equal record at the same attempt counts as written, as in writeStep.
        await runtime.heap.writeStep({
          lease: holder,
          stepKey: "branch-b",
          stepAttempt: 2,
          done: true,
          stepResponseJson: cancelledStep("branch-b", 2).stepResponseJson,
        });
        advance(500);

        expect(
          await peer.heap.recordCancellation({
            lease: holder,
            steps: [cancelledStep("branch-a"), cancelledStep("branch-b", 2)],
          })
        ).toEqual([
          {
            stepResponseJson: JSON.stringify({ cancelled: "branch-a" }),
            meta: { attempt: 1, done: true },
          },
          {
            stepResponseJson: JSON.stringify({ cancelled: "branch-b" }),
            meta: { attempt: 2, done: true },
          },
        ]);
        expect(
          await runtime.heap.readStep({ executionId: "order-42", stepKey: "branch-a" })
        ).toEqual({
          stepResponseJson: JSON.stringify({ cancelled: "branch-a" }),
          meta: { attempt: 1, done: true },
        });
        expect(await cancellationOf(runtime, "order-42")).toEqual({
          reason: "abandoned",
          forced: false,
          requestedAt,
          deliveredAt: clock.now,
        });
        expect(await runtime.executions.get("order-42")).toMatchObject({ status: "running" });
      });

      it("writes nothing and returns the stored records when a step has a different record", async ({
        runtime,
        peer,
      }) => {
        await admitSuspended(runtime, "order-42");
        await waitOnPayment(runtime, "order-42", "branch-a");
        await cancel(runtime, { executionId: "order-42", reason: "abandoned" });
        const holder = await claimOne(runtime, "order-42");
        await runtime.heap.writeStep({
          lease: holder,
          stepKey: "branch-b",
          stepAttempt: 2,
          done: false,
          stepResponseJson: JSON.stringify({ pending: true }),
        });

        expect(
          await peer.heap.recordCancellation({
            lease: holder,
            steps: [cancelledStep("branch-a"), cancelledStep("branch-b", 2)],
          })
        ).toEqual([
          null,
          {
            stepResponseJson: JSON.stringify({ pending: true }),
            meta: { attempt: 2, done: false },
          },
        ]);
        expect(
          await runtime.heap.readStep({ executionId: "order-42", stepKey: "branch-a" })
        ).toBeNull();
        expect(await cancellationOf(runtime, "order-42")).not.toHaveProperty("deliveredAt");
        await suspend(runtime, holder);
        await markPaid(peer, "order-42");
        expect(await claimedIds(runtime)).toEqual(["order-42"]);
      });

      it("writes the step records, deletes the waiters and sets deliveredAt together, or none of them", async (
        { runtime, harness },
        testContext
      ) => {
        if (!harness.failExecutionWrites) {
          testContext.skip();
          return;
        }
        await admitSuspended(runtime, "order-42");
        await waitOnPayment(runtime, "order-42", "wait-payment");
        await cancel(runtime, { executionId: "order-42", reason: "abandoned" });
        const holder = await claimOne(runtime, "order-42");

        const restore = await harness.failExecutionWrites("order-42");
        try {
          await expect(
            runtime.heap.recordCancellation({
              lease: holder,
              steps: [cancelledStep("wait-payment")],
            })
          ).rejects.toThrow();
        } finally {
          await restore();
        }
        expect(
          await runtime.heap.readStep({ executionId: "order-42", stepKey: "wait-payment" })
        ).toBeNull();
        expect(await cancellationOf(runtime, "order-42")).not.toHaveProperty("deliveredAt");
        await suspend(runtime, holder);
        await markPaid(runtime, "order-42");
        expect(await claimedIds(runtime)).toEqual(["order-42"]);
      });

      it("returns lost for a stale lease and writes nothing", async (ctx) => {
        const { runtime, peer } = ctx;
        await admitSuspended(runtime, "order-42");
        await cancel(runtime, { executionId: "order-42", reason: "abandoned" });
        const stale = await claimOne(runtime, "order-42");
        ctx.advance(REDELIVER_AFTER);
        const current = await claimOne(peer, "order-42");

        expect(
          await runtime.heap.recordCancellation({
            lease: stale,
            steps: [cancelledStep("wait-payment")],
          })
        ).toEqual({ status: "lost" });
        expect(
          await runtime.heap.readStep({ executionId: "order-42", stepKey: "wait-payment" })
        ).toBeNull();
        expect(await cancellationOf(runtime, "order-42")).not.toHaveProperty("deliveredAt");

        expect(
          await peer.heap.recordCancellation({
            lease: current,
            steps: [cancelledStep("wait-payment")],
          })
        ).toHaveLength(1);
        expect(await cancellationOf(runtime, "order-42")).toHaveProperty("deliveredAt");
      });

      it("returns lost after a forced cancel ends the delivery's lease", async ({ runtime }) => {
        await admitSuspended(runtime, "order-42");
        await cancel(runtime, { executionId: "order-42", reason: "abandoned" });
        const holder = await claimOne(runtime, "order-42");
        await cancel(runtime, { executionId: "order-42", force: true });

        expect(
          await runtime.heap.recordCancellation({
            lease: holder,
            steps: [cancelledStep("wait-payment")],
          })
        ).toEqual({ status: "lost" });
        expect(await cancellationOf(runtime, "order-42")).not.toHaveProperty("deliveredAt");
      });

      it("rejects and writes nothing when no request is recorded", async ({ runtime }) => {
        await admitSuspended(runtime, "order-42");
        const holder = await lease(runtime, "order-42");
        await expect(
          runtime.heap.recordCancellation({
            lease: holder,
            steps: [cancelledStep("wait-payment")],
          })
        ).rejects.toThrow();
        expect(
          await runtime.heap.readStep({ executionId: "order-42", stepKey: "wait-payment" })
        ).toBeNull();
        expect(await runtime.executions.get("order-42")).not.toHaveProperty("cancellation");
      });

      it("deletes the steps' store, effect and join waiters, so they no longer wake the execution", async ({
        runtime,
        peer,
      }) => {
        await admitSuspended(runtime, "fulfil-42");
        await admitSuspended(runtime, "order-42");
        await waitOnPayment(runtime, "order-42", "wait-payment");
        await watchEffect(runtime, "charge:42", "order-42", "charge");
        await join(runtime, "order-42", "join:fulfil", "fulfil-42");
        await cancel(runtime, { executionId: "order-42", reason: "abandoned" });
        const holder = await claimOne(runtime, "order-42");

        await peer.heap.recordCancellation({
          lease: holder,
          steps: [
            cancelledStep("wait-payment"),
            cancelledStep("charge"),
            cancelledStep("join:fulfil"),
          ],
        });
        await suspend(runtime, holder);

        await markPaid(peer, "order-42");
        expect(await claim(runtime)).toEqual([]);
        expect(await settleEffect(peer, "charge:42")).toMatchObject({ status: "settled" });
        expect(await claim(runtime)).toEqual([]);
        await complete(peer, "fulfil-42", null);
        expect(await claim(runtime)).toEqual([]);
      });

      it("keeps the waiters of steps it does not record", async ({ runtime }) => {
        await admitSuspended(runtime, "order-42");
        await waitOnPayment(runtime, "order-42", "wait-payment");
        await cancel(runtime, { executionId: "order-42", reason: "abandoned" });
        const holder = await claimOne(runtime, "order-42");

        await runtime.heap.recordCancellation({
          lease: holder,
          steps: [cancelledStep("other-step")],
        });
        await suspend(runtime, holder);
        await markPaid(runtime, "order-42");
        expect(await claimedIds(runtime)).toEqual(["order-42"]);
      });

      it("wakes the execution for a cleanup step's waiter registered after delivery", async ({
        runtime,
      }) => {
        await admitSuspended(runtime, "fulfil-42");
        await admitSuspended(runtime, "order-42");
        await waitOnPayment(runtime, "order-42", "wait-payment");
        await cancel(runtime, { executionId: "order-42", reason: "abandoned" });
        const holder = await claimOne(runtime, "order-42");
        await runtime.heap.recordCancellation({
          lease: holder,
          steps: [cancelledStep("wait-payment")],
        });

        await waitOnPayment(runtime, "order-42", "wait-refund");
        await join(runtime, "order-42", "join:refund", "fulfil-42");
        await suspend(runtime, holder);
        expect(await claim(runtime)).toEqual([]);

        await markPaid(runtime, "order-42");
        await suspend(runtime, await claimOne(runtime, "order-42"));
        await complete(runtime, "fulfil-42", null);
        expect(await claimedIds(runtime)).toEqual(["order-42"]);
      });
    });

    describe("durability", () => {
      itAfterRestart("keeps requests, delivery, detached parent links and start requests across a restart", async ({
        runtime,
        restart,
      }) => {
        await admitSuspended(runtime, "order-42");
        const child = await startChild(runtime, "order-42", "start:fulfil");
        const receipt = await startChild(runtime, "order-42", "start:receipt", {
          detached: true,
        });
        await cancel(runtime, { executionId: "order-42", reason: "abandoned" });
        const [holder] = await runtime.executions.claimDue({
          limit: 1,
          duration: LEASE,
          filter: { executionIds: ["order-42"] },
        });
        await runtime.heap.recordCancellation({
          lease: holder,
          steps: [cancelledStep("wait-payment")],
        });
        await suspend(runtime, holder);
        const before = {
          order: await runtime.executions.get("order-42"),
          child: await runtime.executions.get(child),
          receipt: await runtime.executions.get(receipt),
        };

        const restarted = await restart();
        expect(await restarted.executions.get("order-42")).toEqual(before.order);
        expect(await restarted.executions.get(child)).toEqual(before.child);
        expect(await restarted.executions.get(receipt)).toEqual(before.receipt);
        expect(before.order!.cancellation).toHaveProperty("deliveredAt");
        expect(before.child!.cancellation).toMatchObject({ source: "order-42" });
        expect(before.receipt!.parent).toMatchObject({ detached: true });
        expect(
          await restarted.executions.start({
            requestId: { executionId: "order-42", stepKey: "start:receipt" },
            workflowId: "fulfil-order",
            params: { stepKey: "start:receipt" },
          })
        ).toMatchObject({ status: "rejected", code: "START_REQUEST_MISMATCH" });
      });
    });
  });
}

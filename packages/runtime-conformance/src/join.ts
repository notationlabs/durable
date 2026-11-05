import {
  DEFAULT_LEASE_DURATION,
  createExecutionObserver,
  redeliveryDelay,
  registerErrorClass,
  toExecutionView,
  type ExecutionLease,
  type JoinWaiter,
  type Runtime,
} from "@yieldstar/core";
import { describe, expect, test } from "vitest";

export type JoinConformanceHarnessOptions = {
  /** The suite's clock. Construct the runtime with it. */
  clock: () => number;
};

/** A clean runtime under test, built on the suite's clock. */
export type JoinConformanceHarness = {
  runtime: Runtime;
  /**
   * Creates another runtime sharing the same backend, as another process
   * would. Race cases fall back to `runtime` when it is absent.
   */
  createPeer?(): Runtime | Promise<Runtime>;
  /** Opens the same durable backend in a new runtime, as after a restart. */
  restart?(): Runtime | Promise<Runtime>;
  /**
   * Makes every write to `executionId`'s record on `runtime` fail after the
   * surrounding write begins but before it commits, until the returned
   * function is called. Without it, the case for a crash during a terminal
   * acknowledgement is skipped.
   */
  failExecutionWrites?(
    executionId: string
  ): (() => void | Promise<void>) | Promise<() => void | Promise<void>>;
  dispose?(): void | Promise<void>;
};

export type JoinConformanceOptions = {
  name: string;
  create(
    options: JoinConformanceHarnessOptions
  ): JoinConformanceHarness | Promise<JoinConformanceHarness>;
};

const START = 1_000_000;

const LEASE = DEFAULT_LEASE_DURATION;

/** How long after a claim an unacknowledged lease's execution is claimable again. */
const REDELIVER_AFTER = LEASE + redeliveryDelay(1);

type TestContext = {
  runtime: Runtime;
  peer: Runtime;
  harness: JoinConformanceHarness;
  clock: { now: number };
  advance(ms: number): void;
};

class CourierRejectedError extends Error {
  code = "ADDRESS_REJECTED";
  constructor() {
    super("Courier rejected the address");
    this.name = "ConformanceCourierRejectedError";
  }
}
registerErrorClass("ConformanceCourierRejectedError", CourierRejectedError);

/**
 * Registers the runtime-adapter suite for `JoinClient`, the wakes a terminal
 * acknowledgement requests for join waiters, and the deletion of a waiter
 * with its step's result.
 */
export function registerJoinClientConformance({
  name,
  create,
}: JoinConformanceOptions): void {
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

  async function admit(runtime: Runtime, executionId: string, workflowId = "ship-order") {
    const admitted = await runtime.executions.admit({
      executionId,
      workflowId,
      params: { executionId },
      context: new Map(),
    });
    expect(admitted).toEqual({ status: "accepted", executionId });
    return executionId;
  }

  /** Admits an execution and acknowledges its admission delivery as suspended. */
  async function admitSuspended(runtime: Runtime, executionId: string) {
    await admit(runtime, executionId);
    const lease = await claimOne(runtime, executionId);
    await runtime.executions.acknowledge({ lease, outcome: { status: "suspended" } });
    return executionId;
  }

  /** Wakes a suspended execution and claims it. */
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

  async function failWith(runtime: Runtime, executionId: string, error: Error) {
    return runtime.executions.acknowledge({
      lease: await lease(runtime, executionId),
      outcome: { status: "failed", error },
    });
  }

  function waiter(executionId: string, stepKey = "join:ship", target = "ship-42"): JoinWaiter {
    return { targetExecutionId: target, executionId, stepKey };
  }

  function join(runtime: Runtime, executionId: string, stepKey?: string, target?: string) {
    return runtime.joins.join(waiter(executionId, stepKey, target));
  }

  /** Records the join step's result under `lease`, as the driver does. */
  function recordStep(
    runtime: Runtime,
    lease: ExecutionLease,
    options: { stepKey?: string; attempt?: number; done?: boolean } = {}
  ) {
    return runtime.heap.writeStep({
      lease,
      stepKey: options.stepKey ?? "join:ship",
      stepAttempt: options.attempt ?? 1,
      done: options.done ?? true,
      stepResponseJson: JSON.stringify({ trackingNumber: "TRK-1" }),
    });
  }

  describe(`${name} join client conformance`, () => {
    describe("join", () => {
      it("returns a completed target's outcome and registers nothing", async ({
        runtime,
        peer,
      }) => {
        await admitSuspended(runtime, "ship-42");
        await complete(runtime, "ship-42", { trackingNumber: "TRK-1" });
        await admitSuspended(runtime, "charge-42");

        expect(await join(peer, "charge-42")).toEqual({
          status: "terminal",
          outcome: { status: "completed", result: { trackingNumber: "TRK-1" } },
        });
        expect(await runtime.joins.getWaiters("ship-42")).toEqual([]);
        expect(await claim(runtime)).toEqual([]);
      });

      it("returns a failed target's recorded error and registers nothing", async ({
        runtime,
      }) => {
        await admitSuspended(runtime, "ship-42");
        await failWith(runtime, "ship-42", new CourierRejectedError());
        await admitSuspended(runtime, "charge-42");

        const result = await join(runtime, "charge-42");
        expect(result).toMatchObject({ status: "terminal", outcome: { status: "failed" } });
        const { error } = (result as { outcome: { error: Error } }).outcome;
        expect(error).toBeInstanceOf(CourierRejectedError);
        expect(error).toMatchObject({
          name: "ConformanceCourierRejectedError",
          message: "Courier rejected the address",
        });
        expect(await runtime.joins.getWaiters("ship-42")).toEqual([]);
      });

      it("returns not-found for an unknown target and registers nothing", async ({
        runtime,
      }) => {
        await admitSuspended(runtime, "charge-42");
        expect(await join(runtime, "charge-42", "join:ship", "ship-unknown")).toEqual({
          status: "not-found",
        });
        expect(await runtime.joins.getWaiters("ship-unknown")).toEqual([]);

        // A target admitted later does not wake the joiner.
        await admitSuspended(runtime, "ship-unknown");
        await complete(runtime, "ship-unknown", null);
        expect(await claim(runtime)).toEqual([]);
      });

      it("registers a waiter on an admitted, running or suspended target, and requests no wake", async ({
        runtime,
        peer,
      }) => {
        await admitSuspended(runtime, "ship-running");
        const running = await lease(runtime, "ship-running");
        await admitSuspended(runtime, "ship-suspended");
        await admitSuspended(runtime, "charge-42");
        await admit(runtime, "ship-admitted");

        for (const target of ["ship-admitted", "ship-running", "ship-suspended"]) {
          expect(await join(peer, "charge-42", "join:ship", target)).toEqual({
            status: "waiting",
          });
          expect(await runtime.joins.getWaiters(target)).toEqual([
            waiter("charge-42", "join:ship", target),
          ]);
        }
        expect(await claimedIds(runtime)).toEqual(["ship-admitted"]);
        await runtime.executions.acknowledge({
          lease: running,
          outcome: { status: "suspended" },
        });
      });

      it("keeps one registration when a join is repeated", async ({ runtime, peer }) => {
        await admitSuspended(runtime, "ship-42");
        await admitSuspended(runtime, "charge-42");
        await join(runtime, "charge-42");
        await join(peer, "charge-42");
        await join(runtime, "charge-42");
        expect(await runtime.joins.getWaiters("ship-42")).toEqual([waiter("charge-42")]);
      });

      it("returns waiters that callers cannot use to change the registration", async ({
        runtime,
      }) => {
        await admitSuspended(runtime, "ship-42");
        await admitSuspended(runtime, "charge-42");
        await join(runtime, "charge-42");
        const [registered] = await runtime.joins.getWaiters("ship-42");
        registered.executionId = "changed";
        expect(await runtime.joins.getWaiters("ship-42")).toEqual([waiter("charge-42")]);
      });
    });

    describe("terminal acknowledgement", () => {
      it("wakes every join waiter on the target and keeps them registered", async ({
        runtime,
        peer,
      }) => {
        await admitSuspended(runtime, "ship-42");
        await admitSuspended(runtime, "charge-42");
        await admitSuspended(runtime, "audit-42");
        await join(runtime, "charge-42");
        await join(peer, "audit-42", "join:audit");

        await complete(peer, "ship-42", { trackingNumber: "TRK-1" });
        expect(await claimedIds(runtime)).toEqual(["audit-42", "charge-42"]);
        expect(await runtime.joins.getWaiters("ship-42")).toEqual([
          waiter("audit-42", "join:audit"),
          waiter("charge-42"),
        ]);

        // Both joiners now see the same outcome.
        for (const joiner of ["charge-42", "audit-42"]) {
          expect(await join(peer, joiner)).toEqual({
            status: "terminal",
            outcome: { status: "completed", result: { trackingNumber: "TRK-1" } },
          });
        }
      });

      it("wakes join waiters when the target fails", async ({ runtime }) => {
        await admitSuspended(runtime, "ship-42");
        await admitSuspended(runtime, "charge-42");
        await join(runtime, "charge-42");
        await failWith(runtime, "ship-42", new CourierRejectedError());
        expect(await claimedIds(runtime)).toEqual(["charge-42"]);
      });

      it("does not wake join waiters when the target suspends", async ({ runtime }) => {
        await admitSuspended(runtime, "ship-42");
        await admitSuspended(runtime, "charge-42");
        await join(runtime, "charge-42");
        const target = await lease(runtime, "ship-42");
        await runtime.executions.acknowledge({
          lease: target,
          outcome: { status: "suspended" },
        });
        expect(await claim(runtime)).toEqual([]);
      });

      it("wakes a joiner that registered during its own delivery, after it suspends", async ({
        runtime,
        peer,
      }) => {
        await admitSuspended(runtime, "ship-42");
        await admitSuspended(runtime, "charge-42");
        const joiner = await lease(runtime, "charge-42");
        await join(runtime, "charge-42");
        await recordStep(runtime, joiner, { done: false });

        await complete(peer, "ship-42", null);
        await runtime.executions.acknowledge({
          lease: joiner,
          outcome: { status: "suspended" },
        });
        expect(await claimedIds(runtime)).toEqual(["charge-42"]);
      });

      it("wakes the joiner, or returns the outcome, when a join races with the terminal acknowledgement", async ({
        runtime,
        peer,
      }) => {
        for (let i = 0; i < 10; i++) {
          const target = `ship-race-${i}`;
          const joiner = `charge-race-${i}`;
          await admitSuspended(runtime, target);
          await admitSuspended(runtime, joiner);
          const targetLease = await lease(runtime, target);
          const joining = join(i % 2 ? runtime : peer, joiner, "join:ship", target);
          const completing = (i % 2 ? peer : runtime).executions.acknowledge({
            lease: targetLease,
            outcome: { status: "completed", result: i },
          });
          const result =
            i % 4 < 2
              ? (await Promise.all([joining, completing]))[0]
              : (await Promise.all([completing, joining]))[1];

          if (result.status === "terminal") {
            expect(result.outcome).toEqual({ status: "completed", result: i });
            expect(await runtime.joins.getWaiters(target)).toEqual([]);
            expect(await claim(runtime)).toEqual([]);
          } else {
            expect(result).toEqual({ status: "waiting" });
            expect(await claimedIds(runtime)).toEqual([joiner]);
          }
        }
      });

      it("ignores a join waiter whose joiner is already terminal", async ({ runtime }) => {
        await admitSuspended(runtime, "ship-42");
        await admitSuspended(runtime, "charge-42");
        await join(runtime, "charge-42");
        await complete(runtime, "charge-42", null);

        expect(await complete(runtime, "ship-42", null)).toEqual({
          status: "completed",
          result: null,
        });
        expect(await claim(runtime)).toEqual([]);
        expect(await runtime.executions.get("charge-42")).toMatchObject({
          status: "completed",
        });
      });

      it("records the outcome and every joiner's wake together, or neither", async (
        { runtime, harness },
        testContext
      ) => {
        if (!harness.failExecutionWrites) {
          testContext.skip();
          return;
        }
        await admitSuspended(runtime, "ship-42");
        await admitSuspended(runtime, "charge-42");
        await admitSuspended(runtime, "audit-42");
        await join(runtime, "charge-42");
        await join(runtime, "audit-42", "join:audit");
        const target = await lease(runtime, "ship-42");

        const restore = await harness.failExecutionWrites("charge-42");
        try {
          await expect(
            runtime.executions.acknowledge({
              lease: target,
              outcome: { status: "completed", result: null },
            })
          ).rejects.toThrow();
        } finally {
          await restore();
        }
        expect(await runtime.executions.get("ship-42")).toMatchObject({
          status: "running",
        });
        expect(await claim(runtime)).toEqual([]);

        expect(
          await runtime.executions.acknowledge({
            lease: target,
            outcome: { status: "completed", result: null },
          })
        ).toEqual({ status: "completed", result: null });
        expect(await claimedIds(runtime)).toEqual(["audit-42", "charge-42"]);
      });
    });

    describe("waiter deletion", () => {
      it("deletes a waiter in the write that records its step's result, and keeps it for an unfinished write", async ({
        runtime,
      }) => {
        await admitSuspended(runtime, "ship-42");
        await admitSuspended(runtime, "charge-42");
        await join(runtime, "charge-42", "join:ship");
        await join(runtime, "charge-42", "join:other");

        const unfinished = await lease(runtime, "charge-42");
        await recordStep(runtime, unfinished, { attempt: 1, done: false });
        await runtime.executions.acknowledge({
          lease: unfinished,
          outcome: { status: "suspended" },
        });
        expect(await runtime.joins.getWaiters("ship-42")).toEqual([
          waiter("charge-42", "join:other"),
          waiter("charge-42", "join:ship"),
        ]);

        const done = await lease(runtime, "charge-42");
        await recordStep(runtime, done, { attempt: 2 });
        await runtime.executions.acknowledge({ lease: done, outcome: { status: "suspended" } });
        // Another step's waiter is unaffected.
        expect(await runtime.joins.getWaiters("ship-42")).toEqual([
          waiter("charge-42", "join:other"),
        ]);
      });

      it("keeps the waiter when a stale holder's step result is rejected", async (ctx) => {
        const { runtime, peer } = ctx;
        await admitSuspended(runtime, "ship-42");
        await admitSuspended(runtime, "charge-42");
        await join(runtime, "charge-42");
        await runtime.executions.requestWake({ executionId: "charge-42", wakeAt: START });
        const stale = await claimOne(runtime, "charge-42");
        ctx.advance(REDELIVER_AFTER);
        const current = await claimOne(peer, "charge-42");

        expect(await recordStep(runtime, stale)).toEqual({ status: "lost" });
        await peer.executions.acknowledge({ lease: current, outcome: { status: "suspended" } });
        expect(await runtime.joins.getWaiters("ship-42")).toEqual([waiter("charge-42")]);

        await complete(runtime, "ship-42", null);
        expect(await claimedIds(runtime)).toEqual(["charge-42"]);
      });

      it("causes at most one extra delivery when a stale holder registers after the result is recorded", async (ctx) => {
        const { runtime, peer } = ctx;
        await admitSuspended(runtime, "ship-42");
        await admitSuspended(runtime, "charge-42");
        await runtime.executions.requestWake({ executionId: "charge-42", wakeAt: START });
        await claimOne(runtime, "charge-42");
        ctx.advance(REDELIVER_AFTER);
        const current = await claimOne(peer, "charge-42");

        // The current holder records the join step while the target still
        // runs, as a timed join does when its deadline passes.
        expect(await join(peer, "charge-42")).toEqual({ status: "waiting" });
        await recordStep(peer, current);
        await peer.executions.acknowledge({ lease: current, outcome: { status: "suspended" } });
        expect(await runtime.joins.getWaiters("ship-42")).toEqual([]);

        // The stale holder's join is not fenced, so it registers again.
        expect(await join(runtime, "charge-42")).toEqual({ status: "waiting" });
        await complete(runtime, "ship-42", { trackingNumber: "TRK-2" });

        const extra = await claimOne(runtime, "charge-42");
        await runtime.executions.acknowledge({ lease: extra, outcome: { status: "suspended" } });
        expect(await claim(runtime)).toEqual([]);
        expect(
          await runtime.heap.readStep({ executionId: "charge-42", stepKey: "join:ship" })
        ).toEqual({
          stepResponseJson: JSON.stringify({ trackingNumber: "TRK-1" }),
          meta: { attempt: 1, done: true },
        });
        expect(await runtime.executions.get("ship-42")).toMatchObject({
          status: "completed",
          outcome: { status: "completed", result: { trackingNumber: "TRK-2" } },
        });
      });
    });

    describe("parent link", () => {
      it("shows the parent on a started child's record and view, and none on an admitted execution", async ({
        runtime,
        peer,
      }) => {
        await admitSuspended(runtime, "charge-42");
        const started = await runtime.executions.start({
          requestId: { executionId: "charge-42", stepKey: "call:ship" },
          workflowId: "ship-order",
          params: { orderId: 42 },
        });
        if (started.status !== "accepted") throw new Error(started.message);
        const parent = { executionId: "charge-42", stepKey: "call:ship" };

        const record = await peer.executions.get(started.executionId);
        expect(record!.parent).toEqual(parent);
        expect(toExecutionView(record!).parent).toEqual(parent);
        const observer = createExecutionObserver(peer.executions);
        expect((await observer.poll(started.executionId))!.parent).toEqual(parent);
        expect(await observer.poll("charge-42")).not.toHaveProperty("parent");

        // A joiner waiting on its child is woken when the child finishes.
        await join(runtime, "charge-42", "call:ship", started.executionId);
        await complete(peer, started.executionId, "booked");
        expect(await claimedIds(runtime)).toEqual(["charge-42"]);
        expect((await observer.poll(started.executionId))!.parent).toEqual(parent);
      });
    });

    describe("durability", () => {
      itAfterRestart("delivers a joiner's wake after a crash between the terminal acknowledgement and the delivery", async ({
        runtime,
        restart,
      }) => {
        await admitSuspended(runtime, "ship-42");
        await admitSuspended(runtime, "charge-42");
        await join(runtime, "charge-42");
        await complete(runtime, "ship-42", { trackingNumber: "TRK-1" });

        const restarted = await restart();
        expect(await claimedIds(restarted)).toEqual(["charge-42"]);
        expect(await restarted.joins.getWaiters("ship-42")).toEqual([waiter("charge-42")]);
      });

      itAfterRestart("keeps join waiters across a restart", async ({ runtime, restart }) => {
        await admitSuspended(runtime, "ship-42");
        await admitSuspended(runtime, "charge-42");
        await join(runtime, "charge-42");

        const restarted = await restart();
        expect(await restarted.joins.getWaiters("ship-42")).toEqual([waiter("charge-42")]);
        await complete(restarted, "ship-42", null);
        expect(await claimedIds(restarted)).toEqual(["charge-42"]);
      });
    });
  });
}

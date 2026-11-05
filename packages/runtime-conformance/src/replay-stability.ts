import {
  DEFAULT_LEASE_DURATION,
  ExecutionListRequestError,
  redeliveryDelay,
  type ExecutionLease,
  type ExecutionListRequest,
  type HeapStep,
  type Runtime,
  type WorkflowRunOutcome,
} from "@yieldstar/core";
import { describe, expect, test } from "vitest";
import { findLiveRecord } from "./records";

export type ReplayStabilityConformanceHarnessOptions = {
  /** The suite's clock. Construct the runtime with it. */
  clock: () => number;
};

/** A clean runtime under test, built on the suite's clock. */
export type ReplayStabilityConformanceHarness = {
  runtime: Runtime;
  /**
   * Creates another runtime sharing the same backend, as another process
   * would. Cases that need one fall back to `runtime` when it is absent.
   */
  createPeer?(): Runtime | Promise<Runtime>;
  /** Opens the same durable backend in a new runtime, as after a restart. */
  restart?(): Runtime | Promise<Runtime>;
  dispose?(): void | Promise<void>;
};

export type ReplayStabilityConformanceOptions = {
  name: string;
  create(
    options: ReplayStabilityConformanceHarnessOptions
  ): ReplayStabilityConformanceHarness | Promise<ReplayStabilityConformanceHarness>;
};

const START = 1_000_000;

const LEASE = DEFAULT_LEASE_DURATION;

/** Long enough for any wake, lease expiry or redelivery delay in these cases to pass. */
const LONG_AFTER = 10 * (LEASE + redeliveryDelay(30));

type TestContext = {
  runtime: Runtime;
  peer: Runtime;
  harness: ReplayStabilityConformanceHarness;
  clock: { now: number };
  advance(ms: number): void;
};

/**
 * Registers the runtime-adapter suite for replay stability: `HeapClient.listSteps`,
 * step kinds, the recorded workflow version, the `diverged` acknowledgement,
 * `ExecutionClient.clearDivergence` and `list({ diverged })`.
 */
export function registerReplayStabilityClientConformance({
  name,
  create,
}: ReplayStabilityConformanceOptions): void {
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

  function it(title: string, fn: (ctx: TestContext) => Promise<void>) {
    test(title, async () => {
      const ctx = await setup();
      try {
        await fn(ctx);
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

  /** Admits an execution and acknowledges its admission delivery as suspended. */
  async function admitSuspended(runtime: Runtime, executionId: string) {
    await admit(runtime, executionId);
    await acknowledge(runtime, await claimOne(runtime, executionId), { status: "suspended" });
    return executionId;
  }

  /** Wakes a suspended execution and claims it. */
  async function lease(runtime: Runtime, executionId: string) {
    await runtime.executions.requestWake({ executionId, wakeAt: runtime.clock() });
    return claimOne(runtime, executionId);
  }

  function acknowledge(runtime: Runtime, lease: ExecutionLease, outcome: WorkflowRunOutcome) {
    return runtime.executions.acknowledge({ lease, outcome });
  }

  /** Leases a suspended execution and acknowledges the delivery as diverged. */
  async function diverge(
    runtime: Runtime,
    executionId: string,
    stepKey = "charge-v3"
  ) {
    const holder = await lease(runtime, executionId);
    expect(
      await acknowledge(runtime, holder, {
        status: "diverged",
        code: "NONDETERMINISTIC_REPLAY",
        stepKey,
      })
    ).toEqual({ status: "diverged" });
    return holder;
  }

  function writeStep(
    runtime: Runtime,
    lease: ExecutionLease,
    stepKey: string,
    options: { attempt?: number; done?: boolean; kind?: string; workflowVersion?: number } = {}
  ) {
    return runtime.heap.writeStep({
      lease,
      stepKey,
      stepAttempt: options.attempt ?? 1,
      done: options.done ?? true,
      stepResponseJson: JSON.stringify({ stepKey }),
      ...(options.kind !== undefined ? { stepKind: options.kind } : {}),
      ...(options.workflowVersion !== undefined
        ? { workflowVersion: options.workflowVersion }
        : {}),
    });
  }

  async function steps(runtime: Runtime, executionId: string): Promise<HeapStep[]> {
    const listed = await runtime.heap.listSteps({ executionId });
    return [...listed].sort((a, b) => a.stepKey.localeCompare(b.stepKey));
  }

  async function versionOf(runtime: Runtime, executionId: string) {
    return (await findLiveRecord(runtime.executions, executionId))?.workflowVersion;
  }

  async function divergenceOf(runtime: Runtime, executionId: string) {
    return (await findLiveRecord(runtime.executions, executionId))?.divergence;
  }

  async function listIds(runtime: Runtime, request: ExecutionListRequest) {
    return (await runtime.executions.list(request)).executions.map(
      (record) => record.executionId
    );
  }

  describe(`${name} replay stability client conformance`, () => {
    describe("step listing", () => {
      it("lists every recorded step key with the kind of its latest attempt", async ({
        runtime,
        peer,
      }) => {
        await admit(runtime, "order-42");
        const holder = await claimOne(runtime, "order-42");
        await writeStep(runtime, holder, "reserve-stock", { kind: "run" });
        await writeStep(runtime, holder, "wait-courier", { kind: "when", done: false });
        await writeStep(runtime, holder, "charge", { kind: "run", attempt: 1, done: false });
        await writeStep(runtime, holder, "charge", { kind: "effect", attempt: 2 });
        await writeStep(runtime, holder, "legacy");

        expect(await steps(peer, "order-42")).toEqual([
          { stepKey: "charge", kind: "effect" },
          { stepKey: "legacy" },
          { stepKey: "reserve-stock", kind: "run" },
          { stepKey: "wait-courier", kind: "when" },
        ]);
      });

      it("stores the kind on the record as meta.kind, and omits it when none was given", async ({
        runtime,
        peer,
      }) => {
        await admit(runtime, "order-42");
        const holder = await claimOne(runtime, "order-42");
        expect(await writeStep(runtime, holder, "reserve-stock", { kind: "run" })).toEqual({
          stepResponseJson: JSON.stringify({ stepKey: "reserve-stock" }),
          meta: { attempt: 1, done: true, kind: "run" },
        });
        await writeStep(runtime, holder, "legacy");

        expect(
          await peer.heap.readStep({ executionId: "order-42", stepKey: "reserve-stock" })
        ).toEqual({
          stepResponseJson: JSON.stringify({ stepKey: "reserve-stock" }),
          meta: { attempt: 1, done: true, kind: "run" },
        });
        const legacy = await peer.heap.readStep({ executionId: "order-42", stepKey: "legacy" });
        expect(legacy!.meta).toEqual({ attempt: 1, done: true });
      });

      it("returns an empty list for an execution without records or an unknown execution, and lists only the execution's own steps", async ({
        runtime,
      }) => {
        await admit(runtime, "order-4");
        await admit(runtime, "order-42");
        expect(await runtime.heap.listSteps({ executionId: "order-4" })).toEqual([]);
        expect(await runtime.heap.listSteps({ executionId: "order-unknown" })).toEqual([]);

        const [first] = await runtime.executions.claimDue({
          limit: 1,
          duration: LEASE,
          filter: { executionIds: ["order-42"] },
        });
        await writeStep(runtime, first, "reserve-stock", { kind: "run" });
        expect(await runtime.heap.listSteps({ executionId: "order-4" })).toEqual([]);
        expect(await steps(runtime, "order-42")).toEqual([
          { stepKey: "reserve-stock", kind: "run" },
        ]);
      });

      it("lists nothing for a write a stale lease made", async ({ runtime }) => {
        await admit(runtime, "order-42");
        const holder = await claimOne(runtime, "order-42");
        expect(await runtime.executions.release({ lease: holder })).toEqual({
          status: "released",
        });
        expect(await writeStep(runtime, holder, "reserve-stock", { kind: "run" })).toEqual({
          status: "lost",
        });
        expect(await runtime.heap.listSteps({ executionId: "order-42" })).toEqual([]);
      });

      it("records the kind of each step recordCancellation writes", async ({ runtime, peer }) => {
        await admitSuspended(runtime, "order-42");
        await runtime.executions.cancel({ executionId: "order-42", reason: "abandoned" });
        const holder = await claimOne(runtime, "order-42");
        const recorded = await runtime.heap.recordCancellation({
          lease: holder,
          steps: [
            {
              stepKey: "wait-courier",
              stepAttempt: 1,
              stepResponseJson: "{}",
              stepKind: "when",
            },
            { stepKey: "charge", stepAttempt: 1, stepResponseJson: "{}" },
          ],
        });
        expect(recorded).toEqual([
          { stepResponseJson: "{}", meta: { attempt: 1, done: true, kind: "when" } },
          { stepResponseJson: "{}", meta: { attempt: 1, done: true } },
        ]);
        expect(await steps(peer, "order-42")).toEqual([
          { stepKey: "charge" },
          { stepKey: "wait-courier", kind: "when" },
        ]);
      });
    });

    describe("workflow version", () => {
      it("records the version with the first fenced step write", async ({ runtime, peer }) => {
        await admit(runtime, "order-42");
        expect(await runtime.executions.get("order-42")).not.toHaveProperty("workflowVersion");
        const holder = await claimOne(runtime, "order-42");
        expect(await runtime.executions.get("order-42")).not.toHaveProperty("workflowVersion");

        await writeStep(runtime, holder, "reserve-stock", { kind: "run", workflowVersion: 2 });
        expect(await versionOf(peer, "order-42")).toBe(2);
        const [listed] = (await peer.executions.list()).executions;
        expect(listed).toMatchObject({ executionId: "order-42", workflowVersion: 2 });
      });

      it("never overwrites a recorded version", async ({ runtime, advance }) => {
        await admit(runtime, "order-42");
        const first = await claimOne(runtime, "order-42");
        await writeStep(runtime, first, "reserve-stock", { workflowVersion: 2 });
        await writeStep(runtime, first, "charge", { workflowVersion: 3 });
        expect(
          await acknowledge(runtime, first, { status: "suspended", workflowVersion: 3 })
        ).toEqual({ status: "suspended" });
        expect(await versionOf(runtime, "order-42")).toBe(2);

        advance(1_000);
        const second = await lease(runtime, "order-42");
        await writeStep(runtime, second, "ship", { workflowVersion: 4 });
        expect(
          await acknowledge(runtime, second, {
            status: "completed",
            result: null,
            workflowVersion: 4,
          })
        ).toEqual({ status: "completed", result: null });
        expect(await versionOf(runtime, "order-42")).toBe(2);
      });

      it("records no version for a write without one, and records a later write's", async ({
        runtime,
      }) => {
        await admit(runtime, "order-42");
        const holder = await claimOne(runtime, "order-42");
        await writeStep(runtime, holder, "reserve-stock");
        expect(await runtime.executions.get("order-42")).not.toHaveProperty("workflowVersion");
        await writeStep(runtime, holder, "charge", { workflowVersion: 3 });
        expect(await versionOf(runtime, "order-42")).toBe(3);
      });

      it("records no version for a write rejected as lost", async ({ runtime }) => {
        await admit(runtime, "order-42");
        const holder = await claimOne(runtime, "order-42");
        await runtime.executions.release({ lease: holder });
        expect(await writeStep(runtime, holder, "reserve-stock", { workflowVersion: 2 })).toEqual(
          { status: "lost" }
        );
        expect(await runtime.executions.get("order-42")).not.toHaveProperty("workflowVersion");
      });

      it("records the version with an acknowledgement when no write did", async ({ runtime }) => {
        await admit(runtime, "order-42");
        expect(
          await acknowledge(runtime, await claimOne(runtime, "order-42"), {
            status: "suspended",
            workflowVersion: 2,
          })
        ).toEqual({ status: "suspended" });
        expect(await versionOf(runtime, "order-42")).toBe(2);

        await admit(runtime, "order-43");
        expect(
          await acknowledge(runtime, await claimOne(runtime, "order-43"), {
            status: "completed",
            result: { shipped: true },
            workflowVersion: 3,
          })
        ).toEqual({ status: "completed", result: { shipped: true } });
        expect(await runtime.executions.get("order-43")).toMatchObject({
          status: "completed",
          workflowVersion: 3,
        });
      });

      it("records the version with recordCancellation", async ({ runtime }) => {
        await admitSuspended(runtime, "order-42");
        await runtime.executions.cancel({ executionId: "order-42" });
        const holder = await claimOne(runtime, "order-42");
        await runtime.heap.recordCancellation({
          lease: holder,
          steps: [{ stepKey: "wait-courier", stepAttempt: 1, stepResponseJson: "{}" }],
          workflowVersion: 2,
        });
        expect(await versionOf(runtime, "order-42")).toBe(2);
      });

      it("lets the next delivery record its version when a delivery ended before any fenced write", async ({
        runtime,
        advance,
      }) => {
        await admit(runtime, "order-42");
        await runtime.executions.release({ lease: await claimOne(runtime, "order-42") });
        expect(await runtime.executions.get("order-42")).not.toHaveProperty("workflowVersion");

        advance(redeliveryDelay(1));
        const second = await claimOne(runtime, "order-42");
        await writeStep(runtime, second, "reserve-stock", { workflowVersion: 3 });
        expect(await versionOf(runtime, "order-42")).toBe(3);
      });
    });

    describe("diverged acknowledgement", () => {
      it("records divergence, ends the lease and keeps a suspended execution suspended", async ({
        runtime,
        peer,
        clock,
        advance,
      }) => {
        await admit(runtime, "order-42");
        const first = await claimOne(runtime, "order-42");
        await writeStep(runtime, first, "charge", { kind: "run", workflowVersion: 2 });
        await acknowledge(runtime, first, { status: "suspended" });
        advance(1_000);

        const holder = await lease(runtime, "order-42");
        expect(
          await acknowledge(runtime, holder, {
            status: "diverged",
            code: "NONDETERMINISTIC_REPLAY",
            stepKey: "charge-v3",
            workflowVersion: 3,
          })
        ).toEqual({ status: "diverged" });

        const record = await findLiveRecord(peer.executions, "order-42");
        expect(record).toMatchObject({
          status: "suspended",
          updatedAt: clock.now,
          unacknowledgedDeliveryCount: 0,
          workflowVersion: 2,
        });
        expect(record!.outcome).toBeUndefined();
        expect(record!.divergence).toEqual({
          code: "NONDETERMINISTIC_REPLAY",
          stepKey: "charge-v3",
          workflowVersion: 2,
          detectedAt: clock.now,
        });

        expect(await writeStep(runtime, holder, "charge-v3")).toEqual({ status: "lost" });
        expect(await runtime.executions.renew({ lease: holder, duration: LEASE })).toEqual({
          status: "lost",
        });
        expect(await runtime.executions.release({ lease: holder })).toEqual({ status: "lost" });
        expect(
          await acknowledge(runtime, holder, { status: "completed", result: null })
        ).toEqual({ status: "lost" });
        expect(await runtime.heap.readStep({ executionId: "order-42", stepKey: "charge-v3" }))
          .toBeNull();
      });

      it("keeps an admitted execution admitted, and defaults the divergence's version to 1", async ({
        runtime,
        clock,
      }) => {
        await admit(runtime, "order-42");
        expect(
          await acknowledge(runtime, await claimOne(runtime, "order-42"), {
            status: "diverged",
            code: "STEP_SCHEMA_CHANGED",
          })
        ).toEqual({ status: "diverged" });
        const record = await findLiveRecord(runtime.executions, "order-42");
        expect(record).toMatchObject({ status: "admitted" });
        expect(record).not.toHaveProperty("workflowVersion");
        expect(record!.divergence).toEqual({
          code: "STEP_SCHEMA_CHANGED",
          workflowVersion: 1,
          detectedAt: clock.now,
        });
      });

      it("takes the version from the outcome when the record has none, and records it", async ({
        runtime,
      }) => {
        await admit(runtime, "order-42");
        await acknowledge(runtime, await claimOne(runtime, "order-42"), {
          status: "diverged",
          code: "NONDETERMINISTIC_REPLAY",
          stepKey: "charge-v3",
          workflowVersion: 3,
        });
        expect(await runtime.executions.get("order-42")).toMatchObject({
          workflowVersion: 3,
          divergence: { workflowVersion: 3 },
        });
      });

      it("does not deliver a diverged execution, and keeps the wakes it receives pending", async ({
        runtime,
        peer,
        advance,
      }) => {
        await admitSuspended(runtime, "order-42");
        const holder = await lease(runtime, "order-42");
        // A wake requested during the delivery is not covered by its lease.
        await runtime.executions.requestWake({ executionId: "order-42", wakeAt: START });
        await acknowledge(runtime, holder, {
          status: "diverged",
          code: "NONDETERMINISTIC_REPLAY",
          stepKey: "charge-v3",
        });
        expect(await claim(runtime)).toEqual([]);

        expect(
          await peer.executions.requestWake({ executionId: "order-42", wakeAt: START })
        ).toEqual({ status: "requested" });
        advance(LONG_AFTER);
        expect(await claim(runtime)).toEqual([]);
        expect(await claim(peer)).toEqual([]);
        expect(await runtime.executions.get("order-42")).toMatchObject({ status: "suspended" });

        // The pending wakes deliver it once its divergence is cleared.
        expect(await runtime.executions.clearDivergence("order-42")).toEqual({
          status: "cleared",
        });
        expect((await claim(runtime)).map((lease) => lease.executionId)).toEqual(["order-42"]);
      });

      it("does not count a diverged delivery as unacknowledged", async ({ runtime, advance }) => {
        await admitSuspended(runtime, "order-42");
        await runtime.executions.release({ lease: await lease(runtime, "order-42") });
        expect(await runtime.executions.get("order-42")).toMatchObject({
          unacknowledgedDeliveryCount: 1,
        });

        advance(redeliveryDelay(1));
        const holder = await claimOne(runtime, "order-42");
        await acknowledge(runtime, holder, {
          status: "diverged",
          code: "NONDETERMINISTIC_REPLAY",
        });
        expect(await runtime.executions.get("order-42")).toMatchObject({
          unacknowledgedDeliveryCount: 1,
        });
      });

      it("returns lost for a stale lease and records no divergence", async ({ runtime }) => {
        await admitSuspended(runtime, "order-42");
        const holder = await lease(runtime, "order-42");
        await runtime.executions.release({ lease: holder });
        expect(
          await acknowledge(runtime, holder, {
            status: "diverged",
            code: "NONDETERMINISTIC_REPLAY",
          })
        ).toEqual({ status: "lost" });
        expect(await runtime.executions.get("order-42")).not.toHaveProperty("divergence");
      });

      it("records a graceful cancel request on a diverged execution without delivering it", async ({
        runtime,
        advance,
      }) => {
        await admitSuspended(runtime, "order-42");
        await diverge(runtime, "order-42");
        expect(
          await runtime.executions.cancel({ executionId: "order-42", reason: "abandoned" })
        ).toEqual({ status: "requested" });
        advance(LONG_AFTER);
        expect(await claim(runtime)).toEqual([]);
        expect(await runtime.executions.get("order-42")).toMatchObject({
          status: "suspended",
          cancellation: { reason: "abandoned", forced: false },
          divergence: { code: "NONDETERMINISTIC_REPLAY" },
        });
      });

      it("lets a forced cancel end a diverged execution and clear its divergence", async ({
        runtime,
      }) => {
        await admitSuspended(runtime, "order-42");
        await diverge(runtime, "order-42");
        expect(
          await runtime.executions.cancel({ executionId: "order-42", reason: "stuck", force: true })
        ).toEqual({ status: "cancelled", reason: "stuck" });
        const record = await runtime.executions.get("order-42");
        expect(record).toMatchObject({ status: "cancelled" });
        expect(record).not.toHaveProperty("divergence");
        expect(await claim(runtime)).toEqual([]);
      });
    });

    describe("clearDivergence", () => {
      it("removes the divergence and makes the execution due now", async ({
        runtime,
        peer,
        clock,
        advance,
      }) => {
        await admitSuspended(runtime, "order-42");
        const diverged = await diverge(runtime, "order-42");
        advance(1_000);

        expect(await peer.executions.clearDivergence("order-42")).toEqual({ status: "cleared" });
        const record = await runtime.executions.get("order-42");
        expect(record).not.toHaveProperty("divergence");
        expect(record).toMatchObject({ status: "suspended", updatedAt: clock.now });

        const next = await claimOne(runtime, "order-42");
        expect(next.token).toBeGreaterThan(diverged.token);
      });

      it("lets the next delivery diverge again", async ({ runtime, clock, advance }) => {
        await admitSuspended(runtime, "order-42");
        await diverge(runtime, "order-42");
        advance(1_000);
        await runtime.executions.clearDivergence("order-42");
        await acknowledge(runtime, await claimOne(runtime, "order-42"), {
          status: "diverged",
          code: "NONDETERMINISTIC_REPLAY",
          stepKey: "charge-v4",
        });
        expect(await divergenceOf(runtime, "order-42")).toEqual({
          code: "NONDETERMINISTIC_REPLAY",
          stepKey: "charge-v4",
          workflowVersion: 1,
          detectedAt: clock.now,
        });
        expect(await claim(runtime)).toEqual([]);
      });

      it("lets the delivery after a clear complete the execution", async ({ runtime }) => {
        await admitSuspended(runtime, "order-42");
        await diverge(runtime, "order-42");
        await runtime.executions.clearDivergence("order-42");
        expect(
          await acknowledge(runtime, await claimOne(runtime, "order-42"), {
            status: "completed",
            result: { shipped: true },
          })
        ).toEqual({ status: "completed", result: { shipped: true } });
        const record = await runtime.executions.get("order-42");
        expect(record).toMatchObject({ status: "completed" });
        expect(record).not.toHaveProperty("divergence");
      });

      it("returns not-diverged and changes nothing for an execution without a divergence", async ({
        runtime,
        advance,
      }) => {
        await admitSuspended(runtime, "suspended-42");
        await admit(runtime, "completed-42");
        await acknowledge(runtime, await claimOne(runtime, "completed-42"), {
          status: "completed",
          result: null,
        });
        const before = {
          suspended: await runtime.executions.get("suspended-42"),
          completed: await runtime.executions.get("completed-42"),
        };
        advance(1_000);

        expect(await runtime.executions.clearDivergence("suspended-42")).toEqual({
          status: "not-diverged",
        });
        expect(await runtime.executions.clearDivergence("completed-42")).toEqual({
          status: "not-diverged",
        });
        expect(await runtime.executions.get("suspended-42")).toEqual(before.suspended);
        expect(await runtime.executions.get("completed-42")).toEqual(before.completed);
        expect(await claim(runtime)).toEqual([]);
      });

      it("returns not-diverged when the divergence was already cleared", async ({ runtime }) => {
        await admitSuspended(runtime, "order-42");
        await diverge(runtime, "order-42");
        expect(await runtime.executions.clearDivergence("order-42")).toEqual({
          status: "cleared",
        });
        expect(await runtime.executions.clearDivergence("order-42")).toEqual({
          status: "not-diverged",
        });
      });

      it("returns EXECUTION_NOT_FOUND for an unknown execution", async ({ runtime }) => {
        const result = await runtime.executions.clearDivergence("order-unknown");
        expect(result).toMatchObject({ status: "rejected", code: "EXECUTION_NOT_FOUND" });
        expect(typeof (result as { message: string }).message).toBe("string");
        expect(await runtime.executions.get("order-unknown")).toBeUndefined();
      });
    });

    describe("list", () => {
      it("filters on divergence", async ({ runtime, peer }) => {
        await admitSuspended(runtime, "a");
        await admitSuspended(runtime, "b");
        await admitSuspended(runtime, "c");
        await diverge(runtime, "a");
        await diverge(runtime, "c");
        await admit(runtime, "d");

        expect(await listIds(peer, { diverged: true })).toEqual(["c", "a"]);
        expect(await listIds(peer, { diverged: false })).toEqual(["d", "b"]);
        expect(await listIds(peer, { diverged: true, status: "suspended" })).toEqual(["c", "a"]);
        expect(await listIds(peer, { diverged: false, status: "suspended" })).toEqual(["b"]);

        const page = await peer.executions.list({ diverged: true, limit: 1 });
        expect(page.executions.map((record) => record.executionId)).toEqual(["c"]);
        expect(await listIds(peer, { diverged: true, cursor: page.cursor })).toEqual(["a"]);

        await runtime.executions.clearDivergence("a");
        expect(await listIds(peer, { diverged: true })).toEqual(["c"]);
      });

      const invalidRequests: [string, unknown][] = [
        ["a string diverged", { diverged: "true" }],
        ["a numeric diverged", { diverged: 1 }],
        ["a null diverged", { diverged: null }],
      ];
      for (const [label, request] of invalidRequests) {
        it(`throws ExecutionListRequestError for ${label}`, async ({ runtime }) => {
          await admit(runtime, "a");
          const error = await runtime.executions
            .list(request as ExecutionListRequest)
            .then(
              () => undefined,
              (err: unknown) => err
            );
          expect(error).toBeInstanceOf(ExecutionListRequestError);
          expect(error).toMatchObject({ code: "INVALID_LIST_REQUEST" });
        });
      }
    });

    describe("durability", () => {
      itAfterRestart(
        "keeps step kinds, the version and the divergence across a restart, and does not deliver the execution",
        async ({ runtime, restart, advance }) => {
          await admit(runtime, "order-42");
          const first = await claimOne(runtime, "order-42");
          await writeStep(runtime, first, "charge", { kind: "run", workflowVersion: 2 });
          await acknowledge(runtime, first, { status: "suspended" });
          await diverge(runtime, "order-42");
          const before = await runtime.executions.get("order-42");

          const restarted = await restart();
          expect(await restarted.executions.get("order-42")).toEqual(before);
          expect(before).toMatchObject({
            workflowVersion: 2,
            divergence: { code: "NONDETERMINISTIC_REPLAY", stepKey: "charge-v3" },
          });
          expect(await restarted.heap.listSteps({ executionId: "order-42" })).toEqual([
            { stepKey: "charge", kind: "run" },
          ]);
          await restarted.executions.requestWake({ executionId: "order-42", wakeAt: START });
          advance(LONG_AFTER);
          expect(await claim(restarted)).toEqual([]);

          expect(await restarted.executions.clearDivergence("order-42")).toEqual({
            status: "cleared",
          });
          expect((await claim(restarted)).map((lease) => lease.executionId)).toEqual([
            "order-42",
          ]);
        }
      );
    });
  });
}

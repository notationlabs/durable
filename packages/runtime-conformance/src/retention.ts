import {
  DEFAULT_LEASE_DURATION,
  defineStore,
  type ExecutionLease,
  type JoinWaiter,
  type PrunedExecutionRecord,
  type RetentionOptions,
  type Runtime,
  type StandardSchemaV1,
  type WorkflowRunOutcome,
} from "@yieldstar/core";
import { describe, expect, test } from "vitest";
import { liveRecord } from "./records";

export type RetentionConformanceHarnessOptions = {
  /** The suite's clock. Construct the runtime with it. */
  clock: () => number;
  /**
   * The case's retention options, or undefined for a runtime without a
   * policy. Construct the runtime, and every peer and restarted runtime, with
   * them.
   */
  retain?: RetentionOptions;
};

/** A clean runtime under test, built on the suite's clock and retention options. */
export type RetentionConformanceHarness = {
  runtime: Runtime;
  /**
   * Creates another runtime sharing the same backend, as another process
   * would. Race cases fall back to `runtime` when it is absent.
   */
  createPeer?(): Runtime | Promise<Runtime>;
  /** Opens the same durable backend in a new runtime, as after a restart. */
  restart?(): Runtime | Promise<Runtime>;
  dispose?(): void | Promise<void>;
};

export type RetentionConformanceOptions = {
  name: string;
  create(
    options: RetentionConformanceHarnessOptions
  ): RetentionConformanceHarness | Promise<RetentionConformanceHarness>;
};

const DAY = 86_400_000;

const START = 1_000 * DAY;

const LEASE = DEFAULT_LEASE_DURATION;

const ORDER = "order-42";

const RETAIN: RetentionOptions = {
  default: { completed: 7 * DAY, failed: 30 * DAY, cancelled: 30 * DAY },
};

/** Prunes every terminal execution at once. */
const RETAIN_NONE: RetentionOptions = {
  default: { completed: 0, failed: 0, cancelled: 0 },
};

function schema<T>(): StandardSchemaV1<unknown, T> {
  return {
    "~standard": {
      version: 1,
      vendor: "yieldstar-conformance",
      validate(value) {
        return { value: value as T };
      },
    },
  };
}

const OrderStore = defineStore("retention-orders", schema<{ paid?: boolean }>(), {
  initial: () => ({}),
});

const InvoiceStore = defineStore("retention-invoices", schema<{ sent?: boolean }>(), {
  initial: () => ({}),
});

const LedgerStore = defineStore("retention-ledger", schema<{ total?: number }>(), {
  initial: () => ({}),
});

/** The step records `runOrder` writes, by key, with their kinds. */
const ORDER_STEPS = [
  { stepKey: "reserve", kind: "run" },
  { stepKey: "charge", kind: "run" },
  { stepKey: "items", kind: "all" },
  { stepKey: "fastest-courier", kind: "race" },
  { stepKey: "$$patch$$:courier-v2", kind: "patched" },
];

type TestContext = {
  runtime: Runtime;
  peer: Runtime;
  harness: RetentionConformanceHarness;
  clock: { now: number };
  advance(ms: number): void;
};

/**
 * Registers the runtime-adapter suite for retention: `ExecutionClient.prune`,
 * `listPendingPrunes` and `finishPrune`, the cleanup methods of the heap,
 * store, effect and join clients, and `Runtime.prune` over them.
 */
export function registerRetentionClientConformance({
  name,
  create,
}: RetentionConformanceOptions): void {
  async function setup(retain: RetentionOptions | undefined): Promise<TestContext> {
    const clock = { now: START };
    const harness = await create({ clock: () => clock.now, retain });
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
    retain: RetentionOptions | undefined,
    fn: (ctx: TestContext) => Promise<void>
  ) {
    test(title, async () => {
      const ctx = await setup(retain);
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
    retain: RetentionOptions | undefined,
    fn: (ctx: TestContext & { restart(): Promise<Runtime> }) => Promise<void>
  ) {
    test(title, async (testContext) => {
      const ctx = await setup(retain);
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

  async function admit(runtime: Runtime, executionId: string, workflowId = "place-order") {
    const admitted = await runtime.executions.admit({
      executionId,
      workflowId,
      params: { executionId },
      context: new Map(),
    });
    expect(admitted).toEqual({ status: "accepted", executionId });
    return executionId;
  }

  async function claim(runtime: Runtime, executionId: string, duration = LEASE) {
    const leases = await runtime.executions.claimDue({
      limit: 10,
      duration,
      filter: { executionIds: [executionId] },
    });
    expect(leases.map((lease) => lease.executionId)).toEqual([executionId]);
    return leases[0];
  }

  async function claimedIds(runtime: Runtime) {
    const leases = await runtime.executions.claimDue({ limit: 100, duration: LEASE });
    return leases.map((lease) => lease.executionId).sort();
  }

  async function acknowledge(
    runtime: Runtime,
    lease: ExecutionLease,
    outcome: WorkflowRunOutcome
  ) {
    expect(await runtime.executions.acknowledge({ lease, outcome })).not.toEqual({
      status: "lost",
    });
  }

  /** Admits an execution and acknowledges its admission delivery as suspended. */
  async function admitSuspended(
    runtime: Runtime,
    executionId: string,
    workflowId = "place-order"
  ) {
    await admit(runtime, executionId, workflowId);
    await acknowledge(runtime, await claim(runtime, executionId), { status: "suspended" });
    return executionId;
  }

  /** Wakes a suspended execution and claims it. */
  async function wake(runtime: Runtime, executionId: string) {
    await runtime.executions.requestWake({ executionId, wakeAt: 0 });
    return claim(runtime, executionId);
  }

  /** Admits an execution and completes its first delivery. */
  async function runToCompletion(
    runtime: Runtime,
    executionId: string,
    workflowId = "place-order"
  ) {
    await admit(runtime, executionId, workflowId);
    await acknowledge(runtime, await claim(runtime, executionId), {
      status: "completed",
      result: { executionId },
    });
    return executionId;
  }

  function writeStep(
    runtime: Runtime,
    lease: ExecutionLease,
    stepKey: string,
    options: { kind?: string; attempt?: number; done?: boolean; json?: string } = {}
  ) {
    return runtime.heap.writeStep({
      lease,
      stepKey,
      stepAttempt: options.attempt ?? 1,
      done: options.done ?? true,
      stepResponseJson: options.json ?? JSON.stringify({ type: "step-result", result: 1 }),
      ...(options.kind !== undefined ? { stepKind: options.kind } : {}),
    });
  }

  function updateLedger(runtime: Runtime, total: number) {
    return runtime.stores.updateStore({
      definition: LedgerStore,
      id: "shared",
      updater: (draft) => {
        draft.total = total;
      },
      stepId: { executionId: ORDER, stepKey: "ledger" },
    });
  }

  function startShipment(runtime: Runtime, workflowId = "ship-order") {
    return runtime.executions.start({
      requestId: { executionId: ORDER, stepKey: "start:ship" },
      workflowId,
      params: { orderId: 42 },
    });
  }

  /**
   * Runs order-42 to completion with step records of several kinds (two
   * attempts of one, a combinator decision of `step.all` and of `step.race`
   * and a patch marker), a store receipt on a shared store and on its own
   * stores, a started child and an effect it watches.
   */
  async function runOrder(runtime: Runtime) {
    await admit(runtime, ORDER);
    const lease = await claim(runtime, ORDER);
    await writeStep(runtime, lease, "reserve", { kind: "run" });
    await writeStep(runtime, lease, "charge", { kind: "run", done: false });
    await writeStep(runtime, lease, "charge", { kind: "run", attempt: 2 });
    await writeStep(runtime, lease, "items", {
      kind: "all",
      json: JSON.stringify({ decision: "fulfilled", type: "branches-decision" }),
    });
    await writeStep(runtime, lease, "fastest-courier", {
      kind: "race",
      json: JSON.stringify({ decision: "won", index: 0, type: "branches-decision" }),
    });
    await writeStep(runtime, lease, "$$patch$$:courier-v2", { kind: "patched" });
    await updateLedger(runtime, 1);
    await runtime.stores.updateStore({
      definition: OrderStore,
      id: ORDER,
      updater: (draft) => {
        draft.paid = true;
      },
      stepId: { executionId: ORDER, stepKey: "pay" },
    });
    await runtime.stores.updateStore({
      definition: InvoiceStore,
      id: ORDER,
      updater: (draft) => {
        draft.sent = true;
      },
      stepId: { executionId: ORDER, stepKey: "invoice" },
    });
    const started = await startShipment(runtime);
    if (started.status !== "accepted") throw new Error(started.message);
    await runtime.effects.begin({
      effectId: "charge-42",
      operation: "charge",
      input: { amount: 1 },
      executionId: ORDER,
      stepKey: "charge-card",
    });
    await runtime.effects.watch({
      effectId: "charge-42",
      executionId: ORDER,
      stepKey: "charge-card",
    });
    await acknowledge(runtime, lease, { status: "completed", result: "shipped" });
    return { child: started.executionId };
  }

  function tombstone(
    executionId: string,
    prunedAt: number,
    terminalStatus: PrunedExecutionRecord["terminalStatus"] = "completed",
    workflowId = "place-order"
  ): PrunedExecutionRecord {
    return { status: "pruned", executionId, workflowId, terminalStatus, prunedAt };
  }

  async function expectPruned(runtime: Runtime, ...executionIds: string[]) {
    for (const executionId of executionIds) {
      expect(await runtime.executions.get(executionId)).toMatchObject({
        status: "pruned",
        executionId,
      });
    }
  }

  async function expectNotPruned(runtime: Runtime, ...executionIds: string[]) {
    for (const executionId of executionIds) await liveRecord(runtime.executions, executionId);
  }

  async function listedIds(runtime: Runtime, status?: "pruned" | "completed") {
    const page = await runtime.executions.list({ limit: 100, ...(status ? { status } : {}) });
    return page.executions.map((execution) => execution.executionId).sort();
  }

  function joinWaiter(executionId: string, target: string, stepKey = `join:${target}`): JoinWaiter {
    return { targetExecutionId: target, executionId, stepKey };
  }

  describe(`${name} retention client conformance`, () => {
    describe("pruning", () => {
      it("prunes a completed execution once its period has passed, and keeps its tombstone", RETAIN, async ({
        runtime,
        advance,
        clock,
      }) => {
        await runOrder(runtime);
        advance(7 * DAY - 1);
        expect(await runtime.prune()).toEqual({ pruned: 0, cleaned: 0, failed: 0, errors: [] });
        expect(await liveRecord(runtime.executions, ORDER)).toMatchObject({ status: "completed" });

        advance(1);
        expect(await runtime.prune()).toEqual({ pruned: 1, cleaned: 1, failed: 0, errors: [] });
        expect(await runtime.executions.get(ORDER)).toEqual(tombstone(ORDER, clock.now));
        expect(await runtime.executions.listPendingPrunes({ limit: 10 })).toEqual([]);

        // The tombstone is kept for good, and a later pass changes nothing.
        advance(1_000 * DAY);
        expect(await runtime.prune()).toEqual({ pruned: 0, cleaned: 0, failed: 0, errors: [] });
        expect(await runtime.executions.get(ORDER)).toEqual(
          tombstone(ORDER, clock.now - 1_000 * DAY)
        );
      });

      it("deletes every step record, including combinator records, patch markers and earlier attempts", RETAIN_NONE, async ({
        runtime,
      }) => {
        await runOrder(runtime);
        const listed = await runtime.heap.listSteps({ executionId: ORDER });
        expect([...listed].sort((a, b) => (a.stepKey < b.stepKey ? -1 : 1))).toEqual(
          [...ORDER_STEPS].sort((a, b) => (a.stepKey < b.stepKey ? -1 : 1))
        );

        await runtime.prune();
        expect(await runtime.heap.listSteps({ executionId: ORDER })).toEqual([]);
        for (const { stepKey } of ORDER_STEPS) {
          expect(await runtime.heap.readStep({ executionId: ORDER, stepKey })).toBeNull();
        }
      });

      it("deletes the execution's store receipts, and rejects a later commit with its step ID", RETAIN_NONE, async ({
        runtime,
        peer,
      }) => {
        await runOrder(runtime);
        expect((await updateLedger(runtime, 2)).state).toEqual({ total: 1 });

        // Before cleanup the receipt answers the repeat with its recorded result.
        await runtime.executions.prune({ retain: RETAIN_NONE, limit: 10 });
        expect((await updateLedger(runtime, 2)).state).toEqual({ total: 1 });

        // After cleanup there is no receipt, and the commit is rejected, as a
        // stale lease holder's would be.
        await runtime.prune();
        await expect(updateLedger(runtime, 2)).rejects.toThrow(/was pruned/);
        await expect(updateLedger(peer, 2)).rejects.toThrow(/was pruned/);
        const shared = await runtime.stores.getStore({ definition: LedgerStore, id: "shared" });
        expect(shared.state).toEqual({ total: 1 });
        await expect(
          runtime.stores.deleteStoreFrom({
            definition: LedgerStore,
            id: "shared",
            snapshot: shared,
            stepId: { executionId: ORDER, stepKey: "delete-ledger" },
          })
        ).rejects.toThrow(/was pruned/);
        expect(await runtime.stores.listStores(LedgerStore)).toEqual(["shared"]);

        // Commits without a step ID, or with another execution's, still apply.
        await runtime.stores.updateStore({
          definition: LedgerStore,
          id: "shared",
          updater: (draft) => {
            draft.total = 3;
          },
        });
        await admit(runtime, "billing-42");
        const updated = await runtime.stores.updateStore({
          definition: LedgerStore,
          id: "shared",
          updater: (draft) => {
            draft.total = 4;
          },
          stepId: { executionId: "billing-42", stepKey: "ledger" },
        });
        expect(updated.state).toEqual({ total: 4 });
      });

      it("deletes the start requests the execution owns as the caller", RETAIN_NONE, async ({
        runtime,
      }) => {
        const { child } = await runOrder(runtime);
        expect(await startShipment(runtime)).toEqual({ status: "accepted", executionId: child });
        expect(await startShipment(runtime, "other-workflow")).toMatchObject({
          status: "rejected",
          code: "START_REQUEST_MISMATCH",
        });

        await runtime.prune();
        await expect(startShipment(runtime)).rejects.toThrow(/was pruned/);
        await expect(startShipment(runtime, "other-workflow")).rejects.toThrow(/was pruned/);
      });

      it("rejects a start from a pruned caller before cleanup deletes its start requests", RETAIN_NONE, async ({
        runtime,
        peer,
      }) => {
        const { child } = await runOrder(runtime);
        await runtime.executions.prune({ retain: RETAIN_NONE, limit: 10 });

        // Cleanup has not run, so the start request is still recorded.
        await expect(startShipment(runtime)).rejects.toThrow(/was pruned/);
        await expect(startShipment(peer)).rejects.toThrow(/was pruned/);
        await liveRecord(runtime.executions, child);
      });

      it("answers every client with the tombstone", RETAIN_NONE, async ({ runtime, peer, clock }) => {
        const { child } = await runOrder(runtime);
        await runtime.prune();
        const pruned = tombstone(ORDER, clock.now);

        expect(await peer.executions.get(ORDER)).toEqual(pruned);
        expect(await listedIds(peer)).toEqual([child]);
        expect(await listedIds(peer, "completed")).toEqual([]);
        expect((await peer.executions.list({ status: "pruned" })).executions).toEqual([pruned]);

        const request = {
          executionId: ORDER,
          workflowId: "place-order",
          params: { executionId: ORDER },
          context: new Map(),
        };
        expect(await peer.executions.admit(request)).toMatchObject({
          status: "rejected",
          code: "EXECUTION_ID_CONFLICT",
        });
        expect(await peer.executions.admit({ ...request, idempotent: true })).toMatchObject({
          status: "rejected",
          code: "EXECUTION_PRUNED",
        });
        expect(await peer.executions.cancel({ executionId: ORDER, force: true })).toEqual(pruned);
        expect(await peer.executions.clearDivergence(ORDER)).toEqual({ status: "not-diverged" });
        expect(await peer.executions.requestWake({ executionId: ORDER, wakeAt: 0 })).toEqual({
          status: "ignored",
        });
        expect(await claimedIds(peer)).not.toContain(ORDER);
        expect(await peer.executions.get(ORDER)).toEqual(pruned);
      });

      it("starts the retention clock when the outcome is recorded", RETAIN, async ({
        runtime,
        advance,
      }) => {
        await admit(runtime, "completed-42");
        await admit(runtime, "failed-42");
        await admitSuspended(runtime, "cancelled-42");
        const completing = await claim(runtime, "completed-42", 100 * DAY);
        const failing = await claim(runtime, "failed-42", 100 * DAY);
        advance(60 * DAY);
        await acknowledge(runtime, completing, { status: "completed", result: null });
        await acknowledge(runtime, failing, {
          status: "failed",
          error: new Error("card declined"),
        });
        await runtime.executions.cancel({ executionId: "cancelled-42", force: true });

        // Requests that leave a terminal execution unchanged do not restart the clock.
        advance(DAY);
        for (const executionId of ["completed-42", "failed-42", "cancelled-42"]) {
          await runtime.executions.cancel({ executionId, force: true });
          await runtime.executions.requestWake({ executionId, wakeAt: 0 });
          await runtime.executions.clearDivergence(executionId);
        }

        advance(6 * DAY - 1);
        expect(await runtime.prune()).toEqual({ pruned: 0, cleaned: 0, failed: 0, errors: [] });
        advance(1);
        expect(await runtime.prune()).toEqual({ pruned: 1, cleaned: 1, failed: 0, errors: [] });
        await expectPruned(runtime, "completed-42");
        await expectNotPruned(runtime, "failed-42", "cancelled-42");

        advance(23 * DAY - 1);
        expect(await runtime.prune()).toEqual({ pruned: 0, cleaned: 0, failed: 0, errors: [] });
        advance(1);
        expect(await runtime.prune()).toEqual({ pruned: 2, cleaned: 2, failed: 0, errors: [] });
        expect(await runtime.executions.get("failed-42")).toMatchObject({
          terminalStatus: "failed",
        });
        expect(await runtime.executions.get("cancelled-42")).toMatchObject({
          terminalStatus: "cancelled",
        });
      });

      it("never prunes an admitted, running, suspended, cancel-requested or diverged execution, whatever its age", RETAIN_NONE, async ({
        runtime,
        advance,
      }) => {
        await admit(runtime, "admitted-42");
        await admitSuspended(runtime, "running-42");
        await admitSuspended(runtime, "suspended-42");
        await admitSuspended(runtime, "cancelling-42");
        await admitSuspended(runtime, "diverged-42");
        await acknowledge(runtime, await wake(runtime, "diverged-42"), {
          status: "diverged",
          code: "NONDETERMINISTIC_REPLAY",
          stepKey: "reserve",
        });
        expect(
          await runtime.executions.cancel({ executionId: "cancelling-42", reason: "late" })
        ).toEqual({ status: "requested" });
        await runtime.executions.requestWake({ executionId: "running-42", wakeAt: 0 });
        await claim(runtime, "running-42", 10_000 * DAY);

        advance(1_000 * DAY);
        expect(await runtime.prune()).toEqual({ pruned: 0, cleaned: 0, failed: 0, errors: [] });
        expect(await liveRecord(runtime.executions, "admitted-42")).toMatchObject({ status: "admitted" });
        expect(await liveRecord(runtime.executions, "running-42")).toMatchObject({ status: "running" });
        expect(await liveRecord(runtime.executions, "suspended-42")).toMatchObject({ status: "suspended" });
        expect(await liveRecord(runtime.executions, "cancelling-42")).toMatchObject({
          cancellation: { reason: "late" },
        });
        expect(await liveRecord(runtime.executions, "diverged-42")).toMatchObject({
          status: "suspended",
          divergence: { code: "NONDETERMINISTIC_REPLAY" },
        });
        expect(await runtime.executions.listPendingPrunes({ limit: 10 })).toEqual([]);
      });

      it("prunes nothing without a policy", undefined, async ({ runtime, advance }) => {
        await runToCompletion(runtime, "completed-42");
        advance(1_000 * DAY);
        expect(await runtime.prune()).toEqual({ pruned: 0, cleaned: 0, failed: 0, errors: [] });
        expect(await runtime.executions.prune({ retain: {}, limit: 100 })).toEqual([]);
        await expectNotPruned(runtime, "completed-42");
      });

      it("keeps an outcome whose policy has no period for it", { default: { failed: 0 } }, async ({
        runtime,
        advance,
      }) => {
        await runToCompletion(runtime, "completed-42");
        advance(1_000 * DAY);
        expect(await runtime.prune()).toEqual({ pruned: 0, cleaned: 0, failed: 0, errors: [] });
        await expectNotPruned(runtime, "completed-42");
      });

      it("applies a workflow's own policy in place of the default", {
        default: { completed: 0, failed: 0 },
        workflows: {
          "charge-order": { failed: 0 },
          "audit-order": { completed: 365 * DAY },
        },
      }, async ({ runtime, advance }) => {
        await runToCompletion(runtime, "place-42", "place-order");
        await runToCompletion(runtime, "charge-completed-42", "charge-order");
        await admit(runtime, "charge-failed-42", "charge-order");
        await acknowledge(runtime, await claim(runtime, "charge-failed-42"), {
          status: "failed",
          error: new Error("card declined"),
        });
        await runToCompletion(runtime, "audit-42", "audit-order");

        advance(7 * DAY);
        expect(await runtime.prune()).toEqual({ pruned: 2, cleaned: 2, failed: 0, errors: [] });
        await expectPruned(runtime, "place-42", "charge-failed-42");
        await expectNotPruned(runtime, "charge-completed-42", "audit-42");

        advance(358 * DAY);
        expect(await runtime.prune()).toEqual({ pruned: 1, cleaned: 1, failed: 0, errors: [] });
        await expectPruned(runtime, "audit-42");
        await expectNotPruned(runtime, "charge-completed-42");
      });

      it("applies a shortened policy to executions that are already terminal", RETAIN, async ({
        runtime,
        advance,
        clock,
      }) => {
        await runOrder(runtime);
        advance(DAY);
        expect(await runtime.prune()).toEqual({ pruned: 0, cleaned: 0, failed: 0, errors: [] });

        const shortened: RetentionOptions = { default: { completed: DAY } };
        expect(await runtime.executions.prune({ retain: shortened, limit: 10 })).toEqual([
          tombstone(ORDER, clock.now),
        ]);
        expect(await runtime.prune()).toEqual({ pruned: 0, cleaned: 1, failed: 0, errors: [] });
        expect(await runtime.heap.listSteps({ executionId: ORDER })).toEqual([]);
      });

      it("prunes at most `limit` executions in a pass, oldest outcome first", RETAIN_NONE, async ({
        runtime,
        advance,
      }) => {
        for (const executionId of ["order-c", "order-a", "order-b"]) {
          await runToCompletion(runtime, executionId);
          advance(1);
        }
        const first = await runtime.executions.prune({ retain: RETAIN_NONE, limit: 2 });
        expect(first.map((pruned) => pruned.executionId)).toEqual(["order-c", "order-a"]);
        expect(await runtime.prune({ limit: 2 })).toEqual({ pruned: 1, cleaned: 2, failed: 0, errors: [] });
        expect(await runtime.prune({ limit: 2 })).toEqual({ pruned: 0, cleaned: 1, failed: 0, errors: [] });
        await expectPruned(runtime, "order-a", "order-b", "order-c");
      });
    });

    describe("what pruning keeps", () => {
      it("leaves a pruned parent's children as they are", {
        default: { completed: 0 },
        workflows: { "ship-order": {} },
      }, async ({ runtime, clock }) => {
        const { child } = await runOrder(runtime);
        const before = await liveRecord(runtime.executions, child);
        await runtime.prune();

        expect(await runtime.executions.get(ORDER)).toEqual(tombstone(ORDER, clock.now));
        expect(await liveRecord(runtime.executions, child)).toEqual(before);
        expect(before.parent).toEqual({
          executionId: ORDER,
          stepKey: "start:ship",
          detached: false,
        });

        // The child still runs to completion and follows its own policy.
        await acknowledge(runtime, await claim(runtime, child), {
          status: "completed",
          result: null,
        });
        expect(await runtime.prune()).toEqual({ pruned: 0, cleaned: 0, failed: 0, errors: [] });
        expect(await liveRecord(runtime.executions, child)).toMatchObject({ status: "completed" });
      });

      it("leaves the cancellation a pruned parent propagated to its children", RETAIN_NONE, async ({
        runtime,
      }) => {
        const { child } = await runOrder(runtime);
        await admitSuspended(runtime, "parent-42");
        const started = await runtime.executions.start({
          requestId: { executionId: "parent-42", stepKey: "start:ship" },
          workflowId: "ship-order",
        });
        if (started.status !== "accepted") throw new Error(started.message);
        await runtime.executions.cancel({ executionId: "parent-42", reason: "customer" });
        await acknowledge(runtime, await claim(runtime, "parent-42"), { status: "cancelled" });
        expect(await runtime.prune()).toMatchObject({ pruned: 2 });

        await expectPruned(runtime, "parent-42", ORDER);
        expect(await liveRecord(runtime.executions, started.executionId)).toMatchObject({
          parent: { executionId: "parent-42" },
          cancellation: { reason: "customer", source: "parent-42", forced: false },
        });
        await expectNotPruned(runtime, child);
      });

      it("keeps shared stores and effect records", RETAIN_NONE, async ({ runtime }) => {
        await runOrder(runtime);
        await runtime.prune();

        expect(
          (await runtime.stores.getStore({ definition: LedgerStore, id: "shared" })).state
        ).toEqual({ total: 1 });
        expect(await runtime.effects.getEffect("charge-42")).toMatchObject({
          status: "pending",
        });

        // The effect's waiter went with the execution, so settling it wakes nobody.
        expect(
          await runtime.effects.settle({
            effectId: "charge-42",
            operation: "charge",
            status: "settled",
            result: 1,
          })
        ).toMatchObject({ status: "settled" });
        expect(await runtime.effects.getEffect("charge-42")).toMatchObject({
          status: "settled",
        });
        expect(await claimedIds(runtime)).not.toContain(ORDER);
        await expectPruned(runtime, ORDER);
      });

      it("keeps the execution's own stores without pruneExecutionStores", RETAIN_NONE, async ({
        runtime,
      }) => {
        await runOrder(runtime);
        await runtime.prune();
        expect(await runtime.stores.listStores(OrderStore)).toEqual([ORDER]);
        expect(await runtime.stores.listStores(InvoiceStore)).toEqual([ORDER]);
        expect(
          (await runtime.stores.getStore({ definition: OrderStore, id: ORDER })).state
        ).toEqual({ paid: true });
      });

      it("deletes every store whose id is the execution ID with pruneExecutionStores", {
        default: { completed: 0, pruneExecutionStores: true },
        workflows: { "keep-stores": { completed: 0 } },
      }, async ({ runtime }) => {
        await runOrder(runtime);
        await runToCompletion(runtime, "keeper-42", "keep-stores");
        await runtime.stores.updateStore({
          definition: OrderStore,
          id: "keeper-42",
          updater: (draft) => {
            draft.paid = true;
          },
        });

        expect(await runtime.executions.listPendingPrunes({ limit: 10 })).toEqual([]);
        await runtime.executions.prune({ retain: runtime.retain!, limit: 10 });
        expect(
          [...(await runtime.executions.listPendingPrunes({ limit: 10 }))].sort((a, b) =>
            a.executionId < b.executionId ? -1 : 1
          )
        ).toEqual([
          { executionId: "keeper-42", deleteExecutionStore: false },
          { executionId: ORDER, deleteExecutionStore: true },
        ]);
        await runtime.prune();

        expect(await runtime.stores.listStores(OrderStore)).toEqual(["keeper-42"]);
        expect(await runtime.stores.listStores(InvoiceStore)).toEqual([]);
        expect(await runtime.stores.listStores(LedgerStore)).toEqual(["shared"]);
        expect(
          (await runtime.stores.getStore({ definition: OrderStore, id: ORDER })).state
        ).toEqual({});
      });

      it("wakes another execution waiting on a store that pruneExecutionStores deletes", {
        default: { completed: 0, pruneExecutionStores: true },
      }, async ({ runtime }) => {
        await runOrder(runtime);
        await admitSuspended(runtime, "courier-42", "dispatch-courier");
        const snapshot = await runtime.stores.getStore({ definition: OrderStore, id: ORDER });
        await runtime.stores.registerWaiter({
          workflowId: "dispatch-courier",
          executionId: "courier-42",
          stepKey: "wait-for-payment",
          storeName: OrderStore.name,
          storeId: ORDER,
          instanceId: snapshot.instanceId,
          sinceVersion: snapshot.version,
          readPaths: [["paid"]],
        });
        const claimCourier = async () =>
          (
            await runtime.executions.claimDue({
              limit: 1,
              duration: LEASE,
              filter: { executionIds: ["courier-42"] },
            })
          ).map((lease) => lease.executionId);
        expect(await claimCourier()).toEqual([]);

        await runtime.prune();
        expect(await runtime.stores.listStores(OrderStore)).toEqual([]);
        expect(await claimCourier()).toEqual(["courier-42"]);
      });
    });

    describe("crashes and concurrency", () => {
      it("finishes a pass that stopped after the tombstone, and never shows the execution as live", RETAIN_NONE, async ({
        runtime,
        peer,
        clock,
      }) => {
        const { child } = await runOrder(runtime);
        const [pruned] = await runtime.executions.prune({ retain: RETAIN_NONE, limit: 10 });
        expect(pruned).toEqual(tombstone(ORDER, clock.now));

        expect(await peer.executions.get(ORDER)).toEqual(pruned);
        expect(await listedIds(peer)).toEqual([child]);
        expect(await listedIds(peer, "completed")).toEqual([]);
        expect(await listedIds(peer, "pruned")).toEqual([ORDER]);
        expect(await claimedIds(peer)).not.toContain(ORDER);
        expect(await peer.executions.listPendingPrunes({ limit: 10 })).toEqual([
          { executionId: ORDER, deleteExecutionStore: false },
        ]);

        // Part of the cleanup, then the crash.
        await runtime.heap.deleteSteps({ executionId: ORDER });
        expect(await peer.executions.get(ORDER)).toEqual(pruned);

        clock.now += DAY;
        expect(await peer.prune()).toEqual({ pruned: 0, cleaned: 1, failed: 0, errors: [] });
        expect(await peer.executions.listPendingPrunes({ limit: 10 })).toEqual([]);
        expect(await runtime.heap.listSteps({ executionId: ORDER })).toEqual([]);
        await expect(updateLedger(runtime, 2)).rejects.toThrow(/was pruned/);
        expect(await runtime.executions.get(ORDER)).toEqual(pruned);
      });

      itAfterRestart("finishes after a restart a pass that stopped after the tombstone", RETAIN_NONE, async ({
        runtime,
        restart,
        clock,
      }) => {
        await runOrder(runtime);
        await runtime.executions.prune({ retain: RETAIN_NONE, limit: 10 });

        const restarted = await restart();
        expect(await restarted.executions.get(ORDER)).toEqual(tombstone(ORDER, clock.now));
        expect(await restarted.executions.listPendingPrunes({ limit: 10 })).toEqual([
          { executionId: ORDER, deleteExecutionStore: false },
        ]);
        expect(await restarted.prune()).toEqual({ pruned: 0, cleaned: 1, failed: 0, errors: [] });
        expect(await restarted.heap.listSteps({ executionId: ORDER })).toEqual([]);
        expect(await restarted.executions.listPendingPrunes({ limit: 10 })).toEqual([]);
      });

      it("lets every cleanup step be repeated without changing the tombstone", RETAIN_NONE, async ({
        runtime,
        clock,
      }) => {
        await runOrder(runtime);
        await runtime.prune();
        await runtime.heap.deleteSteps({ executionId: ORDER });
        await runtime.stores.deleteExecutionData({
          executionId: ORDER,
          deleteExecutionStore: false,
        });
        await runtime.effects.deleteWaitersForExecution(ORDER);
        await runtime.joins.deleteWaitersForExecution(ORDER);
        await runtime.executions.finishPrune(ORDER);
        expect(await runtime.executions.get(ORDER)).toEqual(tombstone(ORDER, clock.now));
        expect(await runtime.executions.listPendingPrunes({ limit: 10 })).toEqual([]);
        expect(await runtime.stores.listStores(OrderStore)).toEqual([ORDER]);
      });

      it("leaves an execution that is not pruned unchanged by finishPrune", RETAIN_NONE, async ({
        runtime,
      }) => {
        const { child } = await runOrder(runtime);
        const before = await liveRecord(runtime.executions, ORDER);
        await runtime.executions.finishPrune(ORDER);
        await runtime.executions.finishPrune("never-admitted");
        expect(await liveRecord(runtime.executions, ORDER)).toEqual(before);
        expect(await startShipment(runtime)).toEqual({ status: "accepted", executionId: child });
      });

      it("shows a terminal execution or its tombstone while it is pruned, never a partial record", RETAIN_NONE, async ({
        runtime,
        peer,
        clock,
      }) => {
        await runOrder(runtime);
        const before = await liveRecord(runtime.executions, ORDER);
        const pruned = tombstone(ORDER, clock.now);
        const [, ...reads] = await Promise.all([
          runtime.prune(),
          ...Array.from({ length: 20 }, () => peer.executions.get(ORDER)),
        ]);
        for (const read of reads) {
          expect([before, pruned]).toContainEqual(read);
        }
        expect(await peer.executions.get(ORDER)).toEqual(pruned);
      });

      it("reaches the same end state when two runtimes prune at once", RETAIN_NONE, async ({
        runtime,
        peer,
        clock,
      }) => {
        const ids = Array.from({ length: 20 }, (_, i) => `order-${String(i).padStart(2, "0")}`);
        for (const executionId of ids) {
          await admit(runtime, executionId);
          const lease = await claim(runtime, executionId);
          await writeStep(runtime, lease, "reserve", { kind: "run" });
          await writeStep(runtime, lease, "items", { kind: "all" });
          await acknowledge(runtime, lease, { status: "completed", result: null });
        }

        let pruned = 0;
        for (;;) {
          const results = await Promise.all([
            runtime.prune({ limit: 7 }),
            peer.prune({ limit: 7 }),
          ]);
          const counted = results.reduce((sum, result) => sum + result.pruned + result.cleaned, 0);
          pruned += results.reduce((sum, result) => sum + result.pruned, 0);
          if (counted === 0) break;
        }

        expect(pruned).toBe(ids.length);
        for (const each of [runtime, peer]) {
          expect(await listedIds(each, "pruned")).toEqual(ids);
          expect(await listedIds(each)).toEqual([]);
          expect(await each.executions.listPendingPrunes({ limit: 100 })).toEqual([]);
          for (const executionId of ids) {
            expect(await each.executions.get(executionId)).toEqual(
              tombstone(executionId, clock.now)
            );
            expect(await each.heap.listSteps({ executionId })).toEqual([]);
          }
        }
      });
    });

    describe("joins", () => {
      it("keeps a target whose joiner was woken but not delivered, until the joiner records its result", RETAIN, async ({
        runtime,
        peer,
        advance,
      }) => {
        await admitSuspended(runtime, "ship-42", "ship-order");
        await admitSuspended(runtime, "charge-42", "charge-order");
        expect(await runtime.joins.join(joinWaiter("charge-42", "ship-42"))).toEqual({
          status: "waiting",
        });
        await acknowledge(runtime, await wake(runtime, "ship-42"), {
          status: "completed",
          result: { trackingNumber: "TRK-1" },
        });

        advance(30 * DAY);
        expect(await peer.prune()).toEqual({ pruned: 0, cleaned: 0, failed: 0, errors: [] });
        expect(await liveRecord(runtime.executions, "ship-42")).toMatchObject({ status: "completed" });

        // The joiner reads the outcome on its delivery, and records it.
        const delivery = await claim(runtime, "charge-42");
        expect(await runtime.joins.join(joinWaiter("charge-42", "ship-42"))).toEqual({
          status: "terminal",
          outcome: { status: "completed", result: { trackingNumber: "TRK-1" } },
        });
        await writeStep(runtime, delivery, "join:ship-42", { kind: "join" });
        expect(await runtime.joins.getWaiters("ship-42")).toEqual([]);
        await acknowledge(runtime, delivery, { status: "suspended" });

        expect(await peer.prune()).toEqual({ pruned: 1, cleaned: 1, failed: 0, errors: [] });
        await expectPruned(runtime, "ship-42");
        await expectNotPruned(runtime, "charge-42");
      });

      it("keeps a target only until its retention expires once the joiner has recorded its result", RETAIN, async ({
        runtime,
        advance,
      }) => {
        await admitSuspended(runtime, "ship-42", "ship-order");
        await admitSuspended(runtime, "charge-42", "charge-order");
        await runtime.joins.join(joinWaiter("charge-42", "ship-42"));
        await acknowledge(runtime, await wake(runtime, "ship-42"), {
          status: "completed",
          result: null,
        });
        const delivery = await claim(runtime, "charge-42");
        await writeStep(runtime, delivery, "join:ship-42", { kind: "join" });
        await acknowledge(runtime, delivery, { status: "suspended" });
        expect(await runtime.joins.getWaiters("ship-42")).toEqual([]);

        advance(7 * DAY - 1);
        expect(await runtime.prune()).toEqual({ pruned: 0, cleaned: 0, failed: 0, errors: [] });
        advance(1);
        expect(await runtime.prune()).toEqual({ pruned: 1, cleaned: 1, failed: 0, errors: [] });
        await expectPruned(runtime, "ship-42");
      });

      it("returns pruned for a join on a pruned target and registers nothing", RETAIN_NONE, async ({
        runtime,
        peer,
      }) => {
        await runToCompletion(runtime, "ship-42", "ship-order");
        await runtime.prune();
        await admitSuspended(runtime, "charge-42", "charge-order");

        const delivery = await wake(runtime, "charge-42");
        expect(await peer.joins.join(joinWaiter("charge-42", "ship-42"))).toEqual({
          status: "pruned",
        });
        expect(await runtime.joins.getWaiters("ship-42")).toEqual([]);

        // The joiner records the error as the join step's result.
        expect(
          await writeStep(runtime, delivery, "join:ship-42", {
            kind: "join",
            json: JSON.stringify({ type: "step-error", code: "JOIN_TARGET_PRUNED" }),
          })
        ).toMatchObject({ meta: { attempt: 1, done: true } });
        await acknowledge(runtime, delivery, {
          status: "failed",
          error: new Error("JOIN_TARGET_PRUNED"),
        });
        expect(await claimedIds(runtime)).toEqual([]);
      });

      it("keeps a target while a parked joiner waits on it", RETAIN_NONE, async ({ runtime }) => {
        await admitSuspended(runtime, "ship-42", "ship-order");
        await admitSuspended(runtime, "charge-42", "charge-order");
        await runtime.joins.join(joinWaiter("charge-42", "ship-42"));
        await acknowledge(runtime, await wake(runtime, "ship-42"), {
          status: "completed",
          result: null,
        });
        await acknowledge(runtime, await claim(runtime, "charge-42"), {
          status: "diverged",
          code: "NONDETERMINISTIC_REPLAY",
          stepKey: "join:ship-42",
        });

        expect(await runtime.prune()).toEqual({ pruned: 0, cleaned: 0, failed: 0, errors: [] });
        expect(await liveRecord(runtime.executions, "ship-42")).toMatchObject({ status: "completed" });

        // Once the divergence is cleared, the joiner reads the outcome and records it.
        await runtime.executions.clearDivergence("charge-42");
        const delivery = await claim(runtime, "charge-42");
        expect(await runtime.joins.join(joinWaiter("charge-42", "ship-42"))).toMatchObject({
          status: "terminal",
        });
        await writeStep(runtime, delivery, "join:ship-42", { kind: "join" });
        await acknowledge(runtime, delivery, { status: "suspended" });
        expect(await runtime.prune()).toEqual({ pruned: 1, cleaned: 1, failed: 0, errors: [] });
        await expectPruned(runtime, "ship-42");
      });

      it("reads the outcome or returns pruned when a join races the pruner, and never waits on a pruned target", RETAIN_NONE, async ({
        runtime,
        peer,
      }) => {
        const targets = Array.from({ length: 10 }, (_, i) => `ship-${i}`);
        for (const target of targets) await runToCompletion(runtime, target, "ship-order");
        await admitSuspended(runtime, "charge-42", "charge-order");

        const [, ...joins] = await Promise.all([
          peer.prune({ limit: 100 }),
          ...targets.map((target) => runtime.joins.join(joinWaiter("charge-42", target))),
        ]);
        joins.forEach((result, i) => {
          expect([
            { status: "pruned" },
            {
              status: "terminal",
              outcome: { status: "completed", result: { executionId: targets[i] } },
            },
          ]).toContainEqual(result);
        });
        for (const target of targets) {
          expect(await runtime.joins.getWaiters(target)).toEqual([]);
        }
        await runtime.prune({ limit: 100 });
        await expectPruned(runtime, ...targets);
        await expectNotPruned(runtime, "charge-42");
      });

      it("prunes a target whose only waiters belong to terminal joiners, and deletes those waiters", RETAIN_NONE, async ({
        runtime,
      }) => {
        await admitSuspended(runtime, "ship-42", "ship-order");
        await admit(runtime, "charge-42", "charge-order");
        const joiner = await claim(runtime, "charge-42");
        await runtime.joins.join(joinWaiter("charge-42", "ship-42"));
        await acknowledge(runtime, joiner, { status: "failed", error: new Error("gave up") });
        await acknowledge(runtime, await wake(runtime, "ship-42"), {
          status: "completed",
          result: null,
        });

        expect(await runtime.prune()).toEqual({ pruned: 2, cleaned: 2, failed: 0, errors: [] });
        await expectPruned(runtime, "ship-42", "charge-42");
        expect(await runtime.joins.getWaiters("ship-42")).toEqual([]);
      });

      it("deletes the waiters a pruned joiner left on a live target", {
        default: { failed: 0 },
      }, async ({ runtime }) => {
        await admitSuspended(runtime, "ship-42", "ship-order");
        await admit(runtime, "charge-42", "charge-order");
        const joiner = await claim(runtime, "charge-42");
        await runtime.joins.join(joinWaiter("charge-42", "ship-42"));
        await acknowledge(runtime, joiner, { status: "failed", error: new Error("gave up") });

        expect(await runtime.prune()).toEqual({ pruned: 1, cleaned: 1, failed: 0, errors: [] });
        expect(await runtime.joins.getWaiters("ship-42")).toEqual([]);

        await acknowledge(runtime, await wake(runtime, "ship-42"), {
          status: "completed",
          result: null,
        });
        expect(await claimedIds(runtime)).toEqual([]);
        await expectPruned(runtime, "charge-42");
      });
    });
  });
}

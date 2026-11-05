import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import type { Logger } from "pino";
import {
  DEFAULT_LEASE_DURATION,
  InfrastructureError,
  WorkflowRunner,
  type ExecutionLease,
  type ExecutionRecord,
  type Runtime,
  type StoreWaiter,
} from "@yieldstar/core";
import { MemoryRuntime } from "@yieldstar/test-runtime";
import { SqliteRuntime } from "@yieldstar/sqlite-runtime";
import { testSchema } from "@yieldstar/test-utils";
import {
  RetryableError,
  WorkflowCancelledError,
  WorkflowJoinError,
  defineStore,
  workflow,
} from "yieldstar";

const isBun = "Bun" in globalThis;
const { createSqliteDb } = isBun
  ? await import("@yieldstar/sqlite-runtime/bun")
  : await import("@yieldstar/sqlite-runtime/node");

const logger = { info() {}, error() {}, warn() {}, debug() {} } as unknown as Logger;

const START = 1_000_000_000;
const MINUTE = 60_000;
const DAY = 24 * 60 * MINUTE;
const ORDER = "order-42";
const TARGET = "ship-42";
const EFFECT_ID = "acct-1:charge:order-42";

const PaymentStore = defineStore("payments", testSchema<{ paid?: boolean }>(), {
  initial: () => ({}),
});
const WarehouseStore = defineStore("warehouse", testSchema<{ released?: boolean }>(), {
  initial: () => ({}),
});

type SuspendKind =
  | "delay"
  | "when"
  | "timed-when"
  | "take"
  | "join"
  | "effect"
  | "retry"
  | "run";

/** What workflow code caught, on each evaluation. */
let caught: unknown[] = [];
/** Step functions that ran, in order. */
let ran: string[] = [];
/** Resolves a gated step function; set by `gate()`. */
let releaseGate: () => void = () => {};
let gate: Promise<void> = Promise.resolve();
let gateEntered: Promise<void> = Promise.resolve();
let enterGate: () => void = () => {};

function closeGate() {
  gate = new Promise((resolve) => (releaseGate = resolve));
  gateEntered = new Promise((resolve) => (enterGate = resolve));
}

function describeCaught(err: unknown) {
  if (err instanceof WorkflowJoinError) {
    return {
      join: true,
      code: err.code,
      executionId: err.executionId,
      reason: err.reason,
      source: err.source,
    };
  }
  const error = err as WorkflowCancelledError;
  return {
    isCancelled: err instanceof WorkflowCancelledError,
    name: error.name,
    reason: error.reason,
    source: error.source,
  };
}

const router = {
  suspend: workflow(async function* (step, event) {
    const { kind } = event.params as { kind: SuspendKind };
    const payments = step.store(PaymentStore);
    try {
      switch (kind) {
        case "delay":
          yield* step.delay("wait", DAY);
          break;
        case "when":
          yield* payments.when("wait", (state) => state.paid);
          break;
        case "timed-when":
          yield* payments.when("wait", (state) => state.paid, { deadline: START + DAY });
          break;
        case "take":
          yield* payments.take("wait", (state) => state.paid, (draft) => {
            draft.paid = false;
          });
          break;
        case "join":
          yield* step.join("wait", TARGET);
          break;
        case "effect":
          yield* step.effect("wait", {
            effectId: EFFECT_ID,
            operation: "charge-v1",
            input: { amount: 4999 },
            retryIn: DAY,
            lookup: async () => ({ status: "unknown" }),
            perform: async () => {
              throw new Error("timeout");
            },
          });
          break;
        case "retry":
          yield* step.run("wait", () => {
            ran.push("wait");
            throw new RetryableError("provider down", {
              maxAttempts: 5,
              retryInterval: MINUTE,
            });
          });
          break;
        case "run":
          yield* step.run("wait", () => {
            ran.push("wait");
            return 1;
          });
          break;
      }
      return "finished";
    } catch (err) {
      caught.push(describeCaught(err));
      throw err;
    }
  }),
  "place-order": workflow(async function* (step) {
    const payments = step.store(PaymentStore);
    const warehouse = step.store(WarehouseStore);
    const reservation = yield* step.run("reserve", () => {
      ran.push("reserve");
      return "res-1";
    });
    try {
      yield* payments.when("paid", (state) => state.paid);
    } catch (err) {
      caught.push(describeCaught(err));
      if (err instanceof WorkflowCancelledError) {
        yield* step.delay("cool-off", MINUTE);
        yield* warehouse.when("released", (state) => state.released);
        yield* step.run("release", () => {
          ran.push(`release ${reservation}`);
        });
      }
      throw err;
    }
    return "paid";
  }),
  decline: workflow(async function* (step) {
    try {
      yield* step.delay("wait", DAY);
    } catch (err) {
      caught.push(describeCaught(err));
      return "declined";
    }
    return "finished";
  }),
  "throw-other": workflow(async function* (step) {
    try {
      yield* step.delay("wait", DAY);
    } catch {
      throw new TypeError("cleanup failed");
    }
  }),
  "step-throws-cancelled": workflow(async function* (step) {
    yield* step.run("own", () => {
      throw new WorkflowCancelledError({ reason: "from a step" });
    });
  }),
  "throws-cancelled": workflow(async function* () {
    throw new WorkflowCancelledError({ reason: "by itself" });
  }),
  "slow-then-delay": workflow(async function* (step) {
    yield* step.run("slow", async () => {
      ran.push("slow");
      enterGate();
      await gate;
      return "done";
    });
    try {
      yield* step.delay("next", MINUTE);
    } catch (err) {
      caught.push(describeCaught(err));
      throw err;
    }
    yield* step.run("after", () => {
      ran.push("after");
    });
  }),
  "slow-then-start": workflow(async function* (step) {
    yield* step.run("slow", async () => {
      enterGate();
      await gate;
    });
    yield* step.start("child", { workflowId: "leaf" });
    yield* step.delay("next", MINUTE);
  }),
  "join-target": workflow(async function* (step, event) {
    const { timed } = event.params as { timed?: boolean };
    try {
      if (timed) {
        return yield* step.join("join", TARGET, { deadline: START + DAY });
      }
      return yield* step.join("join", TARGET);
    } catch (err) {
      caught.push(describeCaught(err));
      throw err;
    }
  }),
  "join-peer": workflow(async function* (step, event) {
    const { peer } = event.params as { peer: string };
    try {
      return yield* step.join("peer", peer);
    } catch (err) {
      caught.push(describeCaught(err));
      throw err;
    }
  }),
  parent: workflow(async function* (step) {
    yield* step.start("child", { workflowId: "child" });
    yield* step.start("receipt", { workflowId: "leaf" }, { detached: true });
    yield* step.delay("wait", DAY);
  }),
  child: workflow(async function* (step) {
    yield* step.start("grandchild", { workflowId: "leaf" });
    yield* step.delay("wait", DAY);
  }),
  leaf: workflow(async function* (step) {
    yield* step.delay("wait", MINUTE);
    return "leaf done";
  }),
  "cleanup-starts-child": workflow(async function* (step) {
    try {
      yield* step.delay("wait", DAY);
    } catch (err) {
      yield* step.start("refund", { workflowId: "leaf" });
      throw err;
    }
  }),
  "call-detached": workflow(async function* (step) {
    yield* step.call("ship", { workflowId: "leaf" }, { detached: true });
  }),
};

type Harness = {
  runtime: Runtime;
  /** The step keys of the registered store waiters. */
  storeWaiters(): string[];
  dispose(): void;
};

const runtimes: { name: string; create(): Harness }[] = [
  {
    name: "memory",
    create() {
      const runtime = new MemoryRuntime();
      return {
        runtime,
        storeWaiters: () =>
          [
            ...(runtime.stores as unknown as { waiters: Map<string, StoreWaiter> }).waiters.values(),
          ].map((waiter) => waiter.stepKey),
        dispose() {},
      };
    },
  },
  {
    name: isBun ? "sqlite-bun" : "sqlite-node",
    create() {
      const db = createSqliteDb({ path: ":memory:", wal: false });
      return {
        runtime: new SqliteRuntime(db),
        storeWaiters: () =>
          db
            .query<{ step_key: string }>("SELECT step_key FROM store_waiters")
            .all()
            .map((row) => row.step_key),
        dispose: () => db.close(),
      };
    },
  },
];

beforeEach(() => {
  caught = [];
  ran = [];
  // Both runtimes read Date.now().
  vi.useFakeTimers({ toFake: ["Date"], now: START });
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

function advance(ms: number) {
  vi.setSystemTime(Date.now() + ms);
}

const cancelled = (reason?: string, source?: string) => ({
  isCancelled: true,
  name: "WorkflowCancelledError",
  reason,
  source,
});

describe.each(runtimes)("cancellation on $name", ({ create }) => {
  let harness: Harness;
  let runtime: Runtime;
  let runner: WorkflowRunner<typeof router>;

  beforeEach(() => {
    harness = create();
    runtime = harness.runtime;
    runner = new WorkflowRunner(runtime, { router, logger });
  });

  afterEach(() => harness.dispose());

  async function admit(executionId: string, workflowId: keyof typeof router, params?: object) {
    await runtime.executions.admit({
      executionId,
      workflowId,
      params: params as Record<string, unknown> | undefined,
      context: new Map(),
    });
  }

  async function claim(executionId: string): Promise<ExecutionLease | undefined> {
    const leases = await runtime.executions.claimDue({
      limit: 1,
      duration: DEFAULT_LEASE_DURATION,
      filter: { executionIds: [executionId] },
    });
    return leases[0];
  }

  /** Runs one delivery under `lease` as the dispatcher would, without acknowledging it. */
  async function run(lease: ExecutionLease) {
    const record = (await runtime.executions.get(lease.executionId))!;
    return runner.run(
      {
        workflowId: record.workflowId,
        executionId: lease.executionId,
        params: record.params,
        context: new Map(),
      },
      {
        lease,
        signal: new AbortController().signal,
        ...(record.cancellation ? { cancellation: record.cancellation } : {}),
      }
    );
  }

  async function deliver(executionId = ORDER) {
    const lease = await claim(executionId);
    if (!lease) return undefined;
    const outcome = await run(lease);
    await runtime.executions.acknowledge({ lease, outcome });
    return outcome;
  }

  const get = async (executionId = ORDER) => (await runtime.executions.get(executionId))!;

  async function readStep(stepKey = "wait", executionId = ORDER) {
    const record = await runtime.heap.readStep({ executionId, stepKey });
    return record && {
      ...JSON.parse(record.stepResponseJson),
      attempt: record.meta.attempt,
      done: record.meta.done,
    };
  }

  async function childrenOf(caller: string): Promise<ExecutionRecord[]> {
    const page = await runtime.executions.list({ limit: 100 });
    return page.executions.filter((record) => record.parent?.executionId === caller);
  }

  async function childOf(caller: string, stepKey: string) {
    const child = (await childrenOf(caller)).find((record) => record.parent?.stepKey === stepKey);
    if (!child) throw new Error(`"${caller}" has no child started by "${stepKey}"`);
    return child;
  }

  const cancel = (executionId: string, options?: { reason?: string; force?: boolean }) =>
    runtime.executions.cancel({ executionId, ...options });

  async function admitTarget() {
    await admit(TARGET, "suspend", { kind: "delay" });
  }

  describe("graceful delivery", () => {
    test.each([
      { kind: "delay", record: { type: "step-delay", done: true } },
      { kind: "when", record: { type: "store-wait", done: false } },
      { kind: "take", record: { type: "store-wait", done: false } },
      { kind: "join", record: { type: "join-wait", done: false } },
      { kind: "effect", record: { type: "effect-wait", done: false } },
      { kind: "retry", record: { type: "step-error", done: false } },
    ] as const)(
      "an execution suspended on $kind receives WorkflowCancelledError at that step and ends cancelled",
      async ({ kind, record }) => {
        if (kind === "join") await admitTarget();
        await admit(ORDER, "suspend", { kind });
        expect(await deliver()).toMatchObject({ status: "suspended" });
        expect(await readStep()).toMatchObject({ ...record, attempt: 0 });

        expect(await cancel(ORDER, { reason: "abandoned" })).toEqual({ status: "requested" });
        expect(await deliver()).toEqual({ status: "cancelled" });

        expect(caught).toEqual([cancelled("abandoned")]);
        // The throw is the step's next attempt, recorded done.
        expect(await readStep()).toMatchObject({
          type: "step-cancelled",
          err: { $type: "error", name: "WorkflowCancelledError" },
          attempt: 1,
          done: true,
        });
        const execution = await get();
        expect(execution.status).toBe("cancelled");
        expect(execution.outcome).toEqual({ status: "cancelled", reason: "abandoned" });
        expect(execution.cancellation).toEqual({
          reason: "abandoned",
          forced: false,
          requestedAt: START,
          deliveredAt: START,
        });
        if (kind === "retry") expect(ran).toEqual(["wait"]);
      }
    );

    test("a timed when receives the cancellation at its step, and its store waiter no longer wakes the execution", async () => {
      await admit(ORDER, "suspend", { kind: "timed-when" });
      expect(await deliver()).toEqual({ status: "suspended", wakeAt: START + DAY });
      expect(harness.storeWaiters()).toEqual(["wait"]);

      await cancel(ORDER, { reason: "abandoned" });
      expect(await deliver()).toEqual({ status: "cancelled" });
      expect(caught).toEqual([cancelled("abandoned")]);
      expect(await readStep()).toMatchObject({ type: "step-cancelled", attempt: 1, done: true });
      expect(harness.storeWaiters()).toEqual([]);

      await runtime.stores.updateStore({
        definition: PaymentStore,
        id: ORDER,
        updater(draft) {
          draft.paid = true;
        },
      });
      expect(await claim(ORDER)).toBeUndefined();
      expect((await get()).outcome).toEqual({ status: "cancelled", reason: "abandoned" });
    });

    test("an admitted execution that never ran throws at its first step without running it", async () => {
      await admit(ORDER, "suspend", { kind: "run" });
      await cancel(ORDER, { reason: "abandoned" });

      expect(await deliver()).toEqual({ status: "cancelled" });
      expect(ran).toEqual([]);
      expect(caught).toEqual([cancelled("abandoned")]);
      expect(await readStep()).toMatchObject({ type: "step-cancelled", attempt: 0, done: true });
    });

    test("cleanup that suspends runs across deliveries, replay rethrows the recorded cancellation, and the rethrow ends cancelled", async () => {
      const recordCancellation = vi.spyOn(runtime.heap, "recordCancellation");
      await admit(ORDER, "place-order");
      expect(await deliver()).toEqual({ status: "suspended", wakeAt: undefined });
      expect(harness.storeWaiters()).toEqual(["paid"]);

      await cancel(ORDER, { reason: "abandoned" });
      expect(await deliver()).toEqual({ status: "suspended", wakeAt: START + MINUTE });
      expect((await get()).cancellation?.deliveredAt).toBe(START);
      // The payment wait's waiter was deleted when the throw was recorded.
      expect(harness.storeWaiters()).toEqual([]);

      // A wait registered before delivery does not wake the execution.
      await runtime.stores.updateStore({
        definition: PaymentStore,
        id: ORDER,
        updater(draft) {
          draft.paid = true;
        },
      });
      expect(await claim(ORDER)).toBeUndefined();

      advance(MINUTE);
      expect(await deliver()).toEqual({ status: "suspended", wakeAt: undefined });
      expect(harness.storeWaiters()).toEqual(["released"]);

      // A cleanup step's wait wakes the execution.
      await runtime.stores.updateStore({
        definition: WarehouseStore,
        id: ORDER,
        updater(draft) {
          draft.released = true;
        },
      });
      expect(await deliver()).toEqual({ status: "cancelled" });

      expect(ran).toEqual(["reserve", "release res-1"]);
      expect(caught).toEqual([
        cancelled("abandoned"),
        cancelled("abandoned"),
        cancelled("abandoned"),
      ]);
      expect(recordCancellation).toHaveBeenCalledTimes(1);
      expect((await get()).outcome).toEqual({ status: "cancelled", reason: "abandoned" });
    });

    test("a workflow that catches the cancellation and returns completes, and the request stays recorded", async () => {
      await admit(ORDER, "decline");
      await deliver();
      await cancel(ORDER, { reason: "abandoned" });

      expect(await deliver()).toEqual({ status: "completed", result: "declined" });
      expect(caught).toEqual([cancelled("abandoned")]);
      expect((await get()).cancellation).toMatchObject({
        reason: "abandoned",
        deliveredAt: START,
      });
    });

    test("a workflow that catches the cancellation and throws another error fails", async () => {
      await admit(ORDER, "throw-other");
      await deliver();
      await cancel(ORDER);

      const outcome = await deliver();
      expect(outcome).toMatchObject({ status: "failed" });
      expect((outcome as { error: Error }).error).toBeInstanceOf(TypeError);
      expect((await get()).status).toBe("failed");
    });

    test("a workflow that returns with no further steps completes", async () => {
      await admit(ORDER, "suspend", { kind: "delay" });
      await deliver();
      advance(DAY);
      await cancel(ORDER, { reason: "abandoned" });

      // The elapsed delay is finished, and the workflow result is not a step.
      expect(await deliver()).toEqual({ status: "completed", result: "finished" });
      expect(caught).toEqual([]);
      expect((await get()).cancellation?.deliveredAt).toBeUndefined();
    });

    test("a WorkflowCancelledError thrown by a step function is an ordinary step error", async () => {
      await admit(ORDER, "step-throws-cancelled");

      const outcome = await deliver();
      expect(outcome).toMatchObject({ status: "failed" });
      expect((outcome as { error: Error }).error).toBeInstanceOf(WorkflowCancelledError);
      expect(await readStep("own")).toMatchObject({
        type: "step-error",
        err: { name: "WorkflowCancelledError" },
      });
      expect((await get()).status).toBe("failed");
    });

    test("a workflow that throws its own WorkflowCancelledError with no request recorded fails", async () => {
      await admit(ORDER, "throws-cancelled");

      const outcome = await deliver();
      expect(outcome).toMatchObject({ status: "failed" });
      expect((outcome as { error: Error }).error).toBeInstanceOf(WorkflowCancelledError);
      const execution = await get();
      expect(execution.status).toBe("failed");
      expect(execution.outcome).toMatchObject({
        status: "failed",
        error: { name: "WorkflowCancelledError", reason: "by itself" },
      });
    });

    test("a running step function is not interrupted, and the cancellation lands at the next unfinished step", async () => {
      closeGate();
      await admit(ORDER, "slow-then-delay");
      const lease = (await claim(ORDER))!;
      const delivery = run(lease);
      await gateEntered;

      expect(await cancel(ORDER, { reason: "abandoned" })).toEqual({ status: "requested" });
      releaseGate();
      const outcome = await delivery;
      // This delivery began before the request, so it suspends on the delay.
      expect(outcome).toEqual({ status: "suspended", wakeAt: START + MINUTE });
      await runtime.executions.acknowledge({ lease, outcome });
      expect(await readStep("slow")).toMatchObject({ type: "step-result", result: "done" });

      // The request made the execution due before the delay elapses.
      expect(await deliver()).toEqual({ status: "cancelled" });
      expect(ran).toEqual(["slow"]);
      expect(caught).toEqual([cancelled("abandoned")]);
      expect(await readStep("next")).toMatchObject({
        type: "step-cancelled",
        attempt: 1,
        done: true,
      });
    });

    test("a pending effect stays pending, and a later settle succeeds", async () => {
      await admit(ORDER, "suspend", { kind: "effect" });
      await deliver();
      expect(await runtime.effects.getEffect(EFFECT_ID)).toMatchObject({ status: "pending" });

      await cancel(ORDER);
      expect(await deliver()).toEqual({ status: "cancelled" });
      expect(await runtime.effects.getEffect(EFFECT_ID)).toMatchObject({ status: "pending" });

      expect(
        await runtime.effects.settle({
          effectId: EFFECT_ID,
          operation: "charge-v1",
          status: "settled",
          result: { chargeId: "ch-1" },
        })
      ).toEqual({ status: "settled", result: { chargeId: "ch-1" } });
      expect(await runtime.effects.getEffect(EFFECT_ID)).toMatchObject({ status: "settled" });
      // The settlement does not wake the cancelled execution.
      expect(await claim(ORDER)).toBeUndefined();
      expect((await get()).status).toBe("cancelled");
    });

    test("a lost lease when recording the throw ends the delivery without an outcome", async () => {
      await admit(ORDER, "suspend", { kind: "delay" });
      await deliver();
      await cancel(ORDER);
      const lease = (await claim(ORDER))!;
      // A forced cancel issues a higher token, so the recording is lost.
      await cancel(ORDER, { force: true });

      const err = await run(lease).catch((error) => error);
      expect(InfrastructureError.is(err)).toBe(true);
      expect(caught).toEqual([]);
      expect(await readStep()).toMatchObject({ type: "step-delay", attempt: 0 });
    });
  });

  describe("joins", () => {
    test.each([{ timed: false }, { timed: true }])(
      "joining a cancelled target throws JOINED_EXECUTION_CANCELLED with its reason (timed: $timed)",
      async ({ timed }) => {
        await admitTarget();
        await deliver(TARGET);
        await cancel(TARGET, { reason: "abandoned" });
        expect(await deliver(TARGET)).toEqual({ status: "cancelled" });

        caught = [];
        await admit(ORDER, "join-target", { timed });
        expect(await deliver()).toMatchObject({ status: "failed" });
        expect(caught).toEqual([
          {
            join: true,
            code: "JOINED_EXECUTION_CANCELLED",
            executionId: TARGET,
            reason: "abandoned",
            source: undefined,
          },
        ]);
        expect(await readStep("join")).toMatchObject({
          type: "step-error",
          err: { name: "WorkflowJoinError", code: "JOINED_EXECUTION_CANCELLED" },
        });
      }
    );

    test("a join cycle with no deadline ends when one side is cancelled", async () => {
      await admit("a", "join-peer", { peer: "b" });
      await admit("b", "join-peer", { peer: "a" });
      expect(await deliver("a")).toEqual({ status: "suspended", wakeAt: undefined });
      expect(await deliver("b")).toEqual({ status: "suspended", wakeAt: undefined });

      await cancel("a", { reason: "deadlock" });
      expect(await deliver("a")).toEqual({ status: "cancelled" });
      // b's waiter on a is woken by a's terminal acknowledgement.
      expect(await deliver("b")).toMatchObject({ status: "failed" });
      expect(caught).toEqual([
        cancelled("deadlock"),
        expect.objectContaining({ code: "JOINED_EXECUTION_CANCELLED", reason: "deadlock" }),
      ]);
      expect(await runtime.joins.getWaiters("a")).toEqual([]);
      expect(await runtime.joins.getWaiters("b")).toEqual([]);
    });
  });

  describe("forced cancellation", () => {
    test("a forced cancel with a live lease is cancelled at once, and no more workflow code runs", async () => {
      closeGate();
      await admit(ORDER, "slow-then-delay");
      const lease = (await claim(ORDER))!;
      const delivery = run(lease);
      await gateEntered;

      expect(await cancel(ORDER, { reason: "stuck", force: true })).toEqual({
        status: "cancelled",
        reason: "stuck",
      });
      expect((await get()).status).toBe("cancelled");

      releaseGate();
      const err = await delivery.catch((error) => error);
      expect(InfrastructureError.is(err)).toBe(true);
      expect(ran).toEqual(["slow"]);
      expect(caught).toEqual([]);
      expect(
        await runtime.executions.acknowledge({ lease, outcome: { status: "completed", result: 1 } })
      ).toEqual({ status: "lost" });
      expect((await get()).outcome).toEqual({ status: "cancelled", reason: "stuck" });
    });

    test("a forced cancel of a joined target wakes its joiners with JOINED_EXECUTION_CANCELLED", async () => {
      await admitTarget();
      await deliver(TARGET);
      await admit(ORDER, "join-target", { timed: false });
      expect(await deliver()).toEqual({ status: "suspended", wakeAt: undefined });

      await cancel(TARGET, { reason: "stuck", force: true });
      expect(await deliver()).toMatchObject({ status: "failed" });
      expect(await runtime.joins.getWaiters(TARGET)).toEqual([]);
      expect(caught).toEqual([
        expect.objectContaining({ code: "JOINED_EXECUTION_CANCELLED", reason: "stuck" }),
      ]);
    });
  });

  describe("propagation", () => {
    async function startFamily() {
      await admit(ORDER, "parent");
      expect(await deliver()).toMatchObject({ status: "suspended" });
      const child = await childOf(ORDER, "child");
      expect(await deliver(child.executionId)).toMatchObject({ status: "suspended" });
      const grandchild = await childOf(child.executionId, "grandchild");
      const receipt = await childOf(ORDER, "receipt");
      return { child, grandchild, receipt };
    }

    test("children and grandchildren receive the request with the parent as source; a detached child keeps running", async () => {
      const { child, grandchild, receipt } = await startFamily();
      expect(receipt.parent).toEqual({ executionId: ORDER, stepKey: "receipt", detached: true });
      expect(child.parent).toEqual({ executionId: ORDER, stepKey: "child", detached: false });

      await cancel(ORDER, { reason: "abandoned" });
      for (const execution of [child, grandchild]) {
        expect((await get(execution.executionId)).cancellation).toEqual({
          reason: "abandoned",
          source: ORDER,
          forced: false,
          requestedAt: START,
        });
      }
      expect((await get(receipt.executionId)).cancellation).toBeUndefined();

      expect(await deliver()).toEqual({ status: "cancelled" });
      expect(await deliver(child.executionId)).toEqual({ status: "cancelled" });
      expect(await deliver(grandchild.executionId)).toEqual({ status: "cancelled" });
      expect((await get(grandchild.executionId)).outcome).toEqual({
        status: "cancelled",
        reason: "abandoned",
        source: ORDER,
      });

      expect(await deliver(receipt.executionId)).toMatchObject({ status: "suspended" });
      advance(MINUTE);
      expect(await deliver(receipt.executionId)).toEqual({
        status: "completed",
        result: "leaf done",
      });
    });

    test("a forced cancel of a parent force-cancels its non-detached descendants", async () => {
      const { child, grandchild, receipt } = await startFamily();

      await cancel(ORDER, { reason: "stuck", force: true });
      for (const execution of [child, grandchild]) {
        expect(await get(execution.executionId)).toMatchObject({
          status: "cancelled",
          outcome: { status: "cancelled", reason: "stuck", source: ORDER },
          cancellation: { forced: true, source: ORDER },
        });
      }
      expect((await get(receipt.executionId)).status).toBe("admitted");
    });

    test("a child started after the request but before delivery receives the request", async () => {
      closeGate();
      await admit(ORDER, "slow-then-start");
      const lease = (await claim(ORDER))!;
      const delivery = run(lease);
      await gateEntered;
      await cancel(ORDER, { reason: "abandoned" });
      releaseGate();
      await runtime.executions.acknowledge({ lease, outcome: await delivery });

      const child = await childOf(ORDER, "child");
      expect(child.cancellation).toMatchObject({ reason: "abandoned", source: ORDER });
    });

    test("a child started by a cleanup step after delivery receives no request", async () => {
      await admit(ORDER, "cleanup-starts-child");
      await deliver();
      await cancel(ORDER, { reason: "abandoned" });

      expect(await deliver()).toEqual({ status: "cancelled" });
      const refund = await childOf(ORDER, "refund");
      expect(refund.cancellation).toBeUndefined();
      expect(refund.status).toBe("admitted");
    });

    test("step.call with detached records a detached parent link", async () => {
      await admit(ORDER, "call-detached");
      await deliver();
      const ship = await childOf(ORDER, "ship");
      expect(ship.parent).toEqual({ executionId: ORDER, stepKey: "ship", detached: true });

      await cancel(ORDER);
      expect(await deliver()).toEqual({ status: "cancelled" });
      expect((await get(ship.executionId)).cancellation).toBeUndefined();
    });
  });
});

test("step.start and step.call check detached in workflow code", async () => {
  const runtime = new MemoryRuntime();
  const results: string[] = [];
  const runner = new WorkflowRunner(runtime, {
    router: {
      invalid: workflow(async function* (step) {
        for (const call of [
          () => step.start("start", { workflowId: "leaf" }, { detached: "yes" as never }),
          () => step.call("call", { workflowId: "leaf" }, { detached: 1 as never }),
        ]) {
          try {
            call();
          } catch (err) {
            results.push(`${(err as Error).name}: ${(err as Error).message}`);
          }
        }
      }),
    },
    logger,
  });
  await runtime.executions.admit({
    executionId: "invalid",
    workflowId: "invalid",
    params: undefined,
    context: new Map(),
  });
  const [lease] = await runtime.executions.claimDue({ limit: 1, duration: DEFAULT_LEASE_DURATION });
  await runner.run(
    { workflowId: "invalid", executionId: "invalid", params: undefined, context: new Map() },
    { lease: lease!, signal: new AbortController().signal }
  );
  expect(results).toEqual([
    "TypeError: step.start detached must be a boolean",
    "TypeError: step.call detached must be a boolean",
  ]);
});

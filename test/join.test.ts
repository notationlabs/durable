import { afterEach, beforeEach, describe, expect, expectTypeOf, test, vi } from "vitest";
import type { Logger } from "pino";
import {
  DEFAULT_LEASE_DURATION,
  InfrastructureError,
  WORKFLOW_RESULT_STEP_KEY,
  WorkflowRunner,
  createExecutionObserver,
  type ExecutionLease,
  type Runtime,
  type WorkflowEvent,
} from "@yieldstar/core";
import { MemoryRuntime } from "@yieldstar/test-runtime";
import { SqliteRuntime } from "@yieldstar/sqlite-runtime";
import {
  WorkflowJoinError,
  WorkflowStartError,
  registerErrorClass,
  workflow,
  type TimedJoinResult,
} from "yieldstar";

const isBun = "Bun" in globalThis;
const { createSqliteDb } = isBun
  ? await import("@yieldstar/sqlite-runtime/bun")
  : await import("@yieldstar/sqlite-runtime/node");

const logger = { info() {}, error() {}, warn() {}, debug() {} } as unknown as Logger;

const START = 1_000_000_000;
const MINUTE = 60_000;
const DAY = 24 * 60 * MINUTE;
const DEADLINE = START + DAY;
const AFTER_JOIN = 2 * DAY;
const TARGET = "ship-42";
const JOINER = "charge-42";

class CourierRejectedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CourierRejectedError";
  }
}

registerErrorClass("CourierRejectedError", CourierRejectedError);

type JoinParams = { target: string; deadline?: number };

/** What workflow code caught from a join, on each evaluation. */
let caught: unknown[] = [];
/** The params `call-ship` sends to its child, changed by tests between deliveries. */
let shipParams: { orderId: number } = { orderId: 42 };

function describeCaught(err: unknown) {
  const error = err as WorkflowJoinError;
  return {
    isJoinError: err instanceof WorkflowJoinError,
    code: error.code,
    executionId: error.executionId,
    cause: error.cause && {
      isRegisteredClass: error.cause instanceof CourierRejectedError,
      name: error.cause.name,
      message: error.cause.message,
    },
  };
}

const shipOrder = workflow(async function* (step, event: WorkflowEvent<{ orderId: number }>) {
  yield* step.delay("book-courier", MINUTE);
  return { tracking: `TRK-${event.params.orderId}` };
});

const router = {
  "join-ship": workflow(async function* (step, event) {
    const { target } = event.params as JoinParams;
    try {
      return yield* step.join("join", target);
    } catch (err) {
      caught.push(describeCaught(err));
      // A later step, so that the next delivery replays the join.
      yield* step.delay("after-join", AFTER_JOIN);
      return "handled";
    }
  }),
  "join-ship-then-wait": workflow(async function* (step, event) {
    const { target } = event.params as JoinParams;
    const result = yield* step.join("join", target);
    yield* step.delay("after-join", AFTER_JOIN);
    return result;
  }),
  "join-self": workflow(async function* (step, event) {
    try {
      return yield* step.join("join", event.executionId);
    } catch (err) {
      caught.push(describeCaught(err));
      return "handled";
    }
  }),
  // The deadline comes from params, so it is the same on every delivery.
  "timed-join-ship": workflow(async function* (step, event) {
    const { target, deadline } = event.params as JoinParams;
    const result = yield* step.join("join", target, { deadline: deadline! });
    // A later step, so a deadline wake that survived would cause a delivery.
    yield* step.delay("after-join", AFTER_JOIN);
    return result;
  }),
  // The deadline is recomputed from Date.now() on every delivery.
  "timed-join-ship-now": workflow(async function* (step, event) {
    const { target } = event.params as JoinParams;
    return yield* step.join("join", target, { deadline: Date.now() + DAY });
  }),
  "ship-order": shipOrder,
  "call-ship": workflow(async function* (step) {
    try {
      return yield* step.call<typeof shipOrder>("ship", {
        workflowId: "ship-order",
        params: shipParams,
      });
    } catch (err) {
      if (err instanceof WorkflowStartError) return { startError: err.code };
      throw err;
    }
  }),
  "call-ship-then-wait": workflow(async function* (step) {
    const shipped = yield* step.call<typeof shipOrder>("ship", {
      workflowId: "ship-order",
      params: shipParams,
    });
    yield* step.delay("after-join", AFTER_JOIN);
    return shipped;
  }),
  "timed-call-ship": workflow(async function* (step) {
    return yield* step.call<typeof shipOrder>(
      "ship",
      { workflowId: "ship-order", params: shipParams },
      { deadline: Date.now() + DAY }
    );
  }),
  "start-ship": workflow(async function* (step) {
    return yield* step.start("ship", { workflowId: "ship-order", params: shipParams });
  }),
};

type Harness = {
  runtime: Runtime;
  dispose(): void;
};

const runtimes: { name: string; create(): Harness }[] = [
  {
    name: "memory",
    create: () => ({ runtime: new MemoryRuntime(), dispose() {} }),
  },
  {
    name: isBun ? "sqlite-bun" : "sqlite-node",
    create() {
      const db = createSqliteDb({ path: ":memory:", wal: false });
      return { runtime: new SqliteRuntime(db), dispose: () => db.close() };
    },
  },
];

beforeEach(() => {
  caught = [];
  shipParams = { orderId: 42 };
  // Both runtimes read Date.now(), and so do the `-now` workflows.
  vi.useFakeTimers({ toFake: ["Date"], now: START });
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

function advance(ms: number) {
  vi.setSystemTime(Date.now() + ms);
}

function advanceTo(time: number) {
  vi.setSystemTime(time);
}

describe.each(runtimes)("step.join and step.call on $name", ({ create }) => {
  let harness: Harness;
  let runtime: Runtime;
  let runner: WorkflowRunner<typeof router>;

  beforeEach(() => {
    harness = create();
    runtime = harness.runtime;
    runner = new WorkflowRunner(runtime, { router, logger });
  });

  afterEach(() => harness.dispose());

  async function admit(executionId: string, workflowId: string, params?: Record<string, unknown>) {
    await runtime.executions.admit({ executionId, workflowId, params, context: new Map() });
  }

  async function admitJoiner(
    workflowId: keyof typeof router,
    params: JoinParams = { target: TARGET, deadline: DEADLINE },
    executionId = JOINER
  ) {
    await admit(executionId, workflowId, params);
  }

  async function claim(executionId: string): Promise<ExecutionLease | undefined> {
    const leases = await runtime.executions.claimDue({
      limit: 1,
      duration: DEFAULT_LEASE_DURATION,
      filter: { executionIds: [executionId] },
    });
    return leases[0];
  }

  /** Runs one delivery under `lease` without acknowledging it. */
  async function run(lease: ExecutionLease) {
    const record = await runtime.executions.get(lease.executionId);
    return runner.run(
      {
        workflowId: record!.workflowId as keyof typeof router,
        executionId: lease.executionId,
        params: record!.params,
        context: new Map(),
      },
      { lease, signal: new AbortController().signal }
    );
  }

  /** Claims the execution if it is due, runs it and acknowledges the outcome. */
  async function deliver(executionId = JOINER) {
    const lease = await claim(executionId);
    if (!lease) return undefined;
    const outcome = await run(lease);
    await runtime.executions.acknowledge({ lease, outcome });
    return outcome;
  }

  /** A target outside the router, finished directly through its record. */
  async function admitTarget(executionId = TARGET) {
    await admit(executionId, "fake-target");
  }

  async function finishTarget(
    outcome: { status: "completed"; result: unknown } | { status: "failed"; error: Error },
    executionId = TARGET
  ) {
    const lease = await claim(executionId);
    if (!lease) throw new Error(`could not lease "${executionId}"`);
    await runtime.executions.acknowledge({ lease, outcome });
  }

  async function readStep(executionId = JOINER, stepKey = "join") {
    const record = await runtime.heap.readStep({ executionId, stepKey });
    return record && { ...JSON.parse(record.stepResponseJson), done: record.meta.done };
  }

  async function childOf(caller = JOINER) {
    const page = await runtime.executions.list({ workflowId: "ship-order" });
    return page.executions.filter((record) => record.parent?.executionId === caller);
  }

  const waiters = (target = TARGET) => runtime.joins.getWaiters(target);

  const spyJoin = () => vi.spyOn(runtime.joins, "join");

  describe("join", () => {
    test("a completed target returns its result immediately and registers no waiter", async () => {
      await admitTarget();
      await finishTarget({ status: "completed", result: { tracking: "TRK-42" } });
      await admitJoiner("join-ship");

      expect(await deliver()).toEqual({ status: "completed", result: { tracking: "TRK-42" } });
      expect(await waiters()).toEqual([]);
      expect(await readStep()).toEqual({
        type: "step-result",
        result: { tracking: "TRK-42" },
        done: true,
      });
    });

    test("a running target suspends the joiner, and its completion wakes the joiner with the result", async () => {
      await admitTarget();
      await admitJoiner("join-ship");

      expect(await deliver()).toEqual({ status: "suspended", wakeAt: undefined });
      expect(await readStep()).toEqual({ type: "join-wait", done: false });
      expect(await waiters()).toEqual([
        { targetExecutionId: TARGET, executionId: JOINER, stepKey: "join" },
      ]);
      expect(await claim(JOINER)).toBeUndefined();

      await finishTarget({ status: "completed", result: { tracking: "TRK-42" } });
      // Woken, but the joiner has not recorded its result yet.
      expect(await waiters()).toHaveLength(1);

      expect(await deliver()).toEqual({ status: "completed", result: { tracking: "TRK-42" } });
      expect(await waiters()).toEqual([]);
    });

    test("a suspended target is joined like a running one", async () => {
      await admitTarget();
      await runtime.executions.acknowledge({
        lease: (await claim(TARGET))!,
        outcome: { status: "suspended", wakeAt: START + MINUTE },
      });
      await admitJoiner("join-ship");

      expect(await deliver()).toEqual({ status: "suspended", wakeAt: undefined });
      advance(MINUTE);
      await finishTarget({ status: "completed", result: "booked" });
      expect(await deliver()).toEqual({ status: "completed", result: "booked" });
    });

    test("a failed target is thrown as JOINED_EXECUTION_FAILED with its error as cause; replay throws it again without reading the target", async () => {
      await admitTarget();
      await finishTarget({
        status: "failed",
        error: new CourierRejectedError("address rejected"),
      });
      await admitJoiner("join-ship");
      const join = spyJoin();

      await deliver();
      expect(await readStep()).toMatchObject({
        type: "step-error",
        err: { name: "WorkflowJoinError", code: "JOINED_EXECUTION_FAILED" },
        done: true,
      });

      advance(AFTER_JOIN);
      expect(await deliver()).toEqual({ status: "completed", result: "handled" });
      const expected = {
        isJoinError: true,
        code: "JOINED_EXECUTION_FAILED",
        executionId: TARGET,
        cause: {
          isRegisteredClass: true,
          name: "CourierRejectedError",
          message: "address rejected",
        },
      };
      expect(caught).toEqual([expected, expected]);
      expect(join).toHaveBeenCalledTimes(1);
    });

    test("a target that fails while joined wakes the joiner with the failure", async () => {
      await admitTarget();
      await admitJoiner("join-ship");
      await deliver();

      await finishTarget({ status: "failed", error: new CourierRejectedError("address rejected") });
      await deliver();
      expect(caught).toEqual([
        expect.objectContaining({ code: "JOINED_EXECUTION_FAILED", executionId: TARGET }),
      ]);
      expect(await waiters()).toEqual([]);
    });

    test("an unknown target is recorded and thrown as JOIN_TARGET_NOT_FOUND; replay throws it without reading the target", async () => {
      await admitJoiner("join-ship", { target: "missing" });
      const join = spyJoin();

      await deliver();
      expect(await readStep()).toMatchObject({
        type: "step-error",
        err: { name: "WorkflowJoinError", code: "JOIN_TARGET_NOT_FOUND" },
      });

      advance(AFTER_JOIN);
      expect(await deliver()).toEqual({ status: "completed", result: "handled" });
      const expected = {
        isJoinError: true,
        code: "JOIN_TARGET_NOT_FOUND",
        executionId: "missing",
        cause: undefined,
      };
      expect(caught).toEqual([expected, expected]);
      expect(join).toHaveBeenCalledTimes(1);
    });

    test("joining the calling execution is recorded and thrown as JOIN_SELF", async () => {
      await admitJoiner("join-self");
      const join = spyJoin();

      expect(await deliver()).toEqual({ status: "completed", result: "handled" });
      expect(caught).toEqual([
        { isJoinError: true, code: "JOIN_SELF", executionId: JOINER, cause: undefined },
      ]);
      expect(await readStep()).toMatchObject({
        type: "step-error",
        err: { code: "JOIN_SELF" },
      });
      expect(join).not.toHaveBeenCalled();
    });

    test("two joiners of one target are both woken with the same outcome", async () => {
      await admitTarget();
      await admitJoiner("join-ship", { target: TARGET }, "charge-a");
      await admitJoiner("join-ship", { target: TARGET }, "charge-b");
      await deliver("charge-a");
      await deliver("charge-b");
      expect((await waiters()).map((w) => w.executionId)).toEqual(["charge-a", "charge-b"]);

      await finishTarget({ status: "completed", result: "booked" });
      expect(await deliver("charge-a")).toEqual({ status: "completed", result: "booked" });
      expect(await deliver("charge-b")).toEqual({ status: "completed", result: "booked" });
      expect(await waiters()).toEqual([]);
    });

    test("a recorded join replays its value without reading the target", async () => {
      await admitTarget();
      await finishTarget({ status: "completed", result: "booked" });
      await admitJoiner("join-ship-then-wait");
      const join = spyJoin();

      expect(await deliver()).toEqual({ status: "suspended", wakeAt: START + AFTER_JOIN });
      advance(AFTER_JOIN);
      expect(await deliver()).toEqual({ status: "completed", result: "booked" });
      expect(join).toHaveBeenCalledTimes(1);
    });

    test("a JoinClient that throws ends the delivery as an infrastructure error, which workflow code cannot catch", async () => {
      await admitTarget();
      await admitJoiner("join-ship");
      spyJoin().mockRejectedValueOnce(new Error("database is locked"));

      const lease = (await claim(JOINER))!;
      await expect(run(lease)).rejects.toBeInstanceOf(InfrastructureError);
      expect(caught).toEqual([]);
      expect(await readStep()).toBeNull();
    });
  });

  describe("timed join", () => {
    test("an unresolved join records its deadline and suspends with it as wakeAt", async () => {
      await admitTarget();
      await admitJoiner("timed-join-ship");

      expect(await deliver()).toEqual({ status: "suspended", wakeAt: DEADLINE });
      expect(await readStep()).toEqual({ type: "join-wait", wakeAt: DEADLINE, done: false });
      expect(await waiters()).toHaveLength(1);
    });

    test("the deadline passing first records timed-out and deletes the waiter; a later completion changes nothing", async () => {
      await admitTarget();
      await admitJoiner("timed-join-ship");
      await deliver();

      advanceTo(DEADLINE);
      expect(await deliver()).toEqual({ status: "suspended", wakeAt: DEADLINE + AFTER_JOIN });
      expect(await readStep()).toEqual({
        type: "step-result",
        result: { status: "timed-out" },
        done: true,
      });
      expect(await waiters()).toEqual([]);

      await finishTarget({ status: "completed", result: "booked" });
      expect(await claim(JOINER)).toBeUndefined();
      advanceTo(DEADLINE + AFTER_JOIN);
      expect(await deliver()).toEqual({ status: "completed", result: { status: "timed-out" } });
    });

    test("the target completing first records completed, and no delivery follows for the deadline", async () => {
      await admitTarget();
      await admitJoiner("timed-join-ship");
      await deliver();

      advance(5 * MINUTE);
      await finishTarget({ status: "completed", result: "booked" });
      expect(await deliver()).toEqual({
        status: "suspended",
        wakeAt: START + 5 * MINUTE + AFTER_JOIN,
      });
      expect(await readStep()).toEqual({
        type: "step-result",
        result: { status: "completed", result: "booked" },
        done: true,
      });

      advanceTo(DEADLINE);
      expect(await claim(JOINER)).toBeUndefined();
    });

    test("a deadline computed from Date.now() is recorded before the waiter, and replays reuse it", async () => {
      await admitTarget();
      await admitJoiner("timed-join-ship-now");
      // The delivery stops after recording the deadline, before registering.
      spyJoin().mockRejectedValueOnce(new Error("database is locked"));

      const lease = (await claim(JOINER))!;
      await expect(run(lease)).rejects.toBeInstanceOf(InfrastructureError);
      expect(await readStep()).toEqual({ type: "join-wait", wakeAt: DEADLINE, done: false });
      expect(await waiters()).toEqual([]);

      advance(60 * MINUTE);
      expect(await run(lease)).toEqual({ status: "suspended", wakeAt: DEADLINE });
      advance(60 * MINUTE);
      expect(await run(lease)).toEqual({ status: "suspended", wakeAt: DEADLINE });
      expect(await readStep()).toEqual({ type: "join-wait", wakeAt: DEADLINE, done: false });
    });

    test("a terminal target wins after the deadline has passed", async () => {
      await admitTarget();
      await finishTarget({ status: "completed", result: "booked" });
      advanceTo(DEADLINE + MINUTE);
      await admitJoiner("timed-join-ship");

      await deliver();
      expect(await readStep()).toEqual({
        type: "step-result",
        result: { status: "completed", result: "booked" },
        done: true,
      });
    });

    test("a target failing before the deadline throws WorkflowJoinError", async () => {
      await admitTarget();
      await admitJoiner("timed-join-ship");
      await deliver();

      await finishTarget({ status: "failed", error: new CourierRejectedError("address rejected") });
      const outcome = await deliver();
      expect(outcome?.status).toBe("failed");
      const error = outcome?.status === "failed" ? outcome.error : undefined;
      expect(error).toBeInstanceOf(WorkflowJoinError);
      expect(describeCaught(error)).toEqual({
        isJoinError: true,
        code: "JOINED_EXECUTION_FAILED",
        executionId: TARGET,
        cause: {
          isRegisteredClass: true,
          name: "CourierRejectedError",
          message: "address rejected",
        },
      });
      expect(await waiters()).toEqual([]);
    });
  });

  describe("call", () => {
    test("a resolved call records one step under the caller's key and returns the child's result", async () => {
      await admit(JOINER, "call-ship");
      const written = new Set<string>();
      const writeStep = runtime.heap.writeStep.bind(runtime.heap);
      vi.spyOn(runtime.heap, "writeStep").mockImplementation(async (params) => {
        written.add(params.stepKey);
        return writeStep(params);
      });

      expect(await deliver()).toEqual({ status: "suspended", wakeAt: undefined });
      const [child] = await childOf();
      await deliver(child!.executionId);
      advance(MINUTE);
      await deliver(child!.executionId);

      expect(await deliver()).toEqual({ status: "completed", result: { tracking: "TRK-42" } });
      expect(await readStep(JOINER, "ship")).toEqual({
        type: "step-result",
        result: { tracking: "TRK-42" },
        done: true,
      });
      expect([...written].filter((key) => key !== "book-courier").sort()).toEqual(
        ["ship", WORKFLOW_RESULT_STEP_KEY].sort()
      );
    });

    test("a crash between the start and the join leaves one child, and the replay joins it", async () => {
      await admit(JOINER, "call-ship");
      const start = vi.spyOn(runtime.executions, "start");
      spyJoin().mockRejectedValueOnce(new Error("database is locked"));

      const lease = (await claim(JOINER))!;
      await expect(run(lease)).rejects.toBeInstanceOf(InfrastructureError);
      expect(await childOf()).toHaveLength(1);
      expect(await readStep(JOINER, "ship")).toBeNull();

      expect(await run(lease)).toEqual({ status: "suspended", wakeAt: undefined });
      await runtime.executions.acknowledge({ lease, outcome: { status: "suspended" } });
      expect(start).toHaveBeenCalledTimes(2);
      await expect(start.mock.results[1]!.value).resolves.toEqual(
        await start.mock.results[0]!.value
      );
      const children = await childOf();
      expect(children).toHaveLength(1);
      const childId = children[0]!.executionId;
      expect(await waiters(childId)).toEqual([
        { targetExecutionId: childId, executionId: JOINER, stepKey: "ship" },
      ]);

      await deliver(childId);
      advance(MINUTE);
      await deliver(childId);
      expect(await deliver()).toEqual({ status: "completed", result: { tracking: "TRK-42" } });
      expect(start).toHaveBeenCalledTimes(3);
    });

    test("a resolved call replays without starting or joining", async () => {
      await admit(JOINER, "call-ship-then-wait");
      await deliver();
      const [child] = await childOf();
      await deliver(child!.executionId);
      advance(MINUTE);
      await deliver(child!.executionId);
      expect(await deliver()).toEqual({
        status: "suspended",
        wakeAt: START + MINUTE + AFTER_JOIN,
      });

      const start = vi.spyOn(runtime.executions, "start");
      const join = spyJoin();
      advance(AFTER_JOIN);
      expect(await deliver()).toEqual({ status: "completed", result: { tracking: "TRK-42" } });
      expect(start).not.toHaveBeenCalled();
      expect(join).not.toHaveBeenCalled();
    });

    test("a changed request throws WorkflowStartError with START_REQUEST_MISMATCH", async () => {
      await admit(JOINER, "call-ship");
      await deliver();
      const [child] = await childOf();
      await deliver(child!.executionId);
      advance(MINUTE);

      shipParams = { orderId: 43 };
      await deliver(child!.executionId);
      expect(await deliver()).toEqual({
        status: "completed",
        result: { startError: "START_REQUEST_MISMATCH" },
      });
      expect(await readStep(JOINER, "ship")).toMatchObject({
        type: "step-error",
        err: { name: "WorkflowStartError", code: "START_REQUEST_MISMATCH" },
      });
    });

    test("a timed call returns completed with the child's result", async () => {
      await admit(JOINER, "timed-call-ship");
      expect(await deliver()).toEqual({ status: "suspended", wakeAt: DEADLINE });
      const [child] = await childOf();
      await deliver(child!.executionId);
      advance(MINUTE);
      await deliver(child!.executionId);
      expect(await deliver()).toEqual({
        status: "completed",
        result: { status: "completed", result: { tracking: "TRK-42" } },
      });
    });

    test("a timed call whose child outlasts the deadline returns timed-out", async () => {
      await admit(JOINER, "timed-call-ship");
      await deliver();
      advanceTo(DEADLINE);
      expect(await deliver()).toEqual({ status: "completed", result: { status: "timed-out" } });
      const [child] = await childOf();
      expect(await waiters(child!.executionId)).toEqual([]);
    });
  });

  describe("parent link", () => {
    test.each(["call-ship", "start-ship"] as const)(
      "a child started by %s has the caller's ID and step key as parent on its record and view",
      async (workflowId) => {
        await admit(JOINER, workflowId);
        await deliver();
        const [child] = await childOf();
        expect(child?.parent).toEqual({ executionId: JOINER, stepKey: "ship" });
        const observer = createExecutionObserver(runtime.executions);
        expect(await observer.poll(child!.executionId)).toMatchObject({
          parent: { executionId: JOINER, stepKey: "ship" },
        });
        expect((await observer.list({})).executions.find((v) => v.executionId === child!.executionId))
          .toMatchObject({ parent: { executionId: JOINER, stepKey: "ship" } });
      }
    );

    test("an execution admitted by trigger has no parent", async () => {
      await admit(JOINER, "call-ship");
      expect((await runtime.executions.get(JOINER))?.parent).toBeUndefined();
      const observer = createExecutionObserver(runtime.executions);
      expect(await observer.poll(JOINER)).not.toHaveProperty("parent");
    });
  });
});

test("step.join and step.call validate their arguments in workflow code", async () => {
  const runtime = new MemoryRuntime();
  const results: string[] = [];
  const runner = new WorkflowRunner(runtime, {
    router: {
      invalid: workflow(async function* (step) {
        for (const call of [
          () => step.join("", "target"),
          () => step.join("join", ""),
          () => step.join("join", 42 as unknown as string),
          () => step.join("join", "target", { deadline: NaN }),
          () => step.call("", { workflowId: "ship-order" }),
          () => step.call("call", { workflowId: "ship-order" }, { deadline: Infinity }),
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
    "Error: step.join requires a non-empty step key",
    "Error: step.join requires a non-empty execution ID",
    "TypeError: step.join requires the execution ID to be a string",
    "TypeError: step.join deadline must be a finite number",
    "Error: step.call requires a non-empty step key",
    "TypeError: step.call deadline must be a finite number",
  ]);
});

test("step.call checks params and types the result by the child workflow", () => {
  const shipOrder = workflow(async function* (_step, event: WorkflowEvent<{ orderId: number }>) {
    return { tracking: `TRK-${event.params.orderId}` };
  });

  workflow(async function* (step) {
    const shipped = yield* step.call<typeof shipOrder>("ship", {
      workflowId: "ship-order",
      params: { orderId: 42 },
    });
    expectTypeOf(shipped).toEqualTypeOf<{ tracking: string }>();

    const timed = yield* step.call<typeof shipOrder>(
      "ship-timed",
      { workflowId: "ship-order", params: { orderId: 42 } },
      { deadline: 0 }
    );
    expectTypeOf(timed).toEqualTypeOf<TimedJoinResult<{ tracking: string }>>();

    yield* step.call<typeof shipOrder>("ship-wrong", {
      workflowId: "ship-order",
      // @ts-expect-error orderId is a number
      params: { orderId: "42" },
    });

    const joined = yield* step.join<{ tracking: string }>("join", "ship-42");
    expectTypeOf(joined).toEqualTypeOf<{ tracking: string }>();
    const timedJoin = yield* step.join<string>("join-timed", "ship-42", { deadline: 0 });
    expectTypeOf(timedJoin).toEqualTypeOf<TimedJoinResult<string>>();
  });
});

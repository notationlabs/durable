import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import type { Logger } from "pino";
import {
  DEFAULT_LEASE_DURATION,
  InfrastructureError,
  WorkflowRunner,
  redeliveryDelay,
  type ExecutionLease,
  type Runtime,
  type StoreWaiter,
} from "@yieldstar/core";
import { MemoryRuntime } from "@yieldstar/test-runtime";
import { SqliteRuntime } from "@yieldstar/sqlite-runtime";
import { testSchema } from "@yieldstar/test-utils";
import { defineStore, workflow, type TimedWhenResult } from "yieldstar";

const isBun = "Bun" in globalThis;
const { createSqliteDb } = isBun
  ? await import("@yieldstar/sqlite-runtime/bun")
  : await import("@yieldstar/sqlite-runtime/node");

const logger = { info() {}, error() {}, warn() {}, debug() {} } as unknown as Logger;

const START = 1_000_000_000;
const MINUTE = 60_000;
const DAY = 24 * 60 * MINUTE;
const DEADLINE = START + DAY;
const AFTER_DECISION = 2 * DAY;
const EXECUTION_ID = "order-42";
const STEP_KEY = "decision:order-42";

type Approval = { decision?: "approved" | "rejected" };

const ApprovalStore = defineStore("approval", testSchema<Approval>(), {
  initial: () => ({}),
});

let evaluations = 0;

const router = {
  // The deadline comes from params, so it is the same on every delivery.
  "review-order": workflow(async function* (step, event) {
    const approval = step.store(ApprovalStore, { id: EXECUTION_ID });
    evaluations++;
    const result = yield* approval.when(STEP_KEY, (state) => state.decision, {
      deadline: (event.params as { deadline: number }).deadline,
    });
    // A later step, so a deadline wake that survived would cause a delivery.
    yield* step.delay("after-decision", AFTER_DECISION);
    return result;
  }),
  // The deadline is recomputed from Date.now() on every delivery.
  "review-order-now": workflow(async function* (step) {
    const approval = step.store(ApprovalStore, { id: EXECUTION_ID });
    return yield* approval.when(STEP_KEY, (state) => state.decision, {
      deadline: Date.now() + DAY,
    });
  }),
  // A rejection wakes this selector but does not match it.
  "approve-order-now": workflow(async function* (step) {
    const approval = step.store(ApprovalStore, { id: EXECUTION_ID });
    return yield* approval.when(
      STEP_KEY,
      (state) => state.decision === "approved" && state.decision,
      { deadline: Date.now() + DAY }
    );
  }),
};

// An earlier version of `review-order` that waited under the same key
// without a deadline.
const untimedRouter = {
  "review-order": workflow(async function* (step) {
    const approval = step.store(ApprovalStore, { id: EXECUTION_ID });
    return yield* approval.when(STEP_KEY, (state) => state.decision);
  }),
};

type Harness = {
  runtime: Runtime;
  /** Opens the same backend in a new runtime, as after a worker restart. */
  restart(): Runtime;
  /** The step keys of the registered store waiters. */
  waiters(): string[];
  dispose(): void;
};

const runtimes: { name: string; create(): Harness }[] = [
  {
    name: "memory",
    create() {
      const runtime = new MemoryRuntime();
      return {
        runtime,
        restart: () => runtime,
        waiters: () =>
          [...(runtime.stores as unknown as { waiters: Map<string, StoreWaiter> }).waiters.values()].map(
            (waiter) => waiter.stepKey
          ),
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
        restart: () => new SqliteRuntime(db),
        waiters: () =>
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
  evaluations = 0;
  // Both runtimes read Date.now(), and so does `review-order-now`.
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

describe.each(runtimes)("timed store.when on $name", ({ create }) => {
  let harness: Harness;
  let runtime: Runtime;
  let runner: WorkflowRunner<typeof router>;

  beforeEach(() => {
    harness = create();
    useRuntime(harness.runtime);
  });

  afterEach(() => harness.dispose());

  function useRuntime(next: Runtime) {
    runtime = next;
    runner = new WorkflowRunner(runtime, { router, logger });
  }

  async function admit(workflowId: keyof typeof router = "review-order") {
    await runtime.executions.admit({
      executionId: EXECUTION_ID,
      workflowId,
      params: { deadline: DEADLINE },
      context: new Map(),
    });
  }

  async function claim(): Promise<ExecutionLease[]> {
    return runtime.executions.claimDue({ limit: 10, duration: DEFAULT_LEASE_DURATION });
  }

  /** Runs one delivery under `lease` without acknowledging it. */
  async function run(lease: ExecutionLease) {
    const record = await runtime.executions.get(lease.executionId);
    return runner.run(
      {
        workflowId: record!.workflowId,
        executionId: lease.executionId,
        params: record!.params,
        context: new Map(),
      },
      { lease, signal: new AbortController().signal }
    );
  }

  /** Claims the execution if it is due, runs it and acknowledges the outcome. */
  async function deliver() {
    const leases = await claim();
    if (leases.length === 0) return undefined;
    const outcome = await run(leases[0]!);
    await runtime.executions.acknowledge({ lease: leases[0]!, outcome });
    return outcome;
  }

  async function decide(decision: Approval["decision"]) {
    await runtime.stores.updateStore({
      definition: ApprovalStore,
      id: EXECUTION_ID,
      updater(draft) {
        if (decision === undefined) delete draft.decision;
        else draft.decision = decision;
      },
    });
  }

  async function readStep() {
    const record = await runtime.heap.readStep({
      executionId: EXECUTION_ID,
      stepKey: STEP_KEY,
    });
    return record && { ...JSON.parse(record.stepResponseJson), done: record.meta.done };
  }

  test("an unresolved wait records its deadline and suspends with it as wakeAt", async () => {
    await admit();
    expect(await deliver()).toEqual({ status: "suspended", wakeAt: DEADLINE });
    expect(await readStep()).toEqual({ type: "store-wait", wakeAt: DEADLINE, done: false });
    expect(await runtime.executions.get(EXECUTION_ID)).toMatchObject({ status: "suspended" });
  });

  test("a selector that already matches returns matched, even after the deadline", async () => {
    await decide("approved");
    advanceTo(DEADLINE + MINUTE);
    await admit();
    expect(await deliver()).toEqual({
      status: "suspended",
      wakeAt: DEADLINE + MINUTE + AFTER_DECISION,
    });
    expect(await readStep()).toEqual({
      type: "step-result",
      result: { status: "matched", value: "approved" },
      done: true,
    });
  });

  // History 1
  test("a match before the deadline returns the value, and no delivery follows for the deadline", async () => {
    await admit();
    await deliver();

    advance(5 * MINUTE);
    await decide("approved");
    expect(await deliver()).toEqual({
      status: "suspended",
      wakeAt: START + 5 * MINUTE + AFTER_DECISION,
    });
    expect(await readStep()).toMatchObject({
      result: { status: "matched", value: "approved" },
      done: true,
    });

    // The waiter went with the result, and the deadline wake did not survive
    // the acknowledgement.
    await decide("rejected");
    expect(await claim()).toEqual([]);
    advanceTo(DEADLINE);
    expect(await claim()).toEqual([]);

    advanceTo(START + 5 * MINUTE + AFTER_DECISION);
    expect(await deliver()).toEqual({
      status: "completed",
      result: { status: "matched", value: "approved" },
    });
  });

  // History 2
  test("a timeout recorded first is not replaced by a later matching update", async () => {
    await admit();
    await deliver();

    // The deadline never ends the wait early.
    advanceTo(DEADLINE - 1);
    expect(await claim()).toEqual([]);
    await runtime.executions.requestWake({ executionId: EXECUTION_ID, wakeAt: Date.now() });
    expect(await deliver()).toEqual({ status: "suspended", wakeAt: DEADLINE });

    advanceTo(DEADLINE);
    expect(await deliver()).toEqual({
      status: "suspended",
      wakeAt: DEADLINE + AFTER_DECISION,
    });
    expect(await readStep()).toMatchObject({ result: { status: "timed-out" }, done: true });

    advance(60 * MINUTE);
    await decide("approved");
    expect(await claim()).toEqual([]);

    advanceTo(DEADLINE + AFTER_DECISION);
    expect(await deliver()).toEqual({
      status: "completed",
      result: { status: "timed-out" },
    });
  });

  // History 3
  test("a match committed at the deadline wins, and a restart returns the same result", async () => {
    await admit();
    await deliver();

    advanceTo(DEADLINE);
    await decide("approved");
    const [lease] = await claim();
    const outcome = await run(lease!);
    expect(outcome).toEqual({ status: "suspended", wakeAt: DEADLINE + AFTER_DECISION });

    // The worker dies before acknowledging; another worker takes over.
    useRuntime(harness.restart());
    advance(DEFAULT_LEASE_DURATION + redeliveryDelay(1));
    await decide("rejected");
    await deliver();
    advanceTo(DEADLINE + AFTER_DECISION);
    expect(await deliver()).toEqual({
      status: "completed",
      result: { status: "matched", value: "approved" },
    });
  });

  test("a timeout recorded at the deadline before a match commits wins, and a restart returns it", async () => {
    await admit();
    await deliver();

    advanceTo(DEADLINE);
    const [lease] = await claim();
    await run(lease!);
    await decide("approved");

    useRuntime(harness.restart());
    advance(DEFAULT_LEASE_DURATION + redeliveryDelay(1));
    await deliver();
    advanceTo(DEADLINE + AFTER_DECISION);
    expect(await deliver()).toEqual({
      status: "completed",
      result: { status: "timed-out" },
    });
  });

  /**
   * Makes the first step write after a successful waiter registration reject,
   * as when the worker dies after registering the waiter and before the
   * delivery is acknowledged.
   */
  function failWriteAfterRegistration() {
    let registered = false;
    const registerWaiter = runtime.stores.registerWaiter.bind(runtime.stores);
    vi.spyOn(runtime.stores, "registerWaiter").mockImplementation(async (waiter) => {
      await registerWaiter(waiter);
      registered = true;
    });
    const writeStep = runtime.heap.writeStep.bind(runtime.heap);
    let failed = false;
    vi.spyOn(runtime.heap, "writeStep").mockImplementation(async (params) => {
      if (registered && !failed) {
        failed = true;
        throw new Error("disk full");
      }
      return writeStep(params);
    });
  }

  // History 4
  test("a crash after the waiter is registered, before acknowledgement, leaves the waiter and the deadline", async () => {
    await admit();
    failWriteAfterRegistration();
    const [lease] = await claim();
    await expect(run(lease!)).rejects.toBeInstanceOf(InfrastructureError);
    expect(harness.waiters()).toEqual([STEP_KEY]);
    expect(await readStep()).toEqual({ type: "store-wait", wakeAt: DEADLINE, done: false });
    await runtime.executions.release({ lease: lease! });

    // The registered waiter wakes the redelivery with the decision.
    await decide("approved");
    advance(redeliveryDelay(1));
    expect(await deliver()).toEqual({
      status: "suspended",
      wakeAt: Date.now() + AFTER_DECISION,
    });
    expect(await readStep()).toMatchObject({
      result: { status: "matched", value: "approved" },
      done: true,
    });
    expect(harness.waiters()).toEqual([]);
  });

  test("a crash after the deadline is recorded, before the waiter is registered, is finished by redelivery", async () => {
    await admit();
    vi.spyOn(runtime.stores, "registerWaiter").mockRejectedValueOnce(new Error("disk full"));
    const [lease] = await claim();
    await expect(run(lease!)).rejects.toBeInstanceOf(InfrastructureError);
    await runtime.executions.release({ lease: lease! });
    expect(await readStep()).toEqual({ type: "store-wait", wakeAt: DEADLINE, done: false });
    expect(harness.waiters()).toEqual([]);

    advance(redeliveryDelay(1));
    expect(await deliver()).toEqual({ status: "suspended", wakeAt: DEADLINE });
    expect(harness.waiters()).toEqual([STEP_KEY]);

    // Only the waiter can make the execution due before the deadline.
    expect(await claim()).toEqual([]);
    await decide("approved");
    await deliver();
    expect(await readStep()).toMatchObject({
      result: { status: "matched", value: "approved" },
      done: true,
    });
  });

  test("a crash before the deadline is recorded leaves nothing installed, and redelivery arms both wakes", async () => {
    await admit();
    vi.spyOn(runtime.heap, "writeStep").mockRejectedValueOnce(new Error("disk full"));
    const [lease] = await claim();
    await expect(run(lease!)).rejects.toBeInstanceOf(InfrastructureError);
    await runtime.executions.release({ lease: lease! });
    expect(await readStep()).toBeNull();
    expect(harness.waiters()).toEqual([]);

    advance(redeliveryDelay(1));
    expect(await deliver()).toEqual({ status: "suspended", wakeAt: DEADLINE });
    expect(await readStep()).toEqual({ type: "store-wait", wakeAt: DEADLINE, done: false });
    expect(harness.waiters()).toEqual([STEP_KEY]);

    advanceTo(DEADLINE);
    await deliver();
    expect(await readStep()).toMatchObject({ result: { status: "timed-out" }, done: true });
  });

  test("a crash after both wakes are installed, before acknowledgement, loses no outcome", async () => {
    await admit();
    const [lease] = await claim();
    expect(await run(lease!)).toEqual({ status: "suspended", wakeAt: DEADLINE });
    expect(await readStep()).toMatchObject({ type: "store-wait", wakeAt: DEADLINE });

    // The worker dies before acknowledging; a decision lands meanwhile.
    useRuntime(harness.restart());
    await decide("approved");
    advance(DEFAULT_LEASE_DURATION + redeliveryDelay(1));
    await deliver();
    expect(await readStep()).toMatchObject({
      result: { status: "matched", value: "approved" },
      done: true,
    });
  });

  test("a crash before acknowledgement without a decision re-arms the recorded deadline", async () => {
    await admit();
    const [lease] = await claim();
    await run(lease!);

    useRuntime(harness.restart());
    advance(DEFAULT_LEASE_DURATION + redeliveryDelay(1));
    expect(await deliver()).toEqual({ status: "suspended", wakeAt: DEADLINE });
    expect(evaluations).toBe(2);

    advanceTo(DEADLINE);
    await deliver();
    expect(await readStep()).toMatchObject({ result: { status: "timed-out" }, done: true });
  });

  // History 5
  test("a deadline computed from Date.now() is read from the record on later deliveries", async () => {
    await admit("review-order-now");
    expect(await deliver()).toEqual({ status: "suspended", wakeAt: DEADLINE });

    // A wake before the deadline suspends again until the recorded deadline.
    advanceTo(DEADLINE - 60 * MINUTE);
    await runtime.executions.requestWake({ executionId: EXECUTION_ID, wakeAt: Date.now() });
    expect(await deliver()).toEqual({ status: "suspended", wakeAt: DEADLINE });

    // Date.now() + DAY now lies in the future, but the recorded deadline passed.
    advanceTo(DEADLINE + MINUTE);
    expect(await deliver()).toEqual({
      status: "completed",
      result: { status: "timed-out" },
    });
  });

  test("a crash after the waiter is registered does not move a deadline computed from Date.now()", async () => {
    await admit("review-order-now");
    failWriteAfterRegistration();
    const [lease] = await claim();
    await expect(run(lease!)).rejects.toBeInstanceOf(InfrastructureError);
    await runtime.executions.release({ lease: lease! });

    // The redelivery computes Date.now() + DAY later than the first delivery.
    advance(60 * MINUTE);
    expect(await deliver()).toEqual({ status: "suspended", wakeAt: DEADLINE });
    expect(await readStep()).toEqual({ type: "store-wait", wakeAt: DEADLINE, done: false });

    advanceTo(DEADLINE);
    expect(await deliver()).toEqual({
      status: "completed",
      result: { status: "timed-out" },
    });
  });

  test("a commit that does not match wakes the execution, which suspends again with the recorded deadline", async () => {
    await admit("approve-order-now");
    expect(await deliver()).toEqual({ status: "suspended", wakeAt: DEADLINE });

    advance(60 * MINUTE);
    await decide("rejected");
    expect(await deliver()).toEqual({ status: "suspended", wakeAt: DEADLINE });
    expect(await readStep()).toEqual({ type: "store-wait", wakeAt: DEADLINE, done: false });
    expect(harness.waiters()).toEqual([STEP_KEY]);

    await decide("approved");
    expect(await deliver()).toEqual({
      status: "completed",
      result: { status: "matched", value: "approved" },
    });
  });

  test("an unfinished record without a deadline, left by an untimed when under the same key, uses the argument", async () => {
    await admit();
    const untimedRunner = new WorkflowRunner(runtime, { router: untimedRouter, logger });
    const [lease] = await claim();
    const outcome = await untimedRunner.run(
      {
        workflowId: "review-order",
        executionId: EXECUTION_ID,
        params: { deadline: DEADLINE },
        context: new Map(),
      },
      { lease: lease!, signal: new AbortController().signal }
    );
    expect(outcome).toEqual({ status: "suspended" });
    await runtime.executions.acknowledge({ lease: lease!, outcome });
    expect(await readStep()).toEqual({ type: "store-wait", done: false });

    // The timed version of the workflow is deployed and woken.
    await runtime.executions.requestWake({ executionId: EXECUTION_ID, wakeAt: Date.now() });
    expect(await deliver()).toEqual({ status: "suspended", wakeAt: DEADLINE });
    expect(await readStep()).toEqual({ type: "store-wait", wakeAt: DEADLINE, done: false });

    advanceTo(DEADLINE);
    await deliver();
    expect(await readStep()).toMatchObject({ result: { status: "timed-out" }, done: true });
  });

  // History 6
  test("a stale holder's result is rejected and the current holder's waiter stays registered", async () => {
    await decide("approved");
    await admit();

    let releaseStale!: () => void;
    const staleGate = new Promise<void>((resolve) => {
      releaseStale = resolve;
    });
    const writeStep = runtime.heap.writeStep.bind(runtime.heap);
    vi.spyOn(runtime.heap, "writeStep")
      .mockImplementationOnce(async (params) => {
        await staleGate;
        return writeStep(params);
      })
      .mockImplementation(writeStep);

    // The stale holder matches and stalls before recording its result.
    const [stale] = await claim();
    const staleRun = run(stale!);
    staleRun.catch(() => {});
    await vi.waitFor(() => expect(runtime.heap.writeStep).toHaveBeenCalledTimes(1));

    // Its lease expires; the decision is withdrawn; the current holder waits.
    advance(DEFAULT_LEASE_DURATION + redeliveryDelay(1));
    await decide(undefined);
    expect(await deliver()).toEqual({ status: "suspended", wakeAt: DEADLINE });

    releaseStale();
    await expect(staleRun).rejects.toMatchObject({
      name: "InfrastructureError",
      cause: { name: "LeaseLostError" },
    });
    expect(await readStep()).toEqual({ type: "store-wait", wakeAt: DEADLINE, done: false });

    // The current waiter still wakes the execution.
    await decide("rejected");
    await deliver();
    expect(await readStep()).toMatchObject({
      result: { status: "matched", value: "rejected" },
      done: true,
    });
  });
});

test("timed when requires an explicit key and a finite deadline", async () => {
  const runtime = new MemoryRuntime();
  const results: unknown[] = [];
  const runner = new WorkflowRunner(runtime, {
    router: {
      invalid: workflow(async function* (step) {
        const approval = step.store(ApprovalStore, { id: "invalid" });
        for (const call of [
          // @ts-expect-error the timed form has no keyless variant
          () => approval.when((state) => state.decision, { deadline: Date.now() }),
          () => approval.when("nan", (state) => state.decision, { deadline: NaN }),
        ]) {
          try {
            call();
          } catch (err) {
            results.push((err as Error).message);
          }
        }
        const untimed: "approved" | "rejected" = yield* approval.when("untimed", () => "approved" as const);
        const timed: TimedWhenResult<"approved"> = yield* approval.when(
          "timed",
          () => "approved" as const,
          { deadline: Date.now() }
        );
        return { untimed, timed };
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
  const outcome = await runner.run(
    { workflowId: "invalid", executionId: "invalid", params: undefined, context: new Map() },
    { lease: lease!, signal: new AbortController().signal }
  );
  expect(results).toEqual([
    "A timed store.when requires a non-empty step key",
    "store.when deadline must be a finite number",
  ]);
  expect(outcome).toEqual({
    status: "completed",
    result: { untimed: "approved", timed: { status: "matched", value: "approved" } },
  });
});

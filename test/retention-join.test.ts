import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import type { Logger } from "pino";
import {
  DEFAULT_LEASE_DURATION,
  InfrastructureError,
  WorkflowRunner,
  type ExecutionLease,
  type RetentionOptions,
  type Runtime,
  type WorkflowEvent,
} from "@yieldstar/core";
import { MemoryRuntime } from "@yieldstar/test-runtime";
import { SqliteRuntime } from "@yieldstar/sqlite-runtime";
import { WorkflowJoinError, workflow } from "yieldstar";
import { withoutVersion } from "./outcome";
import { liveRecord } from "@yieldstar/runtime-conformance";

const { createSqliteDb } =
  "Bun" in globalThis
    ? await import("@yieldstar/sqlite-runtime/bun")
    : await import("@yieldstar/sqlite-runtime/node");

const logger = { info() {}, error() {}, warn() {}, debug() {} } as unknown as Logger;

const START = 1_000_000_000;
const MINUTE = 60_000;
const DAY = 24 * 60 * MINUTE;
const TARGET = "ship-42";
const JOINER = "charge-42";
// Only targets are pruned, so joiners keep their records.
const RETAIN: RetentionOptions = {
  workflows: { "fake-target": { completed: 0 }, "ship-order": { completed: 0 } },
};

let caught: unknown[] = [];

function describeCaught(err: unknown) {
  const error = err as WorkflowJoinError;
  return { isJoinError: err instanceof WorkflowJoinError, code: error.code, executionId: error.executionId };
}

const router = {
  "join-ship": workflow(async function* (step, event: WorkflowEvent<{ target: string }>) {
    try {
      return yield* step.join("join", event.params.target);
    } catch (err) {
      caught.push(describeCaught(err));
      // A later step, so that the next delivery replays the join.
      yield* step.delay("after-join", DAY);
      return "handled";
    }
  }),
  "call-ship": workflow(async function* (step) {
    try {
      return yield* step.call("ship", { workflowId: "ship-order" });
    } catch (err) {
      caught.push(describeCaught(err));
      yield* step.delay("after-join", DAY);
      return "handled";
    }
  }),
  "ship-order": workflow(async function* () {
    return { tracking: "TRK-42" };
  }),
};

const runtimes: { name: string; create(): { runtime: Runtime; dispose(): void } }[] = [
  { name: "memory", create: () => ({ runtime: new MemoryRuntime({ retain: RETAIN }), dispose() {} }) },
  {
    name: "sqlite",
    create() {
      const db = createSqliteDb({ path: ":memory:", wal: false });
      return { runtime: new SqliteRuntime(db, { retain: RETAIN }), dispose: () => db.close() };
    },
  },
];

beforeEach(() => {
  caught = [];
  vi.useFakeTimers({ toFake: ["Date"], now: START });
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe.each(runtimes)("joins of a pruned target on $name", ({ create }) => {
  let runtime: Runtime;
  let dispose: () => void;
  let runner: WorkflowRunner<typeof router>;

  beforeEach(() => {
    ({ runtime, dispose } = create());
    runner = new WorkflowRunner(runtime, { router, logger });
  });

  afterEach(() => dispose());

  async function admit(executionId: string, workflowId: string, params?: Record<string, unknown>) {
    await runtime.executions.admit({ executionId, workflowId, params, context: new Map() });
  }

  async function claim(executionId: string): Promise<ExecutionLease | undefined> {
    const [lease] = await runtime.executions.claimDue({
      limit: 1,
      duration: DEFAULT_LEASE_DURATION,
      filter: { executionIds: [executionId] },
    });
    return lease;
  }

  async function run(lease: ExecutionLease) {
    const record = await liveRecord(runtime.executions, lease.executionId);
    return withoutVersion(
      await runner.run(
        {
          workflowId: record.workflowId as keyof typeof router,
          executionId: lease.executionId,
          params: record.params,
          context: new Map(),
        },
        { lease, signal: new AbortController().signal }
      )
    );
  }

  async function deliver(executionId = JOINER) {
    const lease = await claim(executionId);
    if (!lease) return undefined;
    const outcome = await run(lease);
    await runtime.executions.acknowledge({ lease, outcome });
    return outcome;
  }

  async function readStep(stepKey: string) {
    const record = await runtime.heap.readStep({ executionId: JOINER, stepKey });
    return record && JSON.parse(record.stepResponseJson);
  }

  test("a join of a pruned target is recorded and thrown as JOIN_TARGET_PRUNED; replay throws it without reading the target", async () => {
    await admit(TARGET, "fake-target");
    const lease = (await claim(TARGET))!;
    await runtime.executions.acknowledge({
      lease,
      outcome: { status: "completed", result: { tracking: "TRK-42" } },
    });
    expect(await runtime.prune()).toMatchObject({ pruned: 1 });
    await admit(JOINER, "join-ship", { target: TARGET });
    const join = vi.spyOn(runtime.joins, "join");

    expect(await deliver()).toEqual({ status: "suspended", wakeAt: START + DAY });
    expect(await readStep("join")).toMatchObject({
      type: "step-error",
      err: { name: "WorkflowJoinError", code: "JOIN_TARGET_PRUNED" },
    });
    expect(await runtime.joins.getWaiters(TARGET)).toEqual([]);

    vi.setSystemTime(START + DAY);
    expect(await deliver()).toEqual({ status: "completed", result: "handled" });
    const expected = { isJoinError: true, code: "JOIN_TARGET_PRUNED", executionId: TARGET };
    expect(caught).toEqual([expected, expected]);
    expect(join).toHaveBeenCalledTimes(1);
  });

  test("a call whose child was pruned after a crash between the start and the join throws JOIN_TARGET_PRUNED", async () => {
    await admit(JOINER, "call-ship");
    vi.spyOn(runtime.joins, "join").mockRejectedValueOnce(new Error("database is locked"));
    const lease = (await claim(JOINER))!;
    await expect(run(lease)).rejects.toBeInstanceOf(InfrastructureError);

    // The child has no waiter, so it is pruned once it completes.
    const page = await runtime.executions.list({ workflowId: "ship-order" });
    const childId = page.executions[0]!.executionId;
    await deliver(childId);
    expect(await runtime.prune()).toMatchObject({ pruned: 1 });

    const outcome = await run(lease);
    await runtime.executions.acknowledge({ lease, outcome });
    expect(outcome).toEqual({ status: "suspended", wakeAt: START + DAY });
    expect(await readStep("ship")).toMatchObject({
      type: "step-error",
      err: { name: "WorkflowJoinError", code: "JOIN_TARGET_PRUNED" },
    });

    vi.setSystemTime(START + DAY);
    expect(await deliver()).toEqual({ status: "completed", result: "handled" });
    const expected = { isJoinError: true, code: "JOIN_TARGET_PRUNED", executionId: childId };
    expect(caught).toEqual([expected, expected]);
  });
});

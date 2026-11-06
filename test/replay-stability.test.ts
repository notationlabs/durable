import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import type { Logger } from "pino";
import {
  DEFAULT_LEASE_DURATION,
  InfrastructureError,
  WorkflowRunner,
  type ExecutionLease,
  type Runtime,
  type StandardSchemaV1,
  type WorkflowRouter,
} from "@yieldstar/core";
import { MemoryRuntime } from "@yieldstar/test-runtime";
import { SqliteRuntime } from "@yieldstar/sqlite-runtime";
import { testSchema } from "@yieldstar/test-utils";
import { WorkflowReplayError, defineStore, workflow } from "yieldstar";

const isBun = "Bun" in globalThis;
const { createSqliteDb } = isBun
  ? await import("@yieldstar/sqlite-runtime/bun")
  : await import("@yieldstar/sqlite-runtime/node");

const START = 1_000_000_000;
const MINUTE = 60_000;
const DAY = 24 * 60 * MINUTE;
const ORDER = "order-42";
const WORKFLOW = "place-order";

let warnings: { obj: unknown; msg: string }[] = [];
const logger = {
  info() {},
  error() {},
  debug() {},
  warn(obj: unknown, msg: string) {
    warnings.push({ obj, msg });
  },
} as unknown as Logger;

/** Step functions that ran, in order. */
let ran: string[] = [];
/** What workflow code saw. */
let observed: unknown[] = [];

function work<T>(name: string, value: T): () => T {
  return () => {
    ran.push(name);
    return value;
  };
}

const numberFromString: StandardSchemaV1<string, number> = {
  "~standard": {
    version: 1,
    vendor: "test",
    validate(value) {
      return typeof value === "string"
        ? { value: Number(value) }
        : { issues: [{ message: "expected a string" }] };
    },
  },
};

const rejectEverything: StandardSchemaV1<string, number> = {
  "~standard": {
    version: 1,
    vendor: "test",
    validate() {
      return { issues: [{ message: "rejected by the new schema" }] };
    },
  },
};

type Harness = { runtime: Runtime; dispose(): void };

const runtimes: { name: string; create(options?: { requireStepKeys?: boolean }): Harness }[] = [
  {
    name: "memory",
    create(options) {
      return { runtime: new MemoryRuntime(options), dispose() {} };
    },
  },
  {
    name: isBun ? "sqlite-bun" : "sqlite-node",
    create(options) {
      const db = createSqliteDb({ path: ":memory:", wal: false });
      return { runtime: new SqliteRuntime(db, options), dispose: () => db.close() };
    },
  },
];

beforeEach(() => {
  ran = [];
  observed = [];
  warnings = [];
  vi.useFakeTimers({ toFake: ["Date"], now: START });
});

afterEach(() => {
  vi.useRealTimers();
});

function advance(ms: number) {
  vi.setSystemTime(Date.now() + ms);
}

/** A runtime that reads `requireStepKeys` as true and shares every client with `runtime`. */
function requiringStepKeys(runtime: Runtime): Runtime {
  return Object.assign(Object.create(runtime), { requireStepKeys: true });
}

describe.each(runtimes)("replay stability on $name", ({ create }) => {
  let harness: Harness;
  let runtime: Runtime;
  let runner: WorkflowRunner<WorkflowRouter>;

  beforeEach(() => {
    harness = create();
    runtime = harness.runtime;
  });

  afterEach(() => harness.dispose());

  /** Deploys `router`: later deliveries run its code. */
  function deploy(router: WorkflowRouter, on: Runtime = runtime) {
    runner = new WorkflowRunner(on, { router, logger });
  }

  async function admit(executionId = ORDER) {
    await runtime.executions.admit({
      executionId,
      workflowId: WORKFLOW,
      params: undefined,
      context: new Map(),
    });
  }

  async function claim(executionId = ORDER): Promise<ExecutionLease | undefined> {
    const leases = await runtime.executions.claimDue({
      limit: 1,
      duration: DEFAULT_LEASE_DURATION,
      filter: { executionIds: [executionId] },
    });
    return leases[0];
  }

  /** Runs one delivery under `lease` as the dispatcher would, without acknowledging it. */
  async function run(lease: ExecutionLease, signal = new AbortController().signal) {
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
        signal,
        ...(record.cancellation ? { cancellation: record.cancellation } : {}),
      }
    );
  }

  /** Claims the execution if it is due, runs it and acknowledges the outcome. */
  async function deliver(executionId = ORDER) {
    const lease = await claim(executionId);
    if (!lease) return undefined;
    const outcome = await run(lease);
    await runtime.executions.acknowledge({ lease, outcome });
    return outcome;
  }

  const record = async (executionId = ORDER) => (await runtime.executions.get(executionId))!;

  const stepKeys = async (executionId = ORDER) =>
    (await runtime.heap.readHistory({ executionId })).stepKeys.sort();

  async function readStep(stepKey: string, executionId = ORDER) {
    const stored = await runtime.heap.readStep({ executionId, stepKey });
    return stored && { ...JSON.parse(stored.stepResponseJson), done: stored.meta.done };
  }

  /** v1 of place-order: reserve, charge, then wait a day for the courier. */
  const placeOrderV1 = workflow(async function* (step) {
    yield* step.run("reserve-stock", work("reserve-stock", "reserved"));
    yield* step.run("charge", work("charge", "charged"));
    yield* step.delay("courier", DAY);
    return "shipped";
  });

  async function suspendOnV1() {
    deploy({ [WORKFLOW]: placeOrderV1 });
    await admit();
    expect(await deliver()).toMatchObject({ status: "suspended" });
    ran = [];
  }

  describe("step kinds", () => {
    // The kind strings are stored with each record, and replay compares
    // them. Renaming one parks every execution that recorded the old string.
    test("each step records its kind under the same string as before", async () => {
      const Approval = defineStore("approval", testSchema<{ decision?: string }>(), {
        initial: () => ({ decision: "approved" }),
      });
      deploy({
        [WORKFLOW]: workflow(async function* (step) {
          const approval = step.store(Approval, { id: ORDER });
          yield* step.run("run", work("run", 1));
          yield* step.poll("poll", { retryInterval: 1, maxAttempts: 1 }, () => true);
          yield* step.patched("patch");
          yield* step.all("all", [step.run("in-all", work("in-all", 1))]);
          yield* step.race("race", [step.run("in-race", work("in-race", 1))]);
          yield* approval.get("store-get");
          yield* approval.select("store-select", (state) => state.decision);
          yield* approval.update("store-update", () => {});
          yield* approval.when("store-when", (state) => state.decision);
          yield* approval.when("store-when-timed", (state) => state.decision, { deadline: START + DAY });
          yield* step.delay("delay", MINUTE);
        }),
      });
      await admit();
      expect(await deliver()).toMatchObject({ status: "suspended" });
      const kinds: Record<string, string | undefined> = {};
      for (const key of await stepKeys()) {
        kinds[key] = (await runtime.heap.readStep({ executionId: ORDER, stepKey: key }))?.meta.kind;
      }
      expect(kinds).toEqual({
        run: "run",
        poll: "poll",
        "$$patch$$:patch": "patched",
        all: "all",
        "in-all": "run",
        race: "race",
        "in-race": "run",
        "store-get": "store.get",
        "store-select": "store.select",
        "store-update": "store.update",
        "store-when": "store.when",
        "store-when-timed": "store.when",
        delay: "delay",
      });
    });
  });

  describe("keyless steps", () => {
    test("a keyless step that is first recorded logs a deprecation warning and runs", async () => {
      deploy({
        [WORKFLOW]: workflow(async function* (step) {
          return yield* step.run(work("keyless", 1));
        }),
      });
      await admit();
      expect(await deliver()).toEqual({ status: "completed", result: 1 });
      expect(ran).toEqual(["keyless"]);
      expect(warnings).toHaveLength(1);
      expect(warnings[0]!.msg).toMatch(/^Deprecated: a step without a key/);
    });

    test("a keyless step without a record under requireStepKeys parks the execution as STEP_KEY_REQUIRED and records nothing", async () => {
      deploy(
        {
          [WORKFLOW]: workflow(async function* (step) {
            yield* step.run("reserve-stock", work("reserve-stock", "reserved"));
            try {
              yield* step.run(work("keyless", 1));
            } catch (err) {
              observed.push(err);
            }
            return "caught";
          }),
        },
        requiringStepKeys(runtime)
      );
      await admit();
      expect(await deliver()).toEqual({
        status: "diverged",
        code: "STEP_KEY_REQUIRED",
        stepKey: expect.any(String),
        message: expect.stringMatching(/^A step without a key has no record/),
      });
      expect(ran).toEqual(["reserve-stock"]);
      expect(observed).toEqual([]);
      expect(await stepKeys()).toEqual(["reserve-stock"]);
      expect((await record()).divergence).toMatchObject({ code: "STEP_KEY_REQUIRED" });
    });

    test("giving a keyless step a key and clearing STEP_KEY_REQUIRED runs the step under its key", async () => {
      await suspendOnV1();
      deploy(
        {
          [WORKFLOW]: workflow(async function* (step) {
            yield* step.run("reserve-stock", work("reserve-stock", "reserved"));
            yield* step.run("charge", work("charge", "charged"));
            yield* step.delay("courier", DAY);
            yield* step.run(work("notify", "notified"));
            return "shipped";
          }),
        },
        requiringStepKeys(runtime)
      );
      advance(DAY);
      expect(await deliver()).toMatchObject({ status: "diverged", code: "STEP_KEY_REQUIRED" });

      deploy(
        {
          [WORKFLOW]: workflow(async function* (step) {
            yield* step.run("reserve-stock", work("reserve-stock", "reserved"));
            yield* step.run("charge", work("charge", "charged"));
            yield* step.delay("courier", DAY);
            yield* step.run("notify", work("notify", "notified"));
            return "shipped";
          }),
        },
        requiringStepKeys(runtime)
      );
      expect(await runtime.executions.clearDivergence(ORDER)).toEqual({ status: "cleared" });
      expect(await deliver()).toEqual({ status: "completed", result: "shipped" });
      expect(ran).toEqual(["notify"]);
    });

    test("a keyless step in a branch of a combinator parks the execution as STEP_KEY_REQUIRED", async () => {
      deploy(
        {
          [WORKFLOW]: workflow(async function* (step) {
            try {
              yield* step.all("lookups", [step.run(work("keyless", 1)), step.run("stock", work("stock", 1))]);
            } catch (err) {
              observed.push(err);
            }
            return "caught";
          }),
        },
        requiringStepKeys(runtime)
      );
      await admit();
      expect(await deliver()).toMatchObject({ status: "diverged", code: "STEP_KEY_REQUIRED" });
      expect(ran).not.toContain("keyless");
      expect(observed).toEqual([]);
    });

    test("a keyless step with an existing record under requireStepKeys replays normally", async () => {
      const keyless = workflow(async function* (step) {
        const value = yield* step.run(work("keyless", 1));
        yield* step.delay("pause", MINUTE);
        return value;
      });
      deploy({ [WORKFLOW]: keyless });
      await admit();
      expect(await deliver()).toMatchObject({ status: "suspended" });

      deploy({ [WORKFLOW]: keyless }, requiringStepKeys(runtime));
      advance(MINUTE);
      expect(await deliver()).toMatchObject({ status: "completed", result: 1 });
      expect(ran).toEqual(["keyless"]);
    });
  });

  describe("workflow versions", () => {
    const versioned = (version: number) =>
      workflow({ version }, async function* (step, event) {
        observed.push(event.workflowVersion);
        yield* step.delay("pause", MINUTE);
        return event.workflowVersion;
      });

    test("an execution first delivered under version 2 replays on version 3 code with version 2", async () => {
      deploy({ [WORKFLOW]: versioned(2) });
      await admit();
      expect(await deliver()).toMatchObject({ status: "suspended" });
      expect((await record()).workflowVersion).toBe(2);

      deploy({ [WORKFLOW]: versioned(3) });
      advance(MINUTE);
      expect(await deliver()).toEqual({ status: "completed", result: 2 });
      expect(observed).toEqual([2, 2]);
      expect((await record()).workflowVersion).toBe(2);
    });

    test("version 3 deployed between admission and first delivery is the recorded version", async () => {
      deploy({ [WORKFLOW]: versioned(2) });
      await admit();
      deploy({ [WORKFLOW]: versioned(3) });
      await deliver();
      expect(observed).toEqual([3]);
      expect((await record()).workflowVersion).toBe(3);
    });

    test("a workflow that declares no version is version 1", async () => {
      deploy({
        [WORKFLOW]: workflow(async function* (_step, event) {
          return event.workflowVersion;
        }),
      });
      await admit();
      expect(await deliver()).toMatchObject({ status: "completed", result: 1 });
      expect((await record()).workflowVersion).toBe(1);
    });

    test("a first delivery that ends before any fenced write leaves no version, so the next records version 3", async () => {
      const controller = new AbortController();
      const aborting = (version: number) =>
        workflow({ version }, async function* (step, event) {
          observed.push(event.workflowVersion);
          yield* step.run("reserve-stock", () => {
            controller.abort(new Error("lease lost"));
            return 1;
          });
          return event.workflowVersion;
        });
      deploy({ [WORKFLOW]: aborting(2) });
      await admit();
      const lease = (await claim())!;
      await expect(run(lease, controller.signal)).rejects.toSatisfy(InfrastructureError.is);
      await runtime.executions.release({ lease });
      expect((await record()).workflowVersion).toBeUndefined();
      expect(await stepKeys()).toEqual([]);

      deploy({ [WORKFLOW]: versioned(3) });
      advance(MINUTE);
      await deliver();
      expect(observed).toEqual([2, 3]);
      expect((await record()).workflowVersion).toBe(3);
    });

    test("a workflow that fails before any step records no version", async () => {
      deploy({
        [WORKFLOW]: workflow({ version: 4 }, async function* () {
          throw new Error("declined");
        }),
      });
      await admit();
      expect(await deliver()).toMatchObject({ status: "failed" });
      expect(await record()).toMatchObject({ status: "failed" });
      expect((await record()).workflowVersion).toBeUndefined();
    });

    test("an execution with step records but no recorded version, from before versions, is version 1", async () => {
      await admit();
      const lease = (await claim())!;
      await runtime.heap.writeStep({
        lease,
        stepKey: "reserve-stock",
        stepAttempt: 0,
        done: true,
        stepResponseJson: `{"result":"reserved","type":"step-result"}`,
      });
      await runtime.executions.release({ lease });
      expect((await record()).workflowVersion).toBeUndefined();

      deploy({
        [WORKFLOW]: workflow({ version: 3 }, async function* (step, event) {
          observed.push(event.workflowVersion);
          yield* step.run("reserve-stock", work("reserve-stock", "reserved"));
          yield* step.delay("courier", DAY);
        }),
      });
      advance(MINUTE);
      expect(await deliver()).toMatchObject({ status: "suspended" });
      expect(observed).toEqual([1]);
      expect((await record()).workflowVersion).toBe(1);
    });

    test("a version that is not a non-negative integer is rejected", () => {
      for (const version of [-1, 1.5, Number.NaN, "2" as unknown as number]) {
        expect(() => workflow({ version }, async function* () {})).toThrow(TypeError);
      }
      expect(() => workflow({ version: 0 }, async function* () {})).not.toThrow();
    });
  });

  describe("divergence", () => {
    test("a completed step whose key changes runs nothing new and parks the execution with the new key", async () => {
      await suspendOnV1();
      deploy({
        [WORKFLOW]: workflow(async function* (step) {
          yield* step.run("reserve-stock", work("reserve-stock", "reserved"));
          yield* step.run("charge-v3", work("charge-v3", "charged with tax"));
          yield* step.delay("courier", DAY);
          return "shipped";
        }),
      });
      advance(DAY);
      const message = expect.stringMatching(
        /^Step "charge-v3" would run, but replay did not reach recorded step "(charge|courier)"$/
      );
      expect(await deliver()).toEqual({
        status: "diverged",
        code: "NONDETERMINISTIC_REPLAY",
        stepKey: "charge-v3",
        message,
      });
      expect(ran).toEqual([]);
      expect(await stepKeys()).toEqual(["charge", "courier", "reserve-stock"]);
      expect(await record()).toMatchObject({
        status: "suspended",
        unacknowledgedDeliveryCount: 0,
        divergence: {
          code: "NONDETERMINISTIC_REPLAY",
          stepKey: "charge-v3",
          message,
          detectedAt: START + DAY,
        },
      });
      expect((await record()).outcome).toBeUndefined();
    });

    test("a recorded key requested as a different kind of step diverges", async () => {
      await suspendOnV1();
      deploy({
        [WORKFLOW]: workflow(async function* (step) {
          yield* step.run("reserve-stock", work("reserve-stock", "reserved"));
          yield* step.delay("charge", MINUTE);
          yield* step.delay("courier", DAY);
        }),
      });
      advance(DAY);
      expect(await deliver()).toMatchObject({ status: "diverged", stepKey: "charge" });
      expect((await record()).divergence).toMatchObject({
        code: "NONDETERMINISTIC_REPLAY",
        stepKey: "charge",
      });
    });

    test("code that returns before a recorded step records no outcome and diverges", async () => {
      await suspendOnV1();
      deploy({
        [WORKFLOW]: workflow(async function* (step) {
          yield* step.run("reserve-stock", work("reserve-stock", "reserved"));
          return "early";
        }),
      });
      advance(DAY);
      expect(await deliver()).toMatchObject({ status: "diverged" });
      expect(await record()).toMatchObject({
        status: "suspended",
        divergence: { code: "NONDETERMINISTIC_REPLAY" },
      });
      expect(await stepKeys()).not.toContain("$$workflow-result$$");
    });

    test("code that throws before a recorded step records no failed outcome and diverges", async () => {
      await suspendOnV1();
      deploy({
        [WORKFLOW]: workflow(async function* (step) {
          yield* step.run("reserve-stock", work("reserve-stock", "reserved"));
          throw new Error("out of stock");
        }),
      });
      advance(DAY);
      expect(await deliver()).toMatchObject({ status: "diverged" });
      const after = await record();
      expect(after.status).toBe("suspended");
      expect(after.outcome).toBeUndefined();
      expect(after.divergence).toBeDefined();
    });

    test("a try/catch around the diverging step cannot catch the divergence", async () => {
      await suspendOnV1();
      deploy({
        [WORKFLOW]: workflow(async function* (step) {
          yield* step.run("reserve-stock", work("reserve-stock", "reserved"));
          try {
            yield* step.run("charge-v3", work("charge-v3", "charged"));
          } catch (err) {
            observed.push(err);
          } finally {
            observed.push("finally");
          }
          return "done";
        }),
      });
      advance(DAY);
      expect(await deliver()).toMatchObject({ status: "diverged", stepKey: "charge-v3" });
      expect(observed).toEqual([]);
      expect(ran).toEqual([]);
    });

    test("a step in flight in one branch when another branch diverges is recorded, and does not run again after clearDivergence", async () => {
      let chargeStarted!: () => void;
      const started = new Promise<void>((resolve) => (chargeStarted = resolve));
      const rejectAfterCharge: StandardSchemaV1<string, number> = {
        "~standard": {
          version: 1,
          vendor: "test",
          async validate(value) {
            await started;
            return rejectEverything["~standard"].validate(value);
          },
        },
      };
      const pair = (schema: StandardSchemaV1<string, number>) =>
        workflow(async function* (step) {
          yield* step.all("pair", [
            step.run("total", () => "42", { schema }),
            (async function* () {
              yield* step.delay("wait", MINUTE);
              return yield* step.run("charge", async () => {
                ran.push("charge");
                chargeStarted();
                await new Promise((resolve) => setTimeout(resolve, 5));
                return "charged";
              });
            })(),
          ]);
          yield* step.delay("courier", DAY);
          return "shipped";
        });
      deploy({ [WORKFLOW]: pair(numberFromString) });
      await admit();
      expect(await deliver()).toMatchObject({ status: "suspended" });

      deploy({ [WORKFLOW]: pair(rejectAfterCharge) });
      advance(MINUTE);
      expect(await deliver()).toMatchObject({ status: "diverged", code: "STEP_SCHEMA_CHANGED" });
      expect(ran).toEqual(["charge"]);
      expect(await readStep("charge")).toMatchObject({ result: "charged", done: true });

      deploy({ [WORKFLOW]: pair(numberFromString) });
      expect(await runtime.executions.clearDivergence(ORDER)).toEqual({ status: "cleared" });
      expect(await deliver()).toMatchObject({ status: "suspended" });
      advance(DAY);
      expect(await deliver()).toMatchObject({ status: "completed", result: "shipped" });
      expect(ran).toEqual(["charge"]);
    });

    test("a wake for a diverged execution does not deliver it, and stays pending", async () => {
      await suspendOnV1();
      deploy({ [WORKFLOW]: workflow(async function* () {}) });
      advance(DAY);
      expect(await deliver()).toMatchObject({ status: "diverged" });

      expect(await runtime.executions.requestWake({ executionId: ORDER, wakeAt: Date.now() })).toEqual(
        { status: "requested" }
      );
      advance(DAY);
      expect(await claim()).toBeUndefined();
      expect((await record()).divergence).toBeDefined();
    });

    test("an error thrown before replay reaches the history parks the execution and is named in the divergence record", async () => {
      await suspendOnV1();
      deploy({
        [WORKFLOW]: workflow(async function* (step) {
          yield* step.run("reserve-stock", work("reserve-stock", "reserved"));
          throw new TypeError("order.items is undefined");
        }),
      });
      advance(DAY);
      const message = expect.stringMatching(
        /^Replay did not reach recorded step "(charge|courier)", because the workflow threw: TypeError: order.items is undefined$/
      );
      expect(await deliver()).toEqual({ status: "diverged", code: "NONDETERMINISTIC_REPLAY", message });
      expect((await record()).divergence).toMatchObject({ message });
      const [warning] = warnings;
      expect((warning!.obj as { err: Error }).err.cause).toBeInstanceOf(TypeError);
    });

    test("fixed code and clearDivergence make the execution due at once, and it continues", async () => {
      await suspendOnV1();
      deploy({ [WORKFLOW]: workflow(async function* () {}) });
      advance(DAY);
      expect(await deliver()).toMatchObject({ status: "diverged" });

      deploy({ [WORKFLOW]: placeOrderV1 });
      expect(await runtime.executions.clearDivergence(ORDER)).toEqual({ status: "cleared" });
      expect((await record()).divergence).toBeUndefined();
      expect(await deliver()).toMatchObject({ status: "completed", result: "shipped" });
      expect(ran).toEqual([]);
      expect(await record()).toMatchObject({ status: "completed" });
      expect(await runtime.executions.clearDivergence(ORDER)).toEqual({ status: "not-diverged" });
    });

    test("a divergent delivery before a pending delay is due does not record the delay as passed", async () => {
      await suspendOnV1();
      deploy({
        [WORKFLOW]: workflow(async function* (step) {
          yield* step.run("reserve-stock", work("reserve-stock", "reserved"));
          yield* step.delay("courier", DAY);
          return "shipped";
        }),
      });
      advance(MINUTE);
      await runtime.executions.requestWake({ executionId: ORDER, wakeAt: Date.now() });
      expect(await deliver()).toMatchObject({ status: "diverged", stepKey: "courier" });

      deploy({ [WORKFLOW]: placeOrderV1 });
      expect(await runtime.executions.clearDivergence(ORDER)).toEqual({ status: "cleared" });
      expect(await deliver()).toMatchObject({ status: "suspended" });
      advance(DAY);
      expect(await deliver()).toMatchObject({ status: "completed", result: "shipped" });
    });

    test("clearDivergence with code that still diverges sets divergence again on the next delivery", async () => {
      await suspendOnV1();
      deploy({ [WORKFLOW]: workflow(async function* () {}) });
      advance(DAY);
      await deliver();
      const first = (await record()).divergence!;

      expect(await runtime.executions.clearDivergence(ORDER)).toEqual({ status: "cleared" });
      advance(MINUTE);
      expect(await deliver()).toMatchObject({ status: "diverged" });
      expect((await record()).divergence).toEqual({ ...first, detectedAt: first.detectedAt + MINUTE });
    });

    test("a diverged execution that is force-cancelled is cancelled, and its divergence cleared", async () => {
      await suspendOnV1();
      deploy({ [WORKFLOW]: workflow(async function* () {}) });
      advance(DAY);
      await deliver();

      expect(await runtime.executions.cancel({ executionId: ORDER, force: true })).toMatchObject({
        status: "cancelled",
      });
      const after = await record();
      expect(after.status).toBe("cancelled");
      expect(after.divergence).toBeUndefined();
    });

    test("graceful cancellation is not delivered into diverged code", async () => {
      await suspendOnV1();
      deploy({ [WORKFLOW]: workflow(async function* () {}) });
      advance(DAY);
      await deliver();

      expect(await runtime.executions.cancel({ executionId: ORDER })).toEqual({ status: "requested" });
      expect(await claim()).toBeUndefined();
      expect((await record()).cancellation?.deliveredAt).toBeUndefined();
    });

    test("list with diverged: true returns only diverged executions", async () => {
      await suspendOnV1();
      await admit("order-43");
      expect(await deliver("order-43")).toMatchObject({ status: "suspended" });
      deploy({ [WORKFLOW]: workflow(async function* () {}) });
      advance(DAY);
      await deliver();

      const diverged = await runtime.executions.list({ diverged: true });
      expect(diverged.executions.map(({ executionId }) => executionId)).toEqual([ORDER]);
      const healthy = await runtime.executions.list({ diverged: false });
      expect(healthy.executions.map(({ executionId }) => executionId)).toEqual(["order-43"]);
    });

    test("a settled race whose losing branch recorded steps replays them without divergence", async () => {
      const raceThenWait = workflow(async function* (step) {
        const winner = yield* step.race("quote", [
          step.run("fast-courier", async () => {
            await new Promise((resolve) => setTimeout(resolve, 20));
            ran.push("fast-courier");
            return "fast";
          }),
          (async function* () {
            yield* step.run("slow-courier", work("slow-courier", "slow"));
            yield* step.delay("slow-wait", DAY);
            return "slow";
          })(),
        ]);
        yield* step.delay("after", MINUTE);
        return winner;
      });
      deploy({ [WORKFLOW]: raceThenWait });
      await admit();
      expect(await deliver()).toMatchObject({ status: "suspended" });
      expect(await stepKeys()).toEqual(["after", "fast-courier", "quote", "slow-courier", "slow-wait"]);

      advance(MINUTE);
      ran = [];
      expect(await deliver()).toMatchObject({
        status: "completed",
        result: { index: 0, value: "fast" },
      });
      expect(ran).toEqual([]);
      expect((await record()).divergence).toBeUndefined();
    });

    test("a settled combinator replayed with a branch step renamed diverges", async () => {
      const allThenWait = (second: string) =>
        workflow(async function* (step) {
          yield* step.all("lookups", [
            step.run("stock", work("stock", 1)),
            step.run(second, work(second, 2)),
          ]);
          yield* step.delay("after", MINUTE);
        });
      deploy({ [WORKFLOW]: allThenWait("fraud") });
      await admit();
      expect(await deliver()).toMatchObject({ status: "suspended" });

      deploy({ [WORKFLOW]: allThenWait("fraud-v2") });
      advance(MINUTE);
      ran = [];
      expect(await deliver()).toMatchObject({ status: "diverged", stepKey: "lookups" });
      expect(ran).toEqual([]);
      expect((await record()).divergence).toMatchObject({ stepKey: "lookups" });
    });

    test("an unsettled all replayed with a branch step renamed ends without acknowledgement and diverges", async () => {
      const allWaiting = (second: string) =>
        workflow(async function* (step) {
          yield* step.all("lookups", [step.run("stock", work("stock", 1)), step.delay(second, DAY)]);
        });
      deploy({ [WORKFLOW]: allWaiting("fraud-wait") });
      await admit();
      expect(await deliver()).toMatchObject({ status: "suspended" });

      deploy({ [WORKFLOW]: allWaiting("fraud-wait-v2") });
      await runtime.executions.requestWake({ executionId: ORDER, wakeAt: Date.now() });
      expect(await deliver()).toMatchObject({ status: "diverged" });
      expect(await record()).toMatchObject({
        status: "suspended",
        divergence: { code: "NONDETERMINISTIC_REPLAY" },
      });
    });

    test("a losing race branch that caught a recorded error replays its catch path without divergence", async () => {
      deploy({
        [WORKFLOW]: workflow(async function* (step) {
          const winner = yield* step.race("quote", [
            (async function* () {
              yield* step.delay("a-wait", MINUTE);
              return "a";
            })(),
            (async function* () {
              try {
                yield* step.run("flaky", () => {
                  throw new Error("courier down");
                });
              } catch {
                observed.push("caught");
              }
              yield* step.delay("b-wait", DAY);
              return "b";
            })(),
          ]);
          yield* step.delay("after", MINUTE);
          return winner;
        }),
      });
      await admit();
      expect(await deliver()).toMatchObject({ status: "suspended" });
      advance(MINUTE);
      expect(await deliver()).toMatchObject({ status: "suspended" });
      advance(MINUTE);
      expect(await deliver()).toMatchObject({
        status: "completed",
        result: { index: 0, value: "a" },
      });
      expect((await record()).divergence).toBeUndefined();
    });

    test("a recorded delay followed by later records counts as passed when this host's clock reads earlier", async () => {
      deploy({
        [WORKFLOW]: workflow(async function* (step) {
          yield* step.run("reserve-stock", work("reserve-stock", "reserved"));
          yield* step.delay("cool-off", MINUTE);
          yield* step.run("charge", work("charge", "charged"));
          yield* step.delay("courier", DAY);
        }),
      });
      await admit();
      await deliver();
      advance(MINUTE);
      await deliver();
      expect(ran).toEqual(["reserve-stock", "charge"]);

      advance(-30_000);
      await runtime.executions.requestWake({ executionId: ORDER, wakeAt: Date.now() });
      expect(await deliver()).toMatchObject({ status: "suspended" });
      expect((await record()).divergence).toBeUndefined();
      expect(ran).toEqual(["reserve-stock", "charge"]);
    });

    test("steps of an execution whose ID extends another's are not listed under the other", async () => {
      deploy({ [WORKFLOW]: placeOrderV1 });
      await admit("order:42");
      await deliver("order:42");
      expect(await stepKeys("order")).toEqual([]);
      expect(await stepKeys("order:42")).toEqual(["charge", "courier", "reserve-stock"]);
    });

    test("a diverged delivery runs and records no step after the divergence", async () => {
      await suspendOnV1();
      deploy({
        [WORKFLOW]: workflow(async function* (step) {
          yield* step.run("reserve-stock", work("reserve-stock", "reserved"));
          yield* step.run("charge-v3", work("charge-v3", "charged"));
        }),
      });
      const writeStep = vi.spyOn(runtime.heap, "writeStep");
      advance(DAY);
      await deliver();
      expect(writeStep).not.toHaveBeenCalled();
    });
  });

  describe("patches", () => {
    const patchedV2 = workflow(async function* (step) {
      yield* step.run("reserve-stock", work("reserve-stock", "reserved"));
      const sendReceipt = yield* step.patched("send-receipt");
      observed.push(sendReceipt);
      if (sendReceipt) yield* step.run("receipt", work("receipt", "sent"));
      yield* step.run("charge", work("charge", "charged"));
      yield* step.delay("courier", DAY);
      return sendReceipt;
    });

    test("a fresh execution reaching patched returns true and records a marker", async () => {
      deploy({ [WORKFLOW]: patchedV2 });
      await admit();
      expect(await deliver()).toMatchObject({ status: "suspended" });
      expect(observed).toEqual([true]);
      expect(ran).toEqual(["reserve-stock", "receipt", "charge"]);
      expect(await readStep("$$patch$$:send-receipt")).toMatchObject({
        type: "step-result",
        result: true,
        done: true,
      });
    });

    test("an execution whose history continues past the patch point returns false and records nothing", async () => {
      await suspendOnV1();
      deploy({ [WORKFLOW]: patchedV2 });
      advance(DAY);
      expect(await deliver()).toMatchObject({ status: "completed", result: false });
      expect(observed).toEqual([false]);
      expect(ran).toEqual([]);
      expect(await stepKeys()).not.toContain("$$patch$$:send-receipt");
    });

    test("replay after a marker was recorded returns true", async () => {
      deploy({ [WORKFLOW]: patchedV2 });
      await admit();
      await deliver();
      advance(DAY);
      ran = [];
      expect(await deliver()).toMatchObject({ status: "completed", result: true });
      expect(observed).toEqual([true, true]);
      expect(ran).toEqual([]);
    });

    test("an execution suspended before the patch point resumes on patched code with true", async () => {
      deploy({
        [WORKFLOW]: workflow(async function* (step) {
          yield* step.run("reserve-stock", work("reserve-stock", "reserved"));
          yield* step.delay("courier", DAY);
          yield* step.run("charge", work("charge", "charged"));
        }),
      });
      await admit();
      await deliver();
      deploy({
        [WORKFLOW]: workflow(async function* (step) {
          yield* step.run("reserve-stock", work("reserve-stock", "reserved"));
          yield* step.delay("courier", DAY);
          const sendReceipt = yield* step.patched("send-receipt");
          observed.push(sendReceipt);
          if (sendReceipt) yield* step.run("receipt", work("receipt", "sent"));
          yield* step.run("charge", work("charge", "charged"));
        }),
      });
      advance(DAY);
      ran = [];
      expect(await deliver()).toMatchObject({ status: "completed" });
      expect(observed).toEqual([true]);
      expect(ran).toEqual(["receipt", "charge"]);
    });

    test("patched inside a combinator branch is thrown as PATCH_IN_COMBINATOR without a record", async () => {
      deploy({
        [WORKFLOW]: workflow(async function* (step) {
          try {
            yield* step.all("lookups", [step.patched("send-receipt"), step.run("stock", work("stock", 1))]);
          } catch (err) {
            observed.push(err);
          }
          return "caught";
        }),
      });
      await admit();
      expect(await deliver()).toMatchObject({ status: "completed", result: "caught" });
      expect(observed).toHaveLength(1);
      expect(observed[0]).toBeInstanceOf(WorkflowReplayError);
      expect(observed[0]).toMatchObject({ code: "PATCH_IN_COMBINATOR" });
      expect(await stepKeys()).not.toContain("$$patch$$:send-receipt");
    });

    test("a caught PATCH_IN_COMBINATOR in a settled combinator is thrown again on replay", async () => {
      deploy({
        [WORKFLOW]: workflow(async function* (step) {
          try {
            yield* step.all("lookups", [step.patched("send-receipt"), step.run("stock", work("stock", 1))]);
          } catch (err) {
            observed.push((err as WorkflowReplayError).code);
          }
          yield* step.delay("courier", DAY);
          return "shipped";
        }),
      });
      await admit();
      expect(await deliver()).toMatchObject({ status: "suspended" });
      advance(DAY);
      expect(await deliver()).toMatchObject({ status: "completed", result: "shipped" });
      expect(observed).toEqual(["PATCH_IN_COMBINATOR", "PATCH_IN_COMBINATOR"]);
    });

    test("a caught PATCH_IN_COMBINATOR does not make a later patched with the same ID return true", async () => {
      await suspendOnV1();
      deploy({
        [WORKFLOW]: workflow(async function* (step) {
          try {
            yield* step.all("lookups", [step.patched("send-receipt"), step.run("stock", work("stock", 1))]);
          } catch {}
          yield* step.run("reserve-stock", work("reserve-stock", "reserved"));
          observed.push(yield* step.patched("send-receipt"));
          yield* step.run("charge", work("charge", "charged"));
          yield* step.delay("courier", DAY);
        }),
      });
      advance(DAY);
      expect(await deliver()).toMatchObject({ status: "completed" });
      expect(observed).toEqual([false]);
    });

    test("patched called twice with the same ID returns the same answer both times", async () => {
      const twice = workflow(async function* (step) {
        yield* step.run("reserve-stock", work("reserve-stock", "reserved"));
        observed.push(yield* step.patched("send-receipt"));
        yield* step.run("charge", work("charge", "charged"));
        observed.push(yield* step.patched("send-receipt"));
        yield* step.delay("courier", DAY);
        return "shipped";
      });
      deploy({ [WORKFLOW]: twice });
      await admit();
      expect(await deliver()).toMatchObject({ status: "suspended" });
      expect(observed).toEqual([true, true]);

      observed = [];
      advance(DAY);
      expect(await deliver()).toMatchObject({ status: "completed", result: "shipped" });
      expect(observed).toEqual([true, true]);
    });

    test("patched called twice with the same ID returns false both times for an execution past the patch point", async () => {
      await suspendOnV1();
      deploy({
        [WORKFLOW]: workflow(async function* (step) {
          yield* step.run("reserve-stock", work("reserve-stock", "reserved"));
          observed.push(yield* step.patched("send-receipt"));
          yield* step.run("charge", work("charge", "charged"));
          yield* step.delay("courier", DAY);
          observed.push(yield* step.patched("send-receipt"));
          return "shipped";
        }),
      });
      advance(DAY);
      expect(await deliver()).toMatchObject({ status: "completed", result: "shipped" });
      expect(observed).toEqual([false, false]);
      expect(await stepKeys()).not.toContain("$$patch$$:send-receipt");
    });

    test("two branches that call patched with the same ID both throw PATCH_IN_COMBINATOR, not a duplicate-key error", async () => {
      deploy({
        [WORKFLOW]: workflow(async function* (step) {
          const results = yield* step.all(
            "lookups",
            [step.patched("send-receipt"), step.patched("send-receipt")],
            { mode: "settle" }
          );
          return results.map((result) =>
            result.status === "fulfilled"
              ? result
              : { status: result.status, error: { code: (result.error as WorkflowReplayError).code } }
          );
        }),
      });
      await admit();
      const outcome = await deliver();
      expect(outcome).toMatchObject({ status: "completed" });
      const results = (outcome as { result: { status: string; error: { code: string } }[] }).result;
      expect(results.map((result) => [result.status, result.error.code])).toEqual([
        ["rejected", "PATCH_IN_COMBINATOR"],
        ["rejected", "PATCH_IN_COMBINATOR"],
      ]);
    });
  });

  describe("schema changes", () => {
    test("a newer schema that rejects a value the first run accepted parks the execution with STEP_SCHEMA_CHANGED", async () => {
      const withSchema = (schema: StandardSchemaV1<string, number>) =>
        workflow(async function* (step) {
          const total = yield* step.run("total", () => "42", { schema });
          yield* step.delay("courier", DAY);
          return total;
        });
      deploy({ [WORKFLOW]: withSchema(numberFromString) });
      await admit();
      expect(await deliver()).toMatchObject({ status: "suspended" });

      deploy({ [WORKFLOW]: withSchema(rejectEverything) });
      advance(DAY);
      expect(await deliver()).toMatchObject({
        status: "diverged",
        code: "STEP_SCHEMA_CHANGED",
        stepKey: "total",
      });
      expect(await record()).toMatchObject({
        status: "suspended",
        divergence: { code: "STEP_SCHEMA_CHANGED", stepKey: "total" },
      });
    });

    test("a value the schema rejected on the first run keeps throwing STEP_SCHEMA_INVALID on replay", async () => {
      deploy({
        [WORKFLOW]: workflow(async function* (step) {
          try {
            yield* step.run("total", () => 42 as unknown as string, { schema: numberFromString });
          } catch (err) {
            observed.push((err as { code: string }).code);
          }
          yield* step.delay("courier", DAY);
          return "done";
        }),
      });
      await admit();
      await deliver();
      advance(DAY);
      expect(await deliver()).toMatchObject({ status: "completed", result: "done" });
      expect(observed).toEqual(["STEP_SCHEMA_INVALID", "STEP_SCHEMA_INVALID"]);
    });
  });
});

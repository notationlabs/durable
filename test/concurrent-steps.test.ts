import { afterEach, beforeEach, describe, expect, expectTypeOf, test, vi } from "vitest";
import type { Logger } from "pino";
import {
  DEFAULT_LEASE_DURATION,
  ExecutionDispatcher,
  InfrastructureError,
  WorkflowRunner,
  redeliveryDelay,
  type ExecutionLease,
  type Runtime,
  type StandardSchemaV1,
  type StoreWaiter,
} from "@yieldstar/core";
import { MemoryRuntime } from "@yieldstar/test-runtime";
import { SqliteRuntime } from "@yieldstar/sqlite-runtime";
import { createWorkflowInvoker } from "@yieldstar/test-invoker";
import { testSchema } from "@yieldstar/test-utils";
import {
  RetryableError,
  WorkflowCancelledError,
  WorkflowRaceError,
  defineStore,
  workflow,
  type SettledResult,
} from "yieldstar";
import { withoutVersion } from "./outcome";

const isBun = "Bun" in globalThis;
const { createSqliteDb } = isBun
  ? await import("@yieldstar/sqlite-runtime/bun")
  : await import("@yieldstar/sqlite-runtime/node");

const logger = { info() {}, error() {}, warn() {}, debug() {} } as unknown as Logger;

const START = 1_000_000_000;
const MINUTE = 60_000;
const DAY = 24 * 60 * MINUTE;
const ORDER = "order-42";

const PaymentStore = defineStore("payments", testSchema<{ paid?: boolean }>(), {
  initial: () => ({}),
});

/** Step functions that ran, in the order they started. */
let ran: string[] = [];
/** What workflow code saw, on each evaluation. */
let observed: unknown[] = [];
let inFlight = 0;
let maxInFlight = 0;
const gates = new Map<string, Gate>();

class Gate {
  private release!: () => void;
  private enter!: () => void;
  readonly opened = new Promise<void>((resolve) => (this.release = resolve));
  readonly entered = new Promise<void>((resolve) => (this.enter = resolve));
  open() {
    this.release();
  }
  async pass() {
    this.enter();
    await this.opened;
  }
}

function closeGate(name: string) {
  const gate = new Gate();
  gates.set(name, gate);
  return gate;
}

/** A step function body: records that it ran, waits at its gate if one is closed, and takes a few milliseconds. */
async function work<T>(name: string, value: T): Promise<T> {
  ran.push(name);
  inFlight++;
  maxInFlight = Math.max(maxInFlight, inFlight);
  try {
    await gates.get(name)?.pass();
    await new Promise((resolve) => setTimeout(resolve, 5));
    return value;
  } finally {
    inFlight--;
  }
}

async function fail(name: string): Promise<never> {
  await work(name, undefined);
  throw new Error(`${name} failed`);
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

function describeError(err: unknown) {
  const error = err as Error & { code?: string; errors?: Error[] };
  return {
    name: error.name,
    message: error.message,
    ...(error.code !== undefined ? { code: error.code } : {}),
    ...(error.errors !== undefined ? { errors: error.errors.map((e) => e.message) } : {}),
  };
}

const router = {
  "fetch-account": workflow(async function* (step, event) {
    const { concurrency } = (event.params ?? {}) as { concurrency?: number };
    const account = yield* step.all(
      "fetch-account",
      [
        step.run("fetch-user", () => work("fetch-user", "ada")),
        step.run("fetch-orders", () => work("fetch-orders", ["o-1"])),
        step.run("fetch-invoices", () => work("fetch-invoices", 3)),
      ],
      concurrency === undefined ? undefined : { concurrency }
    );
    observed.push(account);
    yield* step.delay("after", MINUTE);
    return account;
  }),
  "one-suspends": workflow(async function* (step) {
    return yield* step.all("check", [
      step.run("stock", () => work("stock", "in-stock")),
      (async function* () {
        yield* step.delay("courier-retry", MINUTE);
        return yield* step.run("courier", () => work("courier", "quote"));
      })(),
      step.run("fraud", () => work("fraud", 0.1)),
    ]);
  }),
  "fail-fast": workflow(async function* (step) {
    try {
      yield* step.all("check", [
        step.run("a", () => fail("a")),
        (async function* () {
          const b1 = yield* step.run("b1", () => work("b1", 1));
          return yield* step.run("b2", () => work("b2", b1 + 1));
        })(),
      ]);
    } catch (err) {
      observed.push(describeError(err));
    }
    yield* step.delay("after", MINUTE);
    return "done";
  }),
  settle: workflow(async function* (step) {
    const results = yield* step.all(
      "check",
      [
        step.run("a", () => work("a", 1)),
        step.run("b", () => fail("b")),
        step.run("c", () => work("c", 3)),
      ],
      { mode: "settle" }
    );
    return results.map((result) =>
      result.status === "fulfilled"
        ? result
        : { status: result.status, error: describeError(result.error) }
    );
  }),
  "branch-catches": workflow(async function* (step) {
    return yield* step.all("check", [
      (async function* () {
        try {
          return yield* step.run("a", () => fail("a"));
        } catch {
          return "recovered";
        }
      })(),
      step.run("b", () => work("b", "ok")),
    ]);
  }),
  "join-three": workflow(async function* (step, event) {
    const { children } = event.params as { children: string[] };
    return yield* step.all("children", [
      step.join<string>("join-0", children[0]!),
      step.join<string>("join-1", children[1]!),
      step.join<string>("join-2", children[2]!),
    ]);
  }),
  child: workflow(async function* (step, event) {
    const { name } = event.params as { name: string };
    yield* step.delay("wait", MINUTE);
    return `${name} done`;
  }),
  schema: workflow(async function* (step) {
    const [n] = yield* step.all("parse", [
      step.run("n", () => "42", { schema: numberFromString }),
    ]);
    observed.push(n);
    yield* step.delay("after", MINUTE);
    return n;
  }),
  "when-or-deadline": workflow(async function* (step) {
    const payments = step.store(PaymentStore);
    const first = yield* step.race("payment-or-deadline", [
      payments.when("paid", (state) => state.paid),
      step.delay("deadline", DAY),
    ]);
    observed.push(first);
    yield* step.delay("follow-up", 2 * DAY);
    return first.index;
  }),
  "fail-then-succeed": workflow(async function* (step) {
    return yield* step.race("first", [
      step.run("a", () => fail("a")),
      step.run("b", () => work("b", "b won")),
    ]);
  }),
  "all-fail": workflow(async function* (step) {
    try {
      yield* step.race("first", [step.run("a", () => fail("a")), step.run("b", () => fail("b"))]);
    } catch (err) {
      observed.push({ isRaceError: err instanceof WorkflowRaceError, ...describeError(err) });
    }
    return "caught";
  }),
  "all-fail-uncaught": workflow(async function* (step) {
    yield* step.race("first", [step.run("a", () => fail("a")), step.run("b", () => fail("b"))]);
  }),
  tie: workflow(async function* (step) {
    const first = yield* step.race("first", [
      step.run("a", () => work("a", "a")),
      step.run("b", () => work("b", "b")),
    ]);
    observed.push(first);
    yield* step.delay("after", MINUTE);
    return first;
  }),
  "loser-finally": workflow(async function* (step) {
    const first = yield* step.race("first", [
      step.run("win", () => work("win", "won")),
      (async function* () {
        try {
          yield* step.delay("long", DAY);
        } finally {
          ran.push("finally");
        }
      })(),
    ]);
    yield* step.delay("after", MINUTE);
    return first.index;
  }),
  "settled-replay": workflow(async function* (step) {
    const first = yield* step.race("first", [
      (async function* () {
        yield* step.run("a1", () => work("a1", 1));
        return yield* step.run("a2", () => work("a2", 2));
      })(),
      step.run("b", () => work("b", "b")),
    ]);
    observed.push(first);
    yield* step.delay("after", MINUTE);
    return first;
  }),
  "loser-recorded": workflow(async function* (step) {
    return yield* step.race("first", [
      (async function* () {
        yield* step.run("lose-1", () => work("lose-1", 1));
        yield* step.delay("lose-wait", DAY);
      })(),
      step.run("win", () => work("win", "won")),
    ]);
  }),
  empty: workflow(async function* (step) {
    const all = yield* step.all("empty-all", []);
    try {
      yield* step.race("empty-race", []);
    } catch (err) {
      observed.push({ isRaceError: err instanceof WorkflowRaceError, ...describeError(err) });
    }
    return all;
  }),
  nested: workflow(async function* (step) {
    const raced = yield* step.race("outer-race", [
      step.all("inner-all", [step.run("x", () => work("x", "x")), step.run("y", () => work("y", "y"))]),
      step.delay("never-1", DAY),
    ]);
    const all = yield* step.all("outer-all", [
      step.race("inner-race", [step.run("p", () => work("p", "p")), step.delay("never-2", DAY)]),
      step.run("q", () => work("q", "q")),
    ]);
    observed.push({ raced, all });
    yield* step.delay("after", MINUTE);
    return { raced, all };
  }),
  "duplicate-in-branches": workflow(async function* (step) {
    yield* step.all("check", [step.run("dup", () => work("dup", 1)), step.run("dup", () => work("dup", 2))]);
  }),
  "loser-key-reused": workflow(async function* (step) {
    yield* step.race("first", [step.run("win", () => work("win", 1)), step.delay("K", DAY)]);
    yield* step.run("K", () => work("K", 2));
  }),
  "loser-key-reused-on-replay": workflow(async function* (step) {
    const payments = step.store(PaymentStore);
    yield* step.race("first", [
      payments.when("K", (state) => state.paid),
      step.delay("deadline", MINUTE),
    ]);
    yield* step.run("K", () => work("K", 2));
  }),
  "slow-loser": workflow(async function* (step) {
    return yield* step.race("first", [
      step.run("fast", () => work("fast", "fast")),
      step.run("slow", () => work("slow", "slow")),
    ]);
  }),
  "retry-in-branch": workflow(async function* (step) {
    return yield* step.all("check", [
      step.run("flaky", async () => {
        ran.push("flaky");
        if (ran.filter((name) => name === "flaky").length < 2) {
          throw new RetryableError("try again", { maxAttempts: 3, retryInterval: MINUTE });
        }
        return "ok";
      }),
      step.delay("later", 10 * MINUTE),
    ]);
  }),
  "nested-loser-key-reused": workflow(async function* (step) {
    yield* step.race("first", [
      step.all("inner", [step.run("K", () => work("K", "loser")), step.delay("d", DAY)]),
      step.run("fast", () => work("fast", "fast")),
    ]);
    yield* step.delay("after", MINUTE);
    yield* step.run("K", () => work("K", "reused"));
  }),
  "loser-fails-late": workflow(async function* (step) {
    const first = yield* step.race("first", [
      step.run("fast", () => work("fast", "fast")),
      (async function* () {
        try {
          yield* step.run("slow-fail", () => fail("slow-fail"));
        } catch {
          ran.push("catch");
        } finally {
          ran.push("finally");
        }
      })(),
    ]);
    yield* step.delay("after", MINUTE);
    return first.index;
  }),
  "nested-loser-race": workflow(async function* (step) {
    const first = yield* step.race("first", [
      step.race("inner", [step.run("a", () => work("a", "a")), step.delay("d", DAY)]),
      step.run("fast", () => work("fast", "fast")),
    ]);
    return first.index;
  }),
  "cancel-inside-all": workflow(async function* (step) {
    const payments = step.store(PaymentStore);
    try {
      yield* step.all("check", [
        (async function* () {
          try {
            yield* payments.when("a-wait", (state) => state.paid);
          } catch (err) {
            observed.push({ branch: "a", isCancelled: err instanceof WorkflowCancelledError });
            yield* step.run("undo-a", () => work("undo-a", 1));
            throw err;
          }
        })(),
        (async function* () {
          try {
            yield* step.delay("b-wait", DAY);
          } catch (err) {
            observed.push({ branch: "b", isCancelled: err instanceof WorkflowCancelledError });
            yield* step.run("undo-b", () => work("undo-b", 1));
            throw err;
          }
        })(),
      ]);
    } catch (err) {
      observed.push({ all: describeError(err) });
      throw err;
    }
  }),
  "cancel-caught": workflow(async function* (step) {
    yield* step.all("check", [
      (async function* () {
        try {
          yield* step.delay("a-wait", DAY);
        } catch {
          return "a recovered";
        }
      })(),
      (async function* () {
        try {
          yield* step.delay("b-wait", DAY);
        } catch {
          return "b recovered";
        }
      })(),
    ]);
    return "finished";
  }),
  "cancel-cleanup-suspends": workflow(async function* (step) {
    yield* step.all("check", [
      (async function* () {
        try {
          yield* step.delay("a-wait", DAY);
        } catch (err) {
          yield* step.delay("a-cool-off", MINUTE);
          yield* step.run("undo-a", () => work("undo-a", 1));
          throw err;
        }
      })(),
      (async function* () {
        try {
          yield* step.delay("b-wait", DAY);
        } catch (err) {
          yield* step.run("undo-b", () => work("undo-b", 1));
          throw err;
        }
      })(),
    ]);
  }),
  "cancel-nested": workflow(async function* (step) {
    const branch = (key: string) =>
      (async function* () {
        try {
          yield* step.delay(key, DAY);
        } catch (err) {
          observed.push(key);
          throw err;
        }
      })();
    yield* step.race("outer", [step.all("inner", [branch("a"), branch("b")]), branch("c")]);
  }),
  "cancel-after-race": workflow(async function* (step) {
    const first = yield* step.race("first", [
      step.run("win", () => work("win", "won")),
      step.delay("lose", DAY),
    ]);
    try {
      yield* step.delay("after", DAY);
    } catch (err) {
      observed.push({ first, isCancelled: err instanceof WorkflowCancelledError });
      throw err;
    }
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
  ran = [];
  observed = [];
  inFlight = 0;
  maxInFlight = 0;
  gates.clear();
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

async function until(condition: () => Promise<boolean> | boolean) {
  for (let attempt = 0; attempt < 500; attempt++) {
    if (await condition()) return;
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
  throw new Error("condition not met");
}

test("race narrows its value by index, and all types its results in branch order", () => {
  workflow(async function* (step) {
    const first = yield* step.race("race", [step.run("a", () => 1), step.run("b", () => "b")]);
    if (first.index === 0) expectTypeOf(first.value).toEqualTypeOf<number>();
    else expectTypeOf(first.value).toEqualTypeOf<string>();
    const all = yield* step.all("all", [step.run("c", () => 1), step.run("d", () => "d")]);
    expectTypeOf(all).toEqualTypeOf<[number, string]>();
    const settled = yield* step.all("settle", [step.run("e", () => 1)], { mode: "settle" });
    expectTypeOf(settled).toEqualTypeOf<[SettledResult<number>]>();
  });
});

describe.each(runtimes)("concurrent steps on $name", ({ create }) => {
  let harness: Harness;
  let runtime: Runtime;
  let runner: WorkflowRunner<typeof router>;

  beforeEach(() => {
    harness = create();
    runtime = harness.runtime;
    runner = new WorkflowRunner(runtime, { router, logger });
  });

  afterEach(() => harness.dispose());

  async function admit(workflowId: keyof typeof router, params?: object, executionId = ORDER) {
    await runtime.executions.admit({
      executionId,
      workflowId,
      params: params as Record<string, unknown> | undefined,
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
    return withoutVersion(
      await runner.run(
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
      )
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

  async function readStep(stepKey: string, executionId = ORDER) {
    const record = await runtime.heap.readStep({ executionId, stepKey });
    return (
      record && {
        ...JSON.parse(record.stepResponseJson),
        attempt: record.meta.attempt,
        done: record.meta.done,
      }
    );
  }

  const outcome = async (executionId = ORDER) =>
    (await runtime.executions.get(executionId))!.outcome;

  describe("all", () => {
    test("runs three step.run branches concurrently in one delivery and returns their results in order", async () => {
      await admit("fetch-account");
      expect(await deliver()).toEqual({ status: "suspended", wakeAt: START + MINUTE });
      expect(maxInFlight).toBe(3);
      expect(observed).toEqual([["ada", ["o-1"], 3]]);
      expect(await readStep("fetch-account")).toMatchObject({
        type: "branches-decision",
        decision: "fulfilled",
        done: true,
      });

      // Replay returns the recorded values without running any step function.
      advance(MINUTE);
      expect(await deliver()).toEqual({ status: "completed", result: ["ada", ["o-1"], 3] });
      expect(ran.sort()).toEqual(["fetch-invoices", "fetch-orders", "fetch-user"]);
      expect(observed).toEqual([
        ["ada", ["o-1"], 3],
        ["ada", ["o-1"], 3],
      ]);
    });

    test("acknowledges suspended once when one branch suspends, and the next delivery runs only that branch", async () => {
      await admit("one-suspends");
      expect(await deliver()).toEqual({ status: "suspended", wakeAt: START + MINUTE });
      expect(ran.sort()).toEqual(["fraud", "stock"]);
      expect(await readStep("check")).toBeNull();

      advance(MINUTE);
      expect(await deliver()).toEqual({
        status: "completed",
        result: ["in-stock", "quote", 0.1],
      });
      expect(ran.sort()).toEqual(["courier", "fraud", "stock"]);
    });

    test("with concurrency: 1, no two branches have a step in flight at once", async () => {
      await admit("fetch-account", { concurrency: 1 });
      await deliver();
      expect(maxInFlight).toBe(1);
      expect(observed).toEqual([["ada", ["o-1"], 3]]);
    });

    test("fail-fast throws the first failure; other branches start no new steps, and an in-flight step's result is recorded and unused", async () => {
      const b1 = closeGate("b1");
      await admit("fail-fast");
      const lease = (await claim())!;
      const delivery = run(lease);
      await b1.entered;
      await until(async () => (await readStep("check")) !== null);
      expect(await readStep("check")).toMatchObject({ decision: "rejected", index: 0 });
      // The delivery waits for the abandoned in-flight step.
      expect(await readStep("b1")).toBeNull();
      b1.open();
      const first = await delivery;
      await runtime.executions.acknowledge({ lease, outcome: first });
      expect(first).toEqual({ status: "suspended", wakeAt: START + MINUTE });
      expect(await readStep("b1")).toMatchObject({ type: "step-result", result: 1, done: true });
      expect(await readStep("b2")).toBeNull();
      expect(ran.sort()).toEqual(["a", "b1"]);

      // Replay throws the same failure and still starts no new step.
      advance(MINUTE);
      expect(await deliver()).toEqual({ status: "completed", result: "done" });
      expect(ran.sort()).toEqual(["a", "b1"]);
      const failure = { name: "Error", message: "a failed" };
      expect(observed).toEqual([failure, failure]);
    });

    test("settle mode returns a status for each branch", async () => {
      await admit("settle");
      expect(await deliver()).toEqual({
        status: "completed",
        result: [
          { status: "fulfilled", value: 1 },
          { status: "rejected", error: { name: "Error", message: "b failed" } },
          { status: "fulfilled", value: 3 },
        ],
      });
      expect(await readStep("check")).toMatchObject({ decision: "settled" });
    });

    test("a branch that catches its own step error and returns counts as succeeded", async () => {
      await admit("branch-catches");
      expect(await deliver()).toEqual({ status: "completed", result: ["recovered", "ok"] });
    });

    test("all over three joins returns each child's result once all have completed", async () => {
      const children = ["child-1", "child-2", "child-3"];
      for (const name of children) await admit("child", { name }, name);
      await admit("join-three", { children });
      expect(await deliver()).toEqual({ status: "suspended" });
      for (const name of children) {
        expect(await deliver(name)).toMatchObject({ status: "suspended" });
      }
      expect(await claim()).toBeUndefined();

      advance(MINUTE);
      for (const name of children) {
        expect(await deliver(name)).toEqual({ status: "completed", result: `${name} done` });
      }
      expect(await deliver()).toEqual({
        status: "completed",
        result: ["child-1 done", "child-2 done", "child-3 done"],
      });
    });

    test("a branch's step.run schema transforms its value on the first run and on replay", async () => {
      await admit("schema");
      await deliver();
      advance(MINUTE);
      expect(await deliver()).toEqual({ status: "completed", result: 42 });
      expect(observed).toEqual([42, 42]);
    });

    test("a branch in retry backoff counts as suspended", async () => {
      await admit("retry-in-branch");
      expect(await deliver()).toEqual({ status: "suspended", wakeAt: START + MINUTE });
      advance(MINUTE);
      expect(await deliver()).toEqual({ status: "suspended", wakeAt: START + 10 * MINUTE });
      advance(9 * MINUTE);
      expect(await deliver()).toEqual({ status: "completed", result: ["ok", undefined] });
      expect(ran).toEqual(["flaky", "flaky"]);
    });
  });

  describe("race", () => {
    test("when against delay: the store changes first, so when wins and the deadline delivers nothing later", async () => {
      await admit("when-or-deadline");
      expect(await deliver()).toEqual({ status: "suspended", wakeAt: START + DAY });
      expect(harness.storeWaiters()).toEqual(["paid"]);

      advance(MINUTE);
      await runtime.stores.updateStore({
        definition: PaymentStore,
        id: ORDER,
        updater(draft) {
          draft.paid = true;
        },
      });
      expect(await deliver()).toEqual({ status: "suspended", wakeAt: START + MINUTE + 2 * DAY });
      expect(observed).toEqual([{ index: 0, value: true }]);
      expect(harness.storeWaiters()).toEqual([]);

      // The deadline's wake time is no longer requested.
      advance(DAY);
      expect(await claim()).toBeUndefined();
    });

    test("when against delay: the deadline passes first, so delay wins and a later store change changes nothing", async () => {
      await admit("when-or-deadline");
      expect(await deliver()).toEqual({ status: "suspended", wakeAt: START + DAY });

      advance(DAY);
      expect(await deliver()).toEqual({ status: "suspended", wakeAt: START + 3 * DAY });
      expect(observed).toEqual([{ index: 1, value: undefined }]);
      expect(await readStep("payment-or-deadline")).toMatchObject({ decision: "won", index: 1 });
      // The waiter was deleted with the race's record.
      expect(harness.storeWaiters()).toEqual([]);

      await runtime.stores.updateStore({
        definition: PaymentStore,
        id: ORDER,
        updater(draft) {
          draft.paid = true;
        },
      });
      expect(await claim()).toBeUndefined();
      advance(2 * DAY);
      expect(await deliver()).toEqual({ status: "completed", result: 1 });
    });

    test("a failed branch does not end the race: a later success wins", async () => {
      await admit("fail-then-succeed");
      expect(await deliver()).toEqual({
        status: "completed",
        result: { index: 1, value: "b won" },
      });
    });

    test("when every branch fails, race throws WorkflowRaceError with every error in branch order", async () => {
      await admit("all-fail");
      expect(await deliver()).toEqual({ status: "completed", result: "caught" });
      expect(observed).toEqual([
        {
          isRaceError: true,
          name: "WorkflowRaceError",
          message: 'Every branch of race "first" failed',
          code: "ALL_BRANCHES_FAILED",
          errors: ["a failed", "b failed"],
        },
      ]);
    });

    test("an uncaught WorkflowRaceError keeps its errors in the failed outcome", async () => {
      await admit("all-fail-uncaught");
      expect(await deliver()).toMatchObject({ status: "failed" });
      const recorded = await outcome();
      expect(recorded?.status).toBe("failed");
      const error = (recorded as unknown as { error: Error & { errors: Error[]; code: string } }).error;
      expect(error).toBeInstanceOf(WorkflowRaceError);
      expect(error.code).toBe("ALL_BRANCHES_FAILED");
      expect(error.errors.map((e) => e.message)).toEqual(["a failed", "b failed"]);
    });

    test("two branches that settle in one delivery record one winner, and a restart returns it", async () => {
      await admit("tie");
      await deliver();
      const [first] = observed as { index: number }[];
      expect(await readStep("first")).toMatchObject({ decision: "won", index: first!.index });
      advance(MINUTE);
      expect(await deliver()).toEqual({ status: "completed", result: first });
    });

    test("the record, not timing, decides the winner", async () => {
      await admit("tie");
      const lease = (await claim())!;
      for (const [stepKey, json] of [
        ["a", '{"result":"a","type":"step-result"}'],
        ["b", '{"result":"b","type":"step-result"}'],
        ["first", '{"decision":"won","index":1,"type":"branches-decision"}'],
      ] as const) {
        await runtime.heap.writeStep({ lease, stepKey, stepAttempt: 0, done: true, stepResponseJson: json });
      }
      expect(await run(lease)).toEqual({ status: "suspended", wakeAt: START + MINUTE });
      expect(observed).toEqual([{ index: 1, value: "b" }]);
      expect(ran).toEqual([]);
    });

    test("a losing branch's finally block does not run", async () => {
      await admit("loser-finally");
      expect(await deliver()).toEqual({ status: "suspended", wakeAt: START + MINUTE });
      advance(MINUTE);
      expect(await deliver()).toEqual({ status: "completed", result: 0 });
      expect(ran).toEqual(["win"]);
    });

    test("replay of a settled race replays only recorded steps, starts no new work and returns the recorded winner", async () => {
      const a1 = closeGate("a1");
      await admit("settled-replay");
      const lease = (await claim())!;
      const delivery = run(lease);
      await until(async () => (await readStep("first")) !== null);
      a1.open();
      const first = await delivery;
      await runtime.executions.acknowledge({ lease, outcome: first });
      expect(observed).toEqual([{ index: 1, value: "b" }]);
      expect(await readStep("a1")).toMatchObject({ result: 1, done: true });
      expect(await readStep("a2")).toBeNull();

      advance(MINUTE);
      expect(await deliver()).toEqual({ status: "completed", result: { index: 1, value: "b" } });
      expect(ran.sort()).toEqual(["a1", "b"]);
      expect(await readStep("a2")).toBeNull();
    });

    test("a losing branch's step recorded before a crash is replayed by the next delivery", async () => {
      const win = closeGate("win");
      await admit("loser-recorded");
      const lease = (await claim())!;
      const controller = new AbortController();
      const crashed = run(lease, controller.signal);
      await win.entered;
      await until(async () => (await readStep("lose-wait")) !== null);
      controller.abort(new Error("worker crashed"));
      win.open();
      await expect(crashed).rejects.toSatisfy(InfrastructureError.is);
      expect(await readStep("first")).toBeNull();
      expect(await readStep("lose-1")).toMatchObject({ result: 1, done: true });
      await runtime.executions.release({ lease });
      advance(redeliveryDelay(1));

      closeGate("win").open();
      expect(await deliver()).toEqual({ status: "completed", result: { index: 1, value: "won" } });
      // lose-1 was replayed from its record.
      expect(ran.sort()).toEqual(["lose-1", "win", "win"]);
    });

    test("an empty all returns [] and an empty race throws WorkflowRaceError with no errors", async () => {
      await admit("empty");
      expect(await deliver()).toEqual({ status: "completed", result: [] });
      expect(observed).toEqual([
        {
          isRaceError: true,
          name: "WorkflowRaceError",
          message: 'Race "empty-race" has no branches',
          code: "ALL_BRANCHES_FAILED",
          errors: [],
        },
      ]);
    });
  });

  describe("composition, keys and leases", () => {
    test("all nested in race and race nested in all settle and replay", async () => {
      await admit("nested");
      expect(await deliver()).toEqual({ status: "suspended", wakeAt: START + MINUTE });
      const expected = {
        raced: { index: 0, value: ["x", "y"] },
        all: [{ index: 0, value: "p" }, "q"],
      };
      expect(observed).toEqual([expected]);
      advance(MINUTE);
      expect(await deliver()).toEqual({ status: "completed", result: expected });
      expect(ran.sort()).toEqual(["p", "q", "x", "y"]);
    });

    test("a step key repeated across two branches is rejected as a duplicate key", async () => {
      await admit("duplicate-in-branches");
      const result = await deliver();
      expect(result).toMatchObject({ status: "failed" });
      expect((result as { error: Error }).error.message).toMatch(/unique cache key/);
    });

    test("a step after a race that reuses a key a losing branch reached is rejected on the first run", async () => {
      await admit("loser-key-reused");
      const result = await deliver();
      expect((result as { error: Error }).error.message).toMatch(/unique cache key/);
    });

    test("a step after a race that reuses a key a losing branch reached is rejected on replay", async () => {
      await admit("loser-key-reused-on-replay");
      expect(await deliver()).toEqual({ status: "suspended", wakeAt: START + MINUTE });
      advance(MINUTE);
      const result = await deliver();
      expect((result as { error: Error }).error.message).toMatch(/unique cache key/);
      expect(ran).toEqual([]);
    });

    test("replay reaches the keys of a nested combinator in a losing branch", async () => {
      const k = closeGate("K");
      await admit("nested-loser-key-reused");
      const lease = (await claim())!;
      const delivery = run(lease);
      await until(async () => (await readStep("first")) !== null);
      k.open();
      const first = await delivery;
      await runtime.executions.acknowledge({ lease, outcome: first });
      expect(first).toEqual({ status: "suspended", wakeAt: START + MINUTE });
      expect(await readStep("K")).toMatchObject({ result: "loser" });

      advance(MINUTE);
      const result = await deliver();
      expect((result as { error: Error }).error.message).toMatch(/unique cache key/);
    });

    test("a losing branch whose running step fails after the race settles runs no catch or finally block", async () => {
      const slow = closeGate("slow-fail");
      await admit("loser-fails-late");
      const lease = (await claim())!;
      const delivery = run(lease);
      await until(async () => (await readStep("first")) !== null);
      slow.open();
      const first = await delivery;
      await runtime.executions.acknowledge({ lease, outcome: first });
      expect(await readStep("slow-fail")).toMatchObject({ type: "step-error", done: true });
      advance(MINUTE);
      expect(await deliver()).toEqual({ status: "completed", result: 0 });
      expect(ran.sort()).toEqual(["fast", "slow-fail"]);
    });

    test("a nested combinator in a losing branch records no decision", async () => {
      const a = closeGate("a");
      await admit("nested-loser-race");
      const lease = (await claim())!;
      const delivery = run(lease);
      await until(async () => (await readStep("first")) !== null);
      a.open();
      expect(await delivery).toEqual({ status: "completed", result: 1 });
      expect(await readStep("a")).toMatchObject({ result: "a" });
      expect(await readStep("inner")).toBeNull();
    });

    test("the delivery acknowledges only after a losing branch's running step.run records its result", async () => {
      const slow = closeGate("slow");
      await admit("slow-loser");
      const lease = (await claim())!;
      let settled = false;
      const delivery = run(lease).finally(() => (settled = true));
      await slow.entered;
      await until(async () => (await readStep("first")) !== null);
      expect(await readStep("first")).toMatchObject({ decision: "won", index: 0 });
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(settled).toBe(false);
      slow.open();
      expect(await delivery).toEqual({ status: "completed", result: { index: 0, value: "fast" } });
      expect(await readStep("slow")).toMatchObject({ result: "slow", done: true });
    });

    test("a stale holder's branch write returns lost and ends the delivery", async () => {
      const slow = closeGate("slow");
      await admit("slow-loser");
      const stale = (await claim())!;
      const delivery = run(stale);
      await slow.entered;
      await until(async () => (await readStep("first")) !== null);
      advance(DEFAULT_LEASE_DURATION + redeliveryDelay(1));
      expect(await claim()).toBeDefined();
      slow.open();
      const err = await delivery.catch((e: unknown) => e);
      expect(InfrastructureError.is(err)).toBe(true);
      expect((err as InfrastructureError).cause).toMatchObject({ name: "LeaseLostError" });
      expect(await readStep("slow")).toBeNull();
    });

    test("renewal keeps the lease for a concurrent delivery longer than one lease period", async () => {
      vi.useRealTimers();
      const slowRouter = {
        long: workflow(async function* (step) {
          const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
          return yield* step.all("long", [
            step.run("one", async () => (await sleep(300), 1)),
            step.run("two", async () => (await sleep(300), 2)),
          ]);
        }),
      };
      const slowRunner = new WorkflowRunner(runtime, { router: slowRouter, logger });
      const dispatcher = new ExecutionDispatcher(runtime, {
        invoker: createWorkflowInvoker({ runner: slowRunner, logger }),
        logger,
        pollInterval: 1,
        keepAlive: false,
        leaseDuration: 100,
        renewInterval: 25,
      });
      try {
        await runtime.executions.admit({
          executionId: "long",
          workflowId: "long",
          params: undefined,
          context: new Map(),
        });
        expect(await dispatcher.tick()).toBe(1);
        const stolen: ExecutionLease[] = [];
        const poll = setInterval(async () => {
          stolen.push(...(await runtime.executions.claimDue({ limit: 10, duration: 100 })));
        }, 10);
        try {
          await dispatcher.drain();
        } finally {
          clearInterval(poll);
        }
        expect(stolen).toEqual([]);
        expect(await runtime.executions.get("long")).toMatchObject({
          status: "completed",
          outcome: { status: "completed", result: [1, 2] },
          unacknowledgedDeliveryCount: 0,
        });
      } finally {
        await dispatcher.stop();
      }
    });
  });

  describe("cancellation", () => {
    const cancel = (reason?: string) =>
      runtime.executions.cancel({ executionId: ORDER, ...(reason ? { reason } : {}) });

    test("each unfinished branch of all receives WorkflowCancelledError, runs its cleanup, and all then throws it", async () => {
      await admit("cancel-inside-all");
      expect(await deliver()).toEqual({ status: "suspended", wakeAt: START + DAY });
      expect(harness.storeWaiters()).toEqual(["a-wait"]);

      await cancel("abandoned");
      expect(await deliver()).toEqual({ status: "cancelled" });
      expect(observed).toEqual(
        expect.arrayContaining([
          { branch: "a", isCancelled: true },
          { branch: "b", isCancelled: true },
          { all: { name: "WorkflowCancelledError", message: "Execution was cancelled: abandoned" } },
        ])
      );
      expect(ran.sort()).toEqual(["undo-a", "undo-b"]);
      expect(await readStep("a-wait")).toMatchObject({ type: "step-cancelled", attempt: 1 });
      expect(await readStep("b-wait")).toMatchObject({ type: "step-cancelled", attempt: 1 });
      expect(await readStep("check")).toMatchObject({
        decision: "cancelled",
        err: { name: "WorkflowCancelledError" },
        done: true,
      });
      expect(harness.storeWaiters()).toEqual([]);
      expect((await runtime.executions.get(ORDER))!.cancellation).toMatchObject({
        deliveredAt: START,
      });
    });

    test("a branch that catches cancellation and returns has its value discarded", async () => {
      await admit("cancel-caught");
      expect(await deliver()).toMatchObject({ status: "suspended" });
      await cancel();
      expect(await deliver()).toEqual({ status: "cancelled" });
    });

    test("branch cleanup that suspends resumes on the next delivery, and all still throws WorkflowCancelledError", async () => {
      await admit("cancel-cleanup-suspends");
      await deliver();
      await cancel();
      expect(await deliver()).toEqual({ status: "suspended", wakeAt: START + MINUTE });
      expect(ran).toEqual(["undo-b"]);
      advance(MINUTE);
      expect(await deliver()).toEqual({ status: "cancelled" });
      expect(ran.sort()).toEqual(["undo-a", "undo-b"]);
    });

    test("cancellation inside nested combinators reaches every unfinished branch in one write", async () => {
      await admit("cancel-nested");
      await deliver();
      const recordCancellation = vi.spyOn(runtime.heap, "recordCancellation");
      await cancel();
      expect(await deliver()).toEqual({ status: "cancelled" });
      expect([...(observed as string[])].sort()).toEqual(["a", "b", "c"]);
      expect(recordCancellation).toHaveBeenCalledTimes(1);
      const steps = recordCancellation.mock.calls[0]![0].steps.map((step) => step.stepKey);
      expect(steps.sort()).toEqual(["a", "b", "c", "inner", "outer"]);
      expect(await readStep("outer")).toMatchObject({ decision: "cancelled" });
      expect(await readStep("inner")).toMatchObject({ decision: "cancelled" });
    });

    test("cancellation during a settled race's replay lands at the next unfinished step", async () => {
      await admit("cancel-after-race");
      expect(await deliver()).toEqual({ status: "suspended", wakeAt: START + DAY });
      await cancel();
      expect(await deliver()).toEqual({ status: "cancelled" });
      expect(observed).toEqual([{ first: { index: 0, value: "won" }, isCancelled: true }]);
      // The abandoned branch's delay received nothing.
      expect(await readStep("lose")).toMatchObject({ type: "step-delay", attempt: 0 });
      expect(await readStep("after")).toMatchObject({ type: "step-cancelled", attempt: 1 });
    });
  });
});

import pino, { type Logger } from "pino";
import { afterEach, expect, test, vi } from "vitest";
import {
  createSqliteDb,
  createWorkflowObject,
  type DurableObjectStorageLike,
} from "@durable/cloudflare";
import { createLocalSdk, createWorkflow } from "@durable/sdk";
import { SqliteRuntime } from "@durable/sqlite-runtime";
import { createFakeStorage } from "./durable-object-storage";

const START = Date.UTC(2026, 0, 1);
const LEASE_DURATION = 30_000;
const TERMINAL = new Set(["completed", "failed", "cancelled"]);

// ARM_LOG prints what the host logs, with the fake clock's time.
const print = (...args: unknown[]) => console.log(`+${Date.now() - START}ms`, ...args);
const logger = process.env.ARM_LOG
  ? ({ info: print, warn: print, error: print, debug: print } as unknown as Logger)
  : pino({ level: "silent" });

// ARM_SEED sets the first seed, and then one sequence runs unless ARM_RUNS
// asks for more. ARM_STEPS sets the length of each sequence.
const firstSeed = process.env.ARM_SEED
  ? Number(process.env.ARM_SEED)
  : (Math.random() * 2 ** 31) | 0;
const RUNS = Number(process.env.ARM_RUNS ?? (process.env.ARM_SEED ? 1 : 60));
const STEPS = Number(process.env.ARM_STEPS ?? 40);
const seeds = Array.from({ length: RUNS }, (_, i) => firstSeed + i);

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const quick = createWorkflow(async function* (step) {
  return yield* step.run(() => "done");
});
const work = createWorkflow(async function* (step, event: { params: { ms: number } }) {
  yield* step.run(async () => {
    await sleep(event.params.ms);
    return event.params.ms;
  });
  return yield* step.run(() => "done");
});
const delayed = createWorkflow(async function* (step, event: { params: { ms: number } }) {
  yield* step.delay(event.params.ms);
  return yield* step.run(() => "done");
});
const parent = createWorkflow(async function* (step, event: { params: { ms: number } }) {
  const { executionId } = yield* step.start("start-child", {
    workflowId: "delayed",
    params: { ms: event.params.ms },
  });
  return yield* step.join("join-child", executionId);
});
const router = { quick, work, delayed, parent };
type Router = typeof router;

function mulberry32(seed: number) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// setImmediate is not faked, so this ends the current macrotask and lets every
// promise chain run as far as it can without the clock moving.
const settle = async () => {
  for (let i = 0; i < 3; i++) await new Promise((resolve) => setImmediate(resolve));
};

type Options = {
  concurrency: number;
  /**
   * How late an alarm fires when the handler set it to the handler's own
   * scheduled time (`slow` in the fake storage). Deployed objects took 3 to
   * 60 seconds; local workerd fires such an alarm at once.
   */
  slowAlarmDelay: number;
  /** Fails a sequence that leaves an idle object with only a slow alarm set. */
  rejectSlowAlarm: boolean;
};

// Stands in for the Cloudflare runtime around one object. Unlike
// `createFakeObject`, the test decides when a due alarm fires, an evicted
// instance loses its storage, and the harness knows when the instance is idle.
function createHarness(options: Options) {
  const fake = createFakeStorage();
  const errors: unknown[] = [];
  let instance!: { revoked: boolean; pending: number };
  let host!: ReturnType<typeof createWorkflowObject<Router>>;
  let handler: { scheduled: number } | undefined;
  let inheritedSlowAlarm = false;

  const boot = () => {
    const self = { revoked: false, pending: 0 };
    const live = () => {
      if (self.revoked) throw new Error("This instance was evicted");
    };
    const storage: DurableObjectStorageLike = {
      sql: { exec: (query, ...bindings) => (live(), fake.storage.sql.exec(query, ...bindings)) },
      transactionSync: (closure) => (live(), fake.storage.transactionSync(closure)),
      getAlarm: async () => (live(), fake.storage.getAlarm()),
      setAlarm: async (at) => (live(), fake.storage.setAlarm(at)),
      deleteAlarm: async () => (live(), fake.storage.deleteAlarm()),
    };
    instance = self;
    host = createWorkflowObject(
      {
        storage,
        waitUntil(promise) {
          self.pending++;
          const done = () => void self.pending--;
          promise.then(done, done);
        },
      },
      { router, logger, concurrency: options.concurrency, leaseDuration: LEASE_DURATION }
    );
  };
  boot();

  return {
    fake,
    errors,
    get host() {
      return host;
    },
    get sdk() {
      return createLocalSdk<Router>(host.runtime);
    },
    get handlerRunning() {
      return handler !== undefined;
    },
    /** Whether the instance has a `waitUntil` promise or an alarm handler pending. */
    get busy() {
      return instance.pending > 0 || handler !== undefined;
    },
    /** The earliest time Cloudflare would fire the alarm that is set. */
    get alarmFiresAt() {
      if (fake.alarmAt === null) return null;
      return fake.alarmAt + (fake.slow ? options.slowAlarmDelay : 0);
    },
    /**
     * Whether the alarm is slow and this instance could have set another. A
     * handler cannot set a due alarm that is not slow until its clock moves,
     * so an instance evicted before then leaves a slow alarm behind.
     */
    get slowAlarm() {
      inheritedSlowAlarm &&= fake.slow;
      return fake.slow && !inheritedSlowAlarm;
    },
    get alarmDue() {
      const at = this.alarmFiresAt;
      return handler === undefined && at !== null && at <= Date.now();
    },
    /** Starts the alarm handler if the alarm is due and no handler is running. */
    fireAlarm() {
      if (!this.alarmDue) return false;
      const running = { scheduled: fake.beginAlarm() };
      handler = running;
      host.alarm().then(
        () => {
          // An evicted instance's handler never returns to Cloudflare.
          if (handler !== running) return;
          handler = undefined;
          fake.endAlarm();
        },
        (err) => {
          if (handler !== running) return;
          handler = undefined;
          fake.endAlarm();
          errors.push(err);
        }
      );
      return true;
    },
    /**
     * Builds the object again over the same storage. The old instance can no
     * longer read or write. Cloudflare runs an alarm again when its handler
     * did not return, so an alarm taken by a running handler is put back
     * unless the handler had set another.
     */
    evict() {
      instance.revoked = true;
      inheritedSlowAlarm = fake.slow;
      if (handler) {
        fake.endAlarm();
        if (fake.alarmAt === null) void fake.storage.setAlarm(handler.scheduled);
        handler = undefined;
      }
      boot();
    },
    close() {
      instance.revoked = true;
      fake.close();
    },
  };
}

type Harness = ReturnType<typeof createHarness>;

async function listExecutions(harness: Harness) {
  const all = [];
  let cursor: string | undefined;
  do {
    const page = await harness.host.runtime.executions.list({ limit: 1000, cursor });
    all.push(...page.executions);
    cursor = page.cursor;
  } while (cursor);
  return all;
}

async function runSequence(seed: number, options: Options, steps: number) {
  // Starting the fake clock again drops the timers of the sequence before, so
  // a seed gives the same sequence when it runs alone.
  useFakeClock();
  const random = mulberry32(seed);
  // The runtime orders executions that are due at the same time by ID, so the
  // IDs it makes for child executions come from the seed too.
  const uuid = mulberry32(~seed);
  const hex = (length: number) =>
    Array.from({ length }, () => Math.floor(uuid() * 16).toString(16)).join("");
  vi.spyOn(crypto, "randomUUID").mockImplementation(
    () => `${hex(8)}-${hex(4)}-4${hex(3)}-8${hex(3)}-${hex(12)}`
  );
  const int = (min: number, max: number) => min + Math.floor(random() * (max - min + 1));
  const pick = <T,>(items: readonly T[]) => items[int(0, items.length - 1)]!;
  // Mostly short waits, with some longer than the alarm margin and the lease.
  const duration = () => pick([int(1, 30), int(30, 900), int(900, 5_000), int(5_000, 70_000)]);

  const harness = createHarness(options);
  const log: string[] = [];
  const ids: string[] = [];
  const record = (entry: string) => log.push(`+${Date.now() - START}ms ${entry}`);

  const check = (when: string) => {
    if (harness.errors.length) {
      throw new Error(`The alarm handler rejected ${when}: ${String(harness.errors[0])}`);
    }
    const at = harness.host.runtime.executions.nextClaimAt();
    // A running handler has taken the alarm, and Cloudflare runs it again if
    // it does not return.
    if (at === null || harness.handlerRunning) return;
    const now = Date.now();
    const alarm = harness.fake.alarmAt;
    const slow = options.rejectSlowAlarm && !harness.busy && harness.slowAlarm;
    // The host sets the alarm for a due execution one millisecond ahead of the
    // clock, so that it is never at the time of the handler that sets it.
    if (alarm === null || alarm > Math.max(at, now + 1) || slow) {
      throw new Error(
        `${when}: the earliest claim time is +${at - START}ms, the clock is +${now - START}ms, ` +
          `and the alarm is ${alarm === null ? "not set" : `set to +${alarm - START}ms`}` +
          `${slow ? ", the time of the handler that set it, so it fires late" : ""} ` +
          `(${harness.busy ? "a pump is running" : "the object is idle"})`
      );
    }
  };

  // Moves the clock, and starts the alarm handler shortly after the alarm
  // comes due. One time in five the alarm is left for a later step to fire.
  const advance = async (ms: number, alarms: "fire" | "hold") => {
    const end = Date.now() + ms;
    let fired = 0;
    while (true) {
      const alarm = harness.alarmFiresAt;
      const fireAt =
        alarms === "fire" && !harness.handlerRunning && alarm !== null
          ? Math.max(alarm, Date.now()) + pick([0, 0, 1, int(2, 40)])
          : Infinity;
      const target = Math.min(end, fireAt);
      if (target > Date.now()) {
        fired = 0;
        await vi.advanceTimersByTimeAsync(target - Date.now());
        await settle();
      }
      if (fireAt <= end && harness.fireAlarm()) {
        record("alarm");
        if (++fired > 20) throw new Error("The alarm fired 20 times without the clock moving");
        await settle();
        check("after the alarm fired");
        continue;
      }
      if (Date.now() >= end) return;
    }
  };

  const operations: [weight: number, run: () => Promise<void>][] = [
    [
      6,
      async () => {
        const workflowId = pick(["quick", "work", "delayed", "parent"] as const);
        const ms = duration();
        const executionId = `e${ids.length}`;
        ids.push(executionId);
        record(`trigger ${workflowId} ${ms}ms as ${executionId}`);
        await harness.sdk.trigger({ workflowId, executionId, params: { ms } });
      },
    ],
    [
      2,
      async () => {
        if (!ids.length) return;
        const executionId = pick(ids);
        const force = random() < 0.3;
        record(`cancel ${executionId}${force ? " by force" : ""}`);
        await harness.sdk.cancel(executionId, { force });
      },
    ],
    [
      8,
      async () => {
        const ms = duration();
        const alarms = random() < 0.2 ? "hold" : "fire";
        record(`advance ${ms}ms${alarms === "hold" ? " with the alarm held" : ""}`);
        await advance(ms, alarms);
      },
    ],
    [
      3,
      async () => {
        if (!harness.fireAlarm()) return;
        record("alarm");
      },
    ],
    [
      2,
      async () => {
        record(`evict${harness.busy ? " while busy" : ""}`);
        harness.evict();
      },
    ],
    [
      1,
      async () => {
        // An instance claimed an execution and was evicted before it ran any
        // of it. The alarm is where that instance would have left it.
        const executionId = `e${ids.length}`;
        ids.push(executionId);
        const lease = pick([int(20, 500), int(500, 3_000), LEASE_DURATION]);
        record(`evict with ${executionId} leased for ${lease}ms`);
        const holder = new SqliteRuntime(createSqliteDb(harness.fake.storage));
        await holder.executions.admit({
          executionId,
          workflowId: "quick",
          params: { ms: 0 },
          context: new Map(),
        });
        await holder.executions.claimDue({
          limit: 1,
          duration: lease,
          filter: { executionIds: [executionId] },
        });
        const at = holder.executions.nextClaimAt()!;
        const alarm = harness.fake.alarmAt;
        if (alarm === null || alarm > at) await harness.fake.storage.setAlarm(at);
        harness.evict();
      },
    ],
  ];
  const totalWeight = operations.reduce((sum, [weight]) => sum + weight, 0);

  try {
    for (let i = 0; i < steps; i++) {
      let roll = random() * totalWeight;
      const [, run] = operations.find(([weight]) => (roll -= weight) < 0)!;
      await run();
      await settle();
      check(`after step ${i}`);
    }

    record("drain");
    for (let round = 0; ; round++) {
      const executions = await listExecutions(harness);
      const active = executions.filter((execution) => !TERMINAL.has(execution.status));
      if (active.length === 0) break;
      const stuck = active.map((execution) => `${execution.executionId} ${execution.status}`);
      if (round > 500) throw new Error(`Still not terminal after 500 rounds: ${stuck.join(", ")}`);
      if (harness.alarmDue) {
        harness.fireAlarm();
        record("alarm");
        await settle();
      } else if (harness.alarmFiresAt !== null && !harness.handlerRunning) {
        await advance(harness.alarmFiresAt - Date.now() + 50, "fire");
      } else if (harness.busy) {
        await advance(pick([20, 500, 5_000]), "fire");
      } else {
        throw new Error(`Idle with no alarm set, and not terminal: ${stuck.join(", ")}`);
      }
      check("while draining");
    }

    // Nothing is left to claim, so the pump stops and the alarm is deleted.
    for (let round = 0; harness.busy || harness.fake.alarmAt !== null; round++) {
      if (round > 20) {
        throw new Error(
          harness.fake.alarmAt === null
            ? "The object is still busy after every execution ended"
            : "The alarm is still set after every execution ended"
        );
      }
      const alarm = harness.alarmFiresAt;
      // A step that outlives its cancelled execution keeps the pump waiting.
      await advance(alarm === null ? 10_000 : Math.max(0, alarm - Date.now()) + 50, "fire");
    }
    check("at the end");
  } catch (err) {
    const waiting = harness.fake.db
      .query<{ execution_id: string; next_claim_at: number }>(
        "SELECT execution_id, next_claim_at FROM executions WHERE next_claim_at IS NOT NULL ORDER BY next_claim_at"
      )
      .all()
      .map((row) => `${row.execution_id} at +${Number(row.next_claim_at) - START}ms`);
    const error = new Error(
      `ARM_SEED=${seed} (concurrency ${options.concurrency}, ` +
        `slow alarms ${options.slowAlarmDelay}ms late): ` +
        `${(err as Error).message}\n${log.join("\n")}\nclaim times: ${waiting.join(", ") || "none"}`
    );
    error.cause = err;
    throw error;
  } finally {
    harness.close();
  }
}

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

function useFakeClock() {
  vi.useRealTimers();
  vi.useFakeTimers({
    toFake: ["Date", "setTimeout", "clearTimeout", "setInterval", "clearInterval"],
    now: START,
  });
}

const runSequences = async (rejectSlowAlarm: boolean) => {
  for (const seed of seeds) {
    const concurrency = 1 + (Math.abs(seed) % 3);
    const slowAlarmDelay = 3_000 + (Math.abs(seed) % 58) * 1_000;
    await runSequence(seed, { concurrency, slowAlarmDelay, rejectSlowAlarm }, STEPS);
  }
};

test(`the alarm covers the earliest claim time, and every execution ends (seeds from ${seeds[0]})`, async () => {
  await runSequences(false);
}, 60_000);

// No alarm fires in this test. It fails if `run` in workflow-object.ts leaves
// its loop when the last delivery in flight settles without claiming again:
// the earliest claim time has not changed, so an execution that was waiting
// for a slot stays unclaimed until the alarm fires.
test("a waiting execution is claimed when the only delivery in flight settles", async () => {
  useFakeClock();
  const harness = createHarness({ concurrency: 1, slowAlarmDelay: 0, rejectSlowAlarm: false });
  try {
    for (const executionId of ["first", "second"]) {
      await harness.sdk.trigger({ workflowId: "work", executionId, params: { ms: 50 } });
    }
    await settle();
    expect((await harness.sdk.poll("first"))?.status).toBe("running");
    expect((await harness.sdk.poll("second"))?.status).toBe("admitted");

    await vi.advanceTimersByTimeAsync(60);
    await settle();
    expect((await harness.sdk.poll("first"))?.status).toBe("completed");
    expect((await harness.sdk.poll("second"))?.status).toBe("running");

    await vi.advanceTimersByTimeAsync(60);
    await settle();
    expect((await harness.sdk.poll("second"))?.status).toBe("completed");
  } finally {
    harness.close();
  }
});

// The same failure leaves the waiting execution to the alarm that the handler
// set before its clock moved, and that alarm is at the handler's own time. A
// deployed object fired such an alarm 3 to 60 seconds late.
test(`the same, and an idle object is not left with an alarm that fires late (seeds from ${seeds[0]})`, async () => {
  await runSequences(true);
}, 60_000);

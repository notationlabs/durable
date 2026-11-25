import type { Logger } from "pino";
import {
  WorkflowObject,
  createWorkflowObject,
  type DurableObjectStateLike,
} from "@durable/cloudflare";
import { createLocalSdk, createWorkflow } from "@durable/sdk";

// The pool runs this module in the same isolate as the test files, so the
// tests read these counters directly.
export const live = { running: 0, peak: 0 };
export const attempts = new Map<string, number>();

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const double = createWorkflow(async function* (step, event: { params: { n: number } }) {
  const doubled = yield* step.run(() => event.params.n * 2);
  return doubled + 1;
});

const delayed = createWorkflow(async function* (step, event: { params: { ms: number } }) {
  const before = yield* step.run(() => Date.now());
  yield* step.delay(event.params.ms);
  const after = yield* step.run(() => Date.now());
  return after - before;
});

const slow = createWorkflow(async function* (step, event: { params: { n: number } }) {
  return yield* step.run(async () => {
    live.peak = Math.max(live.peak, ++live.running);
    await sleep(20);
    live.running--;
    return event.params.n;
  });
});

const child = createWorkflow(async function* (step, event: { params: { n: number } }) {
  yield* step.delay(20);
  return event.params.n * 3;
});

const parent = createWorkflow(async function* (step) {
  const { executionId } = yield* step.start("start-child", {
    workflowId: "child",
    params: { n: 7 },
  });
  return yield* step.join("join-child", executionId);
});

// The step never settles on its first attempt, so the delivery is still in
// flight when the test evicts the object.
const stuckOnce = createWorkflow(async function* (step, event: { params: { key: string } }) {
  return yield* step.run(async () => {
    const attempt = (attempts.get(event.params.key) ?? 0) + 1;
    attempts.set(event.params.key, attempt);
    if (attempt === 1) await new Promise(() => {});
    return attempt;
  });
});

// The step records that it started, waits, and records that it finished.
export const held = { started: new Set<string>(), finished: new Set<string>() };
export const HOLD = 1_200;

const hold = createWorkflow(async function* (step, event: { params: { key: string } }) {
  return yield* step.run(async () => {
    held.started.add(event.params.key);
    await sleep(HOLD);
    held.finished.add(event.params.key);
    return "held";
  });
});

const steps = createWorkflow(async function* (step, event: { params: { steps: number } }) {
  for (let i = 0; i < event.params.steps; i++) yield* step.run(`step-${i}`, () => i);
});

const delays = createWorkflow(async function* (step, event: { params: { delays: number } }) {
  for (let i = 0; i < event.params.delays; i++) {
    yield* step.run(`step-${i}`, () => i);
    yield* step.delay(`delay-${i}`, 200);
  }
});

export const router = { double, delayed, slow, child, parent, stuckOnce, hold, steps, delays };
export type Router = typeof router;

export const CONCURRENCY = 2;
export const SHORT_LEASE = 600;

const silent = { info() {}, warn() {}, error() {} } as unknown as Logger;

export class Workflows {
  durable;
  alarms = 0;

  constructor(ctx: DurableObjectStateLike, _env?: unknown, leaseDuration?: number) {
    this.durable = createWorkflowObject(ctx, {
      router,
      logger: silent,
      basePath: "/durable",
      concurrency: CONCURRENCY,
      leaseDuration,
    });
  }

  fetch(request: Request) {
    return this.durable.fetch(request);
  }

  async alarm() {
    this.alarms++;
    await this.durable.alarm();
  }
}

// What Cloudflare bills as rows written: the rows and index entries that each
// SQL statement writes, and one for each alarm write.
export const written = { rows: 0, alarmWrites: 0 };

type Cursor = { toArray(): unknown[]; rowsWritten: number };

// Counts what the runtime writes. The driver reads every cursor to its end
// with `toArray`, and the cursor's count is final after that.
function counted(ctx: DurableObjectStateLike): DurableObjectStateLike {
  const { storage } = ctx;
  return {
    waitUntil: (promise) => ctx.waitUntil(promise),
    storage: {
      sql: {
        exec(query, ...bindings) {
          const cursor = storage.sql.exec(query, ...bindings) as Cursor;
          return {
            toArray() {
              const rows = cursor.toArray();
              written.rows += cursor.rowsWritten;
              return rows;
            },
          };
        },
      },
      transactionSync: (closure) => storage.transactionSync(closure),
      getAlarm: () => storage.getAlarm(),
      setAlarm(at) {
        written.alarmWrites++;
        return storage.setAlarm(at);
      },
      deleteAlarm() {
        written.alarmWrites++;
        return storage.deleteAlarm();
      },
      sync: () => storage.sync?.() ?? Promise.resolve(),
    },
  };
}

export class CountedWorkflows extends Workflows {
  constructor(ctx: DurableObjectStateLike, env?: unknown) {
    super(counted(ctx), env);
  }
}

// A lease short enough for a test to wait for it to expire.
export class ShortLeaseWorkflows extends Workflows {
  constructor(ctx: DurableObjectStateLike, env?: unknown) {
    super(ctx, env, SHORT_LEASE);
  }
}

export const isolated = { constructed: 0 };

// Runs each delivery in another instance of the class. The lease is short, so
// the object learns of a lost lease at a renewal a test can wait for.
export class IsolatedWorkflows extends WorkflowObject({
  router,
  logger: silent,
  basePath: "/durable",
  leaseDuration: SHORT_LEASE,
}) {
  constructor(ctx: DurableObjectStateLike, env: unknown) {
    super(ctx, env);
    isolated.constructed++;
  }

  // An RPC method of a subclass, which triggers through the local SDK.
  async double(n: number) {
    const sdk = createLocalSdk<Router>(this.durable.runtime, { invoker: this.durable.invoker });
    const { executionId } = await sdk.trigger({ workflowId: "double", params: { n } });
    return executionId;
  }

  // Whether this instance has anything in its own storage.
  tables() {
    return this.ctx.storage.sql
      .exec("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE '\\_cf%' ESCAPE '\\'")
      .toArray()
      .map((row) => row.name as string);
  }
}

export default {
  fetch() {
    return new Response("ok");
  },
};

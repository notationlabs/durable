import type { Logger } from "pino";
import { createWorkflowObject, type DurableObjectStateLike } from "@durable/cloudflare";
import { createWorkflow } from "@durable/sdk";

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

export const router = { double, delayed, slow, child, parent, stuckOnce };
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

// A lease short enough for a test to wait for it to expire.
export class ShortLeaseWorkflows extends Workflows {
  constructor(ctx: DurableObjectStateLike, env?: unknown) {
    super(ctx, env, SHORT_LEASE);
  }
}

export default {
  fetch() {
    return new Response("ok");
  },
};

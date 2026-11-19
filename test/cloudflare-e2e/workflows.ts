import { padding } from "./padding";
import {
  WorkflowCancelledError,
  createWorkflowRouter,
  defineStore,
  workflow,
  type StandardSchemaV1,
} from "../../packages/durable/dist/index.js";

type Sql = { exec(query: string, ...bindings: unknown[]): { toArray(): any[] } };

let sql: Sql | undefined;

export function useSql(storage: Sql) {
  sql = storage;
  sql.exec(
    "CREATE TABLE IF NOT EXISTS e2e_counters (name TEXT PRIMARY KEY, count INTEGER NOT NULL)"
  );
}

// Counts how many times a step function body ran, durably, so reruns after
// a restart or redeploy are visible.
export function bump(name: string): number {
  const [row] = sql!
    .exec(
      "INSERT INTO e2e_counters (name, count) VALUES (?, 1) ON CONFLICT (name) DO UPDATE SET count = count + 1 RETURNING count",
      name
    )
    .toArray();
  return row.count;
}

export const live = { active: 0, maxActive: 0 };

function schema<T>(): StandardSchemaV1<unknown, T> {
  return {
    "~standard": { version: 1, vendor: "e2e", validate: (value) => ({ value: value as T }) },
  };
}

const Gate = defineStore("gate", schema<{ open: boolean; by?: string }>(), {
  initial: () => ({ open: false }),
});

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

type Tagged = { tag: string };

const steps = workflow<Tagged & { delayMs?: number }>(async function* (step, event) {
  const tag = event.params.tag;
  const a = yield* step.run(() => ({ n: bump(`${tag}:a`), at: Date.now() }));
  const b = yield* step.run(() => ({ n: bump(`${tag}:b`), at: Date.now() }));
  const keyed = yield* step.run("keyed", () => bump(`${tag}:keyed`));
  yield* step.delay(event.params.delayMs ?? 1000);
  const c = yield* step.run(() => ({ n: bump(`${tag}:c`), at: Date.now() }));
  return { a, b, keyed, c, padding };
});

class E2eError extends Error {
  code = "E2E_FAIL";
  constructor(message: string) {
    super(message);
    this.name = "E2eError";
  }
}

const throws = workflow(async function* (step) {
  yield* step.run("before", () => 1);
  yield* step.run("boom", (): number => {
    throw new E2eError("deliberate failure");
  });
  return "unreachable";
});

const child = workflow<{ n: number }>(async function* (step, event) {
  yield* step.delay("wait", 200);
  return yield* step.run("double", () => event.params.n * 2);
});

const parent = workflow(async function* (step) {
  const first = yield* step.start("start-1", { workflowId: "child", params: { n: 1 } });
  const second = yield* step.start("start-2", { workflowId: "child", params: { n: 2 } });
  const one = yield* step.join<number>("join-1", first.executionId);
  const two = yield* step.join<number>("join-2", second.executionId);
  const three = yield* step.call<typeof child>("call-3", { workflowId: "child", params: { n: 3 } });
  return [one, two, three];
});

const slow = workflow<{ ms: number }>(async function* (step, event) {
  return yield* step.run("work", async () => {
    live.active++;
    live.maxActive = Math.max(live.maxActive, live.active);
    try {
      await sleep(event.params.ms);
    } finally {
      live.active--;
    }
    return Date.now();
  });
});

const cancellable = workflow<Tagged>(async function* (step, event) {
  yield* step.run("started", () => bump(`${event.params.tag}:started`));
  try {
    yield* step.delay("long", 10 * 60_000);
  } catch (err) {
    if (err instanceof WorkflowCancelledError) {
      yield* step.run("cleanup", () => bump(`${event.params.tag}:cleanup`));
    }
    throw err;
  }
  return "not cancelled";
});

const waiter = workflow<{ gate: string }>(async function* (step, event) {
  const gate = step.store(Gate, { id: event.params.gate });
  const by = yield* gate.when("open", (state) => (state.open ? state.by ?? "?" : undefined));
  return { openedBy: by, at: Date.now() };
});

const opener = workflow<{ gate: string }>(async function* (step, event) {
  const gate = step.store(Gate, { id: event.params.gate });
  yield* gate.update("open", (draft) => {
    draft.open = true;
    draft.by = event.executionId;
  });
  return "opened";
});

const fetcher = workflow<{ url: string }>(async function* (step, event) {
  return yield* step.run("fetch", async () => {
    const started = Date.now();
    const res = await fetch(event.params.url);
    const text = await res.text();
    return { status: res.status, length: text.length, ms: Date.now() - started };
  });
});

// Holds one delivery open well past the request that triggered it.
const inflight = workflow<Tagged & { ms: number }>(async function* (step, event) {
  const r = yield* step.run("hold", async () => {
    const attempt = bump(`${event.params.tag}:hold`);
    await sleep(event.params.ms);
    return { attempt, at: Date.now() };
  });
  return r;
});

export const router = createWorkflowRouter({
  steps,
  throws,
  child,
  parent,
  slow,
  cancellable,
  waiter,
  opener,
  fetcher,
  inflight,
});

export type Router = typeof router;

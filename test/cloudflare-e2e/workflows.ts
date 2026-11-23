import { AsyncLocalStorage } from "node:async_hooks";
import { padding } from "./padding";
import {
  WorkflowCancelledError,
  createWorkflowRouter,
  defineStore,
  workflow,
  type StandardSchemaV1,
} from "../../packages/durable/dist/index.js";
import { loadRouter } from "../../packages/runtime-load/src/router";

type Sql = { exec(query: string, ...bindings: unknown[]): { toArray(): any[] } };
type Scope = { sql: Sql; sync(): Promise<void>; isolate: string; instance: string };

// One isolate can hold several objects of the class, so a module variable
// would point every workflow at the storage of the object constructed last.
// The object's storage travels with the async context of its requests and
// alarms instead, and the deliveries they start inherit it.
const scope = new AsyncLocalStorage<Scope>();

export function inObject<R>(object: Scope, fn: () => R): R {
  return scope.run(object, fn);
}

function current(): Scope {
  const object = scope.getStore();
  if (!object) throw new Error("No Durable Object in the async context");
  return object;
}

export function createTables(sql: Sql) {
  sql.exec(
    "CREATE TABLE IF NOT EXISTS e2e_counters (name TEXT PRIMARY KEY, count INTEGER NOT NULL)"
  );
  sql.exec(
    "CREATE TABLE IF NOT EXISTS e2e_log (id INTEGER PRIMARY KEY, at INTEGER, isolate TEXT, instance TEXT, event TEXT, detail TEXT)"
  );
}

// Counts how many times a step function body ran, durably, so reruns after
// a restart or redeploy are visible.
export function bump(name: string): number {
  const [row] = current()
    .sql.exec(
      "INSERT INTO e2e_counters (name, count) VALUES (?, 1) ON CONFLICT (name) DO UPDATE SET count = count + 1 RETURNING count",
      name
    )
    .toArray();
  return row.count;
}

// Writes a row to the debug log from inside a step, with the object instance
// that ran it.
function mark(event: string, detail?: unknown) {
  const { sql, isolate, instance } = current();
  sql.exec(
    "INSERT INTO e2e_log (at, isolate, instance, event, detail) VALUES (?, ?, ?, ?, ?)",
    Date.now(),
    isolate,
    instance,
    event,
    detail === undefined ? null : JSON.stringify(detail)
  );
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
    mark("hold-start", { tag: event.params.tag, attempt, padding });
    await sleep(event.params.ms);
    mark("hold-end", { tag: event.params.tag, attempt, padding });
    return { attempt, at: Date.now(), instance: current().instance };
  });
  return r;
});

// Like `inflight`, but the hold starts after a delay, so the alarm handler
// starts the delivery and is still waiting on it when the hold is long.
const alarmHold = workflow<Tagged & { ms: number }>(async function* (step, event) {
  yield* step.delay("before-hold", 3_000);
  return yield* step.run("hold", async () => {
    const attempt = bump(`${event.params.tag}:hold`);
    await sleep(event.params.ms);
    return { attempt, at: Date.now() };
  });
});

// A long delay between two steps that record the object instance they ran in.
const evict = workflow<Tagged & { delayMs: number }>(async function* (step, event) {
  const { tag, delayMs } = event.params;
  const stamp = (name: string) => {
    const n = bump(`${tag}:${name}`);
    mark(`evict-${name}`, { tag, n });
    return { n, at: Date.now(), instance: current().instance };
  };
  const before = yield* step.run("before", () => stamp("before"));
  yield* step.delay("wait", delayMs);
  const after = yield* step.run("after", () => stamp("after"));
  return { before, after };
});

const SPIN_ROUND = 100_000_000;

// Spins in a synchronous loop with no I/O. The clock does not advance during
// the loop on Cloudflare, so the end time is read after awaited I/O. The
// delay makes the alarm or the pump's timer start the delivery, not the
// trigger request, which would otherwise not return until the spin ends.
const spin = workflow<Tagged & { rounds: number; maxAttempts?: number }>(async function* (step, event) {
  const { tag, rounds, maxAttempts = 3 } = event.params;
  yield* step.delay("before-spin", 1_000);
  return yield* step.run("spin", async () => {
    const attempt = bump(`${tag}:spin`);
    // Cloudflare may reset the object during the loop. Without this, the
    // writes above the loop would be lost with it, and the attempt limit
    // below would never be reached.
    await current().sync();
    const startedAt = Date.now();
    mark("spin-start", { tag, attempt });
    await current().sync();
    if (attempt > maxAttempts) return { attempt, gaveUp: true, startedAt };
    let x = 1;
    for (let round = 0; round < rounds; round++) {
      for (let i = 0; i < SPIN_ROUND; i++) x = (x * 1103515245 + 12345 + i) % 2147483647;
    }
    const clockAfterLoop = Date.now();
    await sleep(1);
    const clockAfterTimer = Date.now();
    await current().sync();
    const endedAt = Date.now();
    mark("spin-end", { tag, attempt, startedAt, clockAfterLoop, clockAfterTimer, endedAt });
    return { attempt, gaveUp: false, x, startedAt, clockAfterLoop, clockAfterTimer, endedAt };
  });
});

const soak = workflow<{ delayMs?: number }>(async function* (step, event) {
  const a = yield* step.run("a", () => Date.now());
  if (event.params.delayMs) yield* step.delay("wait", event.params.delayMs);
  const b = yield* step.run("b", () => Date.now());
  return { a, b };
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
  alarmHold,
  evict,
  spin,
  soak,
  ...loadRouter,
});

export type Router = typeof router;

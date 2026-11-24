import { AsyncLocalStorage } from "node:async_hooks";
import * as workers from "cloudflare:workers";
import { padding } from "./padding";
import {
  WorkflowCancelledError,
  createWorkflowRouter,
  defineStore,
  workflow,
  type StandardSchemaV1,
} from "../../packages/durable/dist/index.js";

type Sql = { exec(query: string, ...bindings: unknown[]): { toArray(): any[] } };
export type Scope = {
  storage: { sql: Sql };
  isolate: string;
  instance: string;
  id: { toString(): string; name?: string; jurisdiction?: string };
};

// One isolate can hold several objects of the class, so a module variable
// would point every log line at the storage of the object constructed last.
// The object's storage travels with the async context of its requests and
// alarms instead.
export const scope = new AsyncLocalStorage<Scope>();

// Random values are not allowed in global scope, so the ID is made on first use.
let isolate: string | undefined;
export const isolateId = () => (isolate ??= crypto.randomUUID().slice(0, 8));

export function createTables(sql: Sql) {
  sql.exec(
    "CREATE TABLE IF NOT EXISTS e2e_counters (name TEXT PRIMARY KEY, count INTEGER NOT NULL)"
  );
  sql.exec(
    "CREATE TABLE IF NOT EXISTS e2e_log (id INTEGER PRIMARY KEY, at INTEGER, isolate TEXT, instance TEXT, event TEXT, detail TEXT)"
  );
}

type Recorder = {
  e2eBump(name: string): Promise<{ count: number; instance: string }>;
  e2eMark(event: string, detail: unknown): Promise<void>;
  e2eLive(delta: number): Promise<void>;
};
type Recorded = { context: ReadonlyMap<string, unknown> };

// A step function may run in another instance than the object that admitted
// its execution. The trigger middleware recorded that object in the context,
// and the step sends its evidence there, where the debug routes read it.
function recorder(event: Recorded): Recorder {
  const jurisdiction = event.context.get("jurisdiction") as string | undefined;
  const binding = (workers as any).env[event.context.get("binding") as string];
  const namespace = jurisdiction ? binding.jurisdiction(jurisdiction) : binding;
  return namespace.get(namespace.idFromName(event.context.get("object")));
}

// Counts how many times a step function body ran, durably, so reruns after
// a restart or redeploy are visible. `instance` is the object that counted.
const count = (event: Recorded, name: string) => recorder(event).e2eBump(name);

export const bump = async (event: Recorded, name: string) => (await count(event, name)).count;

// Writes a row to the object's debug log from inside a step, with the isolate
// the step ran in.
const mark = (event: Recorded, name: string, detail: Record<string, unknown>) =>
  recorder(event).e2eMark(name, { ...detail, ranIn: isolateId() });

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
  const a = yield* step.run(async () => ({ n: await bump(event, `${tag}:a`), at: Date.now() }));
  const b = yield* step.run(async () => ({ n: await bump(event, `${tag}:b`), at: Date.now() }));
  const keyed = yield* step.run("keyed", () => bump(event, `${tag}:keyed`));
  yield* step.delay(event.params.delayMs ?? 1000);
  const c = yield* step.run(async () => ({ n: await bump(event, `${tag}:c`), at: Date.now() }));
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
    await recorder(event).e2eLive(1);
    try {
      await sleep(event.params.ms);
    } finally {
      await recorder(event).e2eLive(-1);
    }
    return Date.now();
  });
});

const cancellable = workflow<Tagged>(async function* (step, event) {
  yield* step.run("started", () => bump(event, `${event.params.tag}:started`));
  try {
    yield* step.delay("long", 10 * 60_000);
  } catch (err) {
    if (err instanceof WorkflowCancelledError) {
      yield* step.run("cleanup", () => bump(event, `${event.params.tag}:cleanup`));
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
    const { count: attempt, instance } = await count(event, `${event.params.tag}:hold`);
    await mark(event, "hold-start", { tag: event.params.tag, attempt, padding });
    await sleep(event.params.ms);
    await mark(event, "hold-end", { tag: event.params.tag, attempt, padding });
    return { attempt, at: Date.now(), instance, ranIn: isolateId() };
  });
  return r;
});

// Like `inflight`, but the hold starts after a delay, so the alarm handler
// starts the delivery and is still waiting on it when the hold is long.
const alarmHold = workflow<Tagged & { ms: number }>(async function* (step, event) {
  yield* step.delay("before-hold", 3_000);
  return yield* step.run("hold", async () => {
    const attempt = await bump(event, `${event.params.tag}:hold`);
    await sleep(event.params.ms);
    return { attempt, at: Date.now() };
  });
});

// A long delay between two steps that record the object instance they ran in.
const evict = workflow<Tagged & { delayMs: number }>(async function* (step, event) {
  const { tag, delayMs } = event.params;
  // `instance` is the object that claimed the delivery, which is the one an
  // eviction replaces.
  const stamp = async (name: string) => {
    const { count: n, instance } = await count(event, `${tag}:${name}`);
    await mark(event, `evict-${name}`, { tag, n });
    return { n, at: Date.now(), instance };
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
    // Each of these writes is confirmed before the loop starts, so a reset
    // during the loop does not lose the attempt count.
    const attempt = await bump(event, `${tag}:spin`);
    const startedAt = Date.now();
    await mark(event, "spin-start", { tag, attempt });
    if (attempt > maxAttempts) return { attempt, gaveUp: true, startedAt };
    let x = 1;
    for (let round = 0; round < rounds; round++) {
      for (let i = 0; i < SPIN_ROUND; i++) x = (x * 1103515245 + 12345 + i) % 2147483647;
    }
    const clockAfterLoop = Date.now();
    await sleep(1);
    const clockAfterTimer = Date.now();
    const endedAt = Date.now();
    await mark(event, "spin-end", { tag, attempt, startedAt, clockAfterLoop, clockAfterTimer, endedAt });
    return { attempt, gaveUp: false, x, startedAt, clockAfterLoop, clockAfterTimer, endedAt };
  });
});

function burn(rounds: number) {
  let x = 1;
  for (let round = 0; round < rounds; round++) {
    for (let i = 0; i < SPIN_ROUND; i++) x = (x * 1103515245 + 12345 + i) % 2147483647;
  }
  return x;
}

// Several steps that each spin for part of the CPU limit. Returns how many
// times each step function started.
const spinSteps = workflow<Tagged & { steps: number; rounds: number }>(async function* (step, event) {
  const { tag, steps: n, rounds } = event.params;
  const attempts: number[] = [];
  for (let i = 0; i < n; i++) {
    attempts.push(
      yield* step.run(`spin-${i}`, async () => {
        const attempt = await bump(event, `${tag}:spin-${i}`);
        burn(rounds);
        return attempt;
      })
    );
  }
  return attempts;
});

// Reports the instance a step function runs in.
const where = workflow(async function* (step) {
  return yield* step.run("where", () => {
    const { id } = scope.getStore()!;
    return { isolate: isolateId(), name: id.name, jurisdiction: id.jurisdiction, id: id.toString() };
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
  spinSteps,
  where,
  soak,
});

export type Router = typeof router;

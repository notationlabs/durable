import { DurableObject } from "cloudflare:workers";
import { createHttpSdkFactory } from "../../../packages/durable/dist/client.js";
import { WorkflowObject } from "../../../packages/durable/dist/cloudflare.js";
import { Gate, router, type Router } from "./durable";
import { NativeWorkflow, type NativeBinding, type NativeInstance } from "./native";
import {
  COST_SCENARIOS,
  SCENARIOS,
  SYSTEMS,
  WAITS,
  range,
  sizes,
  sleep,
  type CostScenario,
  type Namespace,
  type Run,
  type Scenario,
  type WaitScenario,
  type SystemName,
  type Usage,
} from "./shared";

export { NativeWorkflow };

type WorkflowsStub = {
  fetch(input: string, init?: RequestInit): Promise<Response>;
  openGate(id: string): Promise<void>;
  usage(): Promise<Usage>;
};
type BenchStub = { fetch(request: Request): Promise<Response> };
type Env = {
  BENCH: Namespace<BenchStub>;
  WORKFLOWS: Namespace<WorkflowsStub>;
  INLINE: Namespace<WorkflowsStub>;
  NATIVE: NativeBinding;
  BENCH_SECRET?: string;
};

type Storage = {
  sql: {
    exec(
      query: string,
      ...bindings: unknown[]
    ): { toArray(): unknown[]; rowsRead: number; rowsWritten: number };
    databaseSize: number;
  };
  transactionSync<R>(closure: () => R): R;
  getAlarm(): Promise<number | null>;
  setAlarm(at: number | Date): Promise<void>;
  deleteAlarm(): Promise<void>;
  sync(): Promise<void>;
};
type State = {
  storage: Storage;
  waitUntil(promise: Promise<unknown>): void;
  abort(reason?: string): void;
  id: { toString(): string };
  exports?: object;
};

// A `ctx` that counts the rows and the alarm writes of the runtime. The
// driver reads every cursor to its end with `toArray`, and the cursor's
// counts are final after that.
function counted(ctx: State, usage: Usage) {
  const { storage } = ctx;
  return {
    id: ctx.id,
    exports: ctx.exports,
    abort: (reason?: string) => ctx.abort(reason),
    waitUntil: (promise: Promise<unknown>) => ctx.waitUntil(promise),
    storage: {
      sql: {
        exec(query: string, ...bindings: unknown[]) {
          const cursor = storage.sql.exec(query, ...bindings);
          return {
            toArray() {
              const rows = cursor.toArray();
              usage.rowsRead += cursor.rowsRead;
              usage.rowsWritten += cursor.rowsWritten;
              return rows;
            },
          };
        },
      },
      transactionSync: <R>(closure: () => R) => storage.transactionSync(closure),
      getAlarm: () => storage.getAlarm(),
      setAlarm(at: number | Date) {
        usage.alarmWrites++;
        return storage.setAlarm(at);
      },
      deleteAlarm() {
        usage.alarmWrites++;
        return storage.deleteAlarm();
      },
      sync: () => storage.sync(),
    },
  };
}

// The class under test, with counters for what Cloudflare bills. The counters
// add no call and no write of their own.
function createClass(isolateDeliveries: boolean) {
  return class extends WorkflowObject({ router, basePath: "/durable", isolateDeliveries }) {
    private storage: Storage;
    private counts: Usage = {
      instance: crypto.randomUUID(),
      trigger: 0,
      observe: 0,
      alarm: 0,
      rpc: 0,
      deliveries: 0,
      deliveryMs: 0,
      activeMs: 0,
      rowsRead: 0,
      rowsWritten: 0,
      alarmWrites: 0,
      storedBytes: 0,
    };
    private inProgress = 0;
    private activeSince = 0;
    private hooked = false;

    constructor(ctx: State, env: unknown) {
      super(ctx, env);
      this.storage = ctx.storage;
      this.ctx = counted(ctx, this.counts);
    }

    // Counts a request, an alarm or a delivery as in progress until the
    // returned function is called. Cloudflare bills an object's duration once
    // for any number of them at the same time.
    private begin() {
      if (this.inProgress++ === 0) this.activeSince = Date.now();
      return () => {
        if (--this.inProgress === 0) this.counts.activeMs += Date.now() - this.activeSince;
      };
    }

    private get host() {
      const host = this.durable;
      if (!this.hooked) {
        this.hooked = true;
        const { deliver } = host.invoker;
        host.invoker.deliver = async (event, options) => {
          const end = this.begin();
          const started = Date.now();
          this.counts.deliveries++;
          try {
            return await deliver(event, options);
          } finally {
            this.counts.deliveryMs += Date.now() - started;
            end();
          }
        };
      }
      return host;
    }

    async fetch(request: Request) {
      if (!new URL(request.url).pathname.endsWith("/trigger")) {
        this.counts.observe++;
        return this.host.fetch(request);
      }
      this.counts.trigger++;
      const end = this.begin();
      try {
        return await this.host.fetch(request);
      } finally {
        end();
      }
    }

    async alarm() {
      this.counts.alarm++;
      const end = this.begin();
      try {
        await this.host.alarm();
      } finally {
        end();
      }
    }

    async openGate(id: string) {
      this.counts.rpc++;
      const end = this.begin();
      try {
        await this.host.runtime.stores.store(Gate, id).update((draft) => {
          draft.open = true;
        });
      } finally {
        end();
      }
    }

    usage(): Usage {
      return { ...this.counts, storedBytes: this.storage.sql.databaseSize };
    }
  };
}

export class Workflows extends createClass(true) {}
export class InlineWorkflows extends createClass(false) {}

// What the driver does to one system. `Execution` is whatever the system
// returns for a started execution.
type System<Execution = any> = {
  start(scenario: Scenario, run: Run): Promise<Execution>;
  startBurst(run: Run, count: number): Promise<Execution[]>;
  // Starts `count` executions of the workflow whose steps wait.
  startWaits(run: Run, wait: { steps: number; ms: number }, count: number): Promise<Execution[]>;
  // Resolves when the execution has completed, in the way a caller in a
  // Worker waits for a result. `pollMs` is the interval between two reads of
  // the status, where the system has no call that waits.
  completed(execution: Execution, pollMs?: number): Promise<void>;
  signal(execution: Execution, run: Run): Promise<void>;
  // The cost pass reads what a Durable object has been billed for from the
  // object, and what Cloudflare Workflows has, later, from Cloudflare's
  // analytics by instance ID.
  usage?(): Promise<Usage>;
  instanceIds?(execution: Execution, scenario: CostScenario): string[];
};

function durable(namespace: Namespace<WorkflowsStub>, object: string): System<string> {
  const stub = namespace.get(namespace.idFromName(object));
  const sdk = createHttpSdkFactory<Router>()({
    url: "http://workflows/durable",
    fetch: (input, init) => stub.fetch(input, init),
  });
  const trigger = async (workflowId: string, params: unknown) =>
    (await sdk.trigger({ workflowId, params } as never)).executionId;
  return {
    start: (scenario, run) => trigger(scenario, run),
    startBurst: (run, count) =>
      Promise.all(range(count).map((index) => trigger("burst", { ...run, index }))),
    startWaits: (run, wait, count) =>
      Promise.all(range(count).map(() => trigger("wait", { ...run, ...wait }))),
    async completed(executionId) {
      const view = await sdk.wait(executionId, { timeout: 120_000 });
      if (view?.status !== "completed") throw new Error(`execution ${JSON.stringify(view)}`);
    },
    signal: (_, run) => stub.openGate(run.run),
    usage: () => stub.usage(),
  };
}

// The interval between two reads of an instance's status.
const NATIVE_POLL_MS = 10;

function native(binding: NativeBinding): System<NativeInstance> {
  return {
    start: (scenario, run) => binding.create({ params: { scenario, ...run } }),
    async startBurst(run, count) {
      const { created, errors } = await binding.createBatch({
        instances: range(count).map((index) => ({ params: { scenario: "burst", ...run, index } })),
      });
      if (errors.length) throw new Error(`createBatch: ${JSON.stringify(errors.slice(0, 3))}`);
      return created;
    },
    async startWaits(run, wait, count) {
      const { created, errors } = await binding.createBatch({
        instances: range(count).map(() => ({ params: { scenario: "wait", ...run, ...wait } })),
      });
      if (errors.length) throw new Error(`createBatch: ${JSON.stringify(errors.slice(0, 3))}`);
      return created;
    },
    async completed(instance, pollMs = NATIVE_POLL_MS) {
      const deadline = Date.now() + 120_000;
      for (;;) {
        const { status, error } = await instance.status();
        if (status === "complete") return;
        if (status === "errored" || status === "terminated" || Date.now() > deadline) {
          throw new Error(`instance ${status}: ${JSON.stringify(error)}`);
        }
        await sleep(pollMs);
      }
    },
    signal: (instance) => instance.sendEvent({ type: "go", payload: {} }),
    instanceIds: (instance, scenario) => [
      instance.id,
      ...(scenario === "children" ? range(sizes.children).map((i) => `${instance.id}-c${i}`) : []),
    ],
  };
}

// The arrival times of the two timed calls that `mark` in `shared.ts` makes,
// and how long its warm-up call took.
type Marks = Map<string, [number?, number?, number?]>;
type Metrics = Record<string, number | number[]>;
type Context = {
  system: System;
  run: Run;
  // Resolves once both calls of every label have arrived.
  marked(...labels: string[]): Promise<Record<string, Marked>>;
};
type Marked = {
  // When the step function started, and when it carried on after reporting.
  started: number;
  reported: number;
  roundTrip: number;
};

function read(marks: Marks, label: string): Marked {
  const [first, second, warmMs] = marks.get(label) as [number, number, number];
  const roundTrip = second - first;
  return { started: first - warmMs - roundTrip / 2, reported: second + roundTrip / 2, roundTrip };
}

// Each function runs one execution, or one burst, and returns its
// measurements in milliseconds. A time read with `Date.now()` here follows an
// awaited call, so this object's clock is current when it is read.
const measures: Record<Scenario, (context: Context) => Promise<Metrics>> = {
  async start({ system, run, marked }) {
    const t0 = Date.now();
    const execution = await system.start("start", run);
    const accepted = Date.now();
    await system.completed(execution);
    const result = Date.now();
    const { start } = await marked("start");
    return {
      accepted: accepted - t0,
      firstStep: start.started - t0,
      // Without the time the step function spent reporting to the clock.
      result: result - t0 - (start.reported - start.started),
    };
  },

  async chain({ system, run, marked }) {
    const execution = await system.start("chain", run);
    const { a, b } = await marked("a", "b");
    await system.completed(execution);
    return { perStep: (b.started - a.reported) / sizes.chainSteps };
  },

  async payload({ system, run, marked }) {
    const execution = await system.start("payload", run);
    const { a, b } = await marked("a", "b");
    await system.completed(execution);
    return { perStep: (b.started - a.reported) / sizes.payloadSteps };
  },

  async sleep({ system, run, marked }) {
    const execution = await system.start("sleep", run);
    const labels = range(sizes.sleeps + 1).map((i) => `t${i}`);
    const marks = await marked(...labels);
    await system.completed(execution);
    return {
      late: labels
        .slice(1)
        .map((label, i) => marks[label].started - marks[labels[i]].reported - sizes.sleepMs),
    };
  },

  async fanout({ system, run, marked }) {
    const execution = await system.start("fanout", run);
    const { a, b } = await marked("a", "b");
    await system.completed(execution);
    return { over: b.started - a.reported - sizes.fanOutMs };
  },

  async signal({ system, run, marked }) {
    const execution = await system.start("signal", run);
    // The wait is the workflow's first step, so it has begun by now. A signal
    // usually arrives long after that, with no call to the execution in
    // between, so the driver sends none either.
    await sleep(SIGNAL_AFTER_MS);
    const sent = Date.now();
    await system.signal(execution, run);
    const { woke } = await marked("woke");
    await system.completed(execution);
    return { wake: woke.started - sent };
  },

  async children({ system, run, marked }) {
    const execution = await system.start("children", run);
    const { a, b } = await marked("a", "b");
    await system.completed(execution);
    return { total: b.started - a.reported };
  },

  async burst({ system, run, marked }) {
    const t0 = Date.now();
    await system.startBurst(run, sizes.burst);
    const accepted = Date.now();
    const marks = await marked(...range(sizes.burst).map((i) => `done${i}`));
    const times = Object.values(marks)
      .map((done) => done.started - t0)
      .sort((x, y) => x - y);
    return {
      accepted: accepted - t0,
      median: times[Math.floor(times.length / 2)],
      last: times[times.length - 1],
    };
  },
};

const MARK_TIMEOUT_MS = 120_000;
const SIGNAL_AFTER_MS = 3_000;
// A Durable object's last delivery settles just after the caller sees the result.
const COST_SETTLE_MS = 2_000;
const COST_POLL_MS = 250;

// Runs a scenario with step functions that make no call to the clock, and
// returns what it was billed for: the counts of the Durable object, or the
// IDs of the Cloudflare Workflows instances.
async function cost(system: System, scenario: CostScenario, bench: string, repeats: number) {
  const before = await system.usage?.();
  const instances: string[] = [];
  let executions = 0;
  for (let i = 0; i < repeats; i++) {
    const run = { bench, run: crypto.randomUUID(), quiet: true };
    let started: unknown[];
    if (scenario in WAITS) {
      const { steps, ms, burst } = WAITS[scenario as WaitScenario];
      started = await system.startWaits(run, { steps, ms }, burst);
    } else if (scenario === "burst") {
      started = await system.startBurst(run, sizes.burst);
    } else {
      started = [await system.start(scenario as Scenario, run)];
      if (scenario === "signal") {
        await sleep(SIGNAL_AFTER_MS);
        await system.signal(started[0], run);
      }
    }
    await Promise.all(started.map((execution) => system.completed(execution, COST_POLL_MS)));
    executions += started.length;
    instances.push(...started.flatMap((e) => system.instanceIds?.(e, scenario) ?? []));
  }
  if (!before) return { executions, instances };
  await sleep(COST_SETTLE_MS);
  const after = await system.usage!();
  if (after.instance !== before.instance) throw new Error("the object restarted during the pass");
  const usage = Object.fromEntries(
    Object.entries(after).map(([key, value]) => [
      key,
      typeof value === "number" ? value - (before as Record<string, any>)[key] : value,
    ])
  );
  return { executions, usage };
}

const json = (value: unknown, status = 200) =>
  new Response(JSON.stringify(value), { status, headers: { "content-type": "application/json" } });

// Drives one measurement per request, and is the clock of every measurement:
// step functions of all three systems call `mark`, and the driver reads its
// own times in this object too. So no figure compares two machines' clocks.
export class Bench extends DurableObject<Env> {
  private runs = new Map<string, Marks>();
  private pending: { run: string; labels: string[]; resolve(): void }[] = [];

  warm() {}

  mark(run: string, label: string, call: 0 | 1, warmMs = 0) {
    const now = Date.now();
    const marks = this.marksOf(run);
    let times = marks.get(label);
    if (!times) marks.set(label, (times = []));
    // A step function that runs twice keeps its first times.
    if (times[call] === undefined) {
      times[call] = now;
      if (call === 0) times[2] = warmMs;
    }
    this.pending = this.pending.filter((waiter) => {
      if (waiter.run !== run || !this.complete(marks, waiter.labels)) return true;
      waiter.resolve();
      return false;
    });
  }

  private marksOf(run: string) {
    let marks = this.runs.get(run);
    if (!marks) this.runs.set(run, (marks = new Map()));
    return marks;
  }

  private complete(marks: Marks, labels: string[]) {
    return labels.every((label) => marks.get(label)?.[1] !== undefined);
  }

  private async marked(run: string, labels: string[]) {
    const marks = this.marksOf(run);
    if (!this.complete(marks, labels)) {
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        await new Promise<void>((resolve, reject) => {
          this.pending.push({ run, labels, resolve });
          timer = setTimeout(() => {
            const missing = labels.filter((label) => !this.complete(marks, [label]));
            reject(new Error(`no mark after ${MARK_TIMEOUT_MS} ms for: ${missing.slice(0, 5)}`));
          }, MARK_TIMEOUT_MS);
        });
      } finally {
        clearTimeout(timer);
      }
    }
    return Object.fromEntries(labels.map((label) => [label, read(marks, label)]));
  }

  async fetch(request: Request) {
    const { searchParams, pathname } = new URL(request.url);
    const bench = searchParams.get("bench")!;
    const scenario = searchParams.get("scenario") as CostScenario;
    const name = searchParams.get("system") as SystemName;
    const known: readonly string[] = pathname === "/cost" ? COST_SCENARIOS : SCENARIOS;
    if (!known.includes(scenario) || !SYSTEMS.includes(name)) {
      return json({ error: "unknown scenario or system" }, 400);
    }
    const system =
      name === "native"
        ? native(this.env.NATIVE)
        : durable(name === "durable" ? this.env.WORKFLOWS : this.env.INLINE, bench);
    if (pathname === "/cost") {
      try {
        return json(await cost(system, scenario, bench, Number(searchParams.get("repeats") ?? 1)));
      } catch (err) {
        return json({ error: String(err) }, 500);
      }
    }
    const run = { bench, run: crypto.randomUUID() };
    try {
      const metrics = await measures[scenario as Scenario]({
        system,
        run,
        marked: (...labels) => this.marked(run.run, labels),
      });
      const roundTrips = [...this.marksOf(run.run).keys()].map(
        (label) => read(this.marksOf(run.run), label).roundTrip
      );
      return json({ metrics, roundTrips });
    } catch (err) {
      return json({ error: String(err) }, 500);
    } finally {
      this.runs.delete(run.run);
      this.pending = this.pending.filter((waiter) => waiter.run !== run.run);
    }
  }
}

export default {
  async fetch(request: Request, env: Env) {
    // Fails closed: without the secret configured, every request is refused.
    if (!env.BENCH_SECRET || request.headers.get("x-bench-secret") !== env.BENCH_SECRET) {
      return new Response("Forbidden", { status: 403 });
    }
    const bench = new URL(request.url).searchParams.get("bench");
    if (!bench) return new Response("Missing bench", { status: 400 });
    return env.BENCH.get(env.BENCH.idFromName(bench)).fetch(request);
  },
};

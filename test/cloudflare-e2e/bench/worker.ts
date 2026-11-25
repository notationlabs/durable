import { DurableObject } from "cloudflare:workers";
import { createHttpSdkFactory } from "../../../packages/durable/dist/client.js";
import { WorkflowObject } from "../../../packages/durable/dist/cloudflare.js";
import { Gate, router, type Router } from "./durable";
import { NativeWorkflow, type NativeBinding, type NativeInstance } from "./native";
import {
  SCENARIOS,
  SYSTEMS,
  range,
  sizes,
  sleep,
  type Namespace,
  type Run,
  type Scenario,
  type SystemName,
} from "./shared";

export { NativeWorkflow };

type WorkflowsStub = {
  fetch(input: string, init?: RequestInit): Promise<Response>;
  openGate(id: string): Promise<void>;
};
type BenchStub = { fetch(request: Request): Promise<Response> };
type Env = {
  BENCH: Namespace<BenchStub>;
  WORKFLOWS: Namespace<WorkflowsStub>;
  INLINE: Namespace<WorkflowsStub>;
  NATIVE: NativeBinding;
  BENCH_SECRET?: string;
};

function createClass(isolateDeliveries: boolean) {
  return class extends WorkflowObject({ router, basePath: "/durable", isolateDeliveries }) {
    async openGate(id: string) {
      await this.durable.runtime.stores.store(Gate, id).update((draft) => {
        draft.open = true;
      });
    }
  };
}

export class Workflows extends createClass(true) {}
export class InlineWorkflows extends createClass(false) {}

// What the driver does to one system. `Execution` is whatever the system
// returns for a started execution.
type System<Execution = any> = {
  start(scenario: Scenario, run: Run): Promise<Execution>;
  startBurst(run: Run, count: number): Promise<void>;
  // Resolves when the execution has completed, in the way a caller in a
  // Worker waits for a result.
  completed(execution: Execution): Promise<void>;
  signal(execution: Execution, run: Run): Promise<void>;
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
    async startBurst(run, count) {
      await Promise.all(range(count).map((index) => trigger("burst", { ...run, index })));
    },
    async completed(executionId) {
      const view = await sdk.wait(executionId, { timeout: 120_000 });
      if (view?.status !== "completed") throw new Error(`execution ${JSON.stringify(view)}`);
    },
    signal: (_, run) => stub.openGate(run.run),
  };
}

// The interval between two reads of an instance's status.
const NATIVE_POLL_MS = 10;

function native(binding: NativeBinding): System<NativeInstance> {
  return {
    start: (scenario, run) => binding.create({ params: { scenario, ...run } }),
    async startBurst(run, count) {
      const { errors } = await binding.createBatch({
        instances: range(count).map((index) => ({ params: { scenario: "burst", ...run, index } })),
      });
      if (errors.length) throw new Error(`createBatch: ${JSON.stringify(errors.slice(0, 3))}`);
    },
    async completed(instance) {
      const deadline = Date.now() + 120_000;
      for (;;) {
        const { status, error } = await instance.status();
        if (status === "complete") return;
        if (status === "errored" || status === "terminated" || Date.now() > deadline) {
          throw new Error(`instance ${status}: ${JSON.stringify(error)}`);
        }
        await sleep(NATIVE_POLL_MS);
      }
    },
    signal: (instance) => instance.sendEvent({ type: "go", payload: {} }),
  };
}

// The arrival times of the two calls that `mark` in `shared.ts` makes.
type Marks = Map<string, [number?, number?]>;
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
  const [first, second] = marks.get(label) as [number, number];
  const roundTrip = second - first;
  return { started: first - roundTrip / 2, reported: second + roundTrip / 2, roundTrip };
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

const json = (value: unknown, status = 200) =>
  new Response(JSON.stringify(value), { status, headers: { "content-type": "application/json" } });

// Drives one measurement per request, and is the clock of every measurement:
// step functions of all three systems call `mark`, and the driver reads its
// own times in this object too. So no figure compares two machines' clocks.
export class Bench extends DurableObject<Env> {
  private runs = new Map<string, Marks>();
  private pending: { run: string; labels: string[]; resolve(): void }[] = [];

  mark(run: string, label: string, call: 0 | 1) {
    const now = Date.now();
    const marks = this.marksOf(run);
    let times = marks.get(label);
    if (!times) marks.set(label, (times = []));
    // A step function that runs twice keeps its first times.
    times[call] ??= now;
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
    const { searchParams } = new URL(request.url);
    const bench = searchParams.get("bench")!;
    const scenario = searchParams.get("scenario") as Scenario;
    const name = searchParams.get("system") as SystemName;
    if (!SCENARIOS.includes(scenario) || !SYSTEMS.includes(name)) {
      return json({ error: "unknown scenario or system" }, 400);
    }
    const system =
      name === "native"
        ? native(this.env.NATIVE)
        : durable(name === "durable" ? this.env.WORKFLOWS : this.env.INLINE, bench);
    const run = { bench, run: crypto.randomUUID() };
    try {
      const metrics = await measures[scenario]({
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

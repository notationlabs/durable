import pino from "pino";
import { afterEach, expect, test } from "vitest";
import { WorkflowObject, type DurableObjectStateLike } from "@durable/cloudflare";
import { createHttpSdkFactory, createWorkflow, defineStore } from "@durable/sdk";
import type { WorkflowRouter } from "@durable/core";
import { testSchema } from "@durable/test-utils";
import { createFakeObject, createFakeStorage } from "./durable-object-storage";

const logger = pino({ level: "silent" });
const stops: (() => void)[] = [];

afterEach(() => {
  for (const stop of stops.splice(0)) stop();
});

const CPU_RESET = "Durable Object exceeded its CPU time limit and was reset.";

type Runner = {
  name: string;
  /** The executions the instance ran a delivery of, in order. */
  deliveries: string[];
  /** The number of statements the instance ran on its own storage. */
  statements: number;
  aborted?: string;
};

/**
 * A stand-in for the namespace of one Durable Object class. Each name gets an
 * instance of the class over its own storage, which lasts until it aborts, as
 * on Cloudflare. A request and its reply are cloned, as they are between
 * isolates. `ctx.abort` rejects the calls the instance is serving.
 *
 * The class is named as on Cloudflare, because `WorkflowObject` recognises a
 * namespace by that name.
 */
class DurableObjectNamespace {
  /** Every instance made, the aborted ones included. */
  readonly runners: Runner[] = [];
  /** Returns an error to reject a delivery with before it runs, as a reset does. */
  reset: (executionId: string, attempt: number) => Error | undefined = () => undefined;
  private attempts = new Map<string, number>();
  private live = new Map<string, { instance: any; runner: Runner; stopped: Promise<never> }>();

  constructor(
    private name: string,
    private create: (ctx: DurableObjectStateLike, env: unknown) => any
  ) {}

  coordinatorId = () => ({ toString: () => `${this.name}:coordinator` });

  deliveriesOf = (executionId: string) =>
    this.runners.flatMap((runner) => runner.deliveries).filter((id) => id === executionId).length;

  idFromString(id: string) {
    if (!id.startsWith(`${this.name}:`)) {
      throw new TypeError("Invalid Durable Object ID. The ID does not match this Durable Object class.");
    }
    return { toString: () => id };
  }

  idFromName(name: string) {
    return { toString: () => `${this.name}:${name}` };
  }

  private instanceOf(id: { toString(): string }) {
    const name = id.toString();
    const existing = this.live.get(name);
    if (existing) return existing;
    const fake = createFakeStorage();
    stops.push(() => fake.close());
    const runner: Runner = { name, deliveries: [], statements: 0 };
    this.runners.push(runner);
    let stop: (error: Error) => void = () => {};
    const stopped = new Promise<never>((_, reject) => (stop = reject));
    stopped.catch(() => {});
    const { sql } = fake.storage;
    const ctx: DurableObjectStateLike = {
      id,
      storage: {
        ...fake.storage,
        sql: {
          exec: (query, ...bindings) => {
            runner.statements++;
            return sql.exec(query, ...bindings);
          },
        },
      },
      waitUntil() {},
      abort: (reason) => {
        runner.aborted = reason;
        this.live.delete(name);
        stop(new Error(reason));
      },
    };
    const made = { instance: this.create(ctx, { WORKFLOWS: this }), runner, stopped };
    this.live.set(name, made);
    return made;
  }

  get(id: { toString(): string }) {
    return {
      durableRunDelivery: async (request: any, host: unknown) => {
        const { instance, runner, stopped } = this.instanceOf(id);
        const { executionId } = request.event;
        runner.deliveries.push(executionId);
        const attempt = (this.attempts.get(executionId) ?? 0) + 1;
        this.attempts.set(executionId, attempt);
        const error = this.reset(executionId, attempt);
        if (error) throw error;
        const reply = instance.durableRunDelivery(structuredClone(request), host);
        return structuredClone(await Promise.race([reply, stopped]));
      },
      durableStopDelivery: async () => {
        const { instance, stopped } = this.instanceOf(id);
        await Promise.race([instance.durableStopDelivery(), stopped]);
      },
    };
  }
}

function createObject<W extends WorkflowRouter>(
  router: W,
  options?: { leaseDuration?: number; maxExpiredDeliveries?: number }
) {
  const Workflows = WorkflowObject({ router, logger, basePath: "/durable", ...options });
  const namespace = new DurableObjectNamespace("workflows", (ctx, env) => new Workflows(ctx, env));
  const object = createFakeObject(
    (ctx) => new Workflows({ ...ctx, id: namespace.coordinatorId() }, { WORKFLOWS: namespace })
  );
  stops.push(object.stop);
  const sdk = createHttpSdkFactory<W>()({
    url: "http://object/durable",
    fetch: (input, init) => object.host.fetch(new Request(input, init)),
  });
  return { object, namespace, sdk };
}

const Counter = defineStore("counter", testSchema<{ count: number; at: Date }>(), {
  initial: () => ({ count: 0, at: new Date(0) }),
});

test("each delivery runs in another instance, which writes only to the object that claimed it", async () => {
  const child = createWorkflow(async function* (step, event: { params: { n: number } }) {
    yield* step.delay(20);
    return new Map([["tripled", event.params.n * 3]]);
  });
  const parent = createWorkflow(async function* (step) {
    const store = step.store(Counter, { id: "shared" });
    yield* store.update("count", (draft) => {
      draft.count += 2;
      draft.at = new Date(86_400_000);
    });
    const { executionId } = yield* step.start("start-child", {
      workflowId: "child",
      params: { n: 7 },
    });
    const joined = (yield* step.join("join-child", executionId)) as Map<string, number>;
    const state = yield* store.get("read");
    return { tripled: joined.get("tripled"), count: state.state.count, at: state.state.at };
  });
  const { object, namespace, sdk } = createObject({ parent, child });

  const execution = await sdk.trigger({ workflowId: "parent" });

  expect(await execution.waitForResult()).toEqual({
    tripled: 21,
    count: 2,
    at: new Date(86_400_000),
  });
  // The parent's two deliveries and the child's two ran in the pool, which
  // is as large as the most deliveries that ran at once.
  expect(namespace.deliveriesOf(execution.executionId)).toBe(2);
  expect(namespace.runners.map((runner) => runner.name)).toEqual(
    ["workflows:workflows:coordinator/delivery/0", "workflows:workflows:coordinator/delivery/1"].slice(
      0,
      namespace.runners.length
    )
  );
  expect(namespace.runners.map((runner) => runner.statements)).toEqual(
    namespace.runners.map(() => 0)
  );
  expect(object.host.durable.runtime.executions.nextClaimAt()).toBeNull();
});

test("an instance that is reset fails only its own delivery, and the execution is delivered again", async () => {
  let slowRuns = 0;
  const workflow = createWorkflow(async function* (step, event: { params: { ms: number } }) {
    return yield* step.run(async () => {
      if (event.params.ms) slowRuns++;
      await new Promise((resolve) => setTimeout(resolve, event.params.ms));
      return "done";
    });
  });
  const { namespace, sdk } = createObject({ workflow }, { leaseDuration: 300 });
  namespace.reset = (executionId, attempt) =>
    executionId === "cpu-bound" && attempt === 1 ? new Error(CPU_RESET) : undefined;

  const slow = await sdk.trigger({ workflowId: "workflow", executionId: "slow", params: { ms: 150 } });
  const cpuBound = await sdk.trigger({
    workflowId: "workflow",
    executionId: "cpu-bound",
    params: { ms: 0 },
  });

  expect(await slow.waitForResult()).toBe("done");
  expect(await cpuBound.waitForResult()).toBe("done");
  expect(slowRuns).toBe(1);
  expect(namespace.deliveriesOf("cpu-bound")).toBe(2);
}, 10_000);

test("at the delivery limit, only the execution whose instances were reset fails", async () => {
  const workflow = createWorkflow(async function* (step, event: { params: { ms: number } }) {
    return yield* step.run(async () => {
      await new Promise((resolve) => setTimeout(resolve, event.params.ms));
      return "done";
    });
  });
  const { namespace, sdk } = createObject(
    { workflow },
    { leaseDuration: 200, maxExpiredDeliveries: 2 }
  );
  namespace.reset = (executionId) =>
    executionId === "cpu-bound" ? new Error(CPU_RESET) : undefined;

  await sdk.trigger({ workflowId: "workflow", executionId: "cpu-bound", params: { ms: 0 } });
  const slow = await sdk.trigger({ workflowId: "workflow", executionId: "slow", params: { ms: 2_500 } });

  expect(await sdk.wait("cpu-bound", { timeout: 8_000 })).toMatchObject({
    status: "failed",
    error: { name: "DeliveryLimitError", deliveries: 2 },
  });
  expect(await slow.waitForResult()).toBe("done");
  expect(namespace.deliveriesOf("slow")).toBe(1);
}, 15_000);

test("a forced cancellation stops the instance that runs the delivery, and the slot runs the next one", async () => {
  let finished = false;
  const workflow = createWorkflow(async function* (step, event: { params: { ms: number } }) {
    return yield* step.run(async () => {
      await new Promise((resolve) => setTimeout(resolve, event.params.ms));
      finished = true;
      return "done";
    });
  });
  // The dispatcher learns that the lease is lost when it next renews it.
  const { namespace, sdk } = createObject({ workflow }, { leaseDuration: 600 });

  const { executionId } = await sdk.trigger({ workflowId: "workflow", params: { ms: 60_000 } });
  await expect.poll(() => namespace.runners.length).toBe(1);
  await sdk.cancel(executionId, { force: true });

  expect(await sdk.wait(executionId, { timeout: 5_000 })).toMatchObject({ status: "cancelled" });
  await expect
    .poll(() => namespace.runners[0].aborted, { timeout: 3_000 })
    .toBe("The delivery was stopped");
  expect(finished).toBe(false);

  const next = await sdk.trigger({ workflowId: "workflow", params: { ms: 0 } });
  expect(await next.waitForResult()).toBe("done");
  expect(namespace.runners.map((runner) => runner.name)).toEqual([
    "workflows:workflows:coordinator/delivery/0",
    "workflows:workflows:coordinator/delivery/0",
  ]);
}, 10_000);

test("without a namespace for its own class, the object says how to get one", async () => {
  const workflow = createWorkflow(async function* () {});
  const Workflows = WorkflowObject({ router: { workflow }, logger });
  const fake = createFakeStorage();
  stops.push(() => fake.close());
  const object = new Workflows(
    { storage: fake.storage, waitUntil() {}, id: { toString: () => "other:1" } },
    { OTHER: new DurableObjectNamespace("unrelated", () => ({})), KV: {} }
  );

  expect(() => object.durable).toThrow(
    /could not find the Durable Object namespace of its own class.*durable_objects\.bindings.*isolateDeliveries: false/
  );
});

test("with isolateDeliveries off, deliveries run in the object and need no namespace", async () => {
  const workflow = createWorkflow(async function* (step) {
    yield* step.delay(60);
    return yield* step.run(() => "done");
  });
  const Workflows = WorkflowObject({
    router: { workflow },
    logger,
    basePath: "/durable",
    isolateDeliveries: false,
  });
  const object = createFakeObject((ctx) => new Workflows(ctx, {}));
  stops.push(object.stop);
  const sdk = createHttpSdkFactory<{ workflow: typeof workflow }>()({
    url: "http://object/durable",
    fetch: (input, init) => object.host.fetch(new Request(input, init)),
  });

  const execution = await sdk.trigger({ workflowId: "workflow" });
  expect(await execution.waitForResult()).toBe("done");
  expect(object.alarms.length).toBeGreaterThanOrEqual(1);
  expect(object.host.durable.runtime.executions.nextClaimAt()).toBeNull();
});

test("an instance whose object has stopped aborts at its next heartbeat", async () => {
  let finished = false;
  const workflow = createWorkflow(async function* (step) {
    return yield* step.run(async () => {
      await new Promise((resolve) => setTimeout(resolve, 60_000));
      finished = true;
    });
  });
  const Workflows = WorkflowObject({ router: { workflow }, logger });
  const namespace = new DurableObjectNamespace("workflows", (ctx, env) => new Workflows(ctx, env));
  const runner = namespace.get(namespace.idFromName("orphan"));
  const run = runner.durableRunDelivery(
    {
      event: { workflowId: "workflow", executionId: "held", params: "null", context: "[]" },
      lease: { executionId: "held", token: 1, expiresAt: Date.now() + 30_000 },
      heartbeat: 20,
    },
    {
      // The step function starts, and then the object is gone.
      call: async () => ({ ok: true, value: "null" }),
      ping: async () => {
        throw new Error("The Durable Object was reset");
      },
    }
  );

  await expect(run).rejects.toThrow("The object that claimed the delivery stopped");
  expect(namespace.runners[0].aborted).toBe("The object that claimed the delivery stopped");
  expect(finished).toBe(false);
});

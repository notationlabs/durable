import pino from "pino";
import { afterEach, expect, test } from "vitest";
import {
  createSqliteDb,
  createWorkflowObject,
  toPositionalSql,
} from "@durable/cloudflare";
import { SqliteRuntime } from "@durable/sqlite-runtime";
import { createHttpSdkFactory, createLocalSdk, createWorkflow } from "@durable/sdk";
import type { WorkflowRouter } from "@durable/core";
import { createFakeObject, createFakeStorage } from "./durable-object-storage";

const logger = pino({ level: "silent" });
const stops: (() => void)[] = [];

afterEach(() => {
  for (const stop of stops.splice(0)) stop();
});

function createObject<W extends WorkflowRouter>(
  router: W,
  options?: { concurrency?: number; leaseDuration?: number; maxExpiredDeliveries?: number },
  fake = createFakeStorage(),
  objectOptions?: Parameters<typeof createFakeObject>[2]
) {
  const object = createFakeObject(
    (ctx) => createWorkflowObject(ctx, { router, logger, basePath: "/durable", ...options }),
    fake,
    objectOptions
  );
  stops.push(object.stop);
  const sdk = createHttpSdkFactory<W>()({
    url: "http://object/durable",
    fetch: (input, init) => object.host.fetch(new Request(input, init)),
  });
  return { object, sdk };
}

test("toPositionalSql lists a repeated parameter once per use", () => {
  expect(
    toPositionalSql("SELECT '$x' AS a WHERE b = $now OR c = $id OR d <= $now")
  ).toEqual({
    sql: "SELECT '$x' AS a WHERE b = ? OR c = ? OR d <= ?",
    names: ["$now", "$id", "$now"],
  });
});

test("a trigger over the object's routes runs the workflow without an alarm firing late", async () => {
  const workflow = createWorkflow(async function* (step, event: { params: { n: number } }) {
    const doubled = yield* step.run(() => event.params.n * 2);
    return doubled + 1;
  });
  const { object, sdk } = createObject({ workflow }, { leaseDuration: 200 });

  const execution = await sdk.trigger({ workflowId: "workflow", params: { n: 20 } });
  expect(await execution.waitForResult()).toBe(41);
  // Nothing is left to claim. The alarm that covered the lease is left to
  // fire, and no alarm follows it.
  expect(object.alarms).toHaveLength(0);
  await expect.poll(() => object.alarms.length, { timeout: 3_000 }).toBe(1);
  await expect.poll(() => object.fake.alarmAt).toBeNull();
});

// Counts the calls that Cloudflare bills as a row written each.
function countAlarmWrites(fake: ReturnType<typeof createFakeStorage>) {
  const writes = { set: 0, deleted: 0 };
  const { setAlarm, deleteAlarm } = fake.storage;
  fake.storage.setAlarm = (at) => (writes.set++, setAlarm(at));
  fake.storage.deleteAlarm = () => (writes.deleted++, deleteAlarm());
  return writes;
}

test("executions that finish within a lease of the first write the alarm once", async () => {
  const workflow = createWorkflow(async function* (step, event: { params: { n: number } }) {
    return yield* step.run(() => event.params.n * 2);
  });
  const fake = createFakeStorage();
  const writes = countAlarmWrites(fake);
  const { object, sdk } = createObject({ workflow }, undefined, fake);

  const first = await sdk.trigger({ workflowId: "workflow", params: { n: 1 } });
  expect(await first.waitForResult()).toBe(2);
  // One write covers the admission and the lease that follows it, and it is
  // not deleted. The alarm is one lease ahead of the admission.
  expect(writes).toEqual({ set: 1, deleted: 0 });
  expect(fake.alarmAt).toBeGreaterThan(Date.now() + 25_000);
  expect(fake.alarmAt).toBeLessThanOrEqual(Date.now() + 30_000);

  const second = await sdk.trigger({ workflowId: "workflow", params: { n: 2 } });
  expect(await second.waitForResult()).toBe(4);
  expect(writes).toEqual({ set: 1, deleted: 0 });
  expect(object.alarms).toHaveLength(0);
});

test("an alarm further ahead than a lease is deleted when nothing is left to claim", async () => {
  const workflow = createWorkflow(async function* (step) {
    yield* step.delay(120_000);
  });
  const fake = createFakeStorage();
  const writes = countAlarmWrites(fake);
  const { sdk } = createObject({ workflow }, undefined, fake);

  const { executionId } = await sdk.trigger({ workflowId: "workflow" });
  await expect.poll(async () => (await sdk.poll(executionId))?.status).toBe("suspended");
  expect(fake.alarmAt).toBeGreaterThan(Date.now() + 100_000);

  await sdk.cancel(executionId, { force: true });
  expect((await sdk.poll(executionId))?.status).toBe("cancelled");
  await expect.poll(() => fake.alarmAt).toBeNull();
  expect(writes.deleted).toBe(1);
});

test("an execution that is due with every slot busy sets the alarm to the clock", async () => {
  let release!: () => void;
  const held = new Promise<void>((resolve) => {
    release = resolve;
  });
  const workflow = createWorkflow(async function* (step, event: { params: { hold: boolean } }) {
    return yield* step.run(async () => {
      if (event.params.hold) await held;
      return "ran";
    });
  });
  const fake = createFakeStorage();
  // No alarm fires here, so the alarm time is whatever the object last wrote.
  const host = createWorkflowObject(
    { storage: fake.storage, waitUntil() {} },
    { router: { workflow }, logger, concurrency: 1 }
  );
  const local = createLocalSdk<{ workflow: typeof workflow }>(host.runtime);

  const first = await local.trigger({ workflowId: "workflow", params: { hold: true } });
  await expect.poll(async () => (await local.poll(first.executionId))?.status).toBe("running");
  expect(fake.alarmAt).toBeGreaterThan(Date.now() + 25_000);

  // Nothing in this instance can claim the second execution until the first
  // settles, so its alarm is not left a lease away.
  const second = await local.trigger({ workflowId: "workflow", params: { hold: false } });
  await expect.poll(() => fake.alarmAt).toBeLessThanOrEqual(Date.now() + 1);

  release();
  await expect
    .poll(async () => (await local.poll(second.executionId))?.status)
    .toBe("completed");
});

test("a delay ends the pump, and the alarm resumes the workflow", async () => {
  const workflow = createWorkflow(async function* (step) {
    const before = yield* step.run(() => Date.now());
    yield* step.delay(60);
    const after = yield* step.run(() => Date.now());
    return after - before;
  });
  const { object, sdk } = createObject({ workflow });

  const execution = await sdk.trigger({ workflowId: "workflow" });
  expect(await execution.waitForResult()).toBeGreaterThanOrEqual(55);
  expect(object.alarms.length).toBeGreaterThanOrEqual(1);
});

test("the alarm is set to the end of a delay that follows the first delivery", async () => {
  const workflow = createWorkflow(async function* (step) {
    yield* step.delay(5_000);
  });
  const fake = createFakeStorage();
  // No alarm fires here, so the alarm time is whatever the object last wrote.
  const host = createWorkflowObject(
    { storage: fake.storage, waitUntil() {} },
    { router: { workflow }, logger }
  );
  const local = createLocalSdk<{ workflow: typeof workflow }>(host.runtime);

  const { executionId } = await local.trigger({ workflowId: "workflow" });
  await expect.poll(async () => (await local.poll(executionId))?.status).toBe("suspended");

  const claimAt = host.runtime.executions.nextClaimAt();
  expect(claimAt).toBeGreaterThan(Date.now() + 4_000);
  await expect.poll(() => fake.alarmAt).toBe(claimAt);
});

test("an alarm with more due than it can claim does not leave the next alarm late", async () => {
  const workflow = createWorkflow(async function* (step) {
    yield* step.run(async () => {
      await new Promise((resolve) => setTimeout(resolve, 30));
      return "ran";
    });
    yield* step.delay(300);
    return "done";
  });
  const fake = createFakeStorage();

  // Two executions are due when the alarm fires, and the handler can claim
  // one. The alarm it sets for the other is at the clock, which in the handler
  // is the handler's own scheduled time.
  const admitting = new SqliteRuntime(createSqliteDb(fake.storage));
  for (const executionId of ["first", "second"]) {
    await admitting.executions.admit({
      executionId,
      workflowId: "workflow",
      params: undefined,
      context: new Map(),
    });
  }
  await fake.storage.setAlarm(admitting.executions.nextClaimAt()!);

  const { object, sdk } = createObject({ workflow }, { concurrency: 1 }, fake);
  const results = await Promise.all(
    ["first", "second"].map((id) => sdk.wait(id, { timeout: 5_000 }))
  );
  expect(results.map((r) => r?.status)).toEqual(["completed", "completed"]);
  expect(object.failures).toHaveLength(0);
}, 10_000);

test("after the alarm fails, the object sets it to run again", async () => {
  const workflow = createWorkflow(async function* (step) {
    yield* step.delay(400);
    return yield* step.run(() => "done");
  });
  // Cloudflare's six retries take 1.26 s here, and the storage fails for longer.
  const { object, sdk } = createObject({ workflow }, undefined, undefined, { retryDelay: 20 });

  const { executionId } = await sdk.trigger({ workflowId: "workflow" });
  await expect
    .poll(async () => (await sdk.poll(executionId))?.status, { interval: 10 })
    .toBe("suspended");
  object.fake.fault = new Error("storage is failing");
  await expect.poll(() => object.alarms.length, { timeout: 2_000 }).toBeGreaterThanOrEqual(2);
  await new Promise((resolve) => setTimeout(resolve, 1_500));
  object.fake.fault = undefined;

  expect(await sdk.wait(executionId, { timeout: 8_000 })).toMatchObject({
    status: "completed",
    result: "done",
  });
  // `alarm` did not reject, so Cloudflare's retries were never used.
  expect(object.failures).toHaveLength(0);
  expect(object.dropped).toBe(0);
}, 15_000);

test("a write through the local SDK wakes the object", async () => {
  const workflow = createWorkflow(async function* (step) {
    return yield* step.run(() => "done");
  });
  const { object } = createObject({ workflow });
  const local = createLocalSdk<{ workflow: typeof workflow }>(object.host.runtime, {
    invoker: object.host.invoker,
  });

  expect(await local.triggerAndWait({ workflowId: "workflow" })).toBe("done");
});

test("more executions than slots all run", async () => {
  let running = 0;
  let peak = 0;
  const workflow = createWorkflow(async function* (step, event: { params: { n: number } }) {
    return yield* step.run(async () => {
      peak = Math.max(peak, ++running);
      await new Promise((resolve) => setTimeout(resolve, 10));
      running--;
      return event.params.n;
    });
  });
  const { sdk } = createObject({ workflow }, { concurrency: 2 });

  const executions = await Promise.all(
    [1, 2, 3, 4, 5, 6].map((n) => sdk.trigger({ workflowId: "workflow", params: { n } }))
  );
  const results = await Promise.all(executions.map((e) => e.waitForResult()));
  expect(results).toEqual([1, 2, 3, 4, 5, 6]);
  expect(peak).toBe(2);
});

test("a parent joins a child started in the same object", async () => {
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
  const { sdk } = createObject({ child, parent });

  const execution = await sdk.trigger({ workflowId: "parent" });
  expect(await execution.waitForResult()).toBe(21);
});

test("after an eviction during a delivery, the alarm delivers the execution again", async () => {
  const workflow = createWorkflow(async function* (step) {
    return yield* step.run(() => "delivered");
  });
  const fake = createFakeStorage();

  // An object that claimed the execution and was then evicted: the lease is
  // held, and the alarm is set to when the execution can be claimed again.
  const evicted = new SqliteRuntime(createSqliteDb(fake.storage));
  await evicted.executions.admit({
    executionId: "held",
    workflowId: "workflow",
    params: undefined,
    context: new Map(),
  });
  expect(await evicted.executions.claimDue({ limit: 1, duration: 30 })).toHaveLength(1);
  await fake.storage.setAlarm(evicted.executions.nextClaimAt()!);

  const { object, sdk } = createObject({ workflow }, undefined, fake);
  expect(await sdk.poll("held")).toMatchObject({ status: "running" });

  expect(await sdk.wait("held", { timeout: 5_000 })).toMatchObject({
    status: "completed",
    result: "delivered",
  });
  expect(object.alarms).toHaveLength(1);
}, 10_000);

test("at the delivery limit, the alarm fails the execution instead of delivering it again", async () => {
  let runs = 0;
  const workflow = createWorkflow(async function* (step) {
    return yield* step.run(() => ++runs);
  });
  const fake = createFakeStorage();

  const evicted = new SqliteRuntime(createSqliteDb(fake.storage));
  await evicted.executions.admit({
    executionId: "held",
    workflowId: "workflow",
    params: undefined,
    context: new Map(),
  });
  expect(await evicted.executions.claimDue({ limit: 1, duration: 30 })).toHaveLength(1);
  await fake.storage.setAlarm(evicted.executions.nextClaimAt()!);

  const { object, sdk } = createObject({ workflow }, { maxExpiredDeliveries: 1 }, fake);

  expect(await sdk.wait("held", { timeout: 5_000 })).toMatchObject({
    status: "failed",
    error: { name: "DeliveryLimitError", deliveries: 1 },
  });
  expect(runs).toBe(0);
  expect(object.alarms).toHaveLength(1);
  await expect.poll(() => fake.alarmAt).toBeNull();
}, 10_000);

test("a new instance sets the alarm when an execution is due and no alarm is set", async () => {
  const workflow = createWorkflow(async function* (step) {
    return yield* step.run(() => "delivered");
  });
  const fake = createFakeStorage();

  // An object whose alarm write failed and which was then evicted: the
  // execution is due and no alarm is set.
  const evicted = new SqliteRuntime(createSqliteDb(fake.storage));
  await evicted.executions.admit({
    executionId: "due",
    workflowId: "workflow",
    params: undefined,
    context: new Map(),
  });
  expect(fake.alarmAt).toBeNull();

  // `poll` and `wait` write nothing, so only the alarm can start the delivery.
  const { object, sdk } = createObject({ workflow }, undefined, fake);
  expect(await sdk.wait("due", { timeout: 5_000 })).toMatchObject({
    status: "completed",
    result: "delivered",
  });
  expect(object.alarms).toHaveLength(1);
}, 10_000);

test("a new instance leaves an alarm that is already set to the claim time", async () => {
  const workflow = createWorkflow(async function* (step) {
    yield* step.delay(5_000);
  });
  const fake = createFakeStorage();
  const first = createObject({ workflow }, undefined, fake);
  const { executionId } = await first.sdk.trigger({ workflowId: "workflow" });
  await expect.poll(async () => (await first.sdk.poll(executionId))?.status).toBe("suspended");
  await expect.poll(() => fake.alarmAt).toBe(first.object.host.runtime.executions.nextClaimAt());

  let writes = 0;
  const { setAlarm, deleteAlarm } = fake.storage;
  fake.storage.setAlarm = (at) => (writes++, setAlarm(at));
  fake.storage.deleteAlarm = () => (writes++, deleteAlarm());
  first.object.restart();
  await new Promise((resolve) => setTimeout(resolve, 50));
  expect(writes).toBe(0);
});

test("a delivery in the object starts after the claim is flushed", async () => {
  const order: string[] = [];
  const workflow = createWorkflow(async function* (step) {
    return yield* step.run(() => {
      order.push("step");
      return "done";
    });
  });
  const fake = createFakeStorage();
  const host = createWorkflowObject(
    {
      storage: {
        ...fake.storage,
        async sync() {
          const [{ token }] = fake.storage.sql
            .exec("SELECT lease_token AS token FROM executions")
            .toArray();
          order.push(`sync with lease ${token}`);
        },
      },
      waitUntil() {},
    },
    { router: { workflow }, logger }
  );
  const local = createLocalSdk<{ workflow: typeof workflow }>(host.runtime);

  const { executionId } = await local.trigger({ workflowId: "workflow" });
  await expect.poll(async () => (await local.poll(executionId))?.status).toBe("completed");

  expect(order).toEqual(["sync with lease 1", "step"]);
  fake.close();
});

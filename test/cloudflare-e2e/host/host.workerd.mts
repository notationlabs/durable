import { abortAllDurableObjects, env, evictDurableObject, runInDurableObject } from "cloudflare:test";
import { expect, test } from "vitest";
import { createSqliteDb, type DurableObjectStorageLike } from "@durable/cloudflare";
import { createHttpSdkFactory, createLocalSdk } from "@durable/sdk";
import { SqliteRuntime } from "@durable/sqlite-runtime";
import { CONCURRENCY, SHORT_LEASE, attempts, live, type Router, type Workflows } from "./worker";

type Stub = { fetch(request: Request): Promise<Response> };
type Namespace = { newUniqueId(): unknown; get(id: unknown): Stub };
type State = { storage: DurableObjectStorageLike & { getAlarm(): Promise<number | null> } };

// No test calls `runDurableObjectAlarm`: every alarm here is one workerd fired
// at the time the object set.
function createObject(binding: "WORKFLOWS" | "SHORT_LEASE" = "WORKFLOWS") {
  const namespace = (env as unknown as Record<string, Namespace>)[binding];
  const id = namespace.newUniqueId();
  // A stub stays broken after its object is aborted, so each call gets its own.
  const stub = () => namespace.get(id);
  const sdk = createHttpSdkFactory<Router>()({
    url: "http://object/durable",
    fetch: (input, init) => stub().fetch(new Request(input, init)),
  });
  // Each call runs wholly inside the object. A promise made in the test and
  // awaited inside the object resumes in a different order (see
  // ../conformance/cas-race.workerd.mts), so nothing made here is passed in.
  const inObject = <R,>(fn: (instance: Workflows, state: State) => R | Promise<R>) =>
    runInDurableObject(stub() as never, fn as never) as Promise<R>;
  const inspect = () =>
    inObject(async (instance, state) => ({
      alarm: await state.storage.getAlarm(),
      claimAt: instance.durable.runtime.executions.nextClaimAt(),
      alarms: instance.alarms,
    }));
  // The alarm that covers a lease is this far ahead of the claim.
  const lease = (binding === "WORKFLOWS" ? 30_000 : SHORT_LEASE) + 1_000;
  // Whether nothing is left to claim and no alarm is set further ahead than a
  // lease. The object leaves a nearer alarm to fire, and deletes any other.
  const settled = async () => {
    const { alarm, claimAt } = await inspect();
    return claimAt === null && (alarm === null || alarm <= Date.now() + lease);
  };
  return { stub, sdk, inObject, inspect, settled };
}

test("a trigger over the object's routes runs the workflow, and the alarm it leaves fires and sets no other", async () => {
  const { sdk, inspect } = createObject("SHORT_LEASE");

  const execution = await sdk.trigger({ workflowId: "double", params: { n: 20 } });
  expect(await execution.waitForResult()).toBe(41);
  const after = await inspect();
  expect(after).toMatchObject({ claimAt: null, alarms: 0 });
  expect(after.alarm).toBeLessThanOrEqual(Date.now() + SHORT_LEASE + 1_000);

  await expect.poll(inspect, { timeout: 5_000 }).toMatchObject({ alarm: null, alarms: 1 });
  await new Promise((resolve) => setTimeout(resolve, 200));
  expect(await inspect()).toMatchObject({ alarm: null, claimAt: null, alarms: 1 });
});

test("executions that finish within a lease of the first write the alarm once", async () => {
  const { sdk, inObject, inspect } = createObject();

  const first = await sdk.trigger({ workflowId: "double", params: { n: 1 } });
  expect(await first.waitForResult()).toBe(3);
  const { alarm } = await inspect();
  expect(alarm).toBeGreaterThan(Date.now() + 25_000);

  const writes = await inObject((_instance, state) => {
    const counts = { set: 0, deleted: 0 };
    const { storage } = state;
    const setAlarm = storage.setAlarm.bind(storage);
    const deleteAlarm = storage.deleteAlarm.bind(storage);
    storage.setAlarm = (at) => (counts.set++, setAlarm(at));
    storage.deleteAlarm = () => (counts.deleted++, deleteAlarm());
    return counts;
  });
  const second = await sdk.trigger({ workflowId: "double", params: { n: 2 } });
  expect(await second.waitForResult()).toBe(5);
  expect(await inspect()).toMatchObject({ alarm, claimAt: null, alarms: 0 });
  expect(writes).toEqual({ set: 0, deleted: 0 });
});

test("a delay ends the pump, and the alarm resumes the workflow", async () => {
  const { sdk, inspect, settled } = createObject();

  const execution = await sdk.trigger({ workflowId: "delayed", params: { ms: 300 } });
  expect(await execution.waitForResult()).toBeGreaterThanOrEqual(295);
  expect((await inspect()).alarms).toBeGreaterThanOrEqual(1);
  await expect.poll(settled).toBe(true);
});

test("the alarm is set to the end of a delay that follows the first delivery", async () => {
  const { sdk, inspect, settled } = createObject();

  const { executionId } = await sdk.trigger({ workflowId: "delayed", params: { ms: 5_000 } });
  await expect.poll(async () => (await sdk.poll(executionId))?.status).toBe("suspended");

  const { claimAt } = await inspect();
  expect(claimAt).toBeGreaterThan(Date.now() + 4_000);
  await expect.poll(async () => (await inspect()).alarm).toBe(claimAt);
  expect((await inspect()).alarms).toBe(0);

  await sdk.cancel(executionId);
  await expect.poll(async () => (await sdk.poll(executionId))?.status).toBe("cancelled");
  await expect.poll(settled).toBe(true);
});

test("a write through the local SDK wakes the object", async () => {
  const { inObject } = createObject();

  const result = await inObject((instance) =>
    createLocalSdk<Router>(instance.durable.runtime, {
      invoker: instance.durable.invoker,
    }).triggerAndWait({ workflowId: "double", params: { n: 1 } })
  );
  expect(result).toBe(3);
});

test("more executions than slots all run", async () => {
  const { sdk } = createObject();
  live.running = 0;
  live.peak = 0;

  const executions = await Promise.all(
    [1, 2, 3, 4, 5, 6].map((n) => sdk.trigger({ workflowId: "slow", params: { n } }))
  );
  const results = await Promise.all(executions.map((e) => e.waitForResult()));
  expect(results).toEqual([1, 2, 3, 4, 5, 6]);
  expect(live.peak).toBe(CONCURRENCY);
});

test("a parent joins a child started in the same object", async () => {
  const { sdk, settled } = createObject();

  const execution = await sdk.trigger({ workflowId: "parent" });
  expect(await execution.waitForResult()).toBe(21);
  await expect.poll(settled).toBe(true);
});

test("after an eviction with a lease held, the alarm delivers the execution again", async () => {
  const { stub, sdk, inObject, inspect, settled } = createObject();

  // A second runtime over the same storage claims the execution, so the
  // object's own host sees no commit and starts no delivery. The storage is
  // left as an object leaves it when it is evicted during a delivery.
  await inObject(async (_instance, state) => {
    const holder = new SqliteRuntime(createSqliteDb(state.storage));
    await holder.executions.admit({
      executionId: "held",
      workflowId: "double",
      params: { n: 4 },
      context: new Map(),
    });
    const leases = await holder.executions.claimDue({ limit: 1, duration: 400 });
    if (leases.length !== 1) throw new Error("The execution was not claimed");
    await state.storage.setAlarm(holder.executions.nextClaimAt()!);
  });
  expect(await sdk.poll("held")).toMatchObject({ status: "running" });
  await evictDurableObject(stub() as never);

  expect(await sdk.wait("held", { timeout: 5_000 })).toMatchObject({
    status: "completed",
    result: 9,
  });
  expect((await inspect()).alarms).toBeGreaterThanOrEqual(1);
  await expect.poll(settled).toBe(true);
});

test("after an eviction during a delivery, the alarm delivers the execution again", async () => {
  const { stub, sdk, inspect, settled } = createObject("SHORT_LEASE");
  const key = crypto.randomUUID();

  const { executionId } = await sdk.trigger({ workflowId: "stuckOnce", params: { key } });
  await expect.poll(() => attempts.get(key)).toBe(1);
  // Outlive one renewal, so the alarm has been moved with the lease.
  await new Promise((resolve) => setTimeout(resolve, SHORT_LEASE / 2));
  const before = await inspect();
  expect(before.alarm).not.toBeNull();
  expect(before.alarm).toBeLessThanOrEqual(before.claimAt!);
  // `evictDurableObject` waits for the delivery, which never settles. Aborting
  // drops the instance at once and keeps its storage and alarm.
  await abortAllDurableObjects();
  expect(await sdk.poll(executionId)).toMatchObject({ status: "running" });

  expect(await sdk.wait(executionId, { timeout: 10_000 })).toMatchObject({
    status: "completed",
    result: 2,
  });
  expect((await inspect()).alarms).toBeGreaterThanOrEqual(1);
  await expect.poll(settled).toBe(true);
}, 20_000);

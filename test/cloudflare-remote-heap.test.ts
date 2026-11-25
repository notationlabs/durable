import { expect, test } from "vitest";
import { createSqliteDb } from "@durable/cloudflare";
import { SqliteRuntime } from "@durable/sqlite-runtime";
import { RemoteRuntime, serveRuntime, type RuntimeCall } from "../packages/cloudflare/src/remote-runtime";
import { createFakeStorage } from "./durable-object-storage";

// A runtime with one leased execution that has two recorded steps, and a
// remote runtime for its delivery that starts with `sent` of those records.
async function setup(send: (records: Record<string, unknown>) => Record<string, unknown>) {
  const fake = createFakeStorage();
  const runtime = new SqliteRuntime(createSqliteDb(fake.storage));
  await runtime.executions.admit({
    executionId: "order",
    workflowId: "workflow",
    params: undefined,
    context: new Map(),
  });
  const [lease] = await runtime.executions.claimDue({ limit: 1, duration: 30_000 });
  for (const stepKey of ["small", "large"]) {
    await runtime.heap.writeStep({
      lease,
      stepKey,
      stepAttempt: 0,
      done: true,
      stepResponseJson: JSON.stringify(stepKey),
    });
  }
  const calls: string[] = [];
  const serve = serveRuntime(runtime);
  const call: RuntimeCall = (client, method, args) => {
    calls.push(`${client}.${method}`);
    return serve(client, method, args);
  };
  const records = send(Object.fromEntries(runtime.heap.readSteps("order")));
  const remote = new RemoteRuntime(call, {
    steps: { executionId: "order", records: records as never },
  });
  return { runtime, remote, lease, calls, close: () => fake.close() };
}

test("a delivery reads the records it started with, and the steps without one, without a call", async () => {
  const { remote, calls, close } = await setup((records) => records);

  expect(await remote.heap.readStep({ executionId: "order", stepKey: "small" })).toEqual({
    stepResponseJson: '"small"',
    meta: { attempt: 0, done: true },
  });
  expect(await remote.heap.readStep({ executionId: "order", stepKey: "next" })).toBeNull();

  expect(calls).toEqual([]);
  close();
});

test("a record that was too large to send is read from the runtime once", async () => {
  const { remote, calls, close } = await setup((records) => ({ ...records, large: null }));

  const first = await remote.heap.readStep({ executionId: "order", stepKey: "large" });
  const second = await remote.heap.readStep({ executionId: "order", stepKey: "large" });

  expect(first).toEqual({ stepResponseJson: '"large"', meta: { attempt: 0, done: true } });
  expect(second).toEqual(first);
  expect(calls).toEqual(["heap.readStep"]);
  close();
});

test("a delivery reads back the steps it writes, and each later attempt of a step", async () => {
  const { remote, lease, calls, close } = await setup((records) => records);
  const write = (stepAttempt: number, done: boolean) =>
    remote.heap.writeStep({
      lease,
      stepKey: "next",
      stepAttempt,
      done,
      stepResponseJson: JSON.stringify(stepAttempt),
    });

  await write(0, false);
  expect(await remote.heap.readStep({ executionId: "order", stepKey: "next" })).toMatchObject({
    meta: { attempt: 0, done: false },
  });
  await write(1, true);
  expect(await remote.heap.readStep({ executionId: "order", stepKey: "next" })).toEqual({
    stepResponseJson: "1",
    meta: { attempt: 1, done: true },
  });

  expect(calls).toEqual(["heap.writeStep", "heap.writeStep"]);
  close();
});

test("a write that loses the lease changes nothing the delivery reads", async () => {
  const { runtime, remote, lease, close } = await setup((records) => records);
  await runtime.executions.release({ lease });

  const written = await remote.heap.writeStep({
    lease,
    stepKey: "next",
    stepAttempt: 0,
    done: true,
    stepResponseJson: "1",
  });

  expect(written).toEqual({ status: "lost" });
  expect(await remote.heap.readStep({ executionId: "order", stepKey: "next" })).toBeNull();
  close();
});

test("the steps of another execution are read from the runtime", async () => {
  const { remote, calls, close } = await setup((records) => records);

  expect(await remote.heap.readStep({ executionId: "other", stepKey: "small" })).toBeNull();

  expect(calls).toEqual(["heap.readStep"]);
  close();
});

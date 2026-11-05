import { afterEach, expect, test, vi } from "vitest";
import type { Logger } from "pino";
import { ExecutionDispatcher, WorkflowRunner, type WorkflowRouter, type ExecutionRecord } from "@yieldstar/core";
import { createWorkflowInvoker } from "@yieldstar/test-invoker";
import { MemoryRuntime } from "@yieldstar/test-runtime";
import { createLocalSdk, createWorkflow } from "yieldstar";

const logger = { info() {}, error() {}, warn() {} } as unknown as Logger;
const dispatchers: ExecutionDispatcher[] = [];

afterEach(async () => {
  await Promise.all(dispatchers.splice(0).map((dispatcher) => dispatcher.stop()));
});

function start(router: WorkflowRouter) {
  const runtime = new MemoryRuntime();
  const runner = new WorkflowRunner(runtime, { router, logger });
  const invoker = createWorkflowInvoker({ runner, logger });
  const dispatcher = new ExecutionDispatcher(runtime, {
    invoker,
    logger,
    pollInterval: 1,
    keepAlive: false,
  });
  dispatchers.push(dispatcher);
  dispatcher.start();
  const sdk = createLocalSdk(runtime, { invoker, pollInterval: 1 });
  return { runtime, sdk };
}

/** Waits until the execution is parked with a divergence, and returns it. */
async function divergence(runtime: MemoryRuntime, executionId: string) {
  return vi.waitFor(async () => {
    const record = (await runtime.executions.get(executionId) as ExecutionRecord | undefined);
    if (!record?.divergence) throw new Error("not diverged yet");
    return record.divergence;
  });
}

// Each case changes which step the code asks for between its first and second
// delivery. Replay then finds the first step's record unconsumed, so the
// second step does not run and the execution is parked.

test("a keyless step.run at a different call site on the next delivery diverges instead of running", async () => {
  const mock1 = vi.fn(() => 1);
  const mock2 = vi.fn(() => 2);
  let executionIdx = -1;

  const workflow = createWorkflow(async function* (step) {
    executionIdx++;
    if (executionIdx === 0) {
      yield* step.run(mock1);
      yield* step.delay("pause", 10);
    } else {
      yield* step.run(mock2);
    }
  });

  const { runtime, sdk } = start({ workflow });
  const { executionId } = await sdk.trigger({ workflowId: "workflow" });

  expect(await divergence(runtime, executionId)).toMatchObject({
    code: "NONDETERMINISTIC_REPLAY",
    workflowVersion: 1,
  });
  expect(mock1).toBeCalledTimes(1);
  expect(mock2).not.toBeCalled();
  expect((await runtime.executions.get(executionId))!.status).toBe("suspended");
});

test("a keyed step.run renamed on the next delivery diverges with the new key", async () => {
  const mock1 = vi.fn(() => 1);
  const mock2 = vi.fn(() => 2);
  let executionIdx = -1;

  const workflow = createWorkflow(async function* (step) {
    executionIdx++;
    if (executionIdx === 0) {
      yield* step.run("step 1", mock1);
      yield* step.delay("pause", 10);
    } else {
      yield* step.run("step 2", mock2);
    }
  });

  const { runtime, sdk } = start({ workflow });
  const { executionId } = await sdk.trigger({ workflowId: "workflow" });

  expect(await divergence(runtime, executionId)).toMatchObject({
    code: "NONDETERMINISTIC_REPLAY",
    stepKey: "step 2",
  });
  expect(mock1).toBeCalledTimes(1);
  expect(mock2).not.toBeCalled();
});

test("a keyless step.delay at a different call site on the next delivery diverges", async () => {
  let executionIdx = -1;

  const workflow = createWorkflow(async function* (step) {
    executionIdx++;
    if (executionIdx < 1) {
      yield* step.delay(10);
    } else {
      yield* step.delay(10);
    }
  });

  const { runtime, sdk } = start({ workflow });
  const { executionId } = await sdk.trigger({ workflowId: "workflow" });

  expect(await divergence(runtime, executionId)).toMatchObject({
    code: "NONDETERMINISTIC_REPLAY",
  });
});

test("a keyed step.delay renamed on the next delivery diverges", async () => {
  let executionIdx = -1;

  const workflow = createWorkflow(async function* (step) {
    executionIdx++;
    if (executionIdx < 1) {
      yield* step.delay("step 1", 10);
    } else {
      yield* step.delay("step 2", 10);
    }
  });

  const { runtime, sdk } = start({ workflow });
  const { executionId } = await sdk.trigger({ workflowId: "workflow" });

  expect(await divergence(runtime, executionId)).toMatchObject({
    code: "NONDETERMINISTIC_REPLAY",
    stepKey: "step 2",
  });
});

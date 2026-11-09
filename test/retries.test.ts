import { expect, test } from "vitest";
import { createWorkflow, RetryableError } from "@durable/sdk";
import { createTestSdkFactory } from "@durable/test-utils";
import { pino } from "pino";

const logger = pino({ level: "fatal" });
const createSdk = createTestSdkFactory({ logger });

test("retrying an error for maxAttempts", async () => {
  let runs = 0;

  const workflow = createWorkflow(async function* (step) {
    yield* step.run(async () => {
      runs++;
      throw new RetryableError("Step error", {
        maxAttempts: 10,
        retryInterval: 1,
      });
    });
  });

  try {
    const sdk = createSdk({ workflow });
    await sdk.triggerAndWait({ workflowId: "workflow" });
  } catch {
    expect(runs).toEqual(10);
  }
});

test("retrying an for maxAttempts (irrespective of number of times error is thrown)", async () => {
  let runs = 0;

  const workflow = createWorkflow(async function* (step) {
    yield* step.run(async () => {
      runs++;
      if (runs === 5) {
        throw new RetryableError("Step error", {
          maxAttempts: 4,
          retryInterval: 1,
        });
      }
      throw new RetryableError("Step error", {
        maxAttempts: 10,
        retryInterval: 1,
      });
    });
  });

  try {
    const sdk = createSdk({ workflow });
    await sdk.triggerAndWait({ workflowId: "workflow" });
  } catch {
    expect(runs).toEqual(5);
  }
});

test("retrying an for maxAttempts (irrespective of number of number of workflow executions)", async () => {
  let runs = 0;

  const workflow = createWorkflow(async function* (step) {
    try {
      yield* step.run(async () => {
        runs++;
        throw new RetryableError("Step error", {
          maxAttempts: 3,
          retryInterval: 1,
        });
      });
    } catch {}
    try {
      yield* step.run(async () => {
        runs++;
        throw new RetryableError("Step error", {
          maxAttempts: 3,
          retryInterval: 1,
        });
      });
    } catch {}
  });

  const sdk = createSdk({ workflow });
  await sdk.triggerAndWait({ workflowId: "workflow" });

  expect(runs).toEqual(6);
});

test("retrying an error after retry interval", async () => {
  const executions: number[] = [];
  let start = Date.now();

  const workflow = createWorkflow(async function* (step) {
    yield* step.run(async () => {
      executions.push((Date.now() - start) / 100);
      if (executions.length > 3) return;
      throw new RetryableError("Step error", {
        maxAttempts: 4,
        retryInterval: 100,
      });
    });
  });

  const sdk = createSdk({ workflow });
  await sdk.triggerAndWait({ workflowId: "workflow" });

  expect(executions[0]).toBeCloseTo(0, 0);
  expect(executions[1]).toBeCloseTo(1, 0);
  expect(executions[2]).toBeCloseTo(2, 0);
  expect(executions[3]).toBeCloseTo(3, 0);
});

test("a retryInterval of 0 retries without waiting", async () => {
  let runs = 0;

  const workflow = createWorkflow(async function* (step) {
    return yield* step.run(async () => {
      runs++;
      if (runs < 3) {
        throw new RetryableError("Step error", { maxAttempts: 3, retryInterval: 0 });
      }
      return runs;
    });
  });

  const sdk = createSdk({ workflow });
  const startedAt = Date.now();
  await expect(sdk.triggerAndWait({ workflowId: "workflow" })).resolves.toBe(3);
  expect(Date.now() - startedAt).toBeLessThan(10_000);
});

test("RetryableError rejects a retry policy it cannot follow", () => {
  expect(() => new RetryableError("e", { maxAttempts: 0, retryInterval: 1 })).toThrow(RangeError);
  expect(() => new RetryableError("e", { maxAttempts: 1.5, retryInterval: 1 })).toThrow(RangeError);
  expect(() => new RetryableError("e", { maxAttempts: 2, retryInterval: -1 })).toThrow(RangeError);
  expect(() => new RetryableError("e", { maxAttempts: 2, retryInterval: Infinity })).toThrow(RangeError);
});

test("workflow accepts fewer type arguments than it declares", async () => {
  const greet = createWorkflow<{ name: string }, string>(async function* (step, event) {
    return yield* step.run(() => `Hello, ${event.params.name}`);
  });
  const notify = createWorkflow<{ orderId: string }>(async function* (step, event) {
    yield* step.run(() => event.params.orderId);
  });

  const sdk = createSdk({ greet, notify });
  await expect(
    sdk.triggerAndWait({ workflowId: "greet", params: { name: "World" } })
  ).resolves.toBe("Hello, World");
});

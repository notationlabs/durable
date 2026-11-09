# Testing

`@notation/durable/testing` runs workflows inside the test process. It keeps Durable's records in memory, so a test needs no database file and starts no worker process.

This page tests a workflow that reminds a user that their trial is ending. It waits 11 days, reads the account's plan from a store, and sends an email if the account has not paid:

```ts [src/trial-ending.ts]
import { workflow } from "@notation/durable";
import { Account } from "./account";
import { email } from "./email";

export const trialEnding = workflow<{ accountId: string; email: string }, "sent" | "skipped">(
  async function* (step, event) {
    yield* step.delay("wait-for-trial", 11 * DAY);

    const account = step.store(Account, { id: event.params.accountId });
    const plan = yield* account.select("check-plan", (state) => state.plan);

    if (plan === "paid") {
      return "skipped";
    }

    yield* step.run("send-reminder", async () => {
      await email.send(event.params.email, "Your trial ends in 3 days");
    });

    return "sent";
  }
);
```

`Account` is a store with one field, `plan`, which starts as `"trial"`. `email` is an object with a `send` method that calls the email service. The router gives the workflow the ID `trial-ending`.

## Test SDK

`createTestSdk` takes the router, and returns an SDK whose executions run in the test process:

```ts
import { createTestSdk } from "@notation/durable/testing";
import { router } from "./router";

const sdk = createTestSdk(router);
```

The test SDK has the methods of the [local SDK](./sdk-local.md), such as `trigger`, `wait` and `triggerAndWait`. It also has two properties:

| Property | Holds |
| --- | --- |
| `runtime` | The in-memory runtime that holds the records |
| `dispatcher` | The dispatcher that delivers executions |

The dispatcher starts on the first `trigger` or `triggerAndWait`. It does not keep the process alive, so a test does not need to stop it.

**Each call to `createTestSdk` starts with no records and no stores.** Tests that create their own SDK do not share state.

| Option | Default | Does |
| --- | --- | --- |
| `clock` | `Date.now` | The clock that the runtime and the workflow's steps read |
| `logger` | A pino logger at level `"error"` | The logger for the dispatcher and the code that runs workflows |

## Running a workflow to the end

`triggerAndWait` starts an execution and returns the workflow's return value. A test of a workflow without delays can use it directly:

```ts
const result = await sdk.triggerAndWait({
  workflowId: "trial-ending",
  params: { accountId: "acct-1", email: "ada@example.com" },
});
```

If the workflow throws, `triggerAndWait` throws the same error. `trialEnding` waits 11 days first, so a test of it also needs a clock that it can move forward.

## Skipping a delay

`step.delay` reads the time from the runtime's clock. A test that passes its own clock can move time forward without waiting:

```ts
function setup() {
  let now = Date.now();

  const sdk = createTestSdk(router, { clock: () => now });
  const advance = (ms: number) => {
    now += ms;
  };

  return { sdk, advance };
}
```

The first delivery records the delay's end time, 11 days after the clock's time. The test waits until the execution is `suspended`, so that the delay is recorded, and then moves the clock past the end time:

```ts
const { sdk, advance } = setup();

const { executionId } = await sdk.trigger({
  workflowId: "trial-ending",
  params: { accountId: "acct-1", email: "ada@example.com" },
});

await waitForStatus(sdk, executionId, "suspended");
advance(11 * DAY);

const execution = await sdk.wait(executionId);
```

The dispatcher reads the same clock, so it finds the execution due and delivers it. `sdk.wait` then returns the view of the completed execution.

**Move the clock only after the execution is `suspended`.** If the clock moves before the first delivery, the delivery records an end time 11 days after the new time, and the execution keeps waiting.

`waitForStatus` reads the execution until it has the status:

```ts
import type { ExecutionObserver, ExecutionStatus } from "@notation/durable";

async function waitForStatus(sdk: ExecutionObserver, executionId: string, status: ExecutionStatus) {
  while ((await sdk.poll(executionId))?.status !== status) {
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
}
```

The clock affects only the runtime and the steps. A `deadline` that a workflow computes from `Date.now()` does not move with it.

## Replacing side effects

The workflow's code runs in the test process, so the test runner's mocks apply to it. This test replaces `email.send` and checks that the workflow called it:

```ts
import { afterEach, expect, mock, spyOn, test } from "bun:test";

afterEach(() => {
  mock.restore();
});

test("sends a reminder 11 days into the trial", async () => {
  const send = spyOn(email, "send").mockResolvedValue(undefined);
  const { sdk, advance } = setup();

  const { executionId } = await sdk.trigger({
    workflowId: "trial-ending",
    params: { accountId: "acct-1", email: "ada@example.com" },
  });

  await waitForStatus(sdk, executionId, "suspended");
  expect(send).not.toHaveBeenCalled();

  advance(11 * DAY);

  expect(await sdk.wait(executionId)).toMatchObject({ status: "completed", result: "sent" });
  expect(send).toHaveBeenCalledWith("ada@example.com", "Your trial ends in 3 days");
});
```

`mock.restore` puts the original `email.send` back after each test, so the next test starts with a new spy that has no calls.

The examples use `bun:test`. The test SDK does not depend on a test runner, so the same tests work with Vitest or `node:test`, with their own mock functions.

## Setting up store state

`sdk.runtime.stores` is the runtime's [store client](./store-external.md). A test uses it to put a store in the state that the test needs, before or while the workflow runs:

```ts
test("skips the reminder when the account has paid", async () => {
  const send = spyOn(email, "send").mockResolvedValue(undefined);
  const { sdk, advance } = setup();

  await sdk.runtime.stores.store(Account, "acct-2").update((draft) => {
    draft.plan = "paid";
  });

  const { executionId } = await sdk.trigger({
    workflowId: "trial-ending",
    params: { accountId: "acct-2", email: "ada@example.com" },
  });

  await waitForStatus(sdk, executionId, "suspended");
  advance(11 * DAY);

  expect(await sdk.wait(executionId)).toMatchObject({ result: "skipped" });
  expect(send).not.toHaveBeenCalled();
});
```

The test can also read a store after the workflow ends, with `get`, to check what the workflow wrote.

## Building a test setup by hand

`@notation/durable/testing` also exports the parts that `createTestSdk` uses:

| Export | Does |
| --- | --- |
| `MemoryRuntime` | A runtime that keeps its records in memory. Takes a `clock` option |
| `createInProcessInvoker` | An invoker that runs each delivery in the current process, with a `WorkflowRunner` |

A test that needs a different dispatcher configuration, or a local SDK with different options, builds the setup from these parts:

```ts
import pino from "pino";
import { ExecutionDispatcher, WorkflowRunner, createLocalSdk } from "@notation/durable";
import { MemoryRuntime, createInProcessInvoker } from "@notation/durable/testing";
import { router, type Router } from "./router";

const logger = pino({ level: "error" });
const runtime = new MemoryRuntime();
const runner = new WorkflowRunner(runtime, { router, logger });
const invoker = createInProcessInvoker({ runner, logger });

const dispatcher = new ExecutionDispatcher(runtime, { invoker, logger, keepAlive: false });
const sdk = createLocalSdk<Router>(runtime, { invoker });

dispatcher.start();
```

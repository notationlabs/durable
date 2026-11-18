# Testing

`@notation/durable/testing` executes workflows in memory inside your test process, with no SQLite file and no forked worker processes. Every call to `createTestSdk` isolates its own executions, step records, and stores so tests run cleanly in parallel or sequentially.

The examples below test a trial-ending workflow that waits 11 days, reads an account's plan from a store, and sends a reminder email when the account is still on `"trial"`:

```ts [src/workflows/trial-ending.ts]
import { workflow } from "@notation/durable";
import { Account } from "../stores/account";
import { email } from "../email";

const DAY = 24 * 60 * 60 * 1000;

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

## Creating a test SDK

Pass your workflow router to `createTestSdk` to build an in-memory SDK that shares the same methods as the [local SDK](./local-sdk.md):

```ts [test/trial-ending.test.ts]
import { createTestSdk } from "@notation/durable/testing";
import { router } from "../src/workflows/router";

const sdk = createTestSdk(router);
```

`createTestSdk(router, options?)` accepts two optional settings:

| Option | Purpose |
| :--- | :--- |
| `clock` | Returns the current timestamp in milliseconds for the runtime and steps |
| `logger` | Overrides the pino logger used by the dispatcher and runner |

By default, `clock` reads `Date.now` and `logger` logs at level `"error"`.

Alongside the standard execution methods, the returned test helper exposes two test controls:

| Property | Purpose |
| :--- | :--- |
| `runtime` | In-memory runtime holding executions, step records, stores, effects, and joins |
| `dispatcher` | Dispatcher that starts automatically on the first `trigger` or `triggerAndWait` |

The test dispatcher polls every millisecond and sets `keepAlive: false`, so its timers never prevent your test runner from exiting.

## Testing fast workflows with `triggerAndWait`

For workflows without long delays, call `sdk.triggerAndWait` to start an execution and await its return value in one call:

```ts [test/welcome.test.ts]
import { expect, test } from "bun:test";
import { createTestSdk } from "@notation/durable/testing";
import { router } from "../src/workflows/router";

test("returns the welcome receipt", async () => {
  const sdk = createTestSdk(router);

  const result = await sdk.triggerAndWait({
    workflowId: "send-welcome",
    params: { email: "ada@example.com" },
  });

  expect(result).toEqual({ delivered: true });
});
```

If the workflow throws an unhandled error, `triggerAndWait` rejects with that recorded error. If the execution is cancelled, `triggerAndWait` rejects with `WorkflowCancelledError`.

## Advancing fake clocks

### Stepping past `step.delay`

Step delays, poll retries, and dispatcher due checks all read time from the runtime's clock. Pass a controllable `clock` function to `createTestSdk` so tests can skip multi-day delays instantaneously:

```ts [test/clock-setup.ts]
import { createTestSdk } from "@notation/durable/testing";
import type { ExecutionObserver, ExecutionStatus } from "@notation/durable";
import { router } from "../src/workflows/router";

export function setup() {
  let now = 1_700_000_000_000;
  const sdk = createTestSdk(router, { clock: () => now });

  const advance = (ms: number) => {
    now += ms;
  };

  return { sdk, advance };
}

export async function waitForStatus(
  sdk: ExecutionObserver,
  executionId: string,
  status: ExecutionStatus
) {
  while ((await sdk.poll(executionId))?.status !== status) {
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
}
```

Trigger the workflow with `sdk.trigger`, wait until the execution status becomes `"suspended"`, advance the clock past the delay, and await `sdk.wait(executionId)`:

```ts [test/delay.test.ts]
import { expect, test } from "bun:test";
import { setup, waitForStatus } from "./clock-setup";

const DAY = 24 * 60 * 60 * 1000;

test("completes after the 11-day trial delay", async () => {
  const { sdk, advance } = setup();

  const { executionId } = await sdk.trigger({
    workflowId: "trial-ending",
    params: { accountId: "acct-1", email: "ada@example.com" },
  });

  await waitForStatus(sdk, executionId, "suspended");
  advance(11 * DAY);

  const execution = await sdk.wait(executionId);
  expect(execution).toMatchObject({ status: "completed", result: "sent" });
});
```

Always wait for `"suspended"` before advancing `now`. Moving the clock before the first delivery runs causes `step.delay` to compute its wake timestamp from the already-advanced clock, leaving the execution suspended.

### Deadlines in `when` and `join`

Timed waits and joins compare their `deadline` timestamp against the runtime clock (`clock() >= deadline`). If your workflow computes a deadline directly with `Date.now()`, a custom runtime clock function alone will not shift `Date.now()`.

Because `createTestSdk` defaults its clock to `Date.now()`, enabling your test runner's fake system time – such as Vitest's `vi.useFakeTimers()` – advances both clocks together. Alternatively, pass deadlines through workflow parameters when driving a custom clock directly.

## Mocking external side effects

Because `createTestSdk` runs workflow steps inside your test process, test-runner spies and mocks intercept external clients directly:

```ts [test/trial-ending.test.ts]
import { afterEach, expect, mock, spyOn, test } from "bun:test";
import { email } from "../src/email";
import { setup, waitForStatus } from "./clock-setup";

const DAY = 24 * 60 * 60 * 1000;

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

Restore mocks in `afterEach` so each test starts with a clean spy. `createTestSdk` has no dependency on `bun:test` and works identically with Vitest or `node:test`.

## Pre-seeding and inspecting stores

Use `sdk.runtime.stores.store(Definition, id)` to seed store state before triggering a workflow or to inspect state after an execution finishes:

```ts [test/paid-account.test.ts]
import { afterEach, expect, mock, spyOn, test } from "bun:test";
import { Account } from "../src/stores/account";
import { email } from "../src/email";
import { setup, waitForStatus } from "./clock-setup";

const DAY = 24 * 60 * 60 * 1000;

afterEach(() => {
  mock.restore();
});

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

  expect(await sdk.wait(executionId)).toMatchObject({ status: "completed", result: "skipped" });
  expect(send).not.toHaveBeenCalled();

  const snapshot = await sdk.runtime.stores.store(Account, "acct-2").get();
  expect(snapshot.state.plan).toBe("paid");
});
```

## Manual runtime wiring

When a test needs custom dispatcher timing, lease settings, or local SDK options, assemble `MemoryRuntime` and `createInProcessInvoker` directly as shown in [the testing reference](../reference/testing.md).

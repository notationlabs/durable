# Child Workflows

A workflow can start another workflow registered in the same router as a step. Each child workflow runs as its own execution with its own execution ID, step records, and lifecycle, linked to the parent execution.

::code-group

```ts [src/workflows/ship-order.ts]
import { workflow } from "@notation/durable";

export const shipOrder = workflow<{ orderId: string }, { tracking: string }>(
  async function* (step, event) {
    const label = yield* step.run("buy-label", async () => {
      return await courier.buyLabel(event.params.orderId);
    });

    yield* step.run("book-pickup", async () => {
      await courier.bookPickup(label.id);
    });

    return { tracking: label.tracking };
  }
);
```

```ts [src/workflows/place-order.ts]
import { workflow } from "@notation/durable";
import type { shipOrder } from "./ship-order";

export const placeOrder = workflow<{ orderId: string }>(async function* (step, event) {
  const { orderId } = event.params;

  const shipment = yield* step.call<typeof shipOrder>("ship", {
    workflowId: "ship-order",
    params: { orderId },
  });

  yield* step.run("email-tracking", async () => {
    await mailer.sendTracking(orderId, shipment.tracking);
  });
});
```

::

## `step.call`

`step.call(stepKey, target, options?)` starts the child workflow, suspends the parent while the child runs, and returns the child's result under `stepKey`.

Passing `<typeof shipOrder>` lets TypeScript check `target.params` and infer the return type. Because a workflow module cannot import the router directly, TypeScript does not verify that `workflowId` matches the type argument.

Notation Durable identifies each child start by the parent's execution ID and `stepKey`. If a delivery fails after starting the child but before recording the step result, the next delivery repeats the start request and receives the same child execution ID instead of creating a second child.

Once the child finishes, the parent wakes, records the result under `stepKey`, and returns that recorded value on replay without starting or joining again.

Omitting `params` differs from passing an empty object. Child parameters must be [durable values](./step-values.md); passing invalid parameters throws `WorkflowStartError` with code `INVALID_START_PARAMETERS`.

## `step.start` and `step.join`

`step.call` combines starting a child and waiting for it under one step key. When a parent needs to run other work while the child executes, launch multiple children, or wait on an execution started elsewhere, call `step.start` and `step.join` separately:

```ts [src/workflows/place-order-async.ts]
import { workflow } from "@notation/durable";

export const placeOrder = workflow<{ orderId: string }>(async function* (step, event) {
  const { orderId } = event.params;

  const { executionId } = yield* step.start("ship", {
    workflowId: "ship-order",
    params: { orderId },
  });

  yield* step.run("email-receipt", async () => {
    await mailer.sendReceipt(orderId);
  });

  const shipment = yield* step.join<{ tracking: string }>("ship-join", executionId);

  yield* step.run("email-tracking", async () => {
    await mailer.sendTracking(orderId, shipment.tracking);
  });
});
```

`step.start(stepKey, target, options?)` records `{ executionId }` immediately without waiting for the child to finish.

`step.join<Result>(stepKey, executionId, options?)` suspends until `executionId` reaches a terminal status and returns its result; Durable does not validate the type parameter at runtime.

Any workflow can join any execution ID – including executions started via the SDK or HTTP API – and multiple workflows can join the same target simultaneously.

## Deadlines

Both `step.call` and `step.join` accept an optional `deadline` – an absolute Unix timestamp in milliseconds compared against the runtime clock. When set, the step returns a discriminated union result by status:

```ts [src/workflows/place-order-timed.ts]
import { workflow } from "@notation/durable";
import type { shipOrder } from "./ship-order";

const DAY = 24 * 60 * 60 * 1000;

export const placeOrderWithDeadline = workflow<{ orderId: string }>(
  async function* (step, event) {
    const { orderId } = event.params;

    const shipped = yield* step.call<typeof shipOrder>(
      "ship",
      { workflowId: "ship-order", params: { orderId } },
      { deadline: Date.now() + 3 * DAY }
    );

    if (shipped.status === "timed-out") {
      yield* step.run("email-delay-notice", async () => {
        await mailer.sendDelayNotice(orderId);
      });
      return { status: "delayed" as const };
    }

    return { status: "shipped" as const, tracking: shipped.result.tracking };
  }
);
```

| Status | Meaning |
| :--- | :--- |
| `"completed"` | Target finished before timing out, with its output in `result` |
| `"timed-out"` | Deadline elapsed before the target reached a terminal state |

The first evaluation that waits records the deadline so later replays reuse that recorded timestamp instead of pushing `Date.now() + 3 * DAY` forward. On evaluation, Durable checks the target's status before checking the clock, so a terminal target returns `"completed"` even if the deadline has also passed.

A failed or cancelled target still throws `WorkflowJoinError` when `{ deadline }` is set.

A deadline does not stop or cancel the child execution – a timed-out child continues running in the background. Because a timed-out `step.call` does not expose the child's `executionId`, use `step.start` followed by `step.join(key, executionId, { deadline })` if you need to inspect or cancel the child after timing out.

## Child lifecycle and `{ detached: true }`

Every child started by a workflow inherits the parent's request context and records a backlink to the parent execution and step key.

By default, cancelling a parent execution propagates cancellation to every non-detached child execution. Pass `{ detached: true }` to `step.start` or `step.call` so the child runs independently of parent cancellation:

```ts [src/workflows/place-order-detached.ts]
import { workflow } from "@notation/durable";

export const placeOrder = workflow<{ orderId: string }>(async function* (step, event) {
  const { executionId } = yield* step.start(
    "audit-order",
    { workflowId: "audit-log", params: { orderId: event.params.orderId } },
    { detached: true }
  );

  return executionId;
});
```

See [Cancellation](./cancellation.md) for how parent-to-child cancellation propagates across execution trees.

## Child error propagation

When a joined target fails, cancels, or cannot be joined, the step records and throws `WorkflowJoinError` carrying a reason code and the target execution ID:

| Code | Cause |
| :--- | :--- |
| `JOINED_EXECUTION_FAILED` | Target execution failed, with the child's recorded error in `cause` |
| `JOINED_EXECUTION_CANCELLED` | Target execution was cancelled, with optional `reason` and `source` |
| `JOIN_TARGET_NOT_FOUND` | No execution exists with the given `executionId` |
| `JOIN_SELF` | Execution passed its own `executionId` to `step.join` |

```ts [src/workflows/place-order-fallback.ts]
import { WorkflowJoinError, workflow } from "@notation/durable";
import type { shipOrder } from "./ship-order";

export const placeOrder = workflow<{ orderId: string }>(async function* (step, event) {
  try {
    return yield* step.call<typeof shipOrder>("ship", {
      workflowId: "ship-order",
      params: { orderId: event.params.orderId },
    });
  } catch (err) {
    if (err instanceof WorkflowJoinError && err.code === "JOINED_EXECUTION_FAILED") {
      yield* step.run("refund", async () => {
        await payments.refund(event.params.orderId);
      });
      return { tracking: null };
    }
    throw err;
  }
});
```

If the child failed with a registered custom error class, `err.cause` revives as an instance of that class. Durable does not detect mutual join cycles between two executions; two executions joining each other remain suspended until cancelled or until a `{ deadline }` expires.

## Deploy changes

Once `step.start` or `step.call` has sent a start request for a `stepKey`, its request parameters are fixed until the step finishes:

- **Changed start request** – If a new deploy alters the target workflow, parameters, or detached mode on an unfinished start step, it throws `WorkflowStartError` with code `START_REQUEST_MISMATCH`.
- **Added or removed deadline** – Adding or removing a `deadline` changes the return type between a plain result and `TimedJoinResult`. Assign a new step key whenever you add or remove a deadline on an in-flight join.

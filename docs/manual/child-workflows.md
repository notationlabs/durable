# Child workflows

A workflow can start another workflow as a step. The workflow that starts it is the parent, and the workflow it starts is the child. The child runs as its own execution, with its own steps and records. This page uses an order workflow that starts a shipping workflow for each order.

```ts
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

```ts
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

Both workflows are registered in the same router, `shipOrder` under the ID `ship-order`.

## `step.call`

`step.call` starts the child and waits until the child finishes. It returns the child's result.

The type argument is the child workflow. TypeScript uses it to check `params` and to type the result. TypeScript does not check that `workflowId` names that workflow, because a workflow cannot refer to the type of the router that contains it.

Without the type argument, `step.call` accepts no `params`, and its result has the type `DurableValue`.

## Waiting for the child

While the child runs, the parent's delivery ends and its status is `suspended`. When the child finishes, Durable wakes the parent. The parent's next delivery records the child's result under the key `ship` and continues with `email-tracking`.

The child is started once. Durable identifies the start by the parent's execution ID and the step key. A delivery that stops after the child has started, but before `ship` is recorded, sends the same start again. The second start returns the existing child and does not start another.

## Child failures

If the child fails, `step.call` throws `WorkflowJoinError` with the code `JOINED_EXECUTION_FAILED`. The error's `cause` is the error that failed the child:

```ts
import { WorkflowJoinError } from "@notation/durable";

try {
  yield* step.call<typeof shipOrder>("ship", {
    workflowId: "ship-order",
    params: { orderId },
  });
} catch (err) {
  if (err instanceof WorkflowJoinError && err.code === "JOINED_EXECUTION_FAILED") {
    yield* step.run("refund", async () => {
      await payments.refund(orderId);
    });
  }
}
```

If the child is [cancelled](./cancellation.md), the code is `JOINED_EXECUTION_CANCELLED`, and the error has the cancellation's `reason` and `source`.

## `step.start` and `step.join`

`step.call` is a `step.start` followed by a `step.join`, recorded under one key. A parent that has other work to do while the child runs can use the two steps separately:

```ts
const { executionId } = yield* step.start("ship", {
  workflowId: "ship-order",
  params: { orderId },
});

yield* step.run("email-receipt", async () => {
  await mailer.sendReceipt(orderId);
});

const shipment = yield* step.join<{ tracking: string }>("ship:join", executionId);
```

`step.start` returns the child's execution ID without waiting. `step.join` waits until that execution finishes and returns its result. The type argument of `step.join` is the result type, and TypeScript does not check it.

`step.join` can wait for any execution, including one that this workflow did not start. It throws `WorkflowJoinError` in these cases too:

| Code | Cause |
| --- | --- |
| `JOIN_TARGET_NOT_FOUND` | No execution has the ID |
| `JOIN_SELF` | The ID is the parent's own execution ID |

Durable does not detect cycles. If two executions join each other, both wait until one is cancelled or reaches a [deadline](#deadlines).

## Deadlines

`step.call` and `step.join` take a `deadline`, as a time in milliseconds since the Unix epoch. With a deadline, the step returns an object that says whether the child finished in time:

```ts
const shipped = yield* step.call<typeof shipOrder>(
  "ship",
  { workflowId: "ship-order", params: { orderId } },
  { deadline: Date.now() + 3 * DAY }
);

if (shipped.status === "timed-out") {
  yield* step.run("email-delay-notice", async () => {
    await mailer.sendDelayNotice(orderId);
  });
} else {
  shipped.result.tracking;
}
```

| `status` | Meaning |
| --- | --- |
| `completed` | The child finished before the deadline. `result` holds its result |
| `timed-out` | The deadline passed first |

A failed or cancelled child still throws `WorkflowJoinError`.

The first delivery that waits records the deadline. Later deliveries use the recorded deadline, so `Date.now() + 3 * DAY` does not move forward on each delivery.

**The deadline does not stop the child.** A child that times out keeps running. The timed-out result of `step.call` has no execution ID, so to join the child later, use `step.start` and a `step.join` with a deadline.

## Child executions

A child's execution record links to the parent's execution ID and step key. The child receives the parent's `context`.

When the parent is cancelled, Durable cancels the child too. A child started with `{ detached: true }` is not cancelled with the parent. See [Cancellation](./cancellation.md).

## Changing a child step

A start that has been sent cannot change. If a deploy changes the `workflowId`, `params` or `detached` option of a `ship` step that has already started its child, the step throws `WorkflowStartError` with the code `START_REQUEST_MISMATCH`.

Adding or removing a `deadline` changes the type of value that the step returns, but a step that already has a record returns the old type. Give the step a new key when you add or remove a deadline.

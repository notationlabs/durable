# Cancellation

An execution ends when its workflow returns or throws. `cancel` adds a third way: it asks a running workflow to stop, and the workflow receives the request as an error at its next unfinished step.

This page uses one example throughout. A shop triggers `place-order` with the execution ID `order-42`. The workflow reserves stock, starts a `fulfil-order` child that books a courier, and then waits on the payments store until the order is paid:

```ts
import { WorkflowCancelledError, workflow } from "yieldstar";

const placeOrder = workflow<{ orderId: string }>(async function* (step, event) {
  const { orderId } = event.params;
  const reservation = yield* step.run("reserve-stock", () => stock.reserve(orderId));

  yield* step.start("start:fulfilment", {
    workflowId: "fulfil-order",
    params: { orderId },
  });

  const payments = step.store(PaymentStore, { id: orderId });
  try {
    yield* payments.when(`paid:${orderId}`, (state) => state.paid);
  } catch (err) {
    if (err instanceof WorkflowCancelledError) {
      yield* step.run("release-stock", () => stock.release(reservation.id));
    }
    throw err;
  }
});
```

The customer closes the tab and never pays, so `order-42` is suspended on the `when` step. Without cancellation it waits forever, the stock stays reserved and the courier stays booked.

## Requesting cancellation

An operator calls `cancel` with the execution ID and an optional reason:

```ts
const result = await sdk.cancel("order-42", { reason: "abandoned" });
```

`cancel` records a cancellation request on the [execution record](./workflows.md#execution-records), makes the execution due now, and returns. It does not wait for the workflow to stop. The local SDK and the HTTP SDK both have it (see [`sdk.cancel`](./sdk-local.md#sdkcancel)).

The request is stored on the record as `cancellation`:

```ts
type ExecutionCancellation = {
  reason?: string;
  source?: string; // the execution where the cancellation began, when it was propagated
  forced: boolean;
  requestedAt: number;
  deliveredAt?: number; // set when the throw into workflow code is recorded
};
```

Execution views carry `cancellation` too, so `poll`, `wait` and `list` show it. An observer can tell the two phases apart by `deliveredAt`:

| `deliveredAt` | Meaning                                                              |
| ------------- | -------------------------------------------------------------------- |
| Absent        | The request is recorded, and the workflow has not received it yet    |
| Present       | The workflow has received `WorkflowCancelledError` and is cleaning up |

The request stays on the record whatever the outcome.

## Where the workflow receives it

The next delivery replays the finished steps as usual. At the first unfinished step, the driver does not run or suspend that step. It records a `WorkflowCancelledError` as the step's result and throws it into workflow code at that step.

For `order-42`, `reserve-stock` and `start:fulfilment` replay from their records. The `when` step is the first unfinished step, so it throws `WorkflowCancelledError` with `reason: "abandoned"`. The `catch` block runs `release-stock`, and then rethrows.

An unfinished step is one of these:

- a step with no record yet, which would otherwise run;
- a `when` or `take` waiting on a store;
- a `step.join` or `step.call` waiting on its target;
- a `step.effect` waiting to look up an uncertain outcome;
- a step waiting for its next retry after a `RetryableError`;
- a `step.delay` that has not elapsed.

An admitted execution that has never run receives the error at its first step, and that step's function does not run.

**The driver reads the request when a delivery starts.** A delivery that is already running when the request arrives carries on without it. A step function that is already running is not interrupted. In the example, if `cancel` is called while `reserve-stock` is running, that delivery finishes `reserve-stock`, starts `fulfil-order` and suspends on the `when` step. The request had already made the execution due, so the next delivery throws at the `when` step. If the delivery in progress returns instead, the execution ends `completed`.

### Recording the throw

The driver records the throw before it reaches workflow code, so the throw happens once. In one fenced write, the runtime:

1. records the step's result as a cancellation, which is a different record type from a step error;
2. deletes the store, effect and join waiters of that step; and
3. sets `cancellation.deliveredAt`.

Step 2 matters for the example: a payment recorded after the throw does not wake `order-42`, because its store waiter is gone. A pending effect is not changed: its external operation is not cancelled, and a later `settle` still succeeds.

Replay reads the recorded cancellation at the same step and throws the same error again, without reading the request. `WorkflowCancelledError` is registered by default (see [Errors in step records](./steps.md#errors-in-step-records)), so `instanceof` holds on replay, and `reason` and `source` are kept.

A `WorkflowCancelledError` that a step function throws itself is an ordinary step error, not a delivered cancellation.

## Cleaning up

After the throw, workflow code can run more durable steps. They are ordinary steps: they are recorded, and they can suspend and wake like any other. If `release-stock` were a `step.effect` that suspends, a later delivery would replay the recorded throw at the `when` step, enter the `catch` block again and continue the cleanup. A store, effect or join wait that a cleanup step registers wakes the execution as usual.

A child that a cleanup step starts receives no cancellation request.

## Inside `step.all` and `step.race`

When the first unfinished step is inside a [combinator](./concurrent-steps.md), the delivery lands in every unfinished branch at once. Suppose `order-42` waits for payment and for a manager's approval in one `step.all`:

```ts
yield* step.all("payment-and-approval", [
  (async function* () {
    try {
      yield* payments.when("paid", (state) => state.paid);
    } catch (err) {
      yield* step.run("void-invoice", () => billing.void(orderId));
      throw err;
    }
  })(),
  approvals.when("approved", (state) => state.approved),
]);
```

The driver replays each branch to its first unfinished step: `paid` in the first branch and `approved` in the second. One fenced write then:

1. records a cancellation at `paid` and at `approved`;
2. deletes the waiters of both steps;
3. records the decision `cancelled` under `payment-and-approval`, and under any other combinator around those steps that has not settled; and
4. sets `cancellation.deliveredAt`.

Each branch then receives `WorkflowCancelledError`, and its own `catch` and `finally` blocks run. They may run durable steps, such as `void-invoice`. When every branch has finished, `step.all` throws `WorkflowCancelledError`, even if a branch caught the error and returned a value.

A combinator that settled before the request arrived is not interrupted. Replay returns its recorded outcome, and the cancellation lands at the next unfinished step after it. Branches that the combinator abandoned do not receive it.

## Outcomes

The execution's outcome depends on how the workflow ends:

| The workflow                                                    | Outcome     |
| --------------------------------------------------------------- | ----------- |
| Lets `WorkflowCancelledError` escape, by rethrowing it or not catching it | `cancelled` |
| Catches the error and returns a value                           | `completed`. The workflow has declined the request |
| Throws a different error                                        | `failed`    |
| Throws its own `WorkflowCancelledError` with no request recorded | `failed`   |

`cancelled` is a terminal status, like `completed` and `failed`. Its outcome is `{ status: "cancelled", reason?, source? }`, with the request's first `reason` and `source`. A cancelled execution never runs workflow code again, and wakes for it are ignored.

`order-42` rethrows, so it ends `cancelled` with `reason: "abandoned"`.

## Children

When the request for `order-42` is recorded, the runtime also records a request on every child that `order-42` started with [`step.start`](./steps.md#stepstart) or [`step.call`](./steps.md#stepcall), and on their children, all the way down. So `fulfil-order` receives its own `WorkflowCancelledError`, and its own `catch` block can cancel the courier.

Each propagated request has:

- the same `force` value as the parent's request;
- the parent's first `reason`; and
- as `source`, the execution where the cancellation began: the parent's own `source`, or the parent's ID when the cancellation began there. For `fulfil-order` and its children, `source` is `order-42`.

The runtime records the request and every propagated request in one all-or-nothing write, when `cancel` is called. It does not wait for the parent to finish.

These children receive no request:

- a child that is already terminal. The runtime does not look past it, so its own children receive no request either;
- a child started with `detached: true` (see below);
- a child started by a cleanup step, after the parent's `deliveredAt` is set.

A child that the parent starts after the request but before the throw, such as a `step.start` in the delivery that was running when `cancel` was called, receives a request when it is admitted.

A descendant that already has a request keeps its first `reason` and `source`.

### Detached children

A child that should outlive its parent, such as a receipt email, is started with `detached: true`:

```ts
yield* step.start(
  "start:receipt",
  { workflowId: "send-receipt", params: { orderId } },
  { detached: true }
);
```

The child's `parent` link is recorded with `detached: true`, and the child receives no propagated request. `step.call` takes the same option. `detached` is part of the start request, so a replay that changes it throws `WorkflowStartError` with `START_REQUEST_MISMATCH`.

## Forced cancellation

A graceful cancel trusts the workflow to clean up and stop. Suppose `release-stock` waits on a warehouse that never answers. `force: true` ends the execution without running workflow code:

```ts
await sdk.cancel("order-42", { reason: "abandoned", force: true });
```

A forced cancel, in one all-or-nothing write:

1. records the outcome `cancelled`;
2. clears the execution's pending wake and deletes the store, effect and join waiters it holds;
3. issues a higher lease token, so a worker that holds a lease on the execution has its next fenced write rejected as `lost`; and
4. wakes every execution that joins it.

A forced cancel after a graceful one is accepted. It keeps the first `reason`, and sets `forced` to `true`. Force propagates to descendants in the same way as a graceful request, and makes each descendant's request forced.

## Repeated and late requests

- A repeated request keeps the first `reason` and `source`.
- Cancelling a terminal execution, including one that is already `cancelled`, changes nothing and returns its terminal view.
- Cancelling an unknown execution ID returns `undefined`.
- A graceful request makes the execution due now, but the [redelivery delay](./workflows.md#execution-records) still applies to an execution whose earlier deliveries ended without an outcome.
- A graceful request on a [diverged](./replay-stability.md#divergence) execution is recorded but not delivered, because no delivery runs while the divergence is set. Force-cancel the execution, or clear the divergence after deploying code that matches its history. A forced cancel records `cancelled` and removes the divergence.

## Joining a cancelled execution

A [`step.join`](./steps.md#stepjoin) or [`step.call`](./steps.md#stepcall) whose target ends `cancelled` throws `WorkflowJoinError` with code `JOINED_EXECUTION_CANCELLED`. The error carries the target's `reason` and `source`.

```ts
try {
  yield* step.join("fulfilment:join", fulfilmentId);
} catch (err) {
  if (err instanceof WorkflowJoinError && err.code === "JOINED_EXECUTION_CANCELLED") {
    // err.reason and err.source are the target's
  }
}
```

Two executions that join each other without a deadline wait forever. Cancelling either one ends the cycle.

## Reading the outcome

`wait` returns a cancelled execution's view as a terminal result. `triggerAndWait` and the HTTP SDK's `waitForResult` throw `WorkflowCancelledError` with the outcome's `reason` and `source`:

```ts
const execution = await sdk.wait("order-42");
if (execution?.status === "cancelled") {
  execution.reason; // "abandoned"
  execution.cancellation?.deliveredAt; // absent after a forced cancel that ran no workflow code
}
```

An idempotent trigger that matches a cancelled execution returns `"existing"`, as it does for any status.

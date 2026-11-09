# Cancellation

An execution ends when its workflow returns or throws. Cancellation is a third way: code outside the workflow asks the execution to stop. The workflow receives the request as a `WorkflowCancelledError` at its next unfinished step, so it can clean up before it ends.

This page uses a workflow that holds seats at a venue until the customer pays. It also starts a child workflow that emails the customer a payment reminder every hour:

```ts
import { WorkflowCancelledError, workflow } from "@notation/durable";
import { Booking } from "./booking";

export const holdSeats = workflow<{ bookingId: string; seats: string[] }>(async function* (step, event) {
  const { bookingId, seats } = event.params;

  const hold = yield* step.run("hold-seats", async () => {
    return await venue.hold(seats);
  });

  yield* step.start("start-reminders", {
    workflowId: "send-payment-reminders",
    params: { bookingId },
  });

  const booking = step.store(Booking, { id: bookingId });

  try {
    yield* booking.when("paid", (state) => state.paidAt);
  } catch (err) {
    if (err instanceof WorkflowCancelledError) {
      yield* step.run("release-seats", async () => {
        await venue.release(hold.id);
      });
    }
    throw err;
  }
});
```

The customer closes the tab and never pays. Without cancellation, the workflow waits on `paid` forever, the seats stay held and the reminders keep coming.

## Requesting cancellation

The application calls `cancel` on the SDK with the execution ID, and optionally a reason:

```ts
const result = await sdk.cancel(executionId, { reason: "abandoned" });
```

`cancel` records the request on the execution, makes the execution due, and returns. It does not wait for the workflow to stop.

| `cancel` returns | When |
| --- | --- |
| `{ status: "requested", execution }` | The execution has not ended. `execution` is its current view |
| The execution's view | The execution had already ended, or the cancel was [forced](#forced-cancellation) |
| `undefined` | No execution has this ID |

The request is saved on the execution as `cancellation`, and execution views include it:

| Field | Holds |
| --- | --- |
| `reason` | The reason passed to `cancel` |
| `source` | For a child, the ID of the execution where the cancellation began. See [Children](#children) |
| `forced` | Whether the cancel was forced |
| `requestedAt` | When `cancel` recorded the request |
| `deliveredAt` | When the workflow received `WorkflowCancelledError`. Absent until then |

## Where the workflow receives it

The next delivery replays the finished steps as usual. At the first unfinished step, Durable does not run the step. It records a `WorkflowCancelledError` as the step's result, and throws it into the workflow.

In the example, `hold-seats` and `start-reminders` replay from their records. `paid` is the first unfinished step, so it throws `WorkflowCancelledError` with `reason: "abandoned"`. The `catch` block runs `release-seats`, and then throws the error again.

An unfinished step is one of these:

- A step with no record yet
- A `when` or `take` that is waiting on a store
- A `step.join` or `step.call` that is waiting for a child
- A `step.effect` that is waiting to look up its outcome
- A step that is waiting to retry
- A `step.delay` that has not elapsed

**A delivery that is already running does not receive the request.** Durable reads the request when a delivery starts, and does not interrupt a step function that is running. If `cancel` is called while `hold-seats` runs, that delivery carries on to `paid` and suspends. The next delivery throws at `paid`.

If the delivery that is already running reaches the end of the workflow, the execution ends `completed`, and the request has no effect.

## After the throw

The step that throws stops waiting. In the example, `paid` no longer waits on the booking store, so a payment that arrives after the throw does not wake the workflow.

A later delivery replays the same `WorkflowCancelledError` at the same step, with the same `reason`, and the `catch` block runs again on the way to the next unfinished step.

A `step.effect` that was waiting on an external operation does not cancel that operation. If the operation needs to be undone, the cleanup code has to do it.

## Cleaning up

Steps after the throw, such as `release-seats`, are ordinary steps. They are recorded, and they can retry, suspend and wake as any other step can. If `release-seats` waits to retry, a later delivery replays the throw at `paid`, enters the `catch` block again, and continues with `release-seats`.

A child that a cleanup step starts does not receive a cancellation request.

## Outcomes

The execution's final status depends on how the workflow ends:

| The workflow | Status |
| --- | --- |
| Throws `WorkflowCancelledError`, by throwing it again or by not catching it | `cancelled` |
| Catches the error and returns a value | `completed` |
| Throws a different error | `failed` |

`holdSeats` throws the error again, so the execution ends `cancelled`, and its view has `reason: "abandoned"`. A cancelled execution does not run again.

**`WorkflowCancelledError` ends an execution `cancelled` only if a request was recorded before the delivery started.** Without a request, a workflow or step that throws its own `WorkflowCancelledError` ends the execution `failed`, as any other error does.

## Children

When `cancel` records the request for `holdSeats`, it also records a request on every child that `holdSeats` started with `step.start` or `step.call`, and on their children, and so on. So `send-payment-reminders` receives its own `WorkflowCancelledError`, and no more reminders are sent.

Each child's request has the parent's `reason`, and the same `forced` value. Its `source` is the ID of the execution where the cancellation began, so for `send-payment-reminders` and any child it starts, `source` is the ID of the `holdSeats` execution.

`cancel` records all of these requests in one write, before it returns. It does not wait for the parent to clean up.

These children do not receive a request:

| Child | Why |
| --- | --- |
| A child that has already ended | Its own children do not receive one either |
| A child started with `detached: true` | See below |
| A child started by a cleanup step | The parent already received its request |

A child that a running delivery starts after `cancel` was called receives a request when it is created.

A child that should outlive its parent, such as one that sends a receipt, is started with `detached: true`:

```ts
yield* step.start(
  "start-receipt",
  { workflowId: "send-receipt", params: { bookingId } },
  { detached: true }
);
```

`step.call` takes the same option.

## Inside `step.all` and `step.race`

When the next unfinished step is inside a [`step.all` or `step.race`](./concurrent-steps.md), every branch that has not finished receives `WorkflowCancelledError` at its own next unfinished step.

Unlike a losing branch, each branch runs its `catch` and `finally` blocks, which can run cleanup steps. When every branch has finished, `step.all` or `step.race` throws `WorkflowCancelledError`. It throws it even if a branch caught the error and returned a value.

**A request comes before a decision that has not been recorded.** If one branch of a race has already returned but another is still waiting, the race does not pick a winner. Every branch receives the error instead.

A `step.all` or `step.race` that recorded its result before the request arrived is not affected. The error lands at the next unfinished step after it.

## Forced cancellation

A request as described so far relies on the workflow to clean up and end. If `release-seats` waits on a venue system that never answers, the execution never ends. A forced cancel ends the execution without running any more workflow code:

```ts
await sdk.cancel(executionId, { reason: "abandoned", force: true });
```

A forced cancel does the following, in one write:

1. Sets the execution's status to `cancelled`
2. Removes everything the execution was waiting on, so nothing wakes it
3. Makes sure that a delivery that is still running cannot record anything else
4. Wakes executions that are waiting for it with `step.join` or `step.call`

No cleanup code runs, so in the example the seats stay held. The execution's `cancellation` has no `deliveredAt`.

A forced cancel propagates to children as a request does, and forces the children's requests too.

## Repeated requests

| Request | Result |
| --- | --- |
| A second request without `force` | Changes nothing. The first `reason` stays |
| A forced request after one without `force` | Forces the cancel, and keeps the first `reason` |
| Any request on an execution that has ended | Changes nothing, and returns the execution's view |

## Reading the outcome

`sdk.wait` returns the view of a cancelled execution, as for any execution that has ended:

```ts
const execution = await sdk.wait(executionId);

if (execution?.status === "cancelled") {
  console.log(execution.reason); // "abandoned"
}
```

`sdk.triggerAndWait` throws `WorkflowCancelledError` instead, with the `reason` and `source` of the cancellation.

A parent whose `step.join` or `step.call` waits for a cancelled child receives `WorkflowJoinError` with the code `JOINED_EXECUTION_CANCELLED`. See [Child workflows](./child-workflows.md#child-failures).

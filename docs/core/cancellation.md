# Cancellation

Code outside a workflow can cancel an active execution with `sdk.cancel`. Graceful cancellation throws `WorkflowCancelledError` at the execution's next unfinished step so workflow code can run cleanup before ending `cancelled`. Forced cancellation ends the execution immediately without running workflow code.

```ts [src/workflows/hold-seats.ts]
import { WorkflowCancelledError, workflow } from "@notation/durable";
import { Booking } from "../stores/booking";

export const holdSeats = workflow<{ bookingId: string; seats: string[] }>(
  async function* (step, event) {
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
  }
);
```

## Requesting cancellation

Call `sdk.cancel(executionId, options?)` on either the [Local SDK](./local-sdk.md) or [HTTP SDK](./http-sdk.md). `cancel` records the request, makes the execution due when graceful, and returns without waiting for the workflow to stop:

```ts [src/api/cancel-booking.ts]
import { sdk } from "../durable/sdk";

const result = await sdk.cancel("booking-42", { reason: "abandoned" });

if (result?.status === "requested") {
  console.log("Cancellation requested at", result.execution.cancellation?.requestedAt);
}
```

| Execution state | Return value |
| :--- | :--- |
| Active execution | `{ status: "requested", execution }` |
| Terminal execution | `TerminalExecutionView` |
| Unknown execution ID | `undefined` |

After a forced cancel with `force` set to true, `cancel` returns the terminal cancelled execution view directly. Passing invalid arguments throws `ExecutionCancelRequestError` before any request is recorded or sent.

Execution views carry the recorded `cancellation` object once cancellation has been requested:

| Field | Description |
| :--- | :--- |
| `reason` | Optional string passed to `cancel` |
| `source` | Execution ID where a propagated cancellation began |
| `forced` | Boolean indicating whether `{ force: true }` was used |
| `requestedAt` | Timestamp when the cancellation request was recorded |
| `deliveredAt` | Timestamp when `WorkflowCancelledError` was delivered to workflow code |

`source` appears only on child executions cancelled via propagation. `deliveredAt` stays `undefined` until workflow code receives `WorkflowCancelledError`, and remains absent after a forced cancel.

## How graceful cancellation is delivered

On the next delivery after a graceful request, Notation Durable replays completed steps from their records and throws `WorkflowCancelledError` at the first unfinished step instead of running or suspending it. An unfinished step is any of:

- **New step** – A step with no recorded attempt yet
- **Waiting store step** – A `when` or `take` waiting on a store
- **Waiting child step** – A `step.join` or `step.call` waiting for a target execution
- **Pending effect** – A `step.effect` waiting to look up its outcome
- **Retrying step** – A `step.run` or `step.poll` waiting between retry attempts
- **Active delay** – A `step.delay` whose wake time has not passed

A delivery already in progress when `cancel` is called is not interrupted and running step functions finish normally. If that in-flight delivery completes the workflow before suspending, the execution ends `completed` and the request has no effect. Otherwise, the next delivery throws `WorkflowCancelledError` at the next unfinished step.

When `WorkflowCancelledError` is thrown at a waiting step, that step stops waiting – for example, `booking.when("paid", ...)` removes its store waiter so later store updates do not wake the execution. For `step.effect`, cancelling stops the workflow from waiting but does not cancel the external operation itself.

## Catching and cleanup steps

Workflow code can catch `WorkflowCancelledError` to release reservations or compensate earlier work. Steps executed inside `catch` or `finally` blocks – such as `release-seats` above – behave like ordinary durable steps: they record results, retry, suspend, and wake across deliveries.

If a cleanup step suspends, such as on a `step.delay` or `RetryableError`, the next delivery replays the recorded `WorkflowCancelledError` at the cancelled step, re-enters the `catch` block, and resumes from the unfinished cleanup step.

## Terminal outcomes

The execution's terminal status depends on what workflow code does with `WorkflowCancelledError`:

| Workflow behavior | Terminal status |
| :--- | :--- |
| Rethrows or ignores cancellation | `cancelled` |
| Catches and returns | `completed` |
| Throws another error | `failed` |

Throwing a `WorkflowCancelledError` directly from workflow code or a step function without a recorded cancellation request fails the execution with status `failed`.

External callers observe a `cancelled` execution differently depending on the method:

- **`sdk.wait`** – Resolves with `{ status: "cancelled", reason?, source?, ... }`
- **SDK wait helpers** – Reject with `WorkflowCancelledError` carrying the cancellation reason and source
- **Child joins and calls** – Throw `WorkflowJoinError` with code `JOINED_EXECUTION_CANCELLED`, carrying the reason and source

## Child workflow propagation

Requesting cancellation on a parent execution propagates the request recursively to every non-terminal descendant started by `step.start` or `step.call` in one atomic write. Each descendant inherits the parent's first reason, the forced flag, and `source` pointing to the root execution ID.

To allow a child workflow to outlive its parent – such as sending a confirmation receipt after an order is cancelled – pass `{ detached: true }` to `step.start` or `step.call`:

```ts [src/workflows/hold-seats-with-receipt.ts]
yield* step.start(
  "start-receipt",
  { workflowId: "send-receipt", params: { bookingId } },
  { detached: true }
);
```

Three categories of children do not receive propagated cancellation requests:

- **Detached children** – Started with `{ detached: true }`
- **Terminal children** – Already in a completed, failed, or cancelled state
- **Cleanup children** – Started inside an error handler or `finally` block after cancellation was delivered

If an in-flight delivery starts a non-detached child after `cancel` was requested on the parent (before delivery of the request), that child inherits the parent's pending cancellation request when created.

## Inside `step.all` and `step.race`

When cancellation arrives while execution is suspended inside [`step.all` or `step.race`](./concurrent-steps.md), every unfinished branch receives `WorkflowCancelledError` at its own next unfinished step.

Unlike losing branches in a race or siblings after a fail-fast failure, cancelled branches run their `catch` and `finally` blocks across deliveries, including any cleanup steps that suspend. Once every branch finishes, the enclosing combinator throws `WorkflowCancelledError` even if a branch caught the error and returned a value.

A pending cancellation request takes precedence over an unrecorded combinator decision. If one branch of a `step.race` returned while another was waiting and cancellation arrives before the race records a winner, all unfinished branches receive `WorkflowCancelledError`.

A combinator whose decision was already recorded before cancellation replays normally, and cancellation lands at the next unfinished step after it.

## Forced cancellation

When cleanup steps are stuck or immediate termination is required, pass `{ force: true }`:

```ts [src/api/force-cancel.ts]
import { sdk } from "../durable/sdk";

const view = await sdk.cancel("booking-42", {
  reason: "operator-abort",
  force: true,
});
```

A forced cancellation performs four actions in one atomic operation:

1. Sets the execution's status and outcome to `cancelled`
2. Deletes all pending store, effect, and join waiters held by the execution
3. Fences out any in-flight delivery so it cannot record further steps or outcomes
4. Wakes any parent or peer executions waiting on it via `step.join` or `step.call`

No workflow cleanup code runs, and `cancellation.deliveredAt` remains unset. Forced cancellation propagates to all non-terminal, non-detached descendants with `forced: true`.

## Repeated cancel calls

| Request | Behavior |
| :--- | :--- |
| Graceful cancel after graceful cancel | Changes nothing and keeps the first `reason` |
| Forced cancel after graceful cancel | Ends the execution `cancelled` immediately while keeping the first `reason` |
| Any cancel on a terminal execution | Changes nothing and returns the terminal execution view |

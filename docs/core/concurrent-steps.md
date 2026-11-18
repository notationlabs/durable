# Concurrent Steps

A workflow runs its steps sequentially by default. `step.all` and `step.race` run multiple step generators – called branches – concurrently within a single delivery.

```ts [src/workflows/checkout.ts]
import { defineStore, workflow } from "@notation/durable";
import * as v from "valibot";

const MINUTE = 60_000;

export const PaymentStore = defineStore("payments", v.object({
  paidAt: v.optional(v.number()),
}), {
  initial: () => ({}),
});

export const checkout = workflow<{ orderId: string }>(async function* (step, event) {
  const order = yield* step.run("load-order", async () => {
    return await orders.get(event.params.orderId);
  });

  const [stock, fraudScore, shipping] = yield* step.all("checks", [
    step.run("check-stock", () => warehouse.checkStock(order.items)),
    step.run("score-fraud", () => fraud.score(order.card)),
    step.run("quote-shipping", () => courier.quote(order.address)),
  ]);

  const payment = step.store(PaymentStore, { id: order.id });

  const first = yield* step.race("payment-or-timeout", [
    payment.when("paid", (state) => state.paidAt),
    step.delay("payment-timeout", 30 * MINUTE),
  ]);

  if (first.index === 1) {
    yield* step.run("release-stock", () => warehouse.release(order.items));
    return { status: "expired" as const, stock, fraudScore, shipping };
  }

  return { status: "paid" as const, paidAt: first.value };
});
```

## Branches

A branch passed to `step.all` or `step.race` must be a step generator:

- **Single steps** – Direct calls such as `step.run`, `step.delay`, `store.when`, or `step.call`.
- **Multi-step generators** – Invoked `async function*` blocks that sequence several steps and local control flow.
- **Nested combinators** – Another `step.all` or `step.race`.

`Promise.all` cannot run steps concurrently because steps yield generator commands for Notation Durable to record and replay rather than returning standalone promises.

Code inside an `async function*` branch outside `yield*` steps re-executes on every delivery, just like code outside steps in the workflow body. A branch can update local variables between steps so downstream `catch` blocks make identical decisions on replay:

```ts [src/workflows/pay-and-ship.ts]
import { workflow } from "@notation/durable";

export const payAndShip = workflow<{ orderId: string }>(async function* (step, event) {
  let charged = false;

  try {
    yield* step.all("pay-and-ship", [
      (async function* () {
        yield* step.run("charge", () => payments.charge(event.params.orderId));
        charged = true;
      })(),
      step.run("ship", () => courier.ship(event.params.orderId)),
    ]);
  } catch {
    if (charged) {
      yield* step.run("refund", () => payments.refund(event.params.orderId));
    }
  }
});
```

## `step.all`

`step.all(key, branches, options?)` drives every branch concurrently and returns results in branch order as a tuple matching `branches`. An empty array `step.all("empty", [])` returns `[]`.

`step.all` records its own combinator outcome under `key` alongside each branch step's individual record. On replay, recorded branch steps return cached values without re-running step functions.

### Modes and concurrency

| Option | Behavior |
| :--- | :--- |
| `mode` | Chooses between `"fail-fast"` failure throwing and `"settle"` per-branch outcomes |
| `concurrency` | Positive integer capping how many branches may have a live step in flight at once |

By default, `mode` is `"fail-fast"` and `concurrency` is `Infinity`.

- **Fail-fast mode** – `step.all` throws the first uncaught branch error as soon as a branch fails and records the rejection. Other branches start no new steps. A branch that catches its own error with `try` / `catch` counts as fulfilled.
- **Settle mode** – Passing `{ mode: "settle" }` runs every branch to completion or failure and returns `{ status: "fulfilled", value }` or `{ status: "rejected", error }` for each branch in branch order:

```ts [src/workflows/settle-checks.ts]
import { workflow } from "@notation/durable";

export const settleChecks = workflow<{ orderId: string }>(async function* (step, event) {
  const results = yield* step.all(
    "checks",
    [
      step.run("check-stock", () => warehouse.checkStock(event.params.orderId)),
      step.run("score-fraud", () => fraud.score(event.params.orderId)),
    ],
    { mode: "settle" }
  );

  return results.map((entry) =>
    entry.status === "fulfilled" ? entry.value : null
  );
});
```

The `concurrency` limit applies to live, uncached steps rather than whole branches. A branch releases its concurrency slot between steps and while suspended, and cached steps replayed from prior deliveries do not consume slots:

```ts [src/workflows/notify-suppliers.ts]
import { workflow } from "@notation/durable";

export const notifySuppliers = workflow<{ items: { id: string }[] }>(
  async function* (step, event) {
    yield* step.all(
      "notify",
      event.params.items.map((item) =>
        step.run(`notify:${item.id}`, () => suppliers.notify(item.id))
      ),
      { concurrency: 5 }
    );
  }
);
```

## `step.race`

`step.race(key, branches)` drives every branch concurrently and returns the winning zero-based index and value for the first branch that resolves. TypeScript narrows `winner.value` automatically when you branch on `winner.index`:

```ts [src/workflows/race-quotes.ts]
import { WorkflowRaceError, workflow } from "@notation/durable";

export const quoteWithFallback = workflow<{ address: string }>(async function* (step, event) {
  try {
    const winner = yield* step.race("courier-quote", [
      step.run("quote-dhl", () => dhl.quote(event.params.address)),
      step.run("quote-ups", () => ups.quote(event.params.address)),
    ]);

    return winner.index === 0
      ? { carrier: "dhl", price: winner.value }
      : { carrier: "ups", price: winner.value };
  } catch (err) {
    if (err instanceof WorkflowRaceError && err.code === "ALL_BRANCHES_FAILED") {
      return { carrier: "none", errors: err.errors.length };
    }
    throw err;
  }
});
```

A single branch failure does not end `step.race`; the remaining branches keep running until one succeeds. If every branch fails, or if `branches` is empty, the race throws `WorkflowRaceError` with code `ALL_BRANCHES_FAILED` and every branch error in order.

## Suspending branches

A branch suspends whenever it reaches a delay, retry backoff, store wait, pending external effect, or unresolved child join.

While one branch suspends, sibling branches continue executing within the same delivery. Once every branch has returned, failed, or suspended without deciding the combinator, the workflow suspends with the earliest wake time among the suspended branches.

If another branch's store update wakes the execution before a branch's `RetryableError` backoff or `step.effect` `retryIn` timer elapses, that retrying branch stays suspended without running early or consuming an attempt.

## Losing branch abandonment

When `step.race` chooses a winner or a fail-fast `step.all` hits its first failure, every other unfinished branch is abandoned:

- **No new steps** – Abandoned branches do not start any subsequent step.
- **No `catch` or `finally` execution** – Generator `catch` and `finally` blocks in abandoned branches do not run.
- **Waiter cleanup** – Pending store, join, and effect waiters belonging to abandoned branches are deleted when the decision is recorded so late events do not wake the workflow.
- **Replay boundary** – Later deliveries replay abandoned branches only up to the steps they reached before the decision.

An in-flight `step.run` callback in a losing branch is not aborted mid-execution. Durable waits for that running step callback to finish and records its outcome before acknowledging the delivery, though the branch generator never resumes past it.

Because running `step.run` callbacks cannot be cancelled mid-flight, `step.race` with `step.delay` does not act as a timeout for a slow `step.run` call:

```ts [src/workflows/race-limitations.ts]
import { workflow } from "@notation/durable";

const SECOND = 1_000;

export const waitWithTimeout = workflow<{ orderId: string }>(async function* (step, event) {
  // Effective timeout: step.call suspends immediately so delay(5 * SECOND) can win.
  const settled = yield* step.race("confirm-or-timeout", [
    step.call("confirm", { workflowId: "confirm-order", params: event.params }),
    step.delay("timeout", 5 * SECOND),
  ]);

  return settled;
});
```

A `step.delay` branch can only outrace suspending operations such as store waits, child joins, or external effects.

## Delay and wait race priority

When a `step.race` pits a store or join wait against a `step.delay`, both conditions can become ready before the next delivery runs – for example, when payment arrives before the deadline, but the delivery runs after the deadline has passed.

Within a single delivery, an unresolved `store.when` or `step.join` evaluates its condition before an elapsed `step.delay` decides the race, provided the wait is the final step in its branch. If payment arrived before the delivery ran, the `store.when` branch wins even if the deadline clock has also passed.

If additional steps follow the wait in that branch, or if the waiting branch uses `step.effect`, the elapsed delay branch can still finish first and win.

Once `step.race` records its decision under `key`, every later delivery returns that recorded winner regardless of timing or clock changes. Keep side effects after `step.race` behind an `index` check rather than inside the competing branches so an in-flight branch step never runs unwanted work when another branch wins.

## Step key uniqueness

Every step key across all branches and the combinator's own key must be unique within an execution. Reusing a key across two branches – or between a branch and another step in the workflow – fails the execution.

Any step key an abandoned branch actually reached before the combinator decided stays occupied on replay, so steps after `step.all` or `step.race` must not reuse those keys. Keys in an abandoned branch that were never reached before the decision remain unused and available.

## Cancellation

When an execution is cancelled while branches are waiting inside `step.all` or `step.race`, Durable delivers `WorkflowCancelledError` to every unfinished branch at once. Unlike losing branch abandonment, each cancelled branch executes its `catch` and `finally` blocks:

```ts [src/workflows/cancel-branches.ts]
import { WorkflowCancelledError, workflow } from "@notation/durable";

const MINUTE = 60_000;

export const parallelFulfillment = workflow<{ orderId: string }>(async function* (step, event) {
  yield* step.all("fulfill", [
    (async function* () {
      try {
        yield* step.delay("wait-warehouse", 10 * MINUTE);
      } catch (err) {
        if (err instanceof WorkflowCancelledError) {
          yield* step.run("release-warehouse", () => warehouse.release(event.params.orderId));
        }
        throw err;
      }
    })(),
    (async function* () {
      try {
        yield* step.delay("wait-courier", 10 * MINUTE);
      } catch (err) {
        if (err instanceof WorkflowCancelledError) {
          yield* step.run("cancel-courier", () => courier.cancel(event.params.orderId));
        }
        throw err;
      }
    })(),
  ]);
});
```

Cleanup steps inside branch error handlers or `finally` blocks – including suspending delays – run to completion across deliveries. Once every branch finishes, the enclosing combinator throws `WorkflowCancelledError`, even if a branch caught the error and returned a value instead of rethrowing (see [Cancellation](./cancellation.md)).

# Concurrent steps

A workflow runs its steps one after another. `step.all` and `step.race` run several sequences of steps, called branches, at the same time within one delivery.

This page uses a checkout workflow. It runs three checks on an order at the same time, and then waits for payment for at most 30 minutes:

```ts
import { workflow } from "@notation/durable";
import { Payment } from "./payment";

export const checkout = workflow<{ orderId: string }>(async function* (step, event) {
  const order = yield* step.run("load-order", async () => {
    return await orders.get(event.params.orderId);
  });

  const [stock, fraudScore, shipping] = yield* step.all("checks", [
    step.run("check-stock", () => warehouse.checkStock(order.items)),
    step.run("score-fraud", () => fraud.score(order.card)),
    step.run("quote-shipping", () => courier.quote(order.address)),
  ]);

  const payment = step.store(Payment, { id: order.id });

  const first = yield* step.race("payment-or-timeout", [
    payment.when("paid", (state) => state.paidAt),
    step.delay("payment-timeout", 30 * MINUTE),
  ]);

  if (first.index === 1) {
    yield* step.run("release-stock", () => warehouse.release(order.items));
    return "expired";
  }

  return "paid";
});
```

## Branches

A branch can be any of these:

- A single step, such as `step.run(...)`, `step.delay(...)`, `store.when(...)` or `step.join(...)`
- A call to an `async function*` that runs several steps in a row
- Another `step.all` or `step.race`

`Promise.all` cannot run steps at the same time, because a step is not a promise. Durable has to advance each step itself so that it can record it, so the branches are handed to `step.all` or `step.race` instead.

## `step.all`

`step.all` runs every branch and returns their results in branch order:

```ts
const [stock, fraudScore, shipping] = yield* step.all("checks", [
  step.run("check-stock", () => warehouse.checkStock(order.items)),
  step.run("score-fraud", () => fraud.score(order.card)),
  step.run("quote-shipping", () => courier.quote(order.address)),
]);
```

If each check takes one second, the three checks together take about one second, not three. TypeScript types the result as a tuple, so `stock`, `fraudScore` and `shipping` each have the type of their own step.

`step.all` is a step too, with its own key. Each branch's steps have their own records, and `checks` records how `step.all` ended. A later delivery returns the three results from the records and runs none of the checks again.

## Code between steps

Code in a branch that is not inside a step runs again on every delivery, as code outside steps does elsewhere in a workflow. A branch can use this to tell the code after `step.all` how far it got:

```ts
let charged = false;

try {
  yield* step.all("pay-and-ship", [
    (async function* () {
      yield* step.run("charge", () => payments.charge(order.card, order.total));
      charged = true;
    })(),
    step.run("ship", () => courier.ship(order.address)),
  ]);
} catch {
  if (charged) {
    yield* step.run("refund", () => payments.refund(order.card, order.total));
  }
}
```

If `ship` fails after `charge` returned, every later delivery replays `charge` from its record and sets `charged` again. The `catch` block therefore makes the same decision on every delivery.

## Suspended branches

A branch suspends at a `step.delay` that has not elapsed, at a store, join or effect wait, or when a step waits to retry.

Suppose the courier asks to be retried in a minute. `quote-shipping` throws a `RetryableError`, and its branch suspends. The other two checks return. The delivery ends once every branch has returned, failed or suspended, and the execution's status becomes `suspended`.

The next delivery replays `check-stock` and `score-fraud` from their records, and runs only `quote-shipping`.

## Failures

By default, `step.all` throws the first error that a branch does not catch. A branch fails when one of its steps throws after its retries:

| Event | What happens |
| --- | --- |
| `score-fraud` throws | `step.all` records the failure, and throws the `score-fraud` error |
| Another branch finishes its current step | The branch starts no new step. See [Losing branches](#losing-branches) |
| A later delivery | `step.all` throws the same error again, from its record |

A branch that catches its own error and returns counts as a success:

```ts
const [stock, fraudScore] = yield* step.all("checks", [
  step.run("check-stock", () => warehouse.checkStock(order.items)),
  (async function* () {
    try {
      return yield* step.run("score-fraud", () => fraud.score(order.card));
    } catch {
      return null;
    }
  })(),
]);
```

With `mode: "settle"`, every branch runs until it returns or fails, and `step.all` returns a status for each branch instead of throwing:

```ts
const results = yield* step.all("checks", branches, { mode: "settle" });

// [{ status: "fulfilled", value }, { status: "rejected", error }, ...]
```

## Limiting concurrency

`concurrency` sets how many branches can have a step running at once:

```ts
yield* step.all(
  "notify",
  order.items.map((item) => step.run(`notify:${item.id}`, () => supplier.notify(item))),
  { concurrency: 5 }
);
```

The limit applies to steps, not to whole branches. A branch gives up its place between two of its steps, and a suspended branch does not hold a place. With five places and 100 branches of two steps each, most of the first steps run before any second step.

An empty `step.all` returns `[]`.

## `step.race`

`step.race` runs every branch and returns the first one to succeed. In the example, the workflow waits for payment or for 30 minutes to pass, whichever comes first:

```ts
const first = yield* step.race("payment-or-timeout", [
  payment.when("paid", (state) => state.paidAt),
  step.delay("payment-timeout", 30 * MINUTE),
]);
```

The result is `{ index, value }`: the position of the winning branch, and its result. TypeScript narrows `value` by `index`, so `first.value` is the `paidAt` number when `first.index` is `0`.

A branch that fails does not end the race. If every branch fails, `step.race` throws `WorkflowRaceError` with the code `ALL_BRANCHES_FAILED`, and the branches' errors in `errors`, in branch order. An empty race throws `WorkflowRaceError` with no errors.

A single wait with a deadline, such as `when` with a `deadline`, works like this race. Use `step.race` when a branch is something else, such as several steps or a child workflow.

## Losing branches

When `step.race` has a winner, the other branches lose. When `step.all` throws, the branches that have not finished lose in the same way:

- A losing branch starts no new step.
- Its `catch` and `finally` blocks do not run.
- Its store, join and effect waits are deleted. A payment at minute 31 does not wake the checkout workflow.
- A later delivery replays the branch only as far as it got before the race was decided.

**A step function that is already running is not interrupted.** Durable records its result, but the branch never receives it. The race returns only after that step finishes.

This means that a race is not a timeout for `step.run`. In the race below, the delivery ends only after `call-api` finishes, and `call-api` then wins unless it throws:

```ts
yield* step.race("call-with-timeout", [
  step.run("call-api", () => slowApi.call()),
  step.delay("timeout", 5 * SECOND),
]);
```

A delay can only win against a branch that suspends, such as a store, join or effect wait.

## Ties

The payment can arrive in the same delivery in which the 30 minutes pass. If timing chose the winner, a replay after a crash could choose the other branch, and the workflow could release the stock of a paid order.

**The record decides the winner.** `step.race` records the winner under its key, and every later delivery returns that winner. This is what keeps a replay consistent with the first run.

Within one delivery, a store or join wait checks its condition before an elapsed delay counts. If the payment arrived at minute 1, but the delivery that it wakes runs at minute 31, `when` still wins.

This holds only when the wait is the last step in its branch. If a step comes after the wait, the delay's branch can finish first and win. An effect wait does not check its outcome first, so it can lose to an elapsed delay.

**Put side effects after the race, not inside a branch.** In the example, `release-stock` runs after `step.race`, behind a check of `first.index`. A step inside the delay's branch could run even though the payment branch wins.

## Step keys

Steps in a branch use their keys as written. Every key must be unique in the execution, across all branches and the key of `step.all` or `step.race` itself.

The keys that a losing branch reached stay in use, because a later delivery replays the branch up to them. A step after the race that reuses one of these keys fails. A key that the losing branch would only have reached later is free.

## Cancellation

A cancellation that arrives while branches are running reaches every branch that has not finished. Unlike a losing branch, each one runs its `catch` and `finally` blocks. See [Cancellation](./cancellation.md).

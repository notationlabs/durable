# Concurrent Steps

A workflow runs its steps one after another: the driver finishes one step before it asks the workflow for the next. `step.all` and `step.race` run several branches of steps at the same time within one delivery, and record their outcome under their own step key.

The examples on this page use one workflow, `place-order`. Before order 42 goes ahead, it checks stock, scores the card for fraud and gets a shipping quote. Each check is a `step.run` that takes about a second.

## Branches

A branch is any step generator:

- a single step, such as `step.run(...)`, `step.delay(...)`, `store.when(...)` or `step.join(...)`;
- the result of calling an `async function*` that runs several steps in a row; or
- another `step.all` or `step.race`.

`Promise.all` cannot run steps concurrently, because a step is not a promise. The driver must advance each step itself so that it can record it. `step.all` and `step.race` hand their branches to the driver for that reason.

## `step.all`

`place-order` runs its three checks at once:

```ts
const [stock, fraud, quote] = yield* step.all("check-order", [
  step.run("check-stock", () => warehouse.inStock(sku)),
  step.run("score-fraud", () => fraud.score(card)),
  step.run("quote-shipping", () => courier.quote(address)),
]);
```

The three step functions run concurrently, so the checks take about one second instead of three. `step.all` returns the results in branch order, and TypeScript types the result as a tuple of the branches' result types.

`step.all` is itself a step. When every branch has returned, the driver records the decision `fulfilled` under `check-order`. The record holds the decision, not the branch values.

### Replay

On every delivery, the driver resumes every branch through its recorded steps. Each branch returns its values from its own records, as a single `step.run` does on replay, and `step.all` applies its recorded decision to them. For `place-order`, a delivery after `check-order` is recorded runs none of the three step functions.

Because values come from replay, a `step.run` with a [schema](./steps.md#schemas) that transforms its value returns the schema's output inside a branch too, on the first run and on replay.

### Suspended branches

If the courier asks to be retried in a minute, `quote-shipping` throws a `RetryableError` and its branch suspends. The other two branches return. The delivery ends `suspended` once, with `wakeAt` set to the earliest wake time of the suspended branches.

The next delivery replays `check-stock` and `score-fraud` from their records and runs only `quote-shipping`. The delivery ends only when every branch has returned, failed or suspended.

A branch suspends at a `step.delay` that has not elapsed, a store, join or effect wait, or a retry backoff.

### Failure

In the default `fail-fast` mode, the first branch to fail settles `step.all`. A branch fails when one of its steps throws after its retries and the branch does not catch the error.

| Event | What happens |
| --- | --- |
| `score-fraud` throws | The driver records `rejected` with the branch index, and `step.all` throws the `score-fraud` error. |
| Another branch reaches its next step | It does not start it. The branch is abandoned (see [Abandoned branches](#abandoned-branches)). |
| Replay | `score-fraud` throws again from its record, and `step.all` throws that error again. |

A branch that catches its own error and returns counts as succeeded:

```ts
yield* step.all("check-order", [
  (async function* () {
    try {
      return yield* step.run("score-fraud", () => fraud.score(card));
    } catch {
      return { score: null }; // counts as a success
    }
  })(),
  step.run("check-stock", () => warehouse.inStock(sku)),
]);
```

### Settle mode

With `mode: "settle"`, every branch runs to completion or failure, and `step.all` returns a status for each branch:

```ts
const results = yield* step.all("check-order", branches, { mode: "settle" });
// [{ status: "fulfilled", value }, { status: "rejected", error }, ...]
```

### Concurrency

`concurrency` limits how many branches have a step in flight at once. A suspended branch does not hold a slot.

```ts
yield* step.all("notify", recipients.map((r) => step.run(`notify:${r}`, () => send(r))), {
  concurrency: 5,
});
```

An empty `step.all` returns `[]`.

## `step.race`

After the checks, `place-order` waits for payment, but for at most thirty minutes:

```ts
const payments = step.store(PaymentStore, { id: orderId });

const first = yield* step.race("payment-or-deadline", [
  payments.when("paid", (state) => state.paid),
  step.delay("payment-deadline", 30 * MINUTE),
]);

if (first.index === 1) {
  yield* step.run("release-stock", () => warehouse.release(sku));
}
```

`step.race` returns as soon as one branch returns. The result is `{ index, value }`: the winner's position in the array and its value. TypeScript narrows `value` by `index`, so `first.value` is the `when` result when `first.index === 0`.

A branch that fails does not end the race. If every branch fails, `step.race` throws `WorkflowRaceError` with code `ALL_BRANCHES_FAILED` and each branch's error in `errors`, in branch order. An empty race throws `WorkflowRaceError` with no errors. `errors` is kept when the error ends the execution `failed`.

`step.race` replaces the special cases for a wait with a deadline. `store.when(key, selector, { deadline })` and `step.join(key, id, { deadline })` remain, and behave as a race between the wait and a delay.

### Ties

Suppose the payment and the deadline both land in the same delivery. If timing picked the winner, a replay after a crash could pick the other one, and `place-order` could release stock for an order that was paid.

**The record decides every tie.** The driver records the first outcome under `payment-or-deadline`: `won`, with the index of the winner. Every later delivery returns that same winner, whatever order the branches finish in on replay. This is the rule both combinators rely on: a replay sees what the first run saw.

## Abandoned branches

A losing race branch, and an unfinished branch of a `fail-fast` `step.all` after a failure, are abandoned:

- **No new steps.** The branch replays its recorded steps and stops at the first step that needs new work. Its generator is not resumed past that step, so its `finally` blocks do not run.
- **Waiters deleted.** The write that records the decision deletes the store, effect and join waiters of every step the abandoned branches reached. A payment at minute thirty-one does not wake `place-order`. The losing delay's wake time is not requested again.
- **In-flight functions finish.** A `step.run` function that is already running is not interrupted. Its result is recorded but not used, and the combinator returns only after it is recorded. The delivery therefore acknowledges only after every write from an abandoned branch. If the function returned a value, the branch resumes with it, as it would on replay, and stops at its next step. If the function failed, the branch stops there, and its `catch` and `finally` blocks do not run.
- **Nested combinators record nothing.** A `step.all` or `step.race` inside an abandoned branch records no decision. Replay still drives its branches through their recorded steps.
- **Between steps.** The driver checks for abandonment between steps. Workflow code that an abandoned branch runs between two steps is not interrupted.

Cleanup that must follow a race belongs after the combinator, where the winner is known, as `release-stock` is in the example.

## Step keys

Step keys inside branches are used as written; the combinator does not prefix them. Every key must be unique within the execution, across all branches and the combinator's own key, so a key repeated across two branches fails the execution, as a repeated key does outside a combinator.

Replay reaches every key that a branch reached, including an abandoned branch. A step after a race that reuses such a key is therefore rejected on every delivery, not only on the first.

## Cancellation

When [cancellation](./cancellation.md) lands inside a combinator, every unfinished branch receives `WorkflowCancelledError` at its next unfinished step, and each branch's own `catch` and `finally` blocks run. When every branch has finished, the combinator throws `WorkflowCancelledError` and discards any branch values. See [Inside step.all and step.race](./cancellation.md#inside-stepall-and-steprace).

Cancellation runs branch cleanup while abandonment does not, because the two differ in who decides. A loser is abandoned because the workflow already has the value it asked for. A cancel comes from outside and interrupts work in progress, so each branch needs a chance to undo what it did.

## Leases

Branches make deliveries longer. Every branch's step writes and the combinator's decision are ordinary fenced writes, so a write by a holder whose lease is no longer current returns `lost` and ends the delivery. The dispatcher's lease renewal covers a long concurrent delivery as it covers any other.

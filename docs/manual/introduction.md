# Introduction

Durable runs TypeScript workflows that survive process restarts. A workflow is an async generator function. Each unit of work inside it is a step, and the workflow runs each step with `yield*`.

This workflow places an order, waits out a 30-minute cooling-off period, then charges the card:

```ts
import { workflow } from "@notation/durable";

const placeOrder = workflow(async function* (step, event) {
  const order = yield* step.run("create-order", () => orders.create(event.params));
  yield* step.delay("cooling-off", 30 * 60_000);
  const payment = yield* step.run("charge-card", () => payments.charge(order.id));
  return yield* step.run("confirm-order", () => orders.confirm(order.id, payment.id));
});
```

The first argument of each step is its key. The key names the step within the workflow.

## Replay

Durable stores each step's result in a database. At `cooling-off`, the workflow stops, and the process that ran it can exit. Thirty minutes later, Durable runs the workflow again from the first line.

On this second run, `create-order` already has a stored result. `step.run` returns that result and does not call `orders.create` again. The workflow then passes the delay and calls `payments.charge`. Running the workflow again over its stored results is called replay.

**Code between steps runs again on every replay.** Only step results are stored. A workflow that reads `Date.now()` or calls an API outside a step gets a new answer each time. Put anything that has a side effect, or that can return a different value, inside a step.

## Steps

| Step | What it does | Page |
| --- | --- | --- |
| `step.run` | Runs a function and stores its result | [Steps](./steps.md) |
| `step.delay` | Pauses the workflow for a number of milliseconds | [Steps](./steps.md) |
| `step.poll` | Checks a condition at an interval until it is true | [Steps](./steps.md) |
| `step.start`, `step.join`, `step.call` | Starts another workflow, or waits for one to finish | [Child workflows](./child-workflows.md) |
| `step.effect` | Calls an external system so that the operation happens once | [External effects](./effects.md) |
| `step.store` | Reads, updates and waits on shared state | [Stores](./stores.md) |
| `step.all`, `step.race` | Runs several steps at the same time | [Concurrent steps](./concurrent-steps.md) |

## Where workflows run

Durable needs no separate server. Your application keeps workflow records in a SQLite file, and starts a worker process to run each workflow. Durable runs on Bun, and on Node 22.18 or newer.

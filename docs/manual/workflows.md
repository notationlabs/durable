# Defining Workflows

A workflow is an async generator function that yields steps. The `workflow()` factory wraps that generator into a `WorkflowGenerator`.

## Basic definition

```ts
import { workflow } from "yieldstar";

const myWorkflow = workflow(async function* (step, event, logger) {
  const a = yield* step.run("first", () => 1);
  const b = yield* step.run("second", () => a + 1);
  return b;
});
```

The generator function receives three arguments:

| Argument | Type                             | Description                                                   |
| -------- | -------------------------------- | ------------------------------------------------------------- |
| `step`   | `StepRunner`                     | Exposes `run`, `delay`, `poll`, `store`, `start`, `join`, `call`, `effect`, `all`, `race` and `patched` |
| `event`  | `WorkflowEvent<Params, Context>` | Contains `workflowId`, `executionId`, `params`, `context` and `workflowVersion` |
| `logger` | `Logger` (Pino)                  | Structured logger scoped to the execution                     |

`workflow` and `createWorkflow` are aliases. Both return a `WorkflowGenerator`.

## Versions

`workflow` takes an optional first argument, `{ version }`, a non-negative integer:

```ts
const processOrder = workflow({ version: 2 }, async function* (step, event) {
  if (event.workflowVersion < 2) {
    yield* step.run("charge", () => payments.charge(event.params.orderId));
  } else {
    yield* step.run("charge-v2", () => payments.chargeWithTax(event.params.orderId));
  }
});
```

The execution's first delivery records the declared version, and every later delivery exposes the recorded version as `event.workflowVersion`, whatever version the current code declares. A workflow that declares no version is version 1. Use the version to keep executions that started under older code on the steps they recorded. See [Workflow versions](./replay-stability.md#workflow-versions).

## Type parameters

`workflow()` accepts two type parameters: the input params and the return type.

```ts
const processOrder = workflow<{ orderId: string }, boolean>(async function* (step, event) {
  const order = yield* step.run("fetch-order", () => fetchOrder(event.params.orderId));
  return yield* step.run("process-payment", () => processPayment(order));
});
```

These types carry through to the SDK when the workflow is registered in a router, so `workflowId`, `params`, and the return type are all enforced at the call site.

## Execution model

Every time the runtime invokes a workflow, the generator replays from the beginning. 

Steps that have already completed return their cached result from the heap with no re-execution and no side effects. Each step finds its record by its step key, so a step's key must not change while executions that recorded it are in flight. 

When the generator reaches a step that has not yet run, it executes the function, persists the result to the heap, and advances. 

If the generator hits a `delay` or a retryable error, it yields control back to the runtime. The delivery ends as `suspended` with the time the paused step needs (`wakeAt`), the execution record becomes due at that time, and the process terminates.

On the next invocation the same replay happens: cached steps resolve instantly, and the generator picks up where it left off. This means a workflow can span minutes, hours, or days while the process that runs it is ephemeral.

Before a step does new work, the driver checks that the replay has requested every step the execution recorded. If it has not, the code no longer matches the history: the delivery diverges, runs nothing new, and the execution is parked until an operator clears it. See [Divergence](./replay-stability.md#divergence).

```ts
const w = workflow(async function* (step) {
  // Execution 1: runs, caches result
  // Execution 2: replays from cache
  const a = yield* step.run("compute", () => expensiveComputation());

  // Execution 1: suspends here, due again in 5s
  // Execution 2: elapsed, continues
  yield* step.delay("pause", 5000);

  // Execution 2: runs, caches result
  const b = yield* step.run("use-result", () => useResult(a));

  return b;
});
```

## Execution records

Each trigger creates one execution, and the runtime keeps one durable `ExecutionRecord` for it. Admission writes the record with the event (`workflowId`, `params`, `context`) once, and every delivery reads the event back from it.

```ts
interface ExecutionRecord {
  executionId: string;
  workflowId: string;
  params: EventParams;
  context: EventContext;
  status: "admitted" | "running" | "suspended" | "completed" | "failed" | "cancelled";
  outcome?:
    | { status: "completed"; result: unknown }
    | { status: "failed"; error: Error }
    | { status: "cancelled"; reason?: string; source?: string };
  admittedAt: number;
  updatedAt: number;
  unacknowledgedDeliveryCount: number; // consecutive deliveries that ended without an outcome
  parent?: { executionId: string; stepKey: string; detached: boolean }; // set when step.start or step.call admitted it
  cancellation?: ExecutionCancellation; // set when cancel recorded a request
  workflowVersion?: number; // set by the first delivery's first fenced write
  divergence?: ExecutionDivergence; // set while the execution is parked after a divergent replay
}
```

| Status      | Meaning                                                              |
| ----------- | -------------------------------------------------------------------- |
| `admitted`  | No delivery has been acknowledged yet                                |
| `running`   | A dispatcher holds a live lease on the execution and is delivering it |
| `suspended` | The latest delivery paused on a delay, retry, store wait, effect wait or join |
| `completed` | The workflow returned. `outcome.result` holds the return value       |
| `failed`    | Workflow code threw an uncaught error. `outcome.error` holds it      |
| `cancelled` | The workflow let a delivered `WorkflowCancelledError` escape, or a forced cancel ended it. `outcome.reason` and `outcome.source` hold the request's |

`completed`, `failed` and `cancelled` are terminal and permanent. A finished execution never runs workflow code again: a later delivery returns the recorded outcome.

An execution started by another workflow with [`step.start`](./steps.md#stepstart) or [`step.call`](./steps.md#stepcall) records the caller's execution ID and step key as `parent`, and whether the child was started with `detached: true`. The link never changes, and executions created by a trigger have none.

`cancellation` holds the request that [`cancel`](./cancellation.md) recorded, with its `reason`, `source`, `forced`, `requestedAt` and, once the workflow has received it, `deliveredAt`. It stays on the record whatever the outcome.

`workflowVersion` is the version that `event.workflowVersion` exposes. It is set once and never changes. `divergence` holds the `code`, the `stepKey` that would have run and the `detectedAt` time of a divergent replay. While it is set, no wake delivers the execution. `clearDivergence` or a terminal outcome removes it. See [Replay Stability](./replay-stability.md).

**Execution IDs are unique for the lifetime of the runtime.** A trigger may supply its own `executionId`, and the runtime reserves it unchanged; otherwise the runtime chooses one. Reusing an ID that was ever admitted, including one whose execution failed, is rejected with `EXECUTION_ID_CONFLICT`. An [idempotent trigger](./sdk-local.md#idempotent-trigger) that repeats the same request returns the existing execution instead, and admits nothing. This matters because the ID is how a replay finds its own recorded steps, so a reused ID would replay another execution's results.

A record is `running` while a dispatcher holds a lease on it, and otherwise changes status only when a delivery is acknowledged with an outcome. If a delivery hits an infrastructure error (the database throws, the worker exits), it ends without an outcome: the record returns to its earlier status, `unacknowledgedDeliveryCount` goes up by one, and the execution is delivered again after a redelivery delay. The delay starts at 1 second and doubles with each consecutive unacknowledged delivery, and any acknowledgement resets the count. Such errors never reach workflow code; see [Steps](./steps.md#errors).

## Registering workflows

A router maps string IDs to workflow generators:

```ts
import { createWorkflowRouter } from "yieldstar";

const router = createWorkflowRouter({
  "process-order": processOrder,
  "send-reminder": sendReminder,
});

export type Router = typeof router;
```

Exporting the `Router` type and passing it to the SDK factory gives you type-safe `workflowId` and `params` at every call site.

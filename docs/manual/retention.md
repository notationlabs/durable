# Execution Retention

By default, the runtime keeps everything an execution writes: its execution record, with its params and outcome, every attempt of every step, a receipt for every store update, its start requests and its waiters. Nothing is ever deleted, so the database grows with every execution.

With a retention policy, the runtime prunes terminal executions once they are older than a configured period. Pruning deletes the execution's history and replaces its record with a small tombstone. The tombstone stays for good, so the execution ID is never admitted again, and `poll` can still tell a pruned execution from an ID that was never used.

This page uses one example throughout. A shop triggers `place-order` for every order, with execution IDs such as `order-42`. Each `place-order` execution joins a `ship-order` child, and starts a `charge-order` child for the payment.

## Configuring retention

Pass `retain` to the runtime's constructor. `SqliteRuntime` and `MemoryRuntime` both take it:

```ts
import { SqliteRuntime, createSqliteDb } from "@yieldstar/sqlite-runtime/bun";

const DAY = 24 * 60 * 60 * 1000;

const runtime = new SqliteRuntime(createSqliteDb({ path: "./.db/local.sqlite" }), {
  retain: {
    default: { completed: 7 * DAY, failed: 30 * DAY, cancelled: 7 * DAY },
    workflows: {
      // Keep payment executions for a year, for audit.
      "charge-order": { completed: 365 * DAY, failed: 365 * DAY },
    },
  },
});
```

`retain` has the type `RetentionOptions`:

```ts
type RetentionOptions = {
  default?: RetentionPolicy;
  workflows?: Record<string, RetentionPolicy>; // keyed by workflow ID
};

type RetentionPolicy = {
  completed?: number;
  failed?: number;
  cancelled?: number;
  pruneExecutionStores?: boolean; // default false
};
```

- Each period is in milliseconds, counted from when the execution's terminal outcome was recorded.
- An omitted period keeps executions with that outcome forever. In the example, a cancelled `charge-order` execution is never pruned.
- A workflow's entry replaces `default` as a whole. It is not merged with it. That is why `charge-order` has no `cancelled` period, even though `default` has one.
- A workflow with no entry uses `default`. With no `default` either, its executions are never pruned.
- Without `retain`, nothing is pruned.
- A period that is negative, not finite or not a number, a `pruneExecutionStores` that is not a boolean, or a field of the wrong shape throws a `TypeError` from the constructor.

The constructor copies the policy, so a later change to the object you passed has no effect. To change a policy, construct the runtime again with the new one, for example on the next deploy. The runtime reads the policy each time it prunes, so a new period also applies to executions that are already terminal. Shortening `completed` from 7 days to 1 day prunes every completed execution older than 1 day at the next pruning pass.

The policy lives in the runtime's configuration, not in workflow code. Pruning runs in the process that runs the [dispatcher](#running-the-pruner), and with the worker invoker that process does not load workflow code. A forced cancel also records a terminal outcome without running workflow code. Set `retain` on the runtime that the dispatcher uses. A worker process's runtime does not need it.

## Which executions are pruned

Only terminal executions are pruned: `completed`, `failed` and `cancelled`. An execution that is `admitted`, `running` or `suspended` is never pruned, whatever its age. Neither is an execution parked with a [divergence](./replay-stability.md#divergence), because it is not terminal.

The retention period starts when the terminal outcome is recorded. Nothing changes a terminal record after that, so it is the record's `updatedAt`. Expiry is checked against the runtime's clock. With the policy above, `order-42`, completed on 1 October, is pruned at the first pruning pass after 8 October.

An execution that another execution is still joining is kept, even after its period has passed. See [Joins](#joins).

## What pruning deletes

Pruning an execution deletes:

- its step records, every attempt of every step, including `step.all` and `step.race` decisions and `step.patched` markers;
- its store receipts, the applied-steps ledger rows that let replay return an update's result without applying it again;
- the start requests it recorded as a caller with `step.start` and `step.call`;
- any store, effect and join waiters it still holds;
- the contents of its execution record: `params`, `context`, the outcome, `cancellation`, `parent`, `workflowVersion`, `divergence`, and its wake and lease state; and
- with `pruneExecutionStores: true` in its policy, every store whose `id` equals the execution ID, under any store definition, with the waiters registered on those stores.

A store created by `step.store(definition)` without an `id` has the execution ID as its `id`. Such stores are kept unless the policy sets `pruneExecutionStores`. A store's `id` is not tied to the execution: other executions, and external code through `runtime.stores.store(definition, id)`, can read the same store by its ID. Set `pruneExecutionStores` only when nothing else reads the store after the execution ends.

## What pruning keeps

- **The tombstone.** It holds the execution ID, the workflow ID, the terminal status and the time of pruning. Tombstones are never deleted.
- **Children.** A child started by `step.start` or `step.call` follows its own workflow's policy. Its `parent` link still names the pruned execution, and `poll` of that ID returns the tombstone.
- **Shared stores**, the stores with an `id` that is not the execution ID.
- **Effect records.** An effect ID can be shared with other executions and other runtimes, and an effect's settled result must not be deleted. Effect records have no retention.

## Tombstones

`runtime.executions.get` returns a `PrunedExecutionRecord` for a pruned execution, and `undefined` for an ID that was never admitted:

```ts
type ExecutionTombstone = {
  executionId: string;
  workflowId: string;
  terminalStatus: "completed" | "failed" | "cancelled";
  prunedAt: number;
};

type PrunedExecutionRecord = ExecutionTombstone & { status: "pruned" };
```

`isPrunedExecution(value)` from `@yieldstar/core` recognises one. `"pruned"` is not an `ExecutionStatus`: a pruned execution has no record and no outcome left, only its tombstone.

Because the tombstone stays, the execution ID stays in use:

- A trigger with the ID `order-42` throws `ExecutionAdmissionError` with `EXECUTION_ID_CONFLICT`, as it does for any used ID.
- An [idempotent trigger](./sdk-local.md#idempotent-trigger) with the ID `order-42` throws `ExecutionAdmissionError` with `EXECUTION_PRUNED`. The params it would compare are gone, so it can never return `"existing"`, and it never admits a new execution.

So a late retry of the trigger for `order-42` cannot charge the card a second time.

## Joins

[`step.join`](./steps.md#stepjoin) reads the target's outcome from its record. Suppose `ship-order` has finished and its join waiter has woken `order-42`, but `order-42` has not been delivered yet. If `ship-order` were pruned now, `order-42` would find no outcome to read. So a target is not pruned while a join waiter is registered on it whose joiner is not terminal. The check and the tombstone are one atomic write.

The waiter is deleted in the same write that records the joiner's join step. After that, the target can be pruned once its period has passed. A joiner that is parked with a divergence still holds its waiter, so the target stays.

A waiter whose joiner is terminal or missing does not hold the target back, because that joiner never reads the target again. Some waiters stay registered for good, such as one that a stale lease holder registered after the step was recorded. The tombstone write deletes the waiters that are left on the target.

A `step.join` or `step.call` that reads a target after it was pruned records and throws `WorkflowJoinError` with code `JOIN_TARGET_PRUNED`:

```ts
try {
  yield* step.join("ship:join", shipmentId);
} catch (err) {
  if (err instanceof WorkflowJoinError && err.code === "JOIN_TARGET_PRUNED") {
    // the target's outcome is no longer kept
  }
}
```

A join registers a waiter only on a target that is not terminal, and only terminal targets are pruned. So a joiner never waits on a pruned target: it reads either the outcome or the tombstone.

## Observing a pruned execution

| Call | Pruned execution | ID never admitted |
| --- | --- | --- |
| `poll` | The pruned view | `undefined` |
| `wait` | The pruned view, at once | `undefined` |
| `list` without `status` | Left out | – |
| `list({ status: "pruned" })` | Included, as the pruned view | – |
| `cancel` | The pruned view. Nothing changes | `undefined` |
| `clearDivergence` | `{ status: "not-diverged" }` | `undefined` |
| `triggerAndWait`, `waitForResult` | Throws `ExecutionPrunedError` | – |
| `step.join`, `step.call` | Throws `WorkflowJoinError` with `JOIN_TARGET_PRUNED` | Throws `WorkflowJoinError` with `JOIN_TARGET_NOT_FOUND` |

The pruned view has the same fields as the tombstone:

```ts
type PrunedExecutionView = ExecutionTombstone & { status: "pruned" };

const execution = await sdk.poll("order-42");
if (execution?.status === "pruned") {
  execution.terminalStatus; // "completed"
  execution.prunedAt;
}
```

`ExecutionView`, `WaitResult` and `CancelResult` include `PrunedExecutionView`, so code that switches over `status` must handle `"pruned"`. A pruned execution leaves `list` as soon as it is pruned, so an execution can disappear from a `status: "completed"` filter while you page through it.

`ExecutionPrunedError` carries the tombstone's `executionId`, `workflowId`, `terminalStatus` and `prunedAt`, and has `code` `"EXECUTION_PRUNED"`. `yieldstar` and `@yieldstar/core` export it:

```ts
import { ExecutionPrunedError } from "yieldstar";

try {
  await exec.waitForResult();
} catch (err) {
  if (err instanceof ExecutionPrunedError) {
    // the execution ended err.terminalStatus, and its outcome is gone
  }
}
```

### Over HTTP

| Route | Pruned execution |
| --- | --- |
| `POST /poll` | `200` with the pruned view |
| `POST /wait` | `200` with the pruned view, at once |
| `POST /list` | Left out, unless `status` is `"pruned"` |
| `POST /cancel` | `200` with the pruned view |
| `POST /clear-divergence` | `200 { status: "not-diverged" }` |
| `POST /trigger` | `409` `EXECUTION_ID_CONFLICT`, or `409` `EXECUTION_PRUNED` with `idempotent: true` |
| `POST /events` | `410 Gone` with `EXECUTION_PRUNED` |

An ID that was never admitted still responds `404` `EXECUTION_NOT_FOUND`. The deprecated `/events` route responds with the result or the recorded error, and a pruned execution has neither, so it responds `410`. The HTTP SDK's `waitForResult` throws `ExecutionPrunedError`, and `trigger` throws `ExecutionAdmissionError` with `EXECUTION_PRUNED`.

[Observation middleware](./http-server.md#observation-middleware) receives the tombstone from `load()` for a pruned execution. The tombstone has no `params` or `context`, so a policy that reads them must decide what to do with a tombstone.

## Running the pruner

An `ExecutionDispatcher` whose runtime has `retain` prunes in the background. It starts a pruning run when it starts, and another one every `pruneInterval` milliseconds after the previous run ends (60 seconds by default):

```ts
const dispatcher = new ExecutionDispatcher(runtime, { invoker, logger, pruneInterval: 5 * 60_000 });
dispatcher.start();
```

Each run calls `runtime.prune` with a batch of 100 executions (`DEFAULT_PRUNE_BATCH_SIZE`), and calls it again while a pass finds a full batch. A run that throws is logged with `logger.error`, and the next run finishes its work. `stop()` stops pruning and waits for the pass in progress. A dispatcher with `filter` prunes every expired execution in the runtime, not only the filtered ones. Without `retain`, the dispatcher does not prune.

To prune at a time you choose, for example from a scheduled job, call `runtime.prune` yourself:

```ts
const { pruned, cleaned } = await runtime.prune({ limit: 500 });
```

One pass works in two phases:

1. The runtime replaces up to `limit` expired executions with their tombstones, oldest outcome first. Each tombstone is one atomic write, together with the join waiter check. A reader sees either the whole terminal execution or the tombstone, never a partly deleted record.
2. The runtime deletes the step records, store data, waiters and start requests of up to `limit` tombstones whose cleanup has not finished, and then marks each tombstone as cleaned.

The pass resolves with the number of executions it tombstoned and the number it cleaned. `limit` defaults to 100, and a value that is not a positive integer throws a `RangeError`. Without `retain`, a pass tombstones nothing, and only finishes cleanups that are already pending.

If a process stops between the two phases, the tombstones stay marked as pending cleanup, and the next pass cleans them. Every step is safe to repeat, and several processes can prune the same backend at once: each execution is tombstoned once, and every process reaches the same end state.

### Upgrading a SQLite database

`SqliteRuntime` adds the tombstone columns to an existing `executions` table when it opens the file. Every process that shares the file must run a version with retention support before anything prunes. See [`SqliteRuntime`](../packages/sqlite-runtime.md#sqliteruntime).

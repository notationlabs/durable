# Heap Client

`HeapClient` stores immutable step attempt records, returns the latest attempt during replay, removes finished step waiters atomically, and records cancellation delivery across parked steps. Every step result, retryable error, and combinator decision passes through this client.

## Step records

A step record is keyed by `executionId`, `stepKey`, and `stepAttempt`, and conforms to `HeapRecord`:

```ts [src/heap-types.ts]
type HeapRecord = {
  stepResponseJson: string;
  meta: { attempt: number; done: boolean };
};
```

`stepResponseJson` is an opaque JSON string produced and decoded by `@notation/durable`. Store and return it verbatim without re-serializing or reordering keys.

`attempt` is a zero-based counter identifying each attempt of a step. Unfinished steps such as retryable errors, delays, or waits set `done` to false, whereas settled steps mark `done` true.

| Method | Behavior |
| :--- | :--- |
| `readStep` | Returns the `HeapRecord` with the highest `attempt` for `executionId` and `stepKey`, or `null` |
| `writeStep` | Atomically verifies `lease` and inserts an immutable step attempt record |
| `recordCancellation` | Atomically verifies `lease`, inserts step records, deletes waiters, and sets `deliveredAt` |

## Reading the latest attempt

`readStep` is read-only and unfenced, returning the latest attempt record for the given execution ID and step key, or `null` when no attempts exist.

## Writing step records

`writeStep` records one attempt of a step, fenced by `lease`. Check the lease and write the record in one atomic step:

```ts [src/postgres-heap-write.ts]
import {
  isLeaseCurrent,
  type ExecutionLease,
  type HeapRecord,
  type LeaseLost,
} from "@notation/durable/runtime";
import type { Pool } from "pg";
import { withTransaction } from "./postgres-tx";

export async function writeStepRecord(
  pool: Pool,
  params: {
    lease: ExecutionLease;
    stepKey: string;
    stepAttempt: number;
    done: boolean;
    stepResponseJson: string;
    deleteWaitersOf?: readonly string[];
  }
): Promise<HeapRecord | LeaseLost> {
  const { lease, stepKey, stepAttempt, done, stepResponseJson } = params;

  return withTransaction(pool, async (tx) => {
    const state = await tx.getExecutionStateForUpdate(lease.executionId);
    if (!state || !isLeaseCurrent(state, lease)) {
      return { status: "lost" };
    }

    const { record, inserted } = await tx.insertStepIfAbsent({
      executionId: lease.executionId,
      stepKey,
      stepAttempt,
      done,
      stepResponseJson,
    });

    if (inserted && done) {
      await tx.deleteStepWaiters(lease.executionId, stepKey);
      for (const abandoned of params.deleteWaitersOf ?? []) {
        await tx.deleteStepWaiters(lease.executionId, abandoned);
      }
    }

    return record;
  });
}
```

Follow four invariants in every `writeStep` implementation:

1. **Atomic lease check** – Verify `isLeaseCurrent(state, lease)` in the same transaction as the step insert. If the execution is missing or the lease is stale, write nothing and return `{ status: "lost" }`.
2. **Immutable attempts** – Never overwrite an existing attempt row; return the stored `HeapRecord` unchanged on collision so the workflow driver raises `StepConflictError` if the payload or `done` flag differs.
3. **Waiter cleanup on finished writes** – When a write inserts a new finished record (`done` true), delete all store, effect, and join waiters for that step and any keys in `deleteWaitersOf` in the same transaction; keep waiters when unfinished or already present.
4. **Cross-database fencing** – Because `isLeaseCurrent` reads `ExecutionState.leaseToken`, backends that separate step records and execution records across databases cannot share one local transaction. Enforce the fencing token directly on the step store, for example via a replicated conditional token check or a coordinator.

`deleteWaitersOf` is passed when concurrent combinators such as `step.all` or `step.race` settle and abandon waiting branches whose waiters would otherwise linger.

## Recording cancellation delivery

When a delivery encounters an undelivered cancellation request, the driver calls `recordCancellation({ lease, steps })` to persist `WorkflowCancelledError` records for every parked step (and any unsettled parent combinators) and mark the cancellation request delivered in one atomic fenced write:

```ts [src/postgres-heap-cancel.ts]
import {
  isLeaseCurrent,
  markCancellationDelivered,
  type ExecutionLease,
  type HeapRecord,
  type LeaseLost,
} from "@notation/durable/runtime";
import type { Pool } from "pg";
import { withTransaction } from "./postgres-tx";

export async function recordHeapCancellation(
  pool: Pool,
  clock: () => number,
  params: {
    lease: ExecutionLease;
    steps: readonly { stepKey: string; stepAttempt: number; stepResponseJson: string }[];
  }
): Promise<(HeapRecord | null)[] | LeaseLost> {
  const { lease, steps } = params;

  return withTransaction(pool, async (tx) => {
    const state = await tx.getExecutionStateForUpdate(lease.executionId);
    if (!state || !isLeaseCurrent(state, lease)) {
      return { status: "lost" };
    }

    const stored = await Promise.all(
      steps.map(({ stepKey, stepAttempt }) =>
        tx.getStepRecord(lease.executionId, stepKey, stepAttempt)
      )
    );

    const hasConflict = stored.some(
      (record, i) =>
        record !== null &&
        (record.stepResponseJson !== steps[i].stepResponseJson || !record.meta.done)
    );
    if (hasConflict) {
      return stored;
    }

    const nextState = markCancellationDelivered(state, clock());
    const written: HeapRecord[] = [];
    for (const { stepKey, stepAttempt, stepResponseJson } of steps) {
      const { record } = await tx.insertStepIfAbsent({
        executionId: lease.executionId,
        stepKey,
        stepAttempt,
        done: true,
        stepResponseJson,
      });
      await tx.deleteStepWaiters(lease.executionId, stepKey);
      written.push(record);
    }

    await tx.updateExecutionState(nextState);
    return written;
  });
}
```

Implement `recordCancellation` with these exact semantics:

- **Lease check** – Return `{ status: "lost" }` without modifying anything when the execution is missing or `!isLeaseCurrent(state, lease)`.
- **Conflict detection** – Inspect every target step before writing. If any row at that attempt holds a different response payload or an unfinished flag, write nothing and return the array of stored records or nulls (an identical finished row does not conflict).
- **Missing cancellation request** – `markCancellationDelivered(state, clock())` throws when `state.cancellation` is `null`. Run it before mutating storage so no step records or waiter deletions commit without an active cancellation request.
- **Waiter deletion** – Delete store, effect, and join waiters only for the `stepKey` entries in `steps`, leaving waiters on any unrecorded cleanup or sibling steps intact.
- **All-or-nothing commit** – Inserting the step records, deleting their waiters, and persisting `deliveredAt` must succeed or roll back together.

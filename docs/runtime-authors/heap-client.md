# Heap client

The heap client keeps step records. When a step finishes, the worker writes its result to the heap. A later delivery reads the record and returns the result without running the step again.

## Step records

A record belongs to one attempt of one step, and is keyed by execution ID, step key and attempt number:

```ts
type HeapRecord = {
  stepResponseJson: string;
  meta: { attempt: number; done: boolean };
};
```

`stepResponseJson` is a string that Durable writes and reads. The runtime stores it as it is. `done` is `false` for an attempt that did not finish the step, such as an attempt that will be retried, or a wait.

A workflow that charges a card leaves these records when the first attempt times out and the second succeeds:

| Step key | Attempt | `done` | `stepResponseJson` |
| --- | --- | --- | --- |
| `charge-card` | 0 | `false` | `{"type":"step-error","err":{"$type":"error","name":"RetryableError",...},...}` |
| `charge-card` | 1 | `true` | `{"result":{"chargeId":"ch_1"},"type":"step-result"}` |
| `$$workflow-result$$` | 0 | `true` | `{"result":{"chargeId":"ch_1"},"type":"workflow-result"}` |

## Methods

| Method | Does |
| --- | --- |
| `readStep({ executionId, stepKey })` | Returns the record with the highest attempt number, or `null` |
| `writeStep({ lease, stepKey, stepAttempt, done, stepResponseJson, deleteWaitersOf? })` | Writes one record, fenced by `lease` |
| `recordCancellation({ lease, steps })` | Writes the records of a delivered cancellation, fenced by `lease`. See [Cancellation](#cancellation) |

## Writing a step

`writeStep` checks the lease and writes the record in one transaction. If the lease is not current, it writes nothing and returns `{ status: "lost" }`. `isLeaseCurrent(state, lease)` makes the check from the execution's stored state. See [Delivery guarantees](./delivery-guarantees.md).

**A record never changes once written.** If a record already exists for the attempt, `writeStep` keeps it and returns it as stored. The worker compares the returned record with the one it wrote. If they differ, another delivery recorded the step first, and the worker ends its delivery so that the next one uses the stored record.

A `done` record must delete the step's waiters in the same write: its store waiters, its effect waiters and its join waiters. `deleteWaitersOf` lists other steps of the same execution whose waiters go too. The worker passes it when `step.all` or `step.race` stops branches that were waiting. Waiters are deleted only when the record is new.

`SqliteRuntime` does all of this in one transaction:

```ts
return runTransaction(this.db, () => {
  const state = this.executionsDao.getState(executionId);
  if (!state || !isLeaseCurrent(state, lease)) {
    return { status: "lost" as const };
  }

  const { record, inserted } = this.insert(executionId, stepKey, stepAttempt, done, stepResponseJson);
  if (inserted && done) {
    this.deleteStepWaiters(executionId, stepKey);
    for (const other of params.deleteWaitersOf ?? []) {
      this.deleteStepWaiters(executionId, other);
    }
  }
  return record;
});
```

**The lease check reads the execution client's state.** So a runtime that keeps step records and execution records in different databases cannot make the check and the write one transaction, and needs another way to fence the write.

## Cancellation

When a delivery carries a cancellation request, the worker records it with `recordCancellation`. `steps` holds one `done` record for each step at which the cancellation is thrown. In one fenced write, `recordCancellation`:

1. Writes each step's record.
2. Deletes those steps' store, effect and join waiters.
3. Sets the request's `deliveredAt`, with `markCancellationDelivered` from `@notation/durable/runtime`.

If a step already has an identical record at that attempt, the record is kept. If a step has a different record, or one that is not `done`, the write changes nothing at all. Either way, `recordCancellation` returns the records as stored, in the order of `steps`, with `null` for a step that has none.

`recordCancellation` throws if the execution has no cancellation request.

# Join client

The join client keeps the waiters of `step.join` and `step.call`. A waiter records that one execution waits for another to end. When the other execution ends, the runtime makes the waiting execution due, and its next delivery reads the outcome.

## Waiters

A waiter names the execution that is waited for, the execution that waits, and the step that waits:

```ts
type JoinWaiter = {
  targetExecutionId: string;
  executionId: string;
  stepKey: string;
};
```

The client has one method, `join(waiter)`. It reads the target's record and answers:

| The target | `join` returns | And |
| --- | --- | --- |
| Has ended | `{ status: "terminal", outcome }` | Registers nothing |
| Has not ended | `{ status: "waiting" }` | Registers the waiter. A repeat keeps one registration |
| Does not exist | `{ status: "not-found" }` | Registers nothing |

`readJoinTarget(state)` from `@notation/durable/runtime` gives the answer from the target's stored state.

## Example: a parent waits for a child

A workflow `place-order` calls a child `ship-order` with `step.call("ship", ...)`. The records change like this:

| Event | Join waiters | `place-order` |
| --- | --- | --- |
| The `ship` step calls `join`, and the child has not ended | `ship-order` ← `place-order`, `ship` | Records `ship` unfinished, and suspends |
| `ship-order` is acknowledged as completed | Unchanged | Becomes due, in the same write as the outcome |
| The next delivery calls `join` again | Unchanged | `join` returns `terminal`. The step records its result, `done` |
| The `done` record is written | The waiter is deleted in the same write | |

## Reading and registering

**`join` must read the target and register the waiter in one atomic step.** The target can end at any moment. If it ends between the read and the registration, the acknowledgement finds no waiter to wake, and the joining execution waits forever.

`SqliteRuntime` runs both in one transaction, which waits for any acknowledgement in progress:

```ts
async join(waiter: JoinWaiter): Promise<JoinResult> {
  return runTransaction(this.db, () => {
    const result = readJoinTarget(this.executionsDao.getState(waiter.targetExecutionId));
    if (result.status === "waiting") this.joinWaitersDao.upsertWaiter(waiter);
    return result;
  });
}
```

## Writes in other clients

The other clients keep the rest of the contract:

| Write | Client | Also, in the same write |
| --- | --- | --- |
| An acknowledgement that ends the target | Execution client | Makes the execution of every waiter on the target due. Keeps the waiters |
| A forced cancel of the target | Execution client | The same, and deletes the waiters that the cancelled execution holds as a joiner |
| A `done` record of the waiting step | Heap client | Deletes the step's waiter |
| `recordCancellation` | Heap client | Deletes the waiters of the steps it records |

No other write deletes a waiter.

## Waiters that stay

`join` is not fenced. A paused delivery can register a waiter after another delivery has recorded the step. A deploy can also remove a join step that was still waiting. Such a waiter is never deleted.

**A waiter that stays causes at most one extra delivery**, when its target ends. That delivery finds the step already recorded, or no step, and changes no outcome. The runtime does not need to remove these waiters.

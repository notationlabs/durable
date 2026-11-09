# Store client

The store client keeps the state of [stores](../manual/stores.md), and the waiters of `when` and `take`. Workflows reach it through `step.store`, and application code through [store handles](../manual/store-external.md).

## Store records

A runtime keeps one record for each store, found by the store's name and ID. The record holds:

| Field | Holds |
| --- | --- |
| `instanceId` | A UUID, new each time the store is created. A store that is deleted and created again has a new one |
| `version` | `0` when the store is created, and one more after each update |
| `state` | The state, encoded |

Two more kinds of record go with stores:

| Record | Keyed by | Holds |
| --- | --- | --- |
| Applied step | Store name and ID, execution ID and step key | The result of a workflow's update, so that a repeat returns it without updating again |
| Waiter | Store name and ID, execution ID and step key | The `instanceId` and `sinceVersion` that the step read, and the paths of the state it read |

## CasStoreClient

`@notation/durable/runtime` exports two classes. `StoreClient` declares the methods that workflows and handles call. `CasStoreClient` extends it and implements most of them, as loops that read the store, run the application's function, and commit only if the store has not changed since the read.

**Extend `CasStoreClient`.** The runtime then implements these methods, and never runs application code inside a transaction:

| Method | Does |
| --- | --- |
| `readStore` | Returns the store's snapshot, or `undefined`. Never creates a store |
| `insertStore` | Inserts a snapshot at version 0 unless the store exists, and returns the store's snapshot either way |
| `getAppliedStoreStep` | Returns a workflow update's recorded result, if any |
| `commitStoreMutation` | Writes a new state. See [Committing an update](#committing-an-update) |
| `commitStoreDeletion` | Deletes the store and its waiters, if it has not changed |
| `registerWaiter` | Registers a waiter. See [Waiters](#waiters) |
| `listStores` | Returns the IDs of a definition's stores, in ascending order |
| `deleteStore` | Deletes the store and its waiters. Does nothing if the store does not exist |

## Committing an update

`commitStoreMutation(mutation)` receives the snapshot that the update read, as `mutation.expected`, and the new state. In one transaction, it:

1. Returns `already-applied` with the recorded result, if `mutation.stepId` has an applied step record.
2. Returns `conflict` with the current snapshot, if the store's `instanceId` or `version` differs from `expected`. `CasStoreClient` then runs the update again on the current state.
3. Writes the new state at `expected.version + 1`, and the applied step record if there is a `stepId`.
4. Makes the execution of every matching waiter due.

When `expected.created` is `true`, the update read a store that did not exist. The commit inserts the store, and returns `conflict` if it exists by then.

**The new state and the wakes must be one write.** If the state is written and a wake is lost, the waiting workflow does not run again until another update.

## Waiters

A workflow's `when` or `take` reads the state with a selector. When the selector does not match, the workflow registers a waiter and suspends. Durable records the paths that the selector read. A `when` on `state.inbox.length > 0` from [Waiting on state](../manual/store-waiting.md) records these paths:

```ts
[["inbox"], ["inbox", "length"]]
```

An update records the paths that it changed. An update that pushes a message onto `inbox` changes `["inbox", 0]`, among others.

A commit wakes a waiter when both are true:

- The waiter's `sinceVersion` is lower than the new version.
- `storePathsIntersect(waiter.readPaths, mutation.changedPaths)` returns `true`, because one path starts with the other.

An update to `status` does not wake the waiter, because no read path starts with `status`.

**`registerWaiter` must compare the store with the waiter in the same transaction.** An update can commit between the workflow's read and the registration. That update does not see the waiter, so `registerWaiter` makes the execution due at once if the store's `instanceId` differs from the waiter's, or its `version` is higher than `sinceVersion`. `SqliteRuntime` does this:

```ts
async registerWaiter(waiter: StoreWaiter): Promise<void> {
  runTransaction(this.db, () => {
    this.storeWaitersDao.upsertWaiter(waiter);

    const row = this.getStoreRow(waiter.storeName, waiter.storeId);
    if (!row || row.instance_id !== waiter.instanceId || row.version > waiter.sinceVersion) {
      this.executions.wakeInTransaction(waiter.executionId);
    }
  });
}
```

An extra wake is safe. The workflow reads the state again, and registers a new waiter if the selector still does not match.

A waiter stays until the heap writes the step's `done` record, which deletes it in the same write. A store deletion and a forced cancel of the execution delete it too.

## Errors

`CasStoreClient` checks the state against the store's schema and throws `StoreValidationError` if the schema rejects it. It throws `WorkflowValueError` with the code `INVALID_STEP_VALUE` for a state that is not a durable value. A workflow can catch both, and nothing is committed.

Any other error from the client ends the delivery without an outcome. See [Failed deliveries](../manual/error-reference.md#failed-deliveries).

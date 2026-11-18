# Store Wakes

Notation Durable stores combine compare-and-set versioning, unique `instanceId` tokens, and property-level path tracking so suspended workflows wake atomically when watched fields change without missing concurrent updates or colliding with recreated stores.

## Lazy creation & version compare-and-set

Store rows are never pre-allocated. Every operation except unconditional deletion lazily initializes a missing store from its `initial` factory at version 0 with a newly generated UUIDv7 `instanceId`.

Unconditional mutations (`updateStore` and `takeFromStore`) loop on read-compute-commit, advancing the version by one only when the stored `instanceId` and version still match. Conditional writes check a caller-supplied `StoreSnapshot` once and return a conflict result instead of retrying.

| Operation | Behavior |
| :--- | :--- |
| `getStore` | Inserts `definition.initial` at `version = 0` if missing |
| `updateStore` | Initializes if missing and retries on CAS conflict |
| `updateStoreFrom` | Checks `instanceId` and `version` once without retrying |
| `takeFromStore` | Retries CAS loop on match or persists `version = 0` on miss |
| `deleteStoreFrom` | Checks `instanceId` and `version` once and deletes store plus waiters |
| `deleteStore` | Removes store and waiters without creating a missing store |

Workflow mutations carrying a step ID also record an applied-step receipt inside the same transaction. Replaying that step returns the stored result directly without re-running the callback or bumping `version`.

## ABA protection with `instanceId`

Keying state solely by `(storeName, storeId)` and `version` creates an ABA hazard when a store is deleted and recreated at `version = 0`. A caller holding a stale snapshot or waiter from the deleted instance could otherwise mistake the new `version = 0` row for the unchanged original.

The `stores` table pairs a unique store name and ID pair with `instanceId` as its primary key. Deleting a store removes both the state row and all matching `store_waiters` inside one transaction.

Conditional mutations verify both `instanceId` and version, rejecting stale snapshots from deleted instances even if the recreated store sits at version 0. Similarly, `StoreWaiter` binds the snapshot's instance ID and version alongside the workflow waiter key and `readPaths` so waiter registration catches deletion and recreation immediately.

Suppose instance A is read at version 0, deleted, and recreated as instance B at version 0. A conditional write or `registerWaiter` call carrying instance A rejects on instance ID mismatch instead of mistaking version 0 for unchanged state.

## Read-path tracking on waits

When `when` or an unmatched `take` evaluates a selector against a snapshot, Durable wraps the clone in a read-only proxy that rejects writes and records accessed property paths into `readPaths`. Reading built-in dates, maps, or sets records the path to the container because their internal slots bypass property traps.

If an unmatched `take` lazily initialized the store from `initial(id)`, it inserts the `version = 0` row first – retrying if another writer created the store concurrently – before returning `{ matched: false, instanceId, version, readPaths }`.

```mermaid [waiter registration and lost-wakeup guard]
sequenceDiagram
  participant WF as Workflow Driver
  participant Heap as Step Heap
  participant Store as Store Client
  WF->>Store: Read snapshot (instanceId, sinceVersion)
  Note over WF: selector returns no match; capture readPaths
  WF->>Heap: writeStep(unfinished StepStoreWait)
  WF->>Store: registerWaiter(StoreWaiter)
  Note over Store: Single IMMEDIATE transaction
  Store->>Store: Upsert waiter; check row instanceId and version
  alt Store missing or instanceId != waiter.instanceId or version > sinceVersion
    Store->>Store: wakeInTransaction(executionId)
  end
```

Writing the unfinished step before `registerWaiter` ensures any timed `wakeAt` deadline is durable before a wake can re-deliver the execution, while the transaction-scoped version and `instanceId` check closes the lost-wakeup window between snapshot read and registration.

## Write-path matching & atomic waking

`update` and matched `take` wrap the draft in a write-recording proxy that captures mutated property paths into `changedPaths` in `O(changes)` time, including array index and length updates. If an updater returns a replacement object or schema validation transforms the state, the client falls back to a structural diff between previous and next state.

Inside `commitStoreMutation`, one `IMMEDIATE` transaction advances the store version, inspects matching waiters, and wakes executions whose recorded `sinceVersion` is older and whose `readPaths` intersect `changedPaths`. Waiters stay registered across spurious wakes until the step records a final outcome, the execution cancels, or the store deletes.

Suppose a waiter recorded `[["items", 0, "status"]]`:

| Changed path | Wake behavior |
| :--- | :--- |
| `["items"]` | Wakes because the changed path is a prefix of the read path |
| `["items", 0, "status"]` | Wakes on exact path equality |
| `["items", 0, "status", "code"]` | Wakes because the read path is a prefix of the changed path |
| `["items", 1, "status"]` | Skips because sibling paths do not overlap |
| `["summary", "count"]` | Skips because disjoint branches do not intersect |

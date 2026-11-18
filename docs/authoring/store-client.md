# Store Client

`StoreClient` persists schema-validated [stores](../core/stores.md), applied-step receipts, and path-filtered waiters. Workflows read, mutate, and wait on stores through `step.store`, while application handlers outside workflows use [external store access](../core/external-store-access.md).

## StoreClient and CasStoreClient

`@notation/durable/runtime` exports two abstract classes: `StoreClient` and `CasStoreClient`. Implement `CasStoreClient` so storage transactions never stay open while running initializers, updaters, selectors, claims, or schema validation:

```ts [src/store-client.ts]
import {
  CasStoreClient,
  storePathsIntersect,
  type StoreDefinition,
  type StoreDeletion,
  type StoreMutation,
  type StoreMutationCommitResult,
  type StoreSnapshot,
  type StoreStepId,
  type StoreWaiter,
} from "@notation/durable/runtime";

export class CustomStoreClient extends CasStoreClient {
  protected async readStore(params: {
    definition: StoreDefinition;
    id: string;
  }): Promise<StoreSnapshot<unknown> | undefined> {
    return this.db.getStore(params.definition.name, params.id);
  }

  protected async insertStore(params: {
    definition: StoreDefinition;
    id: string;
    snapshot: StoreSnapshot<unknown>;
  }): Promise<StoreSnapshot<unknown>> {
    return this.db.transaction(() => {
      const existing = this.db.getStore(params.definition.name, params.id);
      if (existing) return existing;
      this.db.insertStore(params.definition.name, params.id, params.snapshot);
      return params.snapshot;
    });
  }

  protected async getAppliedStoreStep(params: {
    definition: StoreDefinition;
    id: string;
    stepId: StoreStepId;
  }): Promise<{ result: unknown } | undefined> {
    return this.db.getAppliedStep(
      params.definition.name,
      params.id,
      params.stepId.executionId,
      params.stepId.stepKey
    );
  }

  protected async commitStoreMutation(
    mutation: StoreMutation
  ): Promise<StoreMutationCommitResult> {
    return this.db.transaction(() => {
      if (mutation.stepId) {
        const applied = this.db.getAppliedStep(
          mutation.definition.name,
          mutation.id,
          mutation.stepId.executionId,
          mutation.stepId.stepKey
        );
        if (applied) return { status: "already-applied", result: applied.result };
      }

      const row = this.db.getStore(mutation.definition.name, mutation.id);
      if (!row && !mutation.expected.created) return { status: "missing" };
      if (
        row &&
        (mutation.expected.created ||
          row.instanceId !== mutation.expected.instanceId ||
          row.version !== mutation.expected.version)
      ) {
        return { status: "conflict", snapshot: row };
      }

      const version = mutation.expected.version + 1;
      if (mutation.expected.created) {
        this.db.insertStore(mutation.definition.name, mutation.id, {
          instanceId: mutation.expected.instanceId,
          version,
          state: mutation.nextState,
        });
      } else {
        this.db.updateStore(mutation.expected.instanceId, mutation.expected.version, {
          version,
          state: mutation.nextState,
        });
      }

      if (mutation.stepId) {
        this.db.insertAppliedStep(
          mutation.definition.name,
          mutation.id,
          mutation.stepId,
          mutation.result
        );
      }

      for (const waiter of this.db.getStoreWaiters(mutation.definition.name, mutation.id)) {
        if (
          waiter.sinceVersion < version &&
          storePathsIntersect(waiter.readPaths, mutation.changedPaths)
        ) {
          this.executions.wakeInTransaction(waiter.executionId);
        }
      }

      return { status: "committed" };
    });
  }

  protected async commitStoreDeletion(
    deletion: StoreDeletion
  ): Promise<StoreMutationCommitResult> {
    return this.db.transaction(() => {
      if (deletion.stepId) {
        const applied = this.db.getAppliedStep(
          deletion.definition.name,
          deletion.id,
          deletion.stepId.executionId,
          deletion.stepId.stepKey
        );
        if (applied) return { status: "already-applied", result: applied.result };
      }

      const row = this.db.getStore(deletion.definition.name, deletion.id);
      if (!row) return { status: "missing" };
      if (
        row.instanceId !== deletion.expected.instanceId ||
        row.version !== deletion.expected.version
      ) {
        return { status: "conflict", snapshot: row };
      }

      this.db.deleteStoreWaiters(deletion.definition.name, deletion.id);
      this.db.deleteStoreInstance(row.instanceId);
      if (deletion.stepId) {
        this.db.insertAppliedStep(
          deletion.definition.name,
          deletion.id,
          deletion.stepId,
          deletion.result
        );
      }
      return { status: "committed" };
    });
  }

  async registerWaiter(waiter: StoreWaiter): Promise<void> {
    this.db.transaction(() => {
      this.db.upsertWaiter(waiter);
      const row = this.db.getStore(waiter.storeName, waiter.storeId);
      if (!row || row.instanceId !== waiter.instanceId || row.version > waiter.sinceVersion) {
        this.executions.wakeInTransaction(waiter.executionId);
      }
    });
  }

  async listStores(definition: StoreDefinition): Promise<string[]> {
    return this.db.listStoreIdsAscending(definition.name);
  }

  async deleteStore(params: { definition: StoreDefinition; id: string }): Promise<void> {
    this.db.transaction(() => {
      this.db.deleteStoreByKey(params.definition.name, params.id);
      this.db.deleteStoreWaiters(params.definition.name, params.id);
    });
  }
}
```

A subclass of `CasStoreClient` implements five protected storage primitives and three public `StoreClient` methods:

| Method | Responsibility |
| :--- | :--- |
| `readStore` | Return the current snapshot for a store key or `undefined` without creating a store |
| `insertStore` | Atomically insert a snapshot if missing and return the stored snapshot either way |
| `getAppliedStoreStep` | Return a recorded `{ result }` receipt for a logical store and `stepId` or `undefined` |
| `commitStoreMutation` | Check receipts, compare `expected`, write state, record receipt, and wake matching waiters |
| `commitStoreDeletion` | Check receipts, compare `expected`, delete the store and waiters, and record receipt |
| `registerWaiter` | Upsert a waiter and wake immediately if the store changed or vanished since `sinceVersion` |
| `listStores` | Return all existing store IDs for a definition sorted in ascending lexicographical order |
| `deleteStore` | Delete the store instance and its waiters without creating the store if missing |

Subclasses can also override `createInstanceId()` to customize instance identifiers. By default `CasStoreClient` uses `crypto.randomUUID()`.

## Store records and lazy initialization

A runtime manages three distinct record kinds for stores:

| Record | Purpose |
| :--- | :--- |
| Store instance | Holds `instanceId`, `version`, and encoded `state` keyed by `storeName` and `storeId` |
| Applied step receipt | Holds the encoded `result` keyed by `storeName`, `storeId`, `executionId`, and `stepKey` |
| Store waiter | Holds `workflowId`, `instanceId`, `sinceVersion`, and `readPaths` for a waiting step |

Every store operation except unconditional `deleteStore` initializes a missing store lazily from the initial state factory with a fresh `instanceId`:

- **Reads and conditional operations** – Insert version 0 if missing; conditional updates and deletions on a missing store create version 0 first and then report a version conflict against the caller snapshot.
- **Unconditional updates and matching takes** – Compute and validate the initial state outside any transaction, then atomically insert at version 1 in `commitStoreMutation` if still missing, or retry on the winner's snapshot on conflict.
- **Unmatched takes** – Insert version 0 when the selector does not match on an unsaved snapshot so waiter registration binds to a real persisted `instanceId`, looping to re-evaluate if another writer inserted first.

Evaluating the initial state factory only when the row is absent ensures existing stores never re-run initialization. Falsy values such as `0`, empty strings, `false`, or `null` are valid stored states; if initialization or validation throws, nothing is inserted.

## Atomic commits and step receipts

`commitStoreMutation` and `commitStoreDeletion` return a `StoreMutationCommitResult` describing whether the atomic compare-and-swap succeeded:

| Status | Meaning |
| :--- | :--- |
| `committed` | The state change or deletion, optional receipt, and required wakes committed together |
| `already-applied` | An applied-step receipt already exists and its stored `result` is returned unchanged |
| `conflict` | The store exists with a different `instanceId`, different `version`, or unexpected presence |
| `missing` | The store did not exist for a non-creating mutation or a deletion |

When a `stepId` is provided, store mutations and conditional deletions execute at most once per store name, store ID, execution ID, and step key:

1. **Receipt pre-check** – `CasStoreClient` checks `getAppliedStoreStep` before reading storage or running user callbacks. If a receipt exists, it returns the stored result directly without running initializers, callbacks, or version checks.
2. **In-transaction check** – Inside `commitStoreMutation` and `commitStoreDeletion`, the receipt lookup runs before checking store presence or expectations. If a concurrent replay committed first, the transaction returns `already-applied` with the saved result without modifying state.
3. **Receipt persistence** – Receipts commit in the same transaction as the state mutation or deletion. Unmatched takes, validation failures, and thrown callback errors record no receipts.

Applied-step receipts key by logical store name, store ID, execution ID, and step key rather than `instanceId`. Deleting a store removes its row and waiters while keeping receipts intact so a replayed step never runs twice across delete-and-recreate cycles.

Calls without `stepId` – such as external `RuntimeStore` operations – record no receipts and apply independently every time.

## Path-filtered waiters

When a workflow suspends on a store wait or unmatched take, Notation Durable records every property path read by the selector inside a `StoreWaiter`:

```ts [src/waiter-matching.ts]
import { storePathsIntersect, type StorePath } from "@notation/durable/runtime";

const readPaths: StorePath[] = [["inbox"], ["inbox", "length"]];
const pushPaths: StorePath[] = [["inbox", 0], ["inbox", "length"]];
const statusPaths: StorePath[] = [["status"]];

storePathsIntersect(readPaths, pushPaths); // true
storePathsIntersect(readPaths, statusPaths); // false
```

`CasStoreClient` captures `changedPaths` using a write-recording proxy in O(changes) for draft mutations. When an updater returns a replacement object or schema validation transforms the state, it falls back to `diffStorePaths(previousState, nextState)`. Inside `commitStoreMutation`, a waiter is satisfied when both conditions hold:

- `waiter.sinceVersion < version`
- `storePathsIntersect(waiter.readPaths, mutation.changedPaths)` returns `true`

`storePathsIntersect` returns `true` when any path in `readPaths` is a prefix of any path in `changedPaths`, or vice versa. Every waiter's wake must be requested inside the same transaction as the state update so a crash can never persist a mutation while losing a wake.

`registerWaiter` must close the race between reading state and recording the waiter. Inside one transaction, upsert the waiter, load the current store row, and wake the waiting execution immediately if the store is absent, recreated with a different `instanceId`, or advanced past `sinceVersion`.

Spurious wakes are harmless because replay re-evaluates the selector and re-registers with a newer `sinceVersion` if needed.

Waiters are not deleted when woken or when `HeapClient.writeStep` records an unfinished attempt where `done` is `false`. Waiters are deleted atomically in only four places:

- **`HeapClient.writeStep` with `done: true`** – Deletes waiters for the completed `stepKey` and every step named in `deleteWaitersOf`
- **`HeapClient.recordCancellation`** – Deletes waiters for every cancelled step recorded in that write
- **`deleteStore` and `commitStoreDeletion`** – Purges all waiters for that store name and ID
- **Forced `ExecutionClient.cancel`** – Purges all waiters held by the force-cancelled execution

## Synchronous guards and validation errors

Store callbacks must execute synchronously and without side effects on selectors:

- **`assertSynchronousUpdater`** – Throws `"Store updaters must be synchronous"` if `updater` returns a Promise or thenable.
- **`assertSynchronousClaim`** – Throws `"Store take claims must be synchronous"` if `claim` returns a Promise or thenable.
- **`trackStoreSelector`** – Throws `"Store selectors must not mutate state"` if a selector assigns or deletes properties on the state proxy.

Before committing any initial state or mutation, `CasStoreClient` validates the state with `validateStoreState` and verifies any `takeFromStore` selection with the durable value codec:

| Error | Cause |
| :--- | :--- |
| `StoreValidationError` | Standard Schema validation returned `issues` for an initial or updated state |
| `WorkflowValueError` | Schema output or `takeFromStore` selection is not a valid durable value |

When a value fails durability checks, `WorkflowValueError` throws with code `INVALID_STEP_VALUE` and the step key. Both validation and durability errors leave storage unchanged, record no receipts, and surface to workflow code.

Any other error thrown by `StoreClient` methods aborts the delivery as an infrastructure error without recording a step or acknowledging an outcome; see [Runtime Reference](../reference/runtime.md).

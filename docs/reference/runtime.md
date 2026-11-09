# @notation/durable/runtime

`@notation/durable/runtime` exports the contracts that a runtime implements, the helper functions that decide each operation, and the classes that run workflows. Applications import from [`@notation/durable`](./durable.md) instead. See [Runtimes](../runtime-authors/runtimes.md) for how the parts fit together.

```ts
import { Runtime, isLeaseCurrent } from "@notation/durable/runtime";
```

## Runtime

A runtime extends the abstract class `Runtime`:

```ts
abstract class Runtime {
  abstract readonly clock: () => number;
  abstract readonly executions: ExecutionClient;
  abstract readonly effects: EffectClient;
  abstract readonly heap: HeapClient;
  abstract readonly joins: JoinClient;
  abstract readonly stores: StoreClient;
}
```

`RuntimeOptions` is `{ clock?: () => number }`. `SqliteRuntime` and `MemoryRuntime` take it, and use `Date.now` without it.

## Clients

| Export | Kind | Page |
| --- | --- | --- |
| `ExecutionClient` | Interface | [Execution client](../runtime-authors/execution-client.md) |
| `HeapClient` | Abstract class | [Heap client](../runtime-authors/heap-client.md) |
| `StoreClient`, `CasStoreClient` | Abstract classes | [Store client](../runtime-authors/store-client.md) |
| `EffectClient` | Interface | [Effect client](../runtime-authors/effect-client.md) |
| `JoinClient` | Interface | [Join client](../runtime-authors/join-client.md) |

## Execution helpers

Each helper takes the stored `ExecutionState` and returns a new state or an answer. See [Execution client](../runtime-authors/execution-client.md) for which method calls which.

| Helper | Returns |
| --- | --- |
| `prepareExecutionAdmission`, `matchExecutionAdmission` | The checked request, and the answer to a repeated admission |
| `createExecutionState` | The state of a new execution, due at once |
| `prepareExecutionStart`, `matchStartRequest`, `createStartedExecution` | The same, for an execution that a step starts |
| `cancellationForStartedChild` | The cancellation request that a new child receives from its caller, if any |
| `claimExecution`, `isExecutionClaimable`, `nextClaimAt` | A claimed state and its lease, whether a state can be claimed, and when |
| `renewExecutionLease`, `releaseExecutionLease`, `acknowledgeExecution` | The state after each fenced write, or `lost` |
| `isLeaseCurrent(state, lease)` | Whether a fenced write may go ahead |
| `requestExecutionWake` | The state after a wake |
| `prepareExecutionCancel`, `requestExecutionCancellation`, `propagatedCancellation` | The checked request, the state after a cancel, and the request that children receive |
| `markCancellationDelivered`, `toExecutionCancellation` | The state after `recordCancellation`, and the request that a delivery carries |
| `prepareExecutionListRequest`, `matchesExecutionListRequest`, `toExecutionListPage` | The checked request, whether a state matches it, and the page |
| `executionStatusAt(state, now)` | The status at `now`. A live lease makes it `running` |
| `unacknowledgedDeliveryCountAt(state, now)` | The deliveries in a row that ended without an outcome |
| `redeliveryDelay(count)` | The delay before the next claim, in milliseconds |
| `toExecutionRecord` | The record that `get` returns |
| `executionNotFound`, `executionIdConflict` | The errors for a missing ID and an ID in use |

| Constant | Value |
| --- | --- |
| `DEFAULT_LEASE_DURATION` | `30000` |
| `REDELIVERY_DELAY_BASE` | `1000` |
| `REDELIVERY_DELAY_MAX` | `300000` |
| `DEFAULT_EXECUTION_LIST_LIMIT` | `50` |
| `MAX_EXECUTION_LIST_LIMIT` | `1000` |

`encodeExecutionEvent`, `decodeExecutionEvent`, `encodeExecutionOutcome` and `decodeExecutionOutcome` convert an event and an outcome to the strings that a runtime stores. `encodeExecutionEvent` throws `ExecutionEventEncodingError` for a value that is not durable.

## Effect helpers

See [Effect client](../runtime-authors/effect-client.md).

| Helper | Returns |
| --- | --- |
| `prepareEffectBegin`, `createEffectState`, `matchEffectBegin` | The checked request, a new `pending` state, and the answer from an existing state |
| `prepareEffectSettle`, `settleEffect` | The checked request, and the new state with the result and the `wake` flag |
| `prepareEffectWatch` | The checked request |
| `toEffectRecord` | The record that `getEffect` returns |
| `validateEffectRequest` | A message that names the first invalid value of a `begin` request, if any |
| `effectNotFound` | An `EffectClientInvariantError` for a missing effect |

## Join and store helpers

| Helper | Returns | Page |
| --- | --- | --- |
| `readJoinTarget(state)` | The answer to `join` from the target's state | [Join client](../runtime-authors/join-client.md) |
| `storePathsIntersect(readPaths, changedPaths)` | Whether a read path and a changed path start one with the other | [Store client](../runtime-authors/store-client.md#waiters) |
| `diffStorePaths(previous, next)` | The paths that differ between two states | |
| `encodeStoreValue`, `decodeStoreValue` | A store state as a string, and back | |
| `cloneStoreState` | A copy of a state, as the codec would read it back | |
| `validateStoreState` | The state that the schema returns. Throws `StoreValidationError` or `WorkflowValueError` | [Store client](../runtime-authors/store-client.md#errors) |

`CasStoreClient` calls the other store exports, such as `trackStoreSelector`, `trackStoreUpdater` and `isStoreSelectorMatch`. A runtime that extends `CasStoreClient` does not call them.

## Running workflows

| Export | Does |
| --- | --- |
| `WorkflowRunner` | Runs one delivery. See below |
| `ExecutionDispatcher` | Claims due executions and passes them to an invoker. See [Running workflows](./durable.md#running-workflows) |
| `WorkflowInvoker` | The type of an invoker |
| `WorkerExitedError` | The error with which an invoker rejects when its worker exits before replying |

`runner.run(event, { lease, signal, cancellation? })` runs one delivery and resolves with a `WorkflowRunOutcome`:

```ts
type WorkflowRunOutcome =
  | { status: "completed"; result: unknown }
  | { status: "suspended"; wakeAt?: number }
  | { status: "failed"; error: Error }
  | { status: "cancelled" };
```

`run` rejects with an `InfrastructureError` when the delivery ends without an outcome, for example after a `LeaseLostError`. See [Failed deliveries](../manual/error-reference.md#failed-deliveries).

An invoker has a `deliver(event, options)` method with the same arguments and result as `run`. **The dispatcher handles a rejection by its kind:**

| `deliver` rejects with | The dispatcher |
| --- | --- |
| `WorkerExitedError` | Leaves the lease to expire, because the worker's last writes may still be in progress |
| Any other error | Releases the lease |

Either way, the execution is delivered again after the redelivery delay. An invoker also has `workflowEndEmitter`, a deprecated `EventEmitter` on which the dispatcher reports outcomes.

## Durable values

| Export | Does |
| --- | --- |
| `durableValueCodec` | Encodes a durable value as a string with `encode`, and reads it back with `decode` |
| `decodeParsedValue(parsed)` | Decodes an encoding that is already parsed from JSON |
| `encodeThrownValue(value)` | Encodes a thrown value. A value that is not durable is encoded as `Error(String(value))` |
| `DurableValueError` | Thrown by `decode` for an invalid encoding |

See [Step values](../manual/step-values.md) for which values are durable.

## Observation

| Export | Returns |
| --- | --- |
| `createExecutionObserver(executions, { pollInterval?, notifier? })` | An `ExecutionObserver` over an execution client |
| `createExecutionControl(executions)` | An `ExecutionControl` over an execution client |
| `toExecutionView(record)` | The view of a record |
| `isTerminalExecutionView(view)` | Whether a view has ended |

The local SDK is built from the first two.

## Errors

The runtime entry point exports the error classes of [`@notation/durable`](./durable.md#errors), except `RetryableError` and `HttpRouteError`. It also exports these:

| Export | Is |
| --- | --- |
| `InfrastructureError` | Ends a delivery without an outcome. Copies its cause's `code` |
| `LeaseLostError` | The cause when a fenced write returns `lost` |
| `StepConflictError` | The cause when a step is already recorded with a different value |
| `LeaseLost`, `isLeaseLost` | The `{ status: "lost" }` result of a fenced write, and its check |

See [Errors in a failed delivery](../manual/error-reference.md#errors-in-a-failed-delivery).

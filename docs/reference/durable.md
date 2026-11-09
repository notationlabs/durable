# @notation/durable

The package root exports what an application needs to define workflows and stores, run them, and start executions.

```sh
@pm.add("@notation/durable")
```

The package also has these entry points:

| Entry point | Exports |
| --- | --- |
| [`@notation/durable/client`](./client.md) | The HTTP SDK, without Node modules |
| [`@notation/durable/worker`](./worker.md) | The worker process and the invoker that starts it |
| [`@notation/durable/sqlite`](./sqlite.md) | The SQLite runtime |
| [`@notation/durable/http`](./http.md) | The HTTP routes |
| [`@notation/durable/testing`](./testing.md) | The test SDK and the in-memory runtime |
| [`@notation/durable/runtime`](./runtime.md) | The contracts and helpers for runtime authors |

## Workflows

| Export | Does | Page |
| --- | --- | --- |
| `workflow<Params, Result>(fn)` | Defines a workflow from an async generator function | [Defining workflows](../manual/workflows.md) |
| `createWorkflow` | The same function as `workflow` | |
| `createWorkflowRouter(workflows)` | Gives each workflow its ID, and returns the object unchanged | [Router](../manual/workflows.md#router) |
| `defineStore(name, schema, { initial })` | Defines a kind of store | [Stores](../manual/stores.md) |

## Steps

A workflow function receives the step runner, typed `StepRunner`, as its first argument:

| Member | Does | Page |
| --- | --- | --- |
| `step.run(key?, fn, options?)` | Runs a function once, and records its result | [Steps](../manual/steps.md#steprun) |
| `step.delay(key?, ms)` | Waits | [Steps](../manual/steps.md#stepdelay) |
| `step.poll(key?, options, predicate)` | Checks a condition until it is true | [Steps](../manual/steps.md#steppoll) |
| `step.store(definition, { id? })` | Returns a handle on a store. Records no step | [Stores](../manual/stores.md) |
| `step.start(key, target, options?)` | Starts another execution | [Child workflows](../manual/child-workflows.md#stepstart-and-stepjoin) |
| `step.join(key, executionId, options?)` | Waits for another execution to end | [Child workflows](../manual/child-workflows.md#stepstart-and-stepjoin) |
| `step.call(key, target, options?)` | Starts a child execution and waits for it | [Child workflows](../manual/child-workflows.md#stepcall) |
| `step.effect(key, descriptor)` | Calls an external system once per effect ID | [External effects](../manual/effects.md#stepeffect) |
| `step.all(key, branches, options?)` | Runs branches at once, and returns every result | [Concurrent steps](../manual/concurrent-steps.md#stepall) |
| `step.race(key, branches)` | Runs branches at once, and returns the first result | [Concurrent steps](../manual/concurrent-steps.md#steprace) |
| `step.event` | The execution's event | [Event](../manual/workflows.md#event) |
| `step.clock` | The runtime's clock | |

A store handle, typed `WorkflowStore`, has `get`, `select`, `update`, `updateFrom`, `deleteFrom`, `when` and `take`. See [Stores](../manual/stores.md) and [Waiting on state](../manual/store-waiting.md).

## SDKs

| Export | Does | Page |
| --- | --- | --- |
| `createLocalSdk<Router>(runtime, options?)` | Starts, reads and cancels executions through a runtime | [Local SDK](../manual/sdk-local.md) |
| `createHttpSdkFactory<Router>()` | Returns a function that creates an SDK for a server's URL | [HTTP SDK](../manual/sdk-http.md) |

`createLocalSdk` takes two options:

| Option | Default | Does |
| --- | --- | --- |
| `pollInterval` | `25` | Milliseconds between reads of the record while `wait` waits |
| `invoker` | None | Lets `wait` read the record as soon as the dispatcher in this process records an outcome |

## Running workflows

These classes run workflows. The [Quick start](../manual/quickstart.md) puts the runner in the worker process and the dispatcher in the server process.

| Export | Does |
| --- | --- |
| `new WorkflowRunner(runtime, { router, logger })` | Runs one delivery of an execution, and returns its outcome |
| `new ExecutionDispatcher(runtime, options)` | Claims due executions, and passes each one to an invoker |

`ExecutionDispatcher` has the methods `start()`, `stop()`, `tick()` and `drain()`. `stop()` stops claiming and waits for the deliveries in progress. `tick()` claims once and returns the number of claims, and `drain()` waits for every delivery started so far.

The dispatcher takes these options:

| Option | Default | Does |
| --- | --- | --- |
| `invoker` | Required | Runs each delivery, for example in a worker process |
| `logger` | Required | A pino logger |
| `concurrency` | `10` | The most deliveries that run at once |
| `pollInterval` | `10` | Milliseconds between claims when nothing is due |
| `keepAlive` | `true` | Whether the dispatcher's timers keep the process alive |
| `leaseDuration` | `30000` | Milliseconds that a lease lasts unless it is renewed |
| `renewInterval` | `leaseDuration / 3` | Milliseconds between renewals. Must be less than `leaseDuration` |
| `abortGracePeriod` | `1000` | Milliseconds to wait for a delivery after its lease is lost, before the dispatcher frees its slot |
| `filter` | Every due execution | `{ executionIds }`. Claims only the executions in the list. An empty list throws a `RangeError` |

See [Delivery guarantees](../runtime-authors/delivery-guarantees.md) for what leases and renewals do.

## Errors

| Export | Thrown by | Page |
| --- | --- | --- |
| `RetryableError` | Workflow code, to retry a step | [Retries](../manual/retries.md) |
| `WorkflowValueError` | Steps, for a value that is not durable | [Error reference](../manual/error-reference.md#errors-in-workflow-code) |
| `WorkflowStartError` | `step.start` and `step.call` | [Error reference](../manual/error-reference.md#errors-in-workflow-code) |
| `WorkflowJoinError` | `step.join` and `step.call` | [Error reference](../manual/error-reference.md#errors-in-workflow-code) |
| `WorkflowEffectError` | `step.effect` | [Error reference](../manual/error-reference.md#errors-in-workflow-code) |
| `WorkflowRaceError` | `step.race` | [Error reference](../manual/error-reference.md#errors-in-workflow-code) |
| `WorkflowCancelledError` | Steps, after a cancellation request | [Cancellation](../manual/cancellation.md) |
| `StoreValidationError` | Store operations, for state that the schema rejects | [Stores](../manual/stores.md#invalid-state) |
| `ExecutionAdmissionError` | `trigger` | [Error reference](../manual/error-reference.md#errors-from-the-sdk) |
| `ExecutionListRequestError` | `list` | [Error reference](../manual/error-reference.md#errors-from-the-sdk) |
| `ExecutionCancelRequestError` | `cancel` | [Error reference](../manual/error-reference.md#errors-from-the-sdk) |
| `HttpRouteError` | The HTTP SDK | [HTTP SDK](../manual/sdk-http.md#errors) |
| `EffectDeliveryError` | Nothing in workflow code. The cause of a failed delivery | [Failed deliveries](../manual/error-reference.md#failed-deliveries) |
| `EffectClientInvariantError` | Effect clients | [Effect client](../runtime-authors/effect-client.md#errors) |

| Export | Is |
| --- | --- |
| `registerErrorClass(name, errorClass)` | Registers a class to rebuild recorded errors of that name. See [Error classes](../manual/errors.md#error-classes) |
| `WORKFLOW_RESULT_STEP_KEY` | `"$$workflow-result$$"`, the step key of the workflow's result |

## Types

| Group | Types |
| --- | --- |
| Workflows | `WorkflowFn`, `DurableWorkflowFn`, `WorkflowEvent`, `EventContext`, `StepRunner`, `StepRunOptions`, `DurableStepFn`, `StartOptions` |
| Waits | `TimedWhenResult`, `TimedJoinResult` |
| Branches | `Branch`, `BranchResults`, `SettledResult`, `SettledResults`, `RaceResult`, `AllOptions` |
| Effects | `EffectDescriptor`, `EffectLookup`, `EffectRecord` |
| Stores | `StoreDefinition`, `WorkflowStore`, `RuntimeStore`, `StoreKey`, `StoreState`, `StoreSnapshot`, `StoreVersion`, `StoreSelector`, `StorePath`, `Draft`, `StoreUpdateResult`, `StoreUpdateFromResult`, `StoreDeleteFromResult`, `DurableStoreSchema`, `StandardSchemaV1` |
| Values | `Durable`, `DurableObject`, `DurableValue` |
| Executions | `ExecutionRecord`, `ExecutionStatus`, `ExecutionOutcome`, `ExecutionParent`, `ExecutionCancellation` |
| SDKs | `ExecutionObserver`, `ExecutionControl`, `ExecutionView`, `ActiveExecutionView`, `TerminalExecutionView`, `ExecutionViewPage`, `ExecutionListRequest`, `ExecutionWaitOptions`, `WaitResult`, `TriggerAck`, `CancelOptions`, `CancelResult` |
| Errors | `ErrorClass`, `ExecutionAdmissionErrorCode`, `WorkflowValueErrorCode`, `WorkflowStartErrorCode`, `WorkflowJoinErrorCode`, `WorkflowEffectErrorCode`, `EffectDeliveryErrorCode` |

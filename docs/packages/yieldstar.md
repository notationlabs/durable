# yieldstar

The main SDK package. Exports the workflow definition API, router factory, error classes, and both SDK clients.

## Install

```sh
bun add yieldstar
```

## Exports

```ts
import { workflow, createWorkflow } from "yieldstar";
import { createWorkflowRouter } from "yieldstar";
import { RetryableError, ExecutionAdmissionError, WorkflowStartError, WorkflowJoinError } from "yieldstar";
import { ExecutionListRequestError, HttpRouteError } from "yieldstar";
import { WorkflowCancelledError, ExecutionCancelRequestError } from "yieldstar";
import { WorkflowRaceError, WorkflowReplayError } from "yieldstar";
import { WorkflowValueError, registerErrorClass, WORKFLOW_RESULT_STEP_KEY } from "yieldstar";
import { WorkflowEffectError, EffectDeliveryError, EffectClientInvariantError } from "yieldstar";
import { defineStore } from "yieldstar";
import { createLocalSdk } from "yieldstar";
import { createHttpSdkFactory } from "yieldstar";
import type {
  WorkflowFn,
  StepRunner,
  StepRunOptions,
  StartOptions,
  DurableStepFn,
  WorkflowStore,
  TimedWhenResult,
  TimedJoinResult,
  Branch,
  BranchResults,
  SettledResult,
  SettledResults,
  RaceResult,
  AllOptions,
  ExecutionRecord,
  ExecutionStatus,
  ExecutionParent,
  ExecutionCancellation,
  ExecutionDivergence,
  ExecutionDivergenceCode,
  ExecutionAdmissionErrorCode,
  TriggerAck,
  ExecutionObserver,
  ExecutionView,
  ActiveExecutionView,
  TerminalExecutionView,
  WaitResult,
  ExecutionWaitOptions,
  ExecutionListRequest,
  ExecutionViewPage,
  ExecutionControl,
  CancelOptions,
  CancelResult,
  ClearDivergenceResult,
  ReplayErrorCode,
  Durable,
  DurableObject,
  DurableValue,
  ErrorClass,
  WorkflowValueErrorCode,
  WorkflowStartErrorCode,
  WorkflowJoinErrorCode,
  EffectDescriptor,
  EffectLookup,
  EffectRecord,
  WorkflowEffectErrorCode,
  EffectDeliveryErrorCode,
} from "yieldstar";
```

## `workflow(fn)` / `workflow({ version }, fn)` / `createWorkflow(fn)`

Defines a workflow. Returns a `WorkflowGenerator`. `Params` and `Result` are checked with `Durable<T>` (see [Step values](../manual/steps.md#step-values)), so a workflow whose params or result type holds a function is a type error. A generic `Result` cannot be checked, so a function that wraps `workflow` must give it a concrete result type.

```ts
const w = workflow<Params, Result>(async function* (step, event, logger) {
  return yield* step.run("compute", () => compute());
});

const v3 = workflow({ version: 3 }, async function* (step, event) {
  event.workflowVersion; // the version recorded by the execution's first delivery
});
```

`version` is a non-negative integer, and defaults to 1. `workflow` throws a `TypeError` for any other value, or when the workflow function is missing. The returned generator's `workflowVersion` property holds the declared version. The first fenced write of an execution's first delivery records the version, and every later delivery exposes the recorded version as `event.workflowVersion`. See [Workflow versions](../manual/replay-stability.md#workflow-versions).

| Type parameter | Constraint                    | Description                                  |
| -------------- | ----------------------------- | -------------------------------------------- |
| `Params`       | `Record<string, any> \| void` | Shape of `event.params`; must be durable     |
| `Result`       | any                           | Return type of the workflow; must be durable |
| `Context`      | `ReadonlyMap<any, any>`       | Shape of `event.context` (set by middleware) |

## `createWorkflowRouter(workflows)`

Registers workflows with string IDs. Returns the same object, typed for SDK consumption.

```ts
const router = createWorkflowRouter({
  order: orderWorkflow,
  reminder: reminderWorkflow,
});
export type Router = typeof router;
```

## `RetryableError`

Thrown inside `step.run` to trigger structured retries. It is registered by default, so replay rethrows a `RetryableError`.

```ts
throw new RetryableError(message, { maxAttempts, retryInterval });
```

| Property        | Type     | Description                        |
| --------------- | -------- | ---------------------------------- |
| `maxAttempts`   | `number` | Total attempts including the first |
| `retryInterval` | `number` | Milliseconds between attempts      |

## `ExecutionAdmissionError`

Thrown by both SDKs' `trigger` when admission is rejected. `err.code` is one of:

| `code`                           | Cause                                                                     |
| -------------------------------- | ------------------------------------------------------------------------- |
| `EXECUTION_ID_CONFLICT`          | The `executionId` is already in use                                       |
| `EXECUTION_REQUEST_MISMATCH`     | An idempotent trigger's `executionId` is in use with a different `workflowId` or `params` |
| `IDEMPOTENT_TRIGGER_REQUIRES_ID` | An idempotent trigger has no `executionId`                                |
| `INVALID_EXECUTION_EVENT`        | `params` or `context` are not durable values, or `idempotent` is not a boolean |

See [Idempotent trigger](../manual/sdk-local.md#idempotent-trigger).

## `ExecutionListRequestError`

Thrown by both SDKs' `list`, and by `ExecutionClient.list`, for a `workflowId` that is not a string, a `limit` that is not a positive integer, an unknown `status`, or a malformed `cursor`. `err.code` is `"INVALID_LIST_REQUEST"`.

## `ExecutionCancelRequestError`

Thrown by both SDKs' `cancel`, before anything is recorded or sent, for an empty execution ID, a `reason` that is not a string, or a `force` that is not a boolean. It extends `TypeError`.

## `HttpRouteError`

Thrown by the HTTP SDK for an error response that has no more specific error, such as an observation middleware denial or an `EXECUTION_NOT_FOUND` from `waitForResult`. `err.status` is the response status, and `err.code` is the response's `code` when the body has one.

## `WorkflowStartError`

Thrown to workflow code by `step.start` when the runtime rejects the start. `err.code` is `"INVALID_START_PARAMETERS"` (`params` are not a durable value, or `workflowId` is empty) or `"START_REQUEST_MISMATCH"` (the step already started a different workflow, different `params` or a different `detached` option). See [`step.start`](../manual/steps.md#stepstart).

## `WorkflowJoinError`

Thrown to workflow code by `step.join` and `step.call`. `err.code` is `"JOINED_EXECUTION_FAILED"` (the target failed, and `err.cause` is its recorded error), `"JOINED_EXECUTION_CANCELLED"` (the target was cancelled, and `err.reason` and `err.source` are its outcome's), `"JOIN_TARGET_NOT_FOUND"` (no execution has the target ID) or `"JOIN_SELF"` (the target is the calling execution). `err.executionId` is the target's ID. It is recorded as the step's error, so replay throws the same class, code and `executionId`. See [`step.join`](../manual/steps.md#stepjoin).

## `WorkflowCancelledError`

Thrown into workflow code at the first unfinished step once a cancellation request is delivered. `err.reason` is the request's reason, and `err.source` is the execution where the cancellation began, when it was propagated from a parent. The throw is recorded as the step's result, so replay throws the same error at the same step. A workflow that lets it escape ends `cancelled`; one that catches it and returns ends `completed`. `triggerAndWait` and `waitForResult` also throw it for a cancelled execution. It is registered by default. See [Cancellation](../manual/cancellation.md).

## `WorkflowRaceError`

Thrown by `step.race` when every branch fails, and by an empty race. `code` is `ALL_BRANCHES_FAILED`, and `errors` holds each branch's error in branch order. `errors` is typed `unknown[]`, because a branch can throw a value that is not an `Error`. `errors` is a non-enumerable array, as on `AggregateError`, so a `failed` outcome keeps it – see [Concurrent Steps](../manual/concurrent-steps.md#steprace).

## `WorkflowReplayError`

Thrown to workflow code when a step cannot be recorded safely for replay. `err.code` is `"STEP_KEY_REQUIRED"` (a keyless step has no record and the runtime was created with `requireStepKeys: true`) or `"PATCH_IN_COMBINATOR"` (`step.patched` was called inside a `step.all` or `step.race` branch), and `err.stepKey` names the step. `STEP_KEY_REQUIRED` is recorded as the step's error, and replay throws the same class and code. `PATCH_IN_COMBINATOR` is not recorded, and replay throws it again from the same call. It is registered by default. See [Replay Stability](../manual/replay-stability.md).

A divergence is not a `WorkflowReplayError`. It never reaches workflow code; see [Divergence](../manual/replay-stability.md#divergence).

## `WorkflowEffectError`

Thrown to workflow code by `step.effect`. `err.code` is `"INVALID_EFFECT_REQUEST"` (the `effectId`, `operation`, `input`, `retryIn`, `maxAttempts`, `lookup`, `perform` or `failsOn` is invalid), `"EFFECT_REQUEST_MISMATCH"` (the effect already exists with a different `operation` or `input`) or `"EFFECT_OUTCOME_UNKNOWN"` (the outcome is still unknown after `maxAttempts` attempts). It is recorded as the step's error, so replay throws the same class and code. See [`step.effect`](../manual/steps.md#stepeffect).

## `EffectDeliveryError`

The `cause` of the `InfrastructureError` with which `step.effect` ends a delivery without recording its step. It never reaches workflow code. `err.code` is `"EFFECT_CLIENT_UNAVAILABLE"`, `"EFFECT_HANDLER_INVALID"` or `"EFFECT_RUNTIME_INVARIANT"`, and `err.effectId` and `err.stepKey` name the effect and step. See [Effect errors](../manual/steps.md#effect-errors).

## `WorkflowValueError`

Thrown to workflow code when a step's value cannot be recorded unchanged. `err.code` is `"INVALID_STEP_VALUE"` (a step result, store state or workflow result is not a durable value) or `"STEP_SCHEMA_INVALID"` (a step's `schema` rejected its value), and `err.stepKey` names the step; it is `WORKFLOW_RESULT_STEP_KEY` (`"$$workflow-result$$"`) for an invalid workflow result. See [Step values](../manual/steps.md#step-values).

## `registerErrorClass(name, errorClass)`

Registers the class that recorded errors named `name` are revived as, in this process. See [Errors in step records](../manual/steps.md#errors-in-step-records).

## `createLocalSdk<Router>(runtime, { invoker?, pollInterval? })`

Creates a local SDK client that admits executions into `runtime` and reads them back with `poll`, `wait` and `list`, cancels them with `cancel`, and clears divergences with `clearDivergence`. It implements `ExecutionObserver` and `ExecutionControl`. See [Local SDK](../manual/sdk-local.md).

## `createHttpSdkFactory<Router>()`

Returns a factory for HTTP SDK clients. Each client implements `ExecutionObserver` and `ExecutionControl`. See [HTTP SDK](../manual/sdk-http.md).

## `defineStore(name, schema, { initial })`

Defines a durable store type from a name, any Standard Schema whose output type is durable (`DurableStoreSchema<Schema>`), and `initial`, a required synchronous function that returns the state of a new store from its `id`. Every operation on a missing store creates it from `initial(id)`. See [Durable Stores](../manual/stores.md#creating-a-store).

```ts
const ConversationStore = defineStore("conversation", schema, {
  initial: (conversationId) => ({ messages: [], status: "idle" }),
});
```

## Step runner

Available on the first argument of a workflow function:

```ts
yield* step.run("key", fn);
yield* step.run("key", fn, { schema });
yield* step.run(fn); // deprecated: keyless

yield* step.delay("key", ms);
yield* step.delay(ms); // deprecated: keyless

yield* step.poll("key", opts, predicate);
yield* step.poll(opts, predicate); // deprecated: keyless

const store = step.store(definition, { id }); // no yield*

yield* step.start("key", { workflowId, params });
yield* step.start("key", { workflowId, params }, { detached: true });

yield* step.join<R>("key", executionId);
yield* step.join<R>("key", executionId, { deadline });

yield* step.call<typeof childWorkflow>("key", { workflowId, params });
yield* step.call<typeof childWorkflow>("key", { workflowId, params }, { deadline });
yield* step.call<typeof childWorkflow>("key", { workflowId, params }, { detached: true, deadline });

yield* step.effect("key", { effectId, operation, input, lookup, perform, retryIn, failsOn, maxAttempts });

yield* step.all("key", branches);
yield* step.all("key", branches, { concurrency, mode: "fail-fast" });
yield* step.all("key", branches, { concurrency, mode: "settle" });

yield* step.race("key", branches);

const applies = yield* step.patched("patch-id"); // boolean
```

A keyless `step.run`, `step.delay` or `step.poll`, and a store's keyless `get()` and `when(selector)`, take their key from a hash of their call site. They are deprecated: the driver logs a warning when one has no record and runs, and a runtime created with `requireStepKeys: true` throws `WorkflowReplayError` (`STEP_KEY_REQUIRED`) instead of running it. See [Step keys](../manual/replay-stability.md#step-keys).

`step.patched(id)` returns `true` when a marker for `id` is recorded, `false` without recording anything when the execution's history has steps that replay has not requested yet, and otherwise records a marker under the key `$$patch$$:<id>` and returns `true`. A second call with the same ID in the same delivery returns the same answer. Inside a `step.all` or `step.race` branch it throws `WorkflowReplayError` (`PATCH_IN_COMBINATOR`) and records nothing. An ID that is not a string throws a `TypeError`, and an empty ID throws an `Error`. See [Patches](../manual/replay-stability.md#patches).

`step.store` returns a `WorkflowStore` handle synchronously and records no step, so a loop or helper can request the same store again. `id` defaults to `event.executionId`. The handle has the readonly properties `definition`, `id` and `key`, and the durable operations `get`, `select`, `update`, `updateFrom`, `deleteFrom`, `when` and `take`, each of which creates a missing store from the definition's `initial` – see [Durable Stores](../manual/stores.md) and [Waiting on State](../manual/store-waiting.md). Helpers that take the step runner can type it as `StepRunner`.

`when(key, selector, { deadline })` waits for the selector to match or for `deadline`, whichever is recorded first, and returns a `TimedWhenResult<R>`: `{ status: "matched", value }` or `{ status: "timed-out" }`. `deadline` is required in this form, so an optional deadline needs a separate call to the untimed form – see [Waiting with a deadline](../manual/store-waiting.md#waiting-with-a-deadline).

`step.start` starts another workflow as its own execution, once per step key, and returns `{ executionId }` – see [Steps](../manual/steps.md#stepstart). Its `StartOptions` has `detached`, which records the target's parent link as detached so that the target receives no cancellation propagated from the caller – see [Detached children](../manual/cancellation.md#detached-children). `step.call` takes `detached` in the same options object as `deadline`.

`step.join` waits until the execution `executionId` is terminal and returns its result, or throws a `WorkflowJoinError` – see [Steps](../manual/steps.md#stepjoin). `step.call` starts a child as `step.start` does and joins it, under one step key; its type parameter checks `params` and types the result – see [Steps](../manual/steps.md#stepcall). `step.join`'s `R` defaults to `DurableValue`. `step.call` without a type parameter accepts no `params` and returns a `DurableValue`. With `{ deadline }`, both return a `TimedJoinResult<R>`: `{ status: "completed", result }` or `{ status: "timed-out" }`.

`step.all` and `step.race` drive their branches concurrently and record their decision under their key – see [Concurrent Steps](../manual/concurrent-steps.md). A branch is any step generator, typed `Branch<T>` (`AsyncGenerator<StepResponse, T, any>`). `step.all` returns `BranchResults<B>`, a tuple of the branches' results, or with `mode: "settle"` a `SettledResults<B>` of `{ status: "fulfilled", value }` and `{ status: "rejected", error }`, where `error` is `unknown`. `step.race` returns a `RaceResult<B>`, `{ index, value }`, which narrows `value` by `index`. For branches in an array that is not a tuple, such as one built with `map`, `index` is a `number`. `AllOptions` has `concurrency`, a positive integer, and `mode`. When `mode` is typed `"fail-fast" | "settle"`, `step.all` returns the union of both result types. `step.delay` and `step.poll` are typed `AsyncGenerator<StepResponse, void, any>`, so they can be branches.

`step.effect` calls an external system under a stable `effectId`, looking up an uncertain earlier attempt before performing it again, and returns the operation's result – see [Steps](../manual/steps.md#stepeffect). Its descriptor is typed `EffectDescriptor<T, Input>`, and `lookup` resolves with an `EffectLookup<T>`:

```ts
type EffectDescriptor<T, Input = DurableObject> = {
  effectId: string;
  operation: string;
  input: Input; // checked with Durable<Input>
  retryIn: number;
  maxAttempts?: number;
  lookup(effectId: string): Promise<EffectLookup<T>>;
  perform(effectId: string): Promise<T>; // T is checked with Durable<T>
  failsOn?(error: unknown): boolean;
};

type EffectLookup<T> =
  | { status: "settled"; result: T }
  | { status: "failed"; error: Error }
  | { status: "not-found" }
  | { status: "unknown" };
```

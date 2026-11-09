# Error reference

Durable's error classes are exported from `@notation/durable`. Most have a `code`, a fixed string that tells apart the cases of one class. Code that handles an error checks the class with `instanceof` and then the `code`, rather than the message.

This page groups the errors by who receives them: workflow code, callers of the SDK, HTTP clients, and the server's log.

A workflow that ships an order with a child workflow handles a cancelled child by its code:

```ts
import { WorkflowJoinError } from "@notation/durable";

try {
  yield* step.call<typeof shipOrder>("ship", {
    workflowId: "ship-order",
    params: { orderId },
  });
} catch (err) {
  if (err instanceof WorkflowJoinError && err.code === "JOINED_EXECUTION_CANCELLED") {
    return { status: "not-shipped", reason: err.reason };
  }
  throw err;
}
```

## Errors in workflow code

Steps throw these errors into the workflow, which can catch them. A step records the error as its outcome, as it does for an error from a step function. So every later delivery throws the same error at the same step. See [Errors](./errors.md#errors-in-steps).

| Class | Code | Thrown by | When |
| --- | --- | --- | --- |
| `WorkflowValueError` | `INVALID_STEP_VALUE` | `step.run`, store steps | A step's result, or a store's state, is not a [durable value](./step-values.md) |
| `WorkflowValueError` | `STEP_SCHEMA_INVALID` | A step with a schema | The [schema](./step-values.md#schemas) rejects the step's value |
| `WorkflowRaceError` | `ALL_BRANCHES_FAILED` | `step.race` | Every branch failed, or the race has no branches. See [`step.race`](./concurrent-steps.md#steprace) |
| `WorkflowStartError` | `INVALID_START_PARAMETERS` | `step.start`, `step.call` | `workflowId` is empty, `params` is not a plain object of durable values, or `detached` is not a boolean |
| `WorkflowStartError` | `START_REQUEST_MISMATCH` | `step.start`, `step.call` | A deploy changed the `workflowId`, `params` or `detached` of a step that has already started its child. See [Changing a child step](./child-workflows.md#changing-a-child-step) |
| `WorkflowJoinError` | `JOINED_EXECUTION_FAILED` | `step.join`, `step.call` | The child failed. See [Child failures](./child-workflows.md#child-failures) |
| `WorkflowJoinError` | `JOINED_EXECUTION_CANCELLED` | `step.join`, `step.call` | The child was cancelled |
| `WorkflowJoinError` | `JOIN_TARGET_NOT_FOUND` | `step.join` | No execution has the ID |
| `WorkflowJoinError` | `JOIN_SELF` | `step.join` | The ID is the workflow's own execution ID |
| `WorkflowEffectError` | `INVALID_EFFECT_REQUEST` | `step.effect` | An option is invalid, such as an `input` that is not a plain object of durable values, or a negative `retryIn` |
| `WorkflowEffectError` | `EFFECT_REQUEST_MISMATCH` | `step.effect` | A deploy changed the `operation` or `input` of an effect that has a record. See [Changing an effect](./effects.md#changing-an-effect) |
| `WorkflowEffectError` | `EFFECT_OUTCOME_UNKNOWN` | `step.effect` | `maxAttempts` attempts ended without a result. See [Giving up](./effects.md#giving-up) |
| `StoreValidationError` | None | Store updates | The store's schema rejects the new state. See [Invalid state](./stores.md#invalid-state) |

Some of these classes have fields besides `code`:

| Class | Fields |
| --- | --- |
| `WorkflowValueError` | `stepKey`, the key of the step whose value was rejected |
| `WorkflowRaceError` | `errors`, each branch's error in branch order |
| `WorkflowJoinError` | `executionId`, the child's ID. `cause`, the child's error, for `JOINED_EXECUTION_FAILED`. `reason` and `source`, for `JOINED_EXECUTION_CANCELLED` |
| `WorkflowCancelledError` | `reason` and `source` |

### Cancellation

`WorkflowCancelledError` is thrown at the first unfinished step after an execution is cancelled. If the workflow does not catch it, the execution ends `cancelled`. See [Where the workflow receives it](./cancellation.md#where-the-workflow-receives-it).

### Invalid workflow result

A workflow that returns a value that is not durable fails. The record keeps a `WorkflowValueError` with the code `INVALID_STEP_VALUE`. Its `stepKey` is `"$$workflow-result$$"`, which is exported as `WORKFLOW_RESULT_STEP_KEY`. The workflow cannot catch this error, because it has already returned.

### `RetryableError`

Durable does not throw `RetryableError`. A step function throws it to have the step run again. See [Retries](./retries.md).

## Errors from the SDK

The local and HTTP SDKs throw these errors to the code that calls them:

| Class | Code | Thrown by | When |
| --- | --- | --- | --- |
| `ExecutionAdmissionError` | `EXECUTION_ID_CONFLICT` | `trigger`, `triggerAndWait` | The ID is in use, and the trigger is not idempotent |
| `ExecutionAdmissionError` | `EXECUTION_REQUEST_MISMATCH` | `trigger`, `triggerAndWait` | The ID is in use by a different request, and the trigger is idempotent |
| `ExecutionAdmissionError` | `IDEMPOTENT_TRIGGER_REQUIRES_ID` | `trigger`, `triggerAndWait` | The trigger is idempotent and has no `executionId` |
| `ExecutionAdmissionError` | `INVALID_EXECUTION_EVENT` | `trigger`, `triggerAndWait` | `params` or `context` are not durable values, or `idempotent` is not a boolean |
| `ExecutionListRequestError` | `INVALID_LIST_REQUEST` | `list` | `limit` is not a positive integer, `status` is unknown, or `list` did not return the cursor |
| `ExecutionCancelRequestError` | None | `cancel` | The execution ID is empty, `reason` is not a string, or `force` is not a boolean |
| `WorkflowCancelledError` | None | `triggerAndWait`, `waitForResult` | The execution was cancelled |
| `HttpRouteError` | The response's `code` | The HTTP SDK | The server sent an error response that has no more specific error above |

When the execution fails, `triggerAndWait` and `waitForResult` throw the error that failed it, as the record keeps it. See [What the record keeps](./errors.md#what-the-record-keeps).

`ExecutionCancelRequestError` extends `TypeError`. `HttpRouteError` has the response's `status`. See [Errors](./sdk-http.md#errors) on the HTTP SDK page.

## HTTP response codes

Error responses from the [HTTP server](./http-server.md) have the body `{ code, message }`. The admission codes and `INVALID_LIST_REQUEST` above are also response codes. These codes are used only in responses:

| Code | Status | When |
| --- | --- | --- |
| `EXECUTION_NOT_FOUND` | `404` | No execution has the ID in a `/poll`, `/wait` or `/cancel` request |
| `INVALID_OBSERVATION_REQUEST` | `400` | A `/poll`, `/wait`, `/list` or `/events` body is invalid |
| `INVALID_CANCEL_REQUEST` | `400` | A `/cancel` body is invalid |
| `UNAUTHORIZED` | `401` | Observation middleware denied the request |
| `FORBIDDEN` | `403` | Observation middleware denied the request |
| `INTERNAL_ERROR` | `500` | Middleware threw, or the runtime failed |

See [Status codes](./http-server.md#status-codes) for the status of each route.

## Failed deliveries

A delivery can fail for a reason outside the workflow's code, such as a database that cannot be reached, or a worker process that exits. Durable then records nothing. The workflow does not receive an error, and the execution does not fail. The dispatcher logs the error, and delivers the execution again later.

The delay before the next delivery starts at 1 second, and doubles after each failed delivery in a row, up to 5 minutes.

**There is no limit on the number of deliveries.** An execution whose deliveries keep failing for the same reason is delivered every 5 minutes until the cause is fixed. The log is the only place where the error appears, so watch it for these messages.

The dispatcher logs one of these messages for each failed delivery:

| Level | Message | What happened |
| --- | --- | --- |
| `error` | `Delivery failed without an outcome; releasing its lease for redelivery` | The worker ended the delivery with an error. The log's `err` is the error |
| `error` | `Worker exited without replying; the lease will expire and the execution will be redelivered` | The worker process exited or was killed. The execution is delivered again after the lease expires, 30 seconds by default |
| `error` | `Failed to dispatch execution; the lease will expire and the execution will be redelivered` | The server process failed to read the record, or to record the outcome |
| `warn` | `Lease was lost to another claim; this delivery stops without an outcome` | The lease expired, and another server delivered the execution. See [Running a server](./running-a-server.md#several-servers-on-one-file) |

Just before the first message, the invoker logs the same error, with the error's own message.

### Errors in a failed delivery

For `Delivery failed without an outcome`, the log's `err` is an `InfrastructureError`. Its `code` and its stack's `caused by:` line show the error that ended the delivery. In this log, `perform` returned a class instance:

```json
{
  "level": 50,
  "msg": "Delivery failed without an outcome; releasing its lease for redelivery",
  "err": {
    "code": "EFFECT_HANDLER_INVALID",
    "stack": "InfrastructureError: Effect \"charge:o-1\" result is not a durable value: $: Receipt instances are not plain objects\n    at ...\ncaused by: EffectDeliveryError: Effect \"charge:o-1\" result is not a durable value: ..."
  }
}
```

The cause is one of these:

| Cause | Code | When | What to do |
| --- | --- | --- | --- |
| `EffectDeliveryError` | `EFFECT_CLIENT_UNAVAILABLE` | A read or write of the effect's record failed | Nothing, if the database recovers |
| `EffectDeliveryError` | `EFFECT_HANDLER_INVALID` | `lookup` returned something other than its [four answers](./effects.md#how-the-step-decides-what-to-call), `lookup` or `perform` returned a value that is not durable, or `failsOn` threw | Fix the effect's code, and deploy |
| `EffectDeliveryError` | `EFFECT_RUNTIME_INVARIANT` | The runtime's effect records broke one of their rules | Report it to the runtime's author |
| `StepConflictError` | None | The step already has a record with a different value, written by an earlier delivery | Nothing. The next delivery uses the recorded value |
| `LeaseLostError` | None | Another server delivered the execution after this delivery's lease expired | Nothing |
| Any other error | The error's `code`, if any | The runtime failed, such as SQLite's `database is locked` after its 5-second wait | Fix the runtime's database |

`EffectDeliveryError` also has the effect's `effectId` and the step's `stepKey`.

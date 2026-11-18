# Errors

This reference catalogs all workflow-facing errors, SDK client errors, HTTP server error codes, and delivery or runtime invariant errors across `@notation/durable`, `@notation/durable/client`, `@notation/durable/http`, and `@notation/durable/runtime`.

## Workflow errors

Workflow errors are thrown inside workflow code during step execution, store operations, or cancellation, or thrown by `triggerAndWait` and `waitForResult` when an execution finishes in a failed or cancelled state. All eight classes are exported from `@notation/durable` and registered by default for durable error revival.

| Error | Description |
| :--- | :--- |
| `RetryableError` | Signals `step.run` to retry a failed step with fixed backoff |
| `WorkflowValueError` | Raised when a step value, store state, or workflow result is invalid |
| `WorkflowStartError` | Raised by `step.start` or `step.call` when the runtime rejects admission |
| `WorkflowJoinError` | Raised by `step.join` or `step.call` when the target fails, cancels, or is invalid |
| `WorkflowRaceError` | Raised by `step.race` when every branch fails or the branch list is empty |
| `WorkflowEffectError` | Raised by `step.effect` on invalid descriptors, mismatches, or exhausted attempts |
| `WorkflowCancelledError` | Thrown into workflow code on cancellation delivery and by wait helpers on cancellation |
| `StoreValidationError` | Raised when initial or updated store state fails the store schema |

### `RetryableError`

```ts [@notation/durable]
export class RetryableError extends Error {
  maxAttempts: number;
  retryInterval: number;

  constructor(
    message: string,
    opts: { maxAttempts: number; retryInterval: number }
  );
}
```

When thrown from `step.run`, the step retries up to the configured attempt limit while suspending between attempts; a retry interval of 0 retries immediately without waiting. After the final attempt fails, the step records that `RetryableError` and throws it to workflow code.

Constructing `RetryableError` throws `RangeError` if the attempt limit is not a positive integer or the interval is negative or non-finite. `step.poll` emits the same error internally whenever its predicate returns false.

### `WorkflowValueError` and `WORKFLOW_RESULT_STEP_KEY`

```ts [@notation/durable]
export type WorkflowValueErrorCode =
  | "INVALID_STEP_VALUE"
  | "STEP_SCHEMA_INVALID";

export const WORKFLOW_RESULT_STEP_KEY = "$$workflow-result$$";

export class WorkflowValueError extends Error {
  readonly code: WorkflowValueErrorCode;
  readonly stepKey: string;

  constructor(code: WorkflowValueErrorCode, stepKey: string, message: string);
}
```

| Code | Description |
| :--- | :--- |
| `"INVALID_STEP_VALUE"` | Step result, store state, `take` selection, or workflow result failed durable encoding |
| `"STEP_SCHEMA_INVALID"` | Decoded `step.run` value failed its Standard Schema on first run or replay |

`stepKey` identifies the failing step; invalid workflow return values use `WORKFLOW_RESULT_STEP_KEY`, replaying identically on later deliveries.

### `WorkflowStartError`

```ts [@notation/durable]
export type WorkflowStartErrorCode =
  | "INVALID_START_PARAMETERS"
  | "START_REQUEST_MISMATCH";

export class WorkflowStartError extends Error {
  readonly code: WorkflowStartErrorCode;

  constructor(code: WorkflowStartErrorCode, message: string);
}
```

| Code | Description |
| :--- | :--- |
| `"INVALID_START_PARAMETERS"` | `workflowId` is empty, `detached` is not boolean, or `params` is not a plain durable object |
| `"START_REQUEST_MISMATCH"` | Repeated `step.start` or `step.call` changed `workflowId`, canonical `params`, or `detached` |

`step.start` and `step.call` record `WorkflowStartError` as the step error so replay throws the same class and code without starting a child execution.

### `WorkflowJoinError`

```ts [@notation/durable]
export type WorkflowJoinErrorCode =
  | "JOINED_EXECUTION_FAILED"
  | "JOINED_EXECUTION_CANCELLED"
  | "JOIN_TARGET_NOT_FOUND"
  | "JOIN_SELF";

export class WorkflowJoinError extends Error {
  readonly code: WorkflowJoinErrorCode;
  readonly executionId: string;
  readonly reason?: string;
  readonly source?: string;
  declare readonly cause?: Error;

  constructor(
    code: WorkflowJoinErrorCode,
    executionId: string,
    message: string,
    options?: { cause?: Error; reason?: string; source?: string }
  );
}
```

| Code | Description |
| :--- | :--- |
| `"JOINED_EXECUTION_FAILED"` | Target execution ended `"failed"` with its recorded error attached as `cause` |
| `"JOINED_EXECUTION_CANCELLED"` | Target execution ended `"cancelled"` with `reason` and `source` copied when present |
| `"JOIN_TARGET_NOT_FOUND"` | No execution record exists for `executionId` |
| `"JOIN_SELF"` | Workflow attempted to join its own `executionId` |

`executionId` holds the target execution ID. Both `step.join` and `step.call` record `WorkflowJoinError` under their step key so replay throws the same error without re-reading the target.

### `WorkflowRaceError`

```ts [@notation/durable]
export class WorkflowRaceError extends Error {
  readonly code: "ALL_BRANCHES_FAILED";
  declare readonly errors: unknown[];

  constructor(stepKey: string, errors: unknown[]);
}
```

`step.race` throws `WorkflowRaceError` when every branch throws or when given an empty branch array. `errors` holds each branch's error in branch order as a non-enumerable array property preserved by the durable value codec.

### `WorkflowEffectError`

```ts [@notation/durable]
export type WorkflowEffectErrorCode =
  | "INVALID_EFFECT_REQUEST"
  | "EFFECT_REQUEST_MISMATCH"
  | "EFFECT_OUTCOME_UNKNOWN";

export class WorkflowEffectError extends Error {
  readonly code: WorkflowEffectErrorCode;

  constructor(code: WorkflowEffectErrorCode, message: string);
}
```

| Code | Description |
| :--- | :--- |
| `"INVALID_EFFECT_REQUEST"` | Effect descriptor failed validation before running |
| `"EFFECT_REQUEST_MISMATCH"` | `effectId` was already begun with a different `operation` or canonical `input` |
| `"EFFECT_OUTCOME_UNKNOWN"` | Effect remained uncertain after reaching `maxAttempts` attempts |

`WorkflowEffectError` is recorded as the step's error so replay throws it again without re-running `lookup` or `perform`.

### `WorkflowCancelledError`

```ts [@notation/durable]
export class WorkflowCancelledError extends Error {
  readonly reason?: string;
  readonly source?: string;

  constructor(options?: { reason?: string; source?: string });
}
```

`WorkflowCancelledError` throws inside workflow code at the first unfinished step upon cancellation delivery, and throws from `triggerAndWait` or `waitForResult` when an execution ends cancelled. Optional `reason` and propagated ancestor `source` attach when present.

If workflow code catches the error and returns normally, the execution completes; otherwise it ends cancelled. `@notation/durable`, `@notation/durable/client`, and `@notation/durable/runtime` all export `WorkflowCancelledError` (see [Cancellation](../core/cancellation.md)).

### `StoreValidationError`

```ts [@notation/durable]
export class StoreValidationError extends Error {
  constructor(message: string);
}
```

`StoreValidationError` is thrown when a store's initial or mutated state fails Standard Schema validation, leaving storage unchanged so workflow handlers can catch it.

## SDK errors

SDK errors are thrown by local and HTTP SDK methods and exported from both `@notation/durable` and `@notation/durable/client`.

| Error | Description |
| :--- | :--- |
| `ExecutionAdmissionError` | Thrown by `trigger` and `triggerAndWait` when admission is rejected |
| `ExecutionListRequestError` | Thrown by `list` on an invalid filter, limit, or cursor |
| `ExecutionCancelRequestError` | Thrown by `cancel` before recording or sending invalid arguments |
| `HttpRouteError` | Thrown by the HTTP SDK for unhandled HTTP error responses |

### `ExecutionAdmissionError`

```ts [@notation/durable]
export type ExecutionAdmissionErrorCode =
  | "EXECUTION_ID_CONFLICT"
  | "INVALID_EXECUTION_EVENT"
  | "EXECUTION_REQUEST_MISMATCH"
  | "IDEMPOTENT_TRIGGER_REQUIRES_ID";

export class ExecutionAdmissionError extends Error {
  readonly code: ExecutionAdmissionErrorCode;

  constructor(code: ExecutionAdmissionErrorCode, message: string);
}
```

| Code | Description |
| :--- | :--- |
| `"EXECUTION_ID_CONFLICT"` | Requested `executionId` already exists and `idempotent` is not `true` |
| `"INVALID_EXECUTION_EVENT"` | `workflowId` or `executionId` is invalid, or `params` or `context` failed encoding |
| `"EXECUTION_REQUEST_MISMATCH"` | Idempotent trigger reused `executionId` with a different `workflowId` or `params` |
| `"IDEMPOTENT_TRIGGER_REQUIRES_ID"` | `idempotent: true` was passed without an `executionId` |

The HTTP SDK also throws `ExecutionAdmissionError` locally before sending if parameters or context fail durable value encoding.

### `ExecutionListRequestError`

```ts [@notation/durable]
export class ExecutionListRequestError extends Error {
  readonly code = "INVALID_LIST_REQUEST";

  constructor(message: string);
}
```

Thrown when `list` receives an invalid filter object, workflow ID, status, limit, or cursor.

### `ExecutionCancelRequestError`

```ts [@notation/durable]
export class ExecutionCancelRequestError extends TypeError {
  constructor(message: string);
}
```

Extends `TypeError`. Thrown locally by `cancel` before sending when execution ID, reason, or force flag has an invalid type.

### `HttpRouteError`

```ts [@notation/durable/client]
export class HttpRouteError extends Error {
  readonly status: number;
  readonly code?: string;

  constructor(status: number, code: string | undefined, message: string);
}
```

`status` holds the HTTP status code, and `code` holds the JSON body's `code` string when present.

The HTTP SDK throws `HttpRouteError` for any error response without a dedicated SDK error class:

- **Middleware denials** – `401` and `403` responses from trigger or observation middleware
- **Validation and server failures** – `400` observation or cancel validation failures and `500` internal errors
- **Unexpected `404` or malformed responses** – `404` responses from a wrong `basePath` and invalid response bodies
- **Missing execution in `waitForResult`** – Status `404` with code `"EXECUTION_NOT_FOUND"`

## HTTP server error responses

Error responses returned by routes from `@notation/durable/http` use a JSON body with `{ code, message }`:

| Code | Description |
| :--- | :--- |
| `"EXECUTION_ID_CONFLICT"` | Admission rejected because `executionId` already exists |
| `"EXECUTION_REQUEST_MISMATCH"` | Idempotent admission mismatched existing `workflowId` or `params` |
| `"INVALID_EXECUTION_EVENT"` | Malformed trigger JSON body or invalid event fields |
| `"IDEMPOTENT_TRIGGER_REQUIRES_ID"` | Trigger set `idempotent: true` without `executionId` |
| `"EXECUTION_NOT_FOUND"` | No execution exists for the requested `executionId` |
| `"INVALID_OBSERVATION_REQUEST"` | Invalid body, `executionId`, or `timeout` on `/poll`, `/wait`, or `/events` |
| `"INVALID_LIST_REQUEST"` | Invalid body, filter, `limit`, or `cursor` on `/list` |
| `"INVALID_CANCEL_REQUEST"` | Invalid body, `executionId`, `reason`, or `force` on `/cancel` |
| `"UNAUTHORIZED"` | `observationMiddleware` denied the request with status `401` |
| `"FORBIDDEN"` | `observationMiddleware` denied the request with status `403` |
| `"INTERNAL_ERROR"` | Unhandled route exception logged by the server |

HTTP status codes and routes for each response code:

| Status | Mapped codes and routes |
| :--- | :--- |
| `400 Bad Request` | `INVALID_EXECUTION_EVENT` and `IDEMPOTENT_TRIGGER_REQUIRES_ID` on `/trigger`; `INVALID_OBSERVATION_REQUEST` on `/poll`, `/wait`, and `/events`; `INVALID_LIST_REQUEST` on `/list`; `INVALID_CANCEL_REQUEST` on `/cancel` |
| `401 Unauthorized` | `UNAUTHORIZED` on `/poll`, `/wait`, `/list`, `/events`, and `/cancel` |
| `403 Forbidden` | `FORBIDDEN` on `/poll`, `/wait`, `/list`, `/events`, and `/cancel` |
| `404 Not Found` | `EXECUTION_NOT_FOUND` on `/poll`, `/wait`, `/events`, and `/cancel` |
| `409 Conflict` | `EXECUTION_ID_CONFLICT` and `EXECUTION_REQUEST_MISMATCH` on `/trigger` |
| `500 Internal Server Error` | `INTERNAL_ERROR` with `"Internal error"` on unhandled server errors |

Trigger middleware may also return custom HTTP responses before admission. On deprecated `/events`, terminal failures return HTTP 500 with a durable-encoded error body rather than `{ code, message }` (see [HTTP Server](../core/http-server.md)).

## Delivery and runtime errors

Delivery and runtime invariant errors indicate failures in runtime adapters, worker processes, lease ownership, or external effect handlers. They never reach workflow `try`/`catch` blocks; instead, they end the delivery without acknowledgement so the execution is redelivered after the redelivery delay.

| Error | Description |
| :--- | :--- |
| `EffectDeliveryError` | Cause of `InfrastructureError` when `step.effect` fails outside workflow code |
| `EffectClientInvariantError` | Thrown by an `EffectClient` when a request or stored data violates invariants |
| `InfrastructureError` | Wraps runtime, lease, or effect delivery failures to abort a delivery |
| `LeaseLostError` | Signals that another claim superseded the delivery's lease token |
| `StepConflictError` | Signals that a fenced write encountered a conflicting existing step record |
| `WorkerExitedError` | Rejects `WorkflowInvoker.deliver` when a worker exits before replying |

Effect errors export from both `@notation/durable` and `@notation/durable/runtime`, whereas `InfrastructureError`, `LeaseLostError`, `StepConflictError`, and `WorkerExitedError` export only from `@notation/durable/runtime`.

### `EffectDeliveryError`

```ts [@notation/durable]
export type EffectDeliveryErrorCode =
  | "EFFECT_CLIENT_UNAVAILABLE"
  | "EFFECT_HANDLER_INVALID"
  | "EFFECT_RUNTIME_INVARIANT";

export class EffectDeliveryError extends Error {
  readonly code: EffectDeliveryErrorCode;
  readonly effectId: string;
  readonly stepKey: string;

  constructor(
    code: EffectDeliveryErrorCode,
    params: { effectId: string; stepKey: string; message: string },
    options?: ErrorOptions
  );
}
```

| Code | Description |
| :--- | :--- |
| `"EFFECT_CLIENT_UNAVAILABLE"` | `EffectClient` call rejected with a transient or non-invariant error |
| `"EFFECT_HANDLER_INVALID"` | `lookup` or `perform` returned an invalid shape or non-durable value, or `failsOn` threw |
| `"EFFECT_RUNTIME_INVARIANT"` | `EffectClient` threw `EffectClientInvariantError` or returned a malformed response |

`step.effect` wraps `EffectDeliveryError` inside `InfrastructureError` so its error code surfaces in infrastructure logs.

### `EffectClientInvariantError`

```ts [@notation/durable]
export class EffectClientInvariantError extends Error {
  readonly code = "EFFECT_RUNTIME_INVARIANT" as const;

  constructor(message: string, options?: ErrorOptions);
}
```

Thrown by `EffectClient` implementations when a request or stored record breaks client invariants, such as settling an unknown `effectId` or reading corrupt stored data.

### `InfrastructureError`

```ts [@notation/durable/runtime]
export class InfrastructureError extends Error {
  readonly code?: string;

  constructor(cause: unknown);
  static is(value: unknown): value is InfrastructureError;
}
```

`InfrastructureError` forwards string error codes from its underlying cause and aborts `WorkflowRunner.run` without acknowledging failure so `ExecutionDispatcher` frees or expires the lease for redelivery. Use `InfrastructureError.is` to detect instances across realms or worker boundaries.

### `LeaseLostError`

```ts [@notation/durable/runtime]
export class LeaseLostError extends Error {
  readonly executionId: string;

  constructor(executionId: string);
}
```

`ExecutionDispatcher` aborts the delivery signal with `LeaseLostError` when lease renewal reports a loss, and the workflow driver wraps `LeaseLostError` inside `InfrastructureError` on a lost fenced step write.

### `StepConflictError`

```ts [@notation/durable/runtime]
export class StepConflictError extends Error {
  readonly executionId: string;
  readonly stepKey: string;

  constructor(executionId: string, stepKey: string);
}
```

Raised inside `InfrastructureError` when a fenced step write finds an existing record for `executionId` and `stepKey` with a different payload or completion flag. The existing record stands, and the next delivery replays it.

### `WorkerExitedError`

```ts [@notation/durable/runtime]
export class WorkerExitedError extends Error {
  constructor(message: string);
  static is(value: unknown): value is WorkerExitedError;
}
```

Rejects `WorkflowInvoker.deliver` when a forked worker exits or is killed before replying. When `WorkerExitedError.is(err)` is true, `ExecutionDispatcher` leaves the lease to expire naturally rather than releasing it immediately, preventing another worker from starting while final writes from the exited worker could still land. See [Leases & Delivery](../authoring/delivery-guarantees.md) for redelivery backoff details.

## Error serialization and registration

```ts [@notation/durable]
export type ErrorClass = abstract new (...args: any[]) => Error;

export function registerErrorClass(name: string, errorClass: ErrorClass): void;
```

The durable value codec preserves error metadata, causes, `AggregateError` nested lists, and durable own fields, reviving instances via `Reflect.construct` without calling subclass constructors.

Registration is per process. Unregistered classes revive as plain `Error` instances with their recorded properties intact.

Default registered classes:

| Category | Registered classes |
| :--- | :--- |
| Built-in JavaScript | `TypeError`, `RangeError`, `SyntaxError`, `ReferenceError`, `EvalError`, `URIError`, `AggregateError` |
| Workflow errors | `RetryableError`, `StoreValidationError`, `WorkflowStartError`, `WorkflowJoinError`, `WorkflowCancelledError`, `WorkflowValueError`, `WorkflowRaceError`, `WorkflowEffectError` |
| Runtime errors | `EffectClientInvariantError`, `EffectDeliveryError` |

`RetryableError` registers whenever `@notation/durable` or `@notation/durable/client` loads, while the remaining 16 default classes register in `@notation/durable/runtime` as well.

`registerErrorClass` throws `TypeError` if given an empty name or non-`Error` constructor (see [Errors & Retries](../core/errors-and-retries.md)).

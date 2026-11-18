# @notation/durable/client

`@notation/durable/client` exports the HTTP SDK factory, client-side error classes, error class registration, and execution observation and control types. It imports no Node built-ins, `pino`, or `ExecutionDispatcher`, so it bundles directly for browser and edge clients.

## `createHttpSdkFactory`

Pass your router type such as `typeof router` as `W` to `createHttpSdkFactory<W>()`, then call the returned factory with the server base URL and optional `fetch` settings:

```ts [@notation/durable/client]
export function createHttpSdkFactory<W extends WorkflowRouter>(): (params: {
  url: string;
  fetchOptions?: Omit<RequestInit, "body" | "method">;
}) => {
  trigger<K extends string & keyof W>(
    event: TriggerEvent<K, EventParamsOf<W[K]>>
  ): Promise<
    TriggerAck & {
      ack: () => Promise<TriggerAck>;
      waitForResult: () => Promise<WorkflowGeneratorReturnType<W[K]>>;
    }
  >;
  poll(executionId: string): Promise<ExecutionView | undefined>;
  wait(
    executionId: string,
    options?: ExecutionWaitOptions
  ): Promise<WaitResult | undefined>;
  list(request?: ExecutionListRequest): Promise<ExecutionViewPage>;
  cancel(
    executionId: string,
    options?: CancelOptions
  ): Promise<CancelResult | undefined>;
  waitForResult<K extends string & keyof W>(
    ack: Pick<TriggerAck, "executionId">
  ): Promise<WorkflowGeneratorReturnType<W[K]>>;
};
```

| Option | Description |
| :--- | :--- |
| `url` | Base URL of the Notation Durable HTTP server without a trailing slash |
| `fetchOptions` | Additional `RequestInit` options merged into every `POST` request |

Each request uses HTTP `POST` with a JSON content type, preserving any extra headers passed in `fetchOptions`.

The returned client satisfies `ExecutionObserver & ExecutionControl` and encodes trigger payloads and decodes execution views using the durable value codec. See [HTTP SDK](../core/http-sdk.md) for usage examples.

## SDK methods

| Method | Description |
| :--- | :--- |
| `trigger` | Admits an execution through `POST /trigger` and returns a `TriggerAck` handle |
| `poll` | Reads the current `ExecutionView` through `POST /poll` |
| `wait` | Waits for a terminal outcome or timeout through `POST /wait` |
| `list` | Pages through execution views newest first through `POST /list` |
| `cancel` | Requests graceful or forced cancellation through `POST /cancel` |
| `waitForResult` | Waits without a timeout and resolves the result or throws the recorded error |

### `trigger`

```ts [@notation/durable/client]
trigger<K extends string & keyof W>(
  event: TriggerEvent<K, EventParamsOf<W[K]>>
): Promise<
  TriggerAck & {
    ack: () => Promise<TriggerAck>;
    waitForResult: () => Promise<WorkflowGeneratorReturnType<W[K]>>;
  }
>;
```

`trigger` encodes workflow parameters and context through the durable value codec locally and throws `ExecutionAdmissionError` (`INVALID_EXECUTION_EVENT`) before sending if encoding fails.

Setting `idempotent` to true requires `executionId`; repeating the same workflow ID and canonical parameters returns `"existing"` instead of admitting a duplicate run. Rejected server responses throw `ExecutionAdmissionError` for admission error codes or `HttpRouteError` otherwise.

### `poll`

```ts [@notation/durable/client]
poll(executionId: string): Promise<ExecutionView | undefined>;
```

`poll` queries `/poll` for the execution ID and returns `undefined` only when the server reports missing execution 404 (`EXECUTION_NOT_FOUND`); any other HTTP error throws `HttpRouteError`.

### `wait`

```ts [@notation/durable/client]
export type ExecutionWaitOptions = {
  timeout?: number;
  signal?: AbortSignal;
};

wait(
  executionId: string,
  options?: ExecutionWaitOptions
): Promise<WaitResult | undefined>;
```

| Option | Description |
| :--- | :--- |
| `timeout` | Maximum milliseconds to wait before returning a `timed-out` result |
| `signal` | `AbortSignal` that aborts the active HTTP request and rejects with `signal.reason` |

Timeout defaults to infinity and throws `RangeError` locally if negative or `NaN`.

Because the HTTP server caps each `/wait` request, the client keeps re-polling with the remaining budget until termination, timeout expiry, or abort signal cancellation. Missing executions return `undefined` only on a server not-found response.

### `list`

```ts [@notation/durable/client]
export type ExecutionListRequest = {
  workflowId?: string;
  status?: ExecutionStatus;
  cursor?: string;
  limit?: number;
};

list(request?: ExecutionListRequest): Promise<ExecutionViewPage>;
```

| Field | Description |
| :--- | :--- |
| `workflowId` | Filters returned views to a single workflow identifier |
| `status` | Filters returned views by execution status |
| `cursor` | Opaque cursor returned by the previous `ExecutionViewPage` |
| `limit` | Maximum number of executions to return per page |

Omitted requests default to an empty filter, with server page limits defaulting to 50 and capping at 1,000 ordered newest first by admission sequence. Malformed list filters throw `ExecutionListRequestError`; other server failures throw `HttpRouteError`.

### `cancel`

```ts [@notation/durable/client]
export type CancelOptions = {
  reason?: string;
  force?: boolean;
};

cancel(
  executionId: string,
  options?: CancelOptions
): Promise<CancelResult | undefined>;
```

| Option | Description |
| :--- | :--- |
| `reason` | Optional string stored on the cancellation request and outcome |
| `force` | Records `cancelled` immediately without waiting for workflow delivery |

`cancel` validates arguments locally and throws `ExecutionCancelRequestError` before sending when malformed. Active executions return `{ status: "requested", execution }`, terminal runs return `TerminalExecutionView`, and missing executions return `undefined`.

### `waitForResult`

```ts [@notation/durable/client]
waitForResult<K extends string & keyof W>(
  ack: Pick<TriggerAck, "executionId">
): Promise<WorkflowGeneratorReturnType<W[K]>>;
```

`waitForResult` calls `wait` without a timeout: returning the completed result, throwing `WorkflowCancelledError` on cancellation, rethrowing revived failures, or throwing `HttpRouteError` (`EXECUTION_NOT_FOUND`) when missing.

## Execution views and types

```ts [@notation/durable/client]
export type ExecutionStatus =
  | "admitted"
  | "running"
  | "suspended"
  | "completed"
  | "failed"
  | "cancelled";

export type TriggerAck = {
  executionId: string;
  status: "accepted" | "existing";
};

export type ExecutionCancellation = {
  reason?: string;
  source?: string;
  forced: boolean;
  requestedAt: number;
  deliveredAt?: number;
};

export type ActiveExecutionView = {
  executionId: string;
  workflowId: string;
  admittedAt: number;
  updatedAt: number;
  parent?: ExecutionParent;
  cancellation?: ExecutionCancellation;
  status: "admitted" | "running" | "suspended";
};

export type TerminalExecutionView =
  | {
      executionId: string;
      workflowId: string;
      admittedAt: number;
      updatedAt: number;
      parent?: ExecutionParent;
      cancellation?: ExecutionCancellation;
      status: "completed";
      result: unknown;
    }
  | {
      executionId: string;
      workflowId: string;
      admittedAt: number;
      updatedAt: number;
      parent?: ExecutionParent;
      cancellation?: ExecutionCancellation;
      status: "failed";
      error: Error;
    }
  | {
      executionId: string;
      workflowId: string;
      admittedAt: number;
      updatedAt: number;
      parent?: ExecutionParent;
      cancellation?: ExecutionCancellation;
      status: "cancelled";
      reason?: string;
      source?: string;
    };

export type ExecutionView = ActiveExecutionView | TerminalExecutionView;

export type WaitResult =
  | TerminalExecutionView
  | { status: "timed-out"; execution: ActiveExecutionView };

export type ExecutionViewPage = {
  executions: ExecutionView[];
  cursor?: string;
};

export type CancelResult =
  | { status: "requested"; execution: ActiveExecutionView }
  | TerminalExecutionView;

export interface ExecutionObserver {
  poll(executionId: string): Promise<ExecutionView | undefined>;
  wait(
    executionId: string,
    options?: ExecutionWaitOptions
  ): Promise<WaitResult | undefined>;
  list(request?: ExecutionListRequest): Promise<ExecutionViewPage>;
}

export interface ExecutionControl {
  cancel(
    executionId: string,
    options?: CancelOptions
  ): Promise<CancelResult | undefined>;
}
```

| Field | Description |
| :--- | :--- |
| `executionId` | Unique execution identifier |
| `workflowId` | Registered workflow name |
| `status` | Current `ExecutionStatus` |
| `admittedAt` | Admission timestamp in milliseconds |
| `updatedAt` | Last state transition timestamp in milliseconds |
| `parent` | Parent execution link when started via `step.start` or `step.call` |
| `cancellation` | Recorded cancellation metadata when `cancel` was requested |

`ExecutionView` excludes parameters, request context, lease fields, and delivery counters. On `ExecutionCancellation`, `source` appears only on propagated child cancellations, and `deliveredAt` records when workflow code received the error.

## Errors and error revival

```ts [@notation/durable/client]
export type ExecutionAdmissionErrorCode =
  | "EXECUTION_ID_CONFLICT"
  | "INVALID_EXECUTION_EVENT"
  | "EXECUTION_REQUEST_MISMATCH"
  | "IDEMPOTENT_TRIGGER_REQUIRES_ID";

export class HttpRouteError extends Error {
  readonly status: number;
  readonly code?: string;
  constructor(status: number, code: string | undefined, message: string);
}

export class ExecutionAdmissionError extends Error {
  readonly code: ExecutionAdmissionErrorCode;
  constructor(code: ExecutionAdmissionErrorCode, message: string);
}

export class ExecutionListRequestError extends Error {
  readonly code = "INVALID_LIST_REQUEST";
  constructor(message: string);
}

export class ExecutionCancelRequestError extends TypeError {
  constructor(message: string);
}

export class WorkflowCancelledError extends Error {
  readonly reason?: string;
  readonly source?: string;
  constructor(options?: { reason?: string; source?: string });
}

export function registerErrorClass(name: string, errorClass: ErrorClass): void;
```

| Export | Description |
| :--- | :--- |
| `HttpRouteError` | Non-admission HTTP errors, middleware denials, and malformed responses |
| `ExecutionAdmissionError` | Rejected `trigger` requests and client-side durable event encoding errors |
| `ExecutionListRequestError` | Invalid `list` filter, limit, or cursor |
| `ExecutionCancelRequestError` | Invalid `executionId`, `reason`, or `force` passed to `cancel` |
| `WorkflowCancelledError` | Thrown by `waitForResult` when an execution ends `cancelled` |
| `registerErrorClass` | Registers custom `Error` subclasses for revival in the client process |

Register custom `Error` subclasses on the client with `registerErrorClass` whenever callers check error types across observation or result helpers; unregistered errors revive as plain `Error` instances with their recorded properties intact (see [Errors](./errors.md)).

# @notation/durable/http

`@notation/durable/http` builds standard Web `Request` and `Response` `POST` route handlers for triggering, polling, waiting on, listing, and cancelling workflow executions.

```ts [src/durable/server.ts]
import pino from "pino";
import { createRoutes } from "@notation/durable/http";
import { invoker } from "./client";
import { runtime } from "./runtime";

const logger = pino();

Bun.serve({
  port: 8080,
  routes: createRoutes({
    basePath: "/durable",
    runtime,
    invoker,
    logger,
  }),
});
```

For trigger and observation policy patterns, see [HTTP Server](../core/http-server.md).

## createRoutes

`createRoutes` returns a `Routes` object mapping each route path to `{ POST: RouteHandler }`:

```ts [@notation/durable/http]
export type RouteHandler = (request: Request) => Response | Promise<Response>;
export type Routes = Record<string, { POST: RouteHandler }>;

export function createRoutes(params: {
  basePath?: `/${string}`;
  runtime: { executions: ExecutionClient };
  invoker?: Pick<WorkflowInvoker, "workflowEndEmitter">;
  logger: Logger;
  middleware?: MiddlewareFunction[];
  observationMiddleware?: ObservationMiddlewareFunction[];
  pollInterval?: number;
  maxWait?: number;
}): Routes;
```

| Option | Description |
| :--- | :--- |
| `runtime` | Runtime whose `executions` client serves reads and writes |
| `logger` | Pino logger instance for warnings and internal error logs |
| `basePath` | Path prefix starting with `/` applied to every route |
| `invoker` | In-process invoker whose `workflowEndEmitter` wakes open waits early |
| `middleware` | Trigger middleware array executed sequentially on `/trigger` |
| `observationMiddleware` | Observation middleware array executed sequentially on all other routes |
| `pollInterval` | Milliseconds between record reads while `/wait` and `/events` wait |
| `maxWait` | Maximum milliseconds `/wait` holds a request open before returning `timed-out` |

`runtime` and `logger` are required; `basePath` defaults to an empty string and middleware arrays default to empty. `pollInterval` defaults to 25 ms and `maxWait` defaults to 8,000 ms (staying below Bun's 10-second idle timeout), throwing `RangeError` if either is not a positive finite number.

If `middleware` is non-empty and `observationMiddleware` is omitted, `createRoutes` logs a startup warning because observation and cancellation routes remain open to all callers; pass `observationMiddleware: []` when open observation is intentional.

## Routes and schemas

Every route accepts `POST` requests with a JSON body decoded through Notation Durable's JSON value codec and responds with `application/json` encoded through the same codec.

| Path | Purpose |
| :--- | :--- |
| `/trigger` | Admits an execution or matches an idempotent repeat |
| `/poll` | Returns the current `ExecutionView` for an execution |
| `/wait` | Waits up to `maxWait` for a terminal outcome or returns `timed-out` |
| `/list` | Returns a cursor-paginated `ExecutionViewPage` |
| `/cancel` | Records a cancellation request and returns an active or terminal view |
| `/events` | Deprecated terminal wait route kept for legacy callers |

### Error response shape

Every error response returns a JSON object `{ code, message }`:

```ts [@notation/durable/http]
export type ObservationErrorCode =
  | "EXECUTION_NOT_FOUND"
  | "INVALID_OBSERVATION_REQUEST"
  | "INVALID_LIST_REQUEST"
  | "INVALID_CANCEL_REQUEST"
  | "UNAUTHORIZED"
  | "FORBIDDEN"
  | "INTERNAL_ERROR";
```

Unexpected exceptions thrown by middleware or the runtime log via `logger.error` and return 500 `INTERNAL_ERROR`; client disconnects during waits skip error logging.

### /trigger

Request body:

```ts [POST /trigger]
type TriggerBody = {
  workflowId: string;
  executionId?: string;
  params?: unknown;
  context?: Record<string, unknown>;
  idempotent?: boolean;
};
```

`workflowId` must be a non-empty string. `executionId`, when present, must be a non-empty string; `context`, when present, must be a plain object; and `idempotent`, when present, must be a boolean.

| Status | Response |
| :--- | :--- |
| `202` | `{ executionId, status: "accepted" }` |
| `200` | `{ executionId, status: "existing" }` when `idempotent: true` matches an earlier request |
| `400` | `INVALID_EXECUTION_EVENT` or `IDEMPOTENT_TRIGGER_REQUIRES_ID` |
| `409` | `EXECUTION_ID_CONFLICT` or `EXECUTION_REQUEST_MISMATCH` |
| `500` | `INTERNAL_ERROR` |

Trigger middleware can also short-circuit `/trigger` with any custom `Response`.

### /poll

Request body:

```ts [POST /poll]
type PollBody = {
  executionId: string;
};
```

| Status | Response |
| :--- | :--- |
| `200` | `ExecutionView` for the requested execution |
| `400` | `INVALID_OBSERVATION_REQUEST` when the body or `executionId` is invalid |
| `401` | `UNAUTHORIZED` when observation middleware denies with `401` |
| `403` | `FORBIDDEN` when observation middleware denies with `403` |
| `404` | `EXECUTION_NOT_FOUND` when no execution has `executionId` |
| `500` | `INTERNAL_ERROR` |

### /wait

Request body:

```ts [POST /wait]
type WaitBody = {
  executionId: string;
  timeout?: number;
};
```

When `timeout` is provided, it must be a non-negative number of milliseconds. The route clamps the effective wait to `Math.min(timeout ?? Infinity, maxWait)` and ends the wait when `req.signal` aborts.

| Status | Response |
| :--- | :--- |
| `200` | `TerminalExecutionView` or `{ status: "timed-out", execution: ActiveExecutionView }` |
| `400` | `INVALID_OBSERVATION_REQUEST` when the body, `executionId`, or `timeout` is invalid |
| `401` | `UNAUTHORIZED` when observation middleware denies with `401` |
| `403` | `FORBIDDEN` when observation middleware denies with `403` |
| `404` | `EXECUTION_NOT_FOUND` when no execution has `executionId` |
| `500` | `INTERNAL_ERROR` |

### /list

Request body:

```ts [POST /list]
type ListBody = {
  workflowId?: string;
  status?: ExecutionStatus;
  cursor?: string;
  limit?: number;
};
```

| Status | Response |
| :--- | :--- |
| `200` | `ExecutionViewPage` containing `{ executions, cursor? }` |
| `400` | `INVALID_OBSERVATION_REQUEST` for non-object JSON or `INVALID_LIST_REQUEST` for invalid filters |
| `401` | `UNAUTHORIZED` when observation middleware denies with `401` |
| `403` | `FORBIDDEN` when observation middleware denies with `403` |
| `500` | `INTERNAL_ERROR` |

### /cancel

Request body:

```ts [POST /cancel]
type CancelBody = {
  executionId: string;
  reason?: string;
  force?: boolean;
};
```

| Status | Response |
| :--- | :--- |
| `202` | `{ status: "requested", execution: ActiveExecutionView }` when cancellation is recorded on an active run |
| `200` | `TerminalExecutionView` when forced or already terminal |
| `400` | `INVALID_CANCEL_REQUEST` when the body, `executionId`, `reason`, or `force` is invalid |
| `401` | `UNAUTHORIZED` when observation middleware denies with `401` |
| `403` | `FORBIDDEN` when observation middleware denies with `403` |
| `404` | `EXECUTION_NOT_FOUND` when no execution has `executionId` |
| `500` | `INTERNAL_ERROR` |

### /events

`/events` is deprecated – use `/wait` instead.

Request body:

```ts [POST /events]
type EventsBody = {
  executionId: string;
};
```

`/events` waits without a `maxWait` cap until the execution becomes terminal or `req.signal` aborts.

| Status | Response |
| :--- | :--- |
| `200` | Codec-encoded workflow result for completed executions |
| `400` | `INVALID_OBSERVATION_REQUEST` when the body or `executionId` is invalid |
| `401` | `UNAUTHORIZED` when observation middleware denies with `401` |
| `403` | `FORBIDDEN` when observation middleware denies with `403` |
| `404` | `EXECUTION_NOT_FOUND` when no execution has `executionId` |
| `500` | Codec-encoded workflow error, codec-encoded `WorkflowCancelledError`, or `INTERNAL_ERROR` |

## Trigger middleware

`createMiddleware` returns a typed `MiddlewareFunction` for `/trigger`:

```ts [@notation/durable/http]
export function createMiddleware(
  middleware: MiddlewareFunction
): MiddlewareFunction;
```

The middleware signature and event types come from [`@notation/durable/runtime`](./runtime.md):

```ts [@notation/durable/runtime]
export type MiddlewareNext = () => Promise<Response>;

export type MiddlewareFunction = (
  req: Request,
  event: MiddlewareEvent,
  next: MiddlewareNext,
  logger: Logger
) => Promise<Response>;
```

Trigger middleware runs sequentially after body decoding and before admission. `event.context` starts as a mutable `FreezableMap` loaded from the request body and freezes before admission executes – clear or overwrite trusted keys inside middleware so callers cannot forge attributes.

## Observation middleware

Observation middleware authorizes observation and cancellation routes after input validation and before reading or mutating any execution record:

```ts [@notation/durable/http]
export type ObservationRequest =
  | {
      operation: "poll" | "wait" | "events";
      executionId: string;
      load(): Promise<ExecutionRecord | undefined>;
    }
  | {
      operation: "list";
      filter: { workflowId?: string; status?: ExecutionStatus };
    }
  | {
      operation: "cancel";
      executionId: string;
      force: boolean;
      load(): Promise<ExecutionRecord | undefined>;
    };

export type ObservationMiddlewareFunction = (
  req: Request,
  request: ObservationRequest,
  logger: Logger
) => Promise<{ allow: true } | { allow: false; status: 401 | 403 }>;
```

| Operation | Available fields |
| :--- | :--- |
| `"poll"` | `executionId` and `load` |
| `"wait"` | `executionId` and `load` |
| `"events"` | `executionId` and `load` |
| `"list"` | `filter` with optional `workflowId` and `status` |
| `"cancel"` | `executionId`, `force`, and `load` |

Functions run in array order. The first denial stops the chain and responds 401 (`UNAUTHORIZED`) or 403 (`FORBIDDEN`). Returning 403 for both unauthorized callers and empty `load()` results avoids leaking whether an execution ID exists.

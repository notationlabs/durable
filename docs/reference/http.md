# @notation/durable/http

HTTP routes and middleware for triggering workflows and reading their executions over the network. Handlers use the standard `Request` and `Response` APIs.

## Install

```sh
@pm.add("@notation/durable")
```

The entry point is part of the `@notation/durable` package.

## `createRoutes({ runtime, invoker?, logger, middleware?, observationMiddleware?, pollInterval?, maxWait?, basePath? })`

Returns route handlers for `POST /trigger`, `POST /poll`, `POST /wait`, `POST /list`, `POST /cancel` and the deprecated `POST /events`. The routes object plugs directly into `Bun.serve({ routes })` or any router that accepts `Request` to `Response` handlers.

```ts
import { createRoutes } from "@notation/durable/http";

Bun.serve({
  port: 8080,
  routes: {
    ...createRoutes({ runtime, invoker, logger }),
  },
});
```

| Param        | Type                   | Description                      |
| ------------ | ---------------------- | -------------------------------- |
| `runtime`    | `{ executions: ExecutionClient }` | The runtime whose execution records the routes admit and read |
| `invoker`    | `WorkflowInvoker`      | Optional. When the dispatcher runs in this process, lets `/wait` and `/events` respond as soon as it records an outcome. No response depends on it |
| `logger`     | `Logger`               | Pino logger                      |
| `middleware` | `MiddlewareFunction[]` | Optional middleware chain for `/trigger` |
| `observationMiddleware` | `ObservationMiddlewareFunction[]` | Optional. Allows or denies `/poll`, `/wait`, `/list`, `/events` and `/cancel` requests. Without it they accept every request, and `createRoutes` logs a warning if `middleware` is set |
| `pollInterval` | `number`             | Optional. How often each open `/wait` and `/events` request reads the record, in milliseconds. Defaults to 25. A value that is not a finite number greater than zero throws a `RangeError` |
| `maxWait`    | `number`               | Optional. The longest the server holds a `/wait` request open, in milliseconds. Defaults to 8000. A value that is not a finite number greater than zero throws a `RangeError` |
| `basePath`   | `` `/${string}` ``     | Optional prefix for routes       |

Every error response has the body `{ code, message }`. A route that throws, including a middleware function, responds `500` with code `INTERNAL_ERROR` and the fixed message `Internal error`, and logs the thrown error with `logger.error`. `ObservationErrorCode` is the union of the codes the observation routes and `/cancel` respond with.

### `POST /trigger`

Accepts a `{ workflowId, params?, context?, executionId?, idempotent? }` body encoded with the durable value codec, of which plain JSON is a subset. Runs the middleware chain, then calls `runtime.executions.admit`. The routes never run workflow code; an `ExecutionDispatcher` must be running against the same runtime.

| Status | Body                                            | Cause                                                          |
| ------ | ----------------------------------------------- | -------------------------------------------------------------- |
| `202`  | `{ executionId, status: "accepted" }`           | Admission committed. The ID is the caller's or the runtime's   |
| `200`  | `{ executionId, status: "existing" }`           | An idempotent trigger matched an execution with the same `workflowId` and `params`, and admitted nothing |
| `409`  | `{ code: "EXECUTION_ID_CONFLICT", message }`    | The supplied `executionId` is already in use                   |
| `409`  | `{ code: "EXECUTION_REQUEST_MISMATCH", message }` | An idempotent trigger's `executionId` is in use with a different `workflowId` or `params` |
| `400`  | `{ code: "INVALID_EXECUTION_EVENT", message }`  | The body is not a valid encoding, `params` or `context` are not durable values, or `idempotent` is not a boolean |
| `400`  | `{ code: "IDEMPOTENT_TRIGGER_REQUIRES_ID", message }` | An idempotent trigger has no `executionId`               |
| `500`  | `{ code: "INTERNAL_ERROR", message: "Internal error" }` | Middleware or admission threw, for example because the database failed |

See [Idempotent trigger](../manual/sdk-local.md#idempotent-trigger) for how a repeat is matched.

### `POST /poll`

Accepts `{ executionId }`. Responds `200` with the execution's `ExecutionView`, encoded with the durable value codec, or `404` `EXECUTION_NOT_FOUND`.

### `POST /wait`

Accepts `{ executionId, timeout? }`. Reads the record, and then waits until it is terminal or until `timeout` milliseconds pass. Responds `200` with a `WaitResult`, or `404` `EXECUTION_NOT_FOUND` for an unknown ID. The server holds each request open for at most `maxWait` milliseconds (8000 by default, below Bun's default 10-second idle timeout), and then responds `{ status: "timed-out", execution }`. The HTTP SDK repeats the request until the execution is terminal or its own timeout passes. When the client disconnects, the request's abort signal ends the wait, and the server stops reading the record.

Each open `/wait` or `/events` request reads the execution record once every `pollInterval` milliseconds, independently of other requests. Many concurrent waits therefore multiply the reads of the backend: 100 open waits at the default 25 ms are about 4000 reads per second. Raise `pollInterval` to read less often, at the cost of a later response when no `invoker` is passed. Lower `maxWait` to end each request sooner. With `invoker`, a wait also reads the record when the dispatcher in this process records an outcome, so a longer `pollInterval` does not delay those responses.

### `POST /list`

Accepts `{ workflowId?, status?, cursor?, limit? }`. Responds `200` with `{ executions, cursor? }`, a page of views newest first by admission. `limit` defaults to 50, and a value above 1000 is lowered to 1000. An invalid `workflowId`, `limit`, `status` or `cursor` responds `400` `INVALID_LIST_REQUEST`, as the local SDK's `list` throws `ExecutionListRequestError`.

A `/poll`, `/wait`, `/list` or `/events` body that is not a JSON object, has no `executionId` where one is needed, or has a negative `timeout` responds `400` `INVALID_OBSERVATION_REQUEST`. The views and pages are described in [Execution views](../manual/sdk-local.md#execution-views).

### `POST /cancel`

Accepts `{ executionId, reason?, force? }`. Runs observation middleware with `operation: "cancel"`, then records a [cancellation](../manual/cancellation.md) request through `runtime.executions.cancel` and reads the record once to build the response. It does not wait for the workflow to stop.

| Status | Body                                            | Cause                                                          |
| ------ | ----------------------------------------------- | -------------------------------------------------------------- |
| `202`  | `{ status: "requested", execution }`            | The request is recorded and the execution is active. `execution` is its view |
| `200`  | The terminal `ExecutionView`                    | The execution is terminal: a forced cancel ended it, or it had already ended. A terminal execution is not changed |
| `404`  | `{ code: "EXECUTION_NOT_FOUND", message }`      | No record has the ID                                           |
| `400`  | `{ code: "INVALID_CANCEL_REQUEST", message }`   | The body is not a JSON object, `executionId` is missing or empty, `reason` is not a string, or `force` is not a boolean |
| `401` or `403` | `{ code: "UNAUTHORIZED" \| "FORBIDDEN", message }` | Observation middleware denied the request, whether or not the execution exists |

### `POST /events`

Deprecated: use `/wait`. Accepts `{ executionId }`. Blocks until the execution record is terminal, with no cap, polling the record every `pollInterval` and re-reading early when `invoker.workflowEndEmitter` fires. Returns the result encoded with the durable value codec, the recorded error encoded the same way with status `500` (a `WorkflowCancelledError` with the outcome's `reason` and `source` for a cancelled execution), or `404` `EXECUTION_NOT_FOUND`. The request's abort signal ends the wait. Observation middleware runs for `/events` with `operation: "events"`.

## Observation middleware

An `ObservationMiddlewareFunction` receives `(req, request, logger)`, where `request` is an `ObservationRequest`, and resolves with `{ allow: true }` or `{ allow: false, status: 401 | 403 }`. The functions run in array order before the route reads any record, and the first denial responds `401` `UNAUTHORIZED` or `403` `FORBIDDEN`. For `poll`, `wait` and `events`, `request` carries the `executionId` and a `load()` function that reads the full `ExecutionRecord`. For `list`, it carries the `filter` and no `load`. For `cancel`, it carries the `executionId`, `force` and `load()`; a policy that should not let readers cancel must check `operation`. See [Observation middleware](../manual/http-server.md#observation-middleware).

Observation middleware does not run for `/trigger`, and trigger middleware does not run for `/cancel`. A caller that trigger middleware allows can tell `existing` from `EXECUTION_REQUEST_MISMATCH` for another execution's ID, and so test guesses at that execution's `params`. Limit who may trigger with trigger middleware, or use execution IDs that other callers cannot guess.

## `createMiddleware(fn)`

Wraps a trigger middleware function for use with `createRoutes`. Middleware receives `(req, event, next, logger)`. It runs only for `/trigger`.

```ts
import { createMiddleware } from "@notation/durable/http";

const auth = createMiddleware(async (req, event, next, logger) => {
  const token = req.headers.get("Authorization");
  if (!token) return new Response("Unauthorized", { status: 401 });
  event.context.set("userId", parseToken(token).sub);
  return next();
});
```

The `event.context` is a `FreezableMap`. Middleware can call `.set()` to add entries. After middleware runs, the context is frozen and admitted with the event; it becomes a read-only `Map` inside the workflow. `event.executionId` is optional: it is `undefined` when the client lets the runtime choose an ID.

Middleware executes in order. Each function must either return a `Response` (to short-circuit) or call `next()` to continue the chain.

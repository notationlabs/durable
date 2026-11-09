# @notation/durable/http

`@notation/durable/http` exports the HTTP routes that the [HTTP SDK](../manual/sdk-http.md) calls. See [HTTP server](../manual/http-server.md) for how to serve them.

```ts
import { createMiddleware, createRoutes } from "@notation/durable/http";
```

## `createRoutes(options)`

Returns a `Routes` object, which maps each path to `{ POST: handler }`. A handler takes a `Request` and returns a `Response`.

| Option | Default | Does |
| --- | --- | --- |
| `runtime` | Required | The runtime whose records the routes read and write. Only `runtime.executions` is used |
| `logger` | Required | A pino logger |
| `basePath` | `""` | A prefix for every path, starting with `/` |
| `middleware` | `[]` | Trigger middleware, which runs before `/trigger` records an execution |
| `observationMiddleware` | `[]` | Observation middleware, which allows or denies every other route |
| `maxWait` | `8000` | The longest, in milliseconds, that `/wait` holds a request open |
| `pollInterval` | `25` | Milliseconds between reads of the record while `/wait` and `/events` wait |
| `invoker` | None | Lets `/wait` and `/events` respond as soon as the dispatcher in this process records an outcome |

`createRoutes` throws a `RangeError` if `maxWait` or `pollInterval` is not a number greater than zero.

| Path | Body | Page |
| --- | --- | --- |
| `/trigger` | `{ workflowId, params?, context?, executionId?, idempotent? }` | [Starting an execution](../manual/sdk-local.md#starting-an-execution) |
| `/poll` | `{ executionId }` | [Reading an execution](../manual/sdk-local.md#reading-an-execution) |
| `/wait` | `{ executionId, timeout? }` | [Long waits](../manual/http-server.md#long-waits) |
| `/list` | `{ workflowId?, status?, limit?, cursor? }` | [Listing executions](../manual/sdk-local.md#listing-executions) |
| `/cancel` | `{ executionId, reason?, force? }` | [Cancelling](../manual/sdk-local.md#cancelling) |
| `/events` | `{ executionId }` | Deprecated. See [`/events`](../manual/http-server.md#events) |

See [Status codes](../manual/http-server.md#status-codes) for the responses.

## `createMiddleware(fn)`

Returns `fn` unchanged, typed as a `MiddlewareFunction`. Trigger middleware has this type:

```ts
type MiddlewareFunction = (
  req: Request,
  event: MiddlewareEvent,
  next: () => Promise<Response>,
  logger: Logger
) => Promise<Response>;
```

See [Trigger middleware](../manual/http-server.md#trigger-middleware).

## Types

| Type | Is |
| --- | --- |
| `ObservationMiddlewareFunction` | `(req, request, logger) => Promise<{ allow: true } \| { allow: false; status: 401 \| 403 }>`. See [Observation middleware](../manual/http-server.md#observation-middleware) |
| `ObservationRequest` | The `request` argument of observation middleware. Its `operation` is `"poll"`, `"wait"`, `"events"`, `"list"` or `"cancel"` |
| `ObservationErrorCode` | The `code` of an error response from a route other than `/trigger` |
| `Routes`, `RouteHandler` | The result of `createRoutes`, and one handler |

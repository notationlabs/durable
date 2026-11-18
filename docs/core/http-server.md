# HTTP Server

`@notation/durable/http` serves Notation Durable's execution lifecycle over Web `Request` and `Response` POST routes. Mount the routes in `Bun.serve` or any Fetch-compatible server so the [HTTP SDK](./http-sdk.md) or plain JSON clients can trigger, inspect, list, wait on, and cancel executions.

The routes only read and write execution records. Start an `ExecutionDispatcher` against the same runtime in this process or another process so admitted executions run:

::code-group

```ts [src/durable/server.ts]
import pino from "pino";
import { createRoutes } from "@notation/durable/http";
import { dispatcher, invoker } from "./client";
import { ownExecutionsOnly, requireUser } from "./auth";
import { runtime } from "./runtime";

const logger = pino();

dispatcher.start();

Bun.serve({
  port: 8080,
  routes: createRoutes({
    basePath: "/durable",
    runtime,
    invoker,
    logger,
    middleware: [requireUser],
    observationMiddleware: [ownExecutionsOnly],
  }),
});
```

```ts [src/durable/auth.ts]
import {
  createMiddleware,
  type ObservationMiddlewareFunction,
} from "@notation/durable/http";
import { userFromRequest } from "../auth";

export const requireUser = createMiddleware(async (req, event, next) => {
  const userId = await userFromRequest(req);
  if (!userId) {
    return Response.json(
      { code: "UNAUTHORIZED", message: "Sign in first" },
      { status: 401 }
    );
  }

  event.context.clear();
  event.context.set("userId", userId);
  return next();
});

export const ownExecutionsOnly: ObservationMiddlewareFunction = async (req, request) => {
  const userId = await userFromRequest(req);
  if (!userId) return { allow: false, status: 401 };
  if (request.operation === "list") return { allow: false, status: 403 };

  const record = await request.load();
  if (record?.context.get("userId") !== userId) {
    return { allow: false, status: 403 };
  }

  return { allow: true };
};
```

::

## Mounting routes

`createRoutes(options)` builds handlers mapping each endpoint path to a `POST` handler. Passing a `basePath` such as `"/durable"` prefixes every route; omitting it mounts them at the root:

| Path | Operation |
| :--- | :--- |
| `/trigger` | Admits an execution and responds `202` or `200` on idempotent match |
| `/poll` | Returns an execution's current `ExecutionView` or `404` |
| `/wait` | Holds open up to `maxWait` for a terminal outcome or `timed-out` |
| `/list` | Returns a cursor-paginated `ExecutionViewPage` |
| `/cancel` | Records a cancellation request and returns active or terminal view |
| `/events` | Deprecated wait endpoint kept for legacy callers |

See [`@notation/durable/http`](../reference/http.md) for the complete `createRoutes` options, request schemas, and HTTP status code tables.

### Deprecation of `/events`

The `/events` route is deprecated – use `/wait` instead. Unlike `/wait`, `/events` has no timeout cap and returns HTTP 200 with the raw return value or HTTP 500 with the encoded error.

Because server failures also return 500, callers cannot tell a failed workflow apart from an infrastructure error by status code alone. `/wait` and the HTTP SDK's `waitForResult` always return a structured `ExecutionView` for completed, failed, and cancelled outcomes.

## Long waits and maxWait

`/wait` keeps a connection open until the execution terminates, the caller aborts the request, or `maxWait` elapses. When `maxWait` expires first, the server returns a `timed-out` status with the current execution view, and the HTTP SDK sends a follow-up request automatically.

`maxWait` defaults to 8000 milliseconds so responses finish below Bun's default 10-second idle timeout. Keep `maxWait` shorter than the idle timeout of your HTTP server and any proxy or load balancer in front of it.

Each open wait reads the execution record every `pollInterval` milliseconds, defaulting to 25. Passing `invoker` when the dispatcher runs in the same process lets open waits respond immediately when an execution finishes without waiting for the next poll tick.

Both `maxWait` and `pollInterval` throw `RangeError` if set to a value that is not a positive, finite number.

## Wire format and plain JSON clients

Request and response bodies use Durable's JSON value codec, which encodes values outside plain JSON – such as undefined values, bigints, dates, maps, sets, and errors – as tagged objects while accepting standard JSON inputs unchanged. Plain HTTP clients such as `curl` or services written in other languages can call every route directly:

```sh [Terminal]
curl -X POST http://localhost:8080/durable/trigger \
  -H "content-type: application/json" \
  -H "authorization: Bearer your-session-token" \
  -d '{"workflowId":"export-report","params":{"accountId":"acct-1","month":"2026-10"}}'
```

Request bodies are parsed and validated before middleware runs, so malformed JSON or invalid field types return `400` even for unauthenticated requests. Any error thrown by middleware or the runtime is logged via `logger.error` and returns `500` with `{ code: "INTERNAL_ERROR", message: "Internal error" }`, keeping internal stack traces and database messages off the wire.

## Auth and execution privacy

Trigger admission and execution observation use separate middleware arrays because `/trigger` creates records with mutable request context, whereas the observation and cancel routes expose execution outcomes and allow termination.

### Trigger middleware

Functions created with `createMiddleware` run sequentially on `/trigger` before admission. Returning a `Response` without calling `next()` rejects the request and admits nothing; calling `next()` proceeds and lets the middleware inspect or add headers to the downstream response.

`event.context` starts with whatever `context` JSON the caller sent in the request body. Always clear or overwrite trusted keys inside trigger middleware before admission so callers cannot forge attributes such as `userId`.

### Observation middleware

Trigger `middleware` does not run on observation or cancel endpoints. When `observationMiddleware` is omitted or empty, any caller who can reach the server can read execution outcomes through `/list` and cancel active runs.

If `middleware` is configured without `observationMiddleware`, `createRoutes` logs a startup warning; pass an empty array explicitly if open observation is intentional.

Each `ObservationMiddlewareFunction` receives the incoming `Request`, an `ObservationRequest` payload, and the logger, running in array order before the route reads or mutates any record. Returning an object with `allow: false` stops the chain and responds with HTTP 401 or 403:

| Operation | Request fields |
| :--- | :--- |
| `"poll"` | `executionId` and `load()` |
| `"wait"` | `executionId` and `load()` |
| `"events"` | `executionId` and `load()` |
| `"cancel"` | `executionId`, `force`, and `load()` |
| `"list"` | `filter` with optional `workflowId` and `status` |

`load()` reads the full `ExecutionRecord`, including params and context, or returns `undefined` if no execution has that ID. Follow three rules when writing observation policies:

- **Hide ID existence on denial** – Deny both unauthorized owners and missing `load()` results with 403 rather than returning 404 for unknown IDs, so callers cannot probe which execution IDs exist.
- **Separate read and cancel permissions** – The same `observationMiddleware` runs for `/cancel` as well as reads; check `request.operation === "cancel"` if a role may inspect executions without cancelling them.
- **Protect caller-chosen IDs on `/trigger`** – Observation middleware does not guard admission, where an existing `executionId` triggers conflict or idempotent match responses. Protect `/trigger` with trigger middleware or let the runtime assign random UUIDs.
